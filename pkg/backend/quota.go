package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

var ErrNoSpace = errors.New("etcdserver: no space")
var ErrQuotaUninitialized = errors.New("quota usage is not initialized")

const quotaAlarmReconcileTimeout = 5 * time.Second

var (
	quotaUsageKey      = []byte("quota/usage")
	quotaTrackingKey   = []byte("quota/tracking")
	quotaTrackingClean = []byte{1}
	quotaTrackingDirty = []byte{0}
	quotaAlarmKey      = []byte("quota/alarm/nospace")
	quotaAlarmSetTag   = byte(2)
)

func decodeQuotaUsage(value []byte) (int64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid quota usage metadata length %d", len(value))
	}
	usage := binary.BigEndian.Uint64(value)
	if usage > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("quota usage overflows int64: %d", usage)
	}
	return int64(usage), nil
}

func encodeQuotaUsage(usage int64) []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(usage))
	return value
}

func encodeQuotaAlarm(memberID uint64) []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, memberID)
	return value
}

func decodeQuotaAlarm(value []byte) (uint64, error) {
	members, err := decodeQuotaAlarms(value)
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, fmt.Errorf("NOSPACE alarm metadata has no members")
	}
	return members[0], nil
}

func encodeQuotaAlarms(memberIDs []uint64) []byte {
	if len(memberIDs) == 1 {
		return encodeQuotaAlarm(memberIDs[0])
	}
	value := make([]byte, 1+8*len(memberIDs))
	value[0] = quotaAlarmSetTag
	for i, memberID := range memberIDs {
		binary.BigEndian.PutUint64(value[1+8*i:], memberID)
	}
	return value
}

func quotaAlarmContains(memberIDs []uint64, memberID uint64) bool {
	index := sort.Search(len(memberIDs), func(i int) bool { return memberIDs[i] >= memberID })
	return index < len(memberIDs) && memberIDs[index] == memberID
}

func decodeQuotaAlarms(value []byte) ([]uint64, error) {
	if len(value) == 1 && value[0] == 1 {
		return []uint64{0}, nil
	}
	if len(value) == 8 {
		return []uint64{binary.BigEndian.Uint64(value)}, nil
	}
	if len(value) < 17 || value[0] != quotaAlarmSetTag || (len(value)-1)%8 != 0 {
		return nil, fmt.Errorf("invalid NOSPACE alarm metadata length %d", len(value))
	}
	members := make([]uint64, 0, (len(value)-1)/8)
	for offset := 1; offset < len(value); offset += 8 {
		memberID := binary.BigEndian.Uint64(value[offset:])
		if len(members) > 0 && memberID <= members[len(members)-1] {
			return nil, fmt.Errorf("NOSPACE alarm members are not strictly ordered")
		}
		members = append(members, memberID)
	}
	return members, nil
}

func (b *backend) quotaAlarmMemberID() uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(b.config.Identity)))
}

func (b *backend) NoSpaceAlarm(ctx context.Context) (memberID uint64, active bool, err error) {
	members, err := b.NoSpaceAlarms(ctx)
	if err != nil {
		return 0, false, err
	}
	if len(members) == 0 {
		return 0, false, nil
	}
	return members[0], true, nil
}

func (b *backend) NoSpaceAlarms(ctx context.Context) ([]uint64, error) {
	raw, err := b.InternalGet(ctx, quotaAlarmKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	members, err := decodeQuotaAlarms(raw)
	if err != nil {
		return nil, err
	}
	// The original one-byte marker had no owner. Only that exact legacy format
	// maps to the local stable ID; an eight-byte zero is a valid explicit owner.
	if len(raw) == 1 && raw[0] == 1 {
		members[0] = b.quotaAlarmMemberID()
	}
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	return members, nil
}

func (b *backend) QuotaStatus(ctx context.Context) (usage, quota int64, noSpace bool, err error) {
	quota = b.config.QuotaBackendBytes
	if quota == 0 {
		_, alarmErr := b.InternalGet(ctx, quotaAlarmKey)
		switch {
		case alarmErr == nil:
			noSpace = true
		case errors.Is(alarmErr, storage.ErrKeyNotFound):
		default:
			return 0, 0, false, alarmErr
		}
		b.emitQuotaMetrics(0, noSpace)
		return 0, 0, noSpace, nil
	}
	tracking, trackingErr := b.InternalGet(ctx, quotaTrackingKey)
	if trackingErr != nil || !bytes.Equal(tracking, quotaTrackingClean) {
		if errors.Is(trackingErr, storage.ErrKeyNotFound) || trackingErr == nil {
			return 0, quota, false, ErrQuotaUninitialized
		}
		return 0, quota, false, trackingErr
	}
	raw, getErr := b.InternalGet(ctx, quotaUsageKey)
	switch {
	case errors.Is(getErr, storage.ErrKeyNotFound):
		return 0, quota, false, ErrQuotaUninitialized
	case getErr != nil:
		return 0, quota, false, getErr
	default:
		usage, err = decodeQuotaUsage(raw)
		if err != nil {
			return 0, quota, false, err
		}
	}
	_, alarmErr := b.InternalGet(ctx, quotaAlarmKey)
	switch {
	case alarmErr == nil:
		noSpace = true
	case errors.Is(alarmErr, storage.ErrKeyNotFound):
	default:
		return 0, quota, false, alarmErr
	}
	b.emitQuotaMetrics(usage, noSpace)
	return usage, quota, noSpace, nil
}

func (b *backend) EnsureQuotaInitialized(ctx context.Context) error {
	if b.config.QuotaBackendBytes == 0 {
		return b.InternalPut(ctx, quotaTrackingKey, quotaTrackingDirty)
	}
	b.logicalWriteMu.Lock()
	defer b.logicalWriteMu.Unlock()
	ctx = b.withLogicalWriteOwnership(ctx)

	trackingKey := b.ks.EncodeInternalKey(quotaTrackingKey)
	usageKey := b.ks.EncodeInternalKey(quotaUsageKey)
	trackingRaw, trackingErr := b.kv.Get(ctx, trackingKey)
	if trackingErr == nil && bytes.Equal(trackingRaw, quotaTrackingClean) {
		if raw, err := b.kv.Get(ctx, usageKey); err == nil {
			usage, decodeErr := decodeQuotaUsage(raw)
			if decodeErr != nil {
				return decodeErr
			}
			return b.ensureNoSpaceForUsageLocked(ctx, usage)
		} else if !errors.Is(err, storage.ErrKeyNotFound) {
			return err
		}
	} else if trackingErr != nil && !errors.Is(trackingErr, storage.ErrKeyNotFound) {
		return trackingErr
	}

	revision, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return err
	}
	usage, err := b.scanQuotaUsage(ctx, revision)
	if err != nil {
		return err
	}
	if err := b.fenceAdmit(ctx); err != nil {
		return err
	}
	usageRaw, usageErr := b.kv.Get(ctx, usageKey)
	if usageErr != nil && !errors.Is(usageErr, storage.ErrKeyNotFound) {
		return usageErr
	}
	batch := b.kv.BeginBatchWrite()
	if errors.Is(usageErr, storage.ErrKeyNotFound) {
		batch.PutIfNotExist(usageKey, encodeQuotaUsage(usage), 0)
	} else {
		batch.CAS(usageKey, encodeQuotaUsage(usage), usageRaw, 0)
	}
	switch {
	case errors.Is(trackingErr, storage.ErrKeyNotFound):
		batch.PutIfNotExist(trackingKey, quotaTrackingClean, 0)
	default:
		batch.CAS(trackingKey, quotaTrackingClean, trackingRaw, 0)
	}
	err = batch.Commit(ctx)
	if errors.Is(err, storage.ErrCASFailed) {
		return storage.ErrCASFailed
	}
	if errors.Is(err, storage.ErrUncertainResult) {
		actualUsage, initialized, reconcileErr := b.reconcileQuotaInitialization(ctx, err)
		if reconcileErr != nil {
			return reconcileErr
		}
		if !initialized {
			return err
		}
		return b.ensureNoSpaceForUsageLocked(ctx, actualUsage)
	}
	if err != nil {
		return err
	}
	return b.ensureNoSpaceForUsageLocked(ctx, usage)
}

func (b *backend) scanQuotaUsage(ctx context.Context, revision uint64) (int64, error) {
	stream := b.scanner.RangeStream(
		ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), revision, false,
	)
	var usage int64
	const maxInt64 = int64(^uint64(0) >> 1)
	for response := range stream {
		if response.GetErr() != "" {
			return 0, errors.New(response.GetErr())
		}
		for _, kv := range response.GetRangeResponse().GetKvs() {
			keyBytes := int64(len(kv.GetKey()))
			valueBytes := logicalStoredValueSize(kv.GetValue())
			if keyBytes > maxInt64-usage || valueBytes > maxInt64-usage-keyBytes {
				return 0, fmt.Errorf("quota usage overflows int64")
			}
			usage += keyBytes + valueBytes
		}
	}
	return usage, nil
}

func (b *backend) reconcileQuotaInitialization(
	ctx context.Context, commitErr error,
) (usage int64, initialized bool, err error) {
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quotaAlarmReconcileTimeout)
	defer cancel()
	usageKey := b.ks.EncodeInternalKey(quotaUsageKey)
	for {
		tracking, trackingErr := b.kv.Get(reconcileCtx, b.ks.EncodeInternalKey(quotaTrackingKey))
		switch {
		case errors.Is(trackingErr, storage.ErrKeyNotFound):
			return 0, false, nil
		case trackingErr == nil && !bytes.Equal(tracking, quotaTrackingClean):
			return 0, false, nil
		case trackingErr != nil && !errors.Is(trackingErr, storage.ErrUnavailable):
			return 0, false, errors.Join(
				commitErr,
				fmt.Errorf("reconcile quota tracking state: %w", trackingErr),
			)
		case errors.Is(trackingErr, storage.ErrUnavailable):
			select {
			case <-reconcileCtx.Done():
				return 0, false, errors.Join(
					commitErr,
					fmt.Errorf("reconcile quota tracking state: %w", reconcileCtx.Err()),
				)
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		raw, getErr := b.kv.Get(reconcileCtx, usageKey)
		switch {
		case errors.Is(getErr, storage.ErrKeyNotFound):
			return 0, false, nil
		case getErr == nil:
			usage, decodeErr := decodeQuotaUsage(raw)
			if decodeErr != nil {
				return 0, false, errors.Join(
					commitErr,
					fmt.Errorf("reconcile quota initialization: %w", decodeErr),
				)
			}
			return usage, true, nil
		case !errors.Is(getErr, storage.ErrUnavailable):
			return 0, false, errors.Join(
				commitErr,
				fmt.Errorf("reconcile quota initialization: %w", getErr),
			)
		}
		select {
		case <-reconcileCtx.Done():
			return 0, false, errors.Join(
				commitErr,
				fmt.Errorf("reconcile quota initialization: %w", reconcileCtx.Err()),
			)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (b *backend) ensureNoSpaceForUsageLocked(ctx context.Context, usage int64) error {
	if usage <= b.config.QuotaBackendBytes {
		_, err := b.kv.Get(ctx, b.ks.EncodeInternalKey(quotaAlarmKey))
		switch {
		case err == nil:
			b.emitQuotaMetrics(usage, true)
			return nil
		case errors.Is(err, storage.ErrKeyNotFound):
			b.emitQuotaMetrics(usage, false)
			return nil
		default:
			return err
		}
	}
	alarmKey := b.ks.EncodeInternalKey(quotaAlarmKey)
	if _, err := b.kv.Get(ctx, alarmKey); err == nil {
		b.emitQuotaMetrics(usage, true)
		return nil
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return err
	}
	if err := b.activateNoSpace(ctx); err != nil {
		return err
	}
	b.emitQuotaMetrics(usage, true)
	return nil
}

func (b *backend) DisarmNoSpace(ctx context.Context, memberID uint64) (bool, error) {
	usage, _, active, err := b.QuotaStatus(ctx)
	if err != nil {
		return false, err
	}
	if !active {
		b.emitQuotaMetrics(usage, false)
		return false, nil
	}
	raw, err := b.InternalGet(ctx, quotaAlarmKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		b.emitQuotaMetrics(usage, false)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	members, err := decodeQuotaAlarms(raw)
	if err != nil {
		return false, err
	}
	legacyWildcard := len(raw) == 1 && raw[0] == 1
	if legacyWildcard {
		members[0] = b.quotaAlarmMemberID()
	}
	if !legacyWildcard && !quotaAlarmContains(members, memberID) {
		return false, nil
	}
	remaining := make([]uint64, 0, len(members)-1)
	for _, current := range members {
		if current != memberID && !legacyWildcard {
			remaining = append(remaining, current)
		}
	}
	op := InternalCASOp{Key: quotaAlarmKey, Expected: raw, ExpectedExists: true}
	if len(remaining) == 0 {
		op.Delete = true
	} else {
		op.Value = encodeQuotaAlarms(remaining)
	}
	err = b.InternalCAS(ctx, []InternalCASOp{{
		Key:            op.Key,
		Expected:       op.Expected,
		ExpectedExists: op.ExpectedExists,
		Value:          op.Value,
		Delete:         op.Delete,
	}})
	if errors.Is(err, storage.ErrCASFailed) {
		return false, nil
	}
	if err == nil {
		b.emitQuotaMetrics(usage, len(remaining) > 0)
		return true, nil
	}
	if !errors.Is(err, storage.ErrUncertainResult) {
		return false, err
	}
	return b.reconcileNoSpaceDisarm(ctx, raw, memberID, usage, err)
}

func (b *backend) reconcileNoSpaceDisarm(
	ctx context.Context, previous []byte, memberID uint64, usage int64, commitErr error,
) (bool, error) {
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quotaAlarmReconcileTimeout)
	defer cancel()
	for {
		current, err := b.InternalGet(reconcileCtx, quotaAlarmKey)
		switch {
		case errors.Is(err, storage.ErrKeyNotFound):
			b.emitQuotaMetrics(usage, false)
			return true, nil
		case err == nil && bytes.Equal(current, previous):
			return false, commitErr
		case err == nil:
			members, decodeErr := decodeQuotaAlarms(current)
			if decodeErr != nil {
				return false, errors.Join(
					commitErr,
					fmt.Errorf("reconcile NOSPACE deactivation: %w", decodeErr),
				)
			}
			if len(current) == 1 && current[0] == 1 {
				members[0] = b.quotaAlarmMemberID()
			}
			sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
			b.emitQuotaMetrics(usage, true)
			if !quotaAlarmContains(members, memberID) {
				return true, nil
			}
			return false, commitErr
		case !errors.Is(err, storage.ErrUnavailable):
			return false, errors.Join(
				commitErr,
				fmt.Errorf("reconcile NOSPACE deactivation: %w", err),
			)
		}
		select {
		case <-reconcileCtx.Done():
			return false, errors.Join(
				commitErr,
				fmt.Errorf("reconcile NOSPACE deactivation: %w", reconcileCtx.Err()),
			)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (b *backend) ArmNoSpace(ctx context.Context, memberID uint64) (uint64, error) {
	return b.activateNoSpaceForMember(ctx, memberID)
}

func (b *backend) activateNoSpace(ctx context.Context) error {
	_, err := b.activateNoSpaceForMember(ctx, b.quotaAlarmMemberID())
	return err
}

func (b *backend) activateNoSpaceForMember(ctx context.Context, memberID uint64) (uint64, error) {
	for {
		raw, readErr := b.InternalGet(ctx, quotaAlarmKey)
		members := []uint64(nil)
		op := InternalCASOp{Key: quotaAlarmKey}
		switch {
		case errors.Is(readErr, storage.ErrKeyNotFound):
			members = []uint64{memberID}
			op.Value = encodeQuotaAlarms(members)
		case readErr != nil:
			return 0, readErr
		default:
			members, readErr = decodeQuotaAlarms(raw)
			if readErr != nil {
				return 0, readErr
			}
			if len(raw) == 1 && raw[0] == 1 {
				members[0] = b.quotaAlarmMemberID()
			}
			sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
			if quotaAlarmContains(members, memberID) {
				b.metricCli.EmitGauge("quota.nospace", 1)
				return memberID, nil
			}
			members = append(members, memberID)
			sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
			op.Expected = raw
			op.ExpectedExists = true
			op.Value = encodeQuotaAlarms(members)
		}
		err := b.InternalCAS(ctx, []InternalCASOp{op})
		switch {
		case err == nil:
			b.metricCli.EmitGauge("quota.nospace", 1)
			return memberID, nil
		case errors.Is(err, storage.ErrCASFailed):
			continue
		case !errors.Is(err, storage.ErrUncertainResult):
			return 0, err
		}
		return b.reconcileNoSpaceActivation(ctx, memberID, err)
	}
}

func (b *backend) reconcileNoSpaceActivation(
	ctx context.Context, memberID uint64, commitErr error,
) (uint64, error) {
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quotaAlarmReconcileTimeout)
	defer cancel()
	for {
		members, reconcileErr := b.NoSpaceAlarms(reconcileCtx)
		if reconcileErr == nil {
			if quotaAlarmContains(members, memberID) {
				b.metricCli.EmitGauge("quota.nospace", 1)
				return memberID, nil
			}
			return 0, commitErr
		}
		if !errors.Is(reconcileErr, storage.ErrUnavailable) {
			return 0, errors.Join(commitErr, fmt.Errorf("reconcile NOSPACE activation: %w", reconcileErr))
		}
		select {
		case <-reconcileCtx.Done():
			return 0, errors.Join(
				commitErr,
				fmt.Errorf("reconcile NOSPACE activation: %w", reconcileCtx.Err()),
			)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (b *backend) emitQuotaMetrics(usage int64, noSpace bool) {
	b.metricCli.EmitGauge("quota.logical_usage_bytes", usage)
	b.metricCli.EmitGauge("quota.backend_bytes", b.config.QuotaBackendBytes)
	alarm := 0
	if noSpace {
		alarm = 1
	}
	b.metricCli.EmitGauge("quota.nospace", alarm)
}

func logicalStoredValueSize(value []byte) int64 {
	_, raw, ok := decodeValueWithMeta(value)
	if ok {
		return int64(len(raw))
	}
	return int64(len(value))
}
