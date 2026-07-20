package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

var ErrNoSpace = errors.New("etcdserver: no space")
var ErrQuotaDisabled = errors.New("quota enforcement is disabled")
var ErrQuotaUninitialized = errors.New("quota usage is not initialized")

const quotaAlarmReconcileTimeout = 5 * time.Second

var (
	quotaUsageKey = []byte("quota/usage")
	quotaAlarmKey = []byte("quota/alarm/nospace")
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
	if len(value) == 1 && value[0] == 1 {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid NOSPACE alarm metadata length %d", len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

func (b *backend) quotaAlarmMemberID() uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(b.config.Identity)))
}

func (b *backend) NoSpaceAlarm(ctx context.Context) (memberID uint64, active bool, err error) {
	raw, err := b.InternalGet(ctx, quotaAlarmKey)
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	memberID, err = decodeQuotaAlarm(raw)
	if err != nil {
		return 0, false, err
	}
	if memberID == 0 {
		memberID = b.quotaAlarmMemberID()
	}
	return memberID, true, nil
}

func (b *backend) QuotaStatus(ctx context.Context) (usage, quota int64, noSpace bool, err error) {
	quota = b.config.QuotaBackendBytes
	if quota == 0 {
		return 0, 0, false, nil
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
		return nil
	}
	b.logicalWriteMu.Lock()
	defer b.logicalWriteMu.Unlock()

	usageKey := b.ks.EncodeInternalKey(quotaUsageKey)
	if raw, err := b.kv.Get(ctx, usageKey); err == nil {
		usage, decodeErr := decodeQuotaUsage(raw)
		if decodeErr != nil {
			return decodeErr
		}
		return b.ensureNoSpaceForUsageLocked(ctx, usage)
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return err
	}

	revision, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return err
	}
	kvs, err := b.scanner.Range(
		ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), revision, 0,
	)
	if err != nil {
		return err
	}
	var usage int64
	const maxInt64 = int64(^uint64(0) >> 1)
	for _, kv := range kvs {
		keyBytes := int64(len(kv.GetKey()))
		valueBytes := logicalStoredValueSize(kv.GetValue())
		if keyBytes > maxInt64-usage || valueBytes > maxInt64-usage-keyBytes {
			return fmt.Errorf("quota usage overflows int64")
		}
		usage += keyBytes + valueBytes
	}
	if err := b.fenceAdmit(ctx); err != nil {
		return err
	}
	batch := b.kv.BeginBatchWrite()
	batch.PutIfNotExist(usageKey, encodeQuotaUsage(usage), 0)
	err = batch.Commit(ctx)
	if errors.Is(err, storage.ErrCASFailed) {
		raw, getErr := b.kv.Get(ctx, usageKey)
		if getErr != nil {
			return getErr
		}
		actualUsage, decodeErr := decodeQuotaUsage(raw)
		if decodeErr != nil {
			return decodeErr
		}
		return b.ensureNoSpaceForUsageLocked(ctx, actualUsage)
	}
	if err != nil {
		return err
	}
	return b.ensureNoSpaceForUsageLocked(ctx, usage)
}

func (b *backend) ensureNoSpaceForUsageLocked(ctx context.Context, usage int64) error {
	if usage <= b.config.QuotaBackendBytes {
		b.emitQuotaMetrics(usage, false)
		return nil
	}
	alarmKey := b.ks.EncodeInternalKey(quotaAlarmKey)
	if _, err := b.kv.Get(ctx, alarmKey); err == nil {
		b.emitQuotaMetrics(usage, true)
		return nil
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return err
	}
	if err := b.fenceAdmit(ctx); err != nil {
		return err
	}
	batch := b.kv.BeginBatchWrite()
	batch.PutIfNotExist(alarmKey, encodeQuotaAlarm(b.quotaAlarmMemberID()), 0)
	err := batch.Commit(ctx)
	if err != nil && !errors.Is(err, storage.ErrCASFailed) {
		return err
	}
	b.emitQuotaMetrics(usage, true)
	return nil
}

func (b *backend) DisarmNoSpace(ctx context.Context, memberID uint64) (bool, error) {
	usage, quota, active, err := b.QuotaStatus(ctx)
	if err != nil {
		return false, err
	}
	if quota > 0 && usage >= quota {
		return false, ErrNoSpace
	}
	if !active {
		b.emitQuotaMetrics(usage, false)
		return false, nil
	}
	raw, err := b.InternalGet(ctx, quotaAlarmKey)
	if err != nil {
		return false, err
	}
	owner, err := decodeQuotaAlarm(raw)
	if err != nil {
		return false, err
	}
	if owner != 0 && owner != memberID {
		return false, nil
	}
	err = b.InternalCAS(ctx, []InternalCASOp{{
		Key:            quotaAlarmKey,
		Expected:       raw,
		ExpectedExists: true,
		Delete:         true,
	}})
	if errors.Is(err, storage.ErrCASFailed) {
		return false, nil
	}
	if err == nil {
		b.emitQuotaMetrics(usage, false)
		return true, nil
	}
	if !errors.Is(err, storage.ErrUncertainResult) {
		return false, err
	}
	return b.reconcileNoSpaceDisarm(ctx, raw, usage, err)
}

func (b *backend) reconcileNoSpaceDisarm(
	ctx context.Context, previous []byte, usage int64, commitErr error,
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
			if _, decodeErr := decodeQuotaAlarm(current); decodeErr != nil {
				return false, errors.Join(
					commitErr,
					fmt.Errorf("reconcile NOSPACE deactivation: %w", decodeErr),
				)
			}
			b.emitQuotaMetrics(usage, true)
			return true, nil
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
	if b.config.QuotaBackendBytes == 0 {
		return 0, ErrQuotaDisabled
	}
	if memberID == 0 {
		memberID = b.quotaAlarmMemberID()
	}
	if err := b.activateNoSpaceForMember(ctx, memberID); err != nil {
		return 0, err
	}
	owner, _, err := b.NoSpaceAlarm(ctx)
	return owner, err
}

func (b *backend) activateNoSpace(ctx context.Context) error {
	return b.activateNoSpaceForMember(ctx, b.quotaAlarmMemberID())
}

func (b *backend) activateNoSpaceForMember(ctx context.Context, memberID uint64) error {
	err := b.InternalCAS(ctx, []InternalCASOp{{
		Key:   quotaAlarmKey,
		Value: encodeQuotaAlarm(memberID),
	}})
	if errors.Is(err, storage.ErrCASFailed) {
		b.metricCli.EmitGauge("quota.nospace", 1)
		return nil
	}
	if err == nil {
		b.metricCli.EmitGauge("quota.nospace", 1)
		return nil
	}
	if !errors.Is(err, storage.ErrUncertainResult) {
		return err
	}

	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quotaAlarmReconcileTimeout)
	defer cancel()
	for {
		_, active, reconcileErr := b.NoSpaceAlarm(reconcileCtx)
		if reconcileErr == nil {
			if active {
				b.metricCli.EmitGauge("quota.nospace", 1)
				return nil
			}
			return err
		}
		if !errors.Is(reconcileErr, storage.ErrUnavailable) {
			return errors.Join(err, fmt.Errorf("reconcile NOSPACE activation: %w", reconcileErr))
		}
		select {
		case <-reconcileCtx.Done():
			return errors.Join(err, fmt.Errorf("reconcile NOSPACE activation: %w", reconcileCtx.Err()))
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
