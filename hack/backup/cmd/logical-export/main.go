package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/backupmetrics"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func nextKey(key []byte) []byte {
	out := append([]byte(nil), key...)
	return append(out, 0)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (retErr error) {
	prefix := os.Getenv("PREFIX")
	output := os.Getenv("OUTPUT")
	batchSize, err := strconv.ParseInt(os.Getenv("BATCH_SIZE"), 10, 64)
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("invalid BATCH_SIZE: %q", os.Getenv("BATCH_SIZE"))
	}

	cli, err := etcdutil.NewClientFromEnv()
	if err != nil {
		return err
	}
	clientClosed := false
	defer func() {
		if !clientClosed {
			retErr = errors.Join(retErr, cli.Close())
		}
	}()
	timeout, err := etcdutil.TimeoutFromEnv()
	if err != nil {
		return err
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootCtx, timeout)
	defer cancel()

	start := []byte(prefix)
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	total := 0
	var snapshotRevision int64
	expectedPageCount := int64(-1)
	var writer *backupfile.AtomicWriter
	exportedLeases := make(map[int64]struct{})
	defer func() {
		if writer != nil {
			retErr = errors.Join(retErr, writer.Abort())
		}
	}()
	for {
		opts := []clientv3.OpOption{
			clientv3.WithRange(string(end)),
			clientv3.WithLimit(batchSize),
		}
		if snapshotRevision > 0 {
			opts = append(opts, clientv3.WithRev(snapshotRevision))
		}
		resp, err := cli.Get(ctx, string(start), opts...)
		if err != nil {
			return err
		}
		remaining, err := validateExportPage(resp, snapshotRevision, start, end, batchSize, expectedPageCount)
		if err != nil {
			return err
		}
		expectedPageCount = remaining
		if snapshotRevision == 0 {
			snapshotRevision = resp.Header.Revision
			writer, err = backupfile.NewAtomicWriter(output, prefix, snapshotRevision)
			if err != nil {
				return err
			}
		}
		for _, kv := range resp.Kvs {
			if kv.Lease != 0 {
				if _, exists := exportedLeases[kv.Lease]; !exists {
					ttl, ttlErr := cli.TimeToLive(ctx, clientv3.LeaseID(kv.Lease))
					if ttlErr != nil {
						return fmt.Errorf("read lease %d TTL: %w", kv.Lease, ttlErr)
					}
					lease, err := exportLeaseRecord(kv.Lease, ttl, snapshotRevision)
					if err != nil {
						return err
					}
					if err := writer.AddLease(lease); err != nil {
						return err
					}
					exportedLeases[kv.Lease] = struct{}{}
				}
			}
			rec := record.Record{
				Key:            base64.StdEncoding.EncodeToString(kv.Key),
				Value:          base64.StdEncoding.EncodeToString(kv.Value),
				ModRevision:    kv.ModRevision,
				CreateRevision: kv.CreateRevision,
				Version:        kv.Version,
				Lease:          kv.Lease,
			}
			if err := writer.Add(rec); err != nil {
				return err
			}
			total++
		}
		if !resp.More {
			break
		}
		start = nextKey(resp.Kvs[len(resp.Kvs)-1].Key)
	}
	if err := cli.Close(); err != nil {
		clientClosed = true
		return err
	}
	clientClosed = true
	status, err := writer.Commit()
	if err != nil {
		return err
	}
	writer = nil
	if metricsOutput := os.Getenv("METRICS_OUTPUT"); metricsOutput != "" {
		info, err := os.Stat(output)
		if err != nil {
			return fmt.Errorf("stat completed backup for metrics: %w", err)
		}
		instance := os.Getenv("BACKUP_INSTANCE")
		if instance == "" {
			instance = "kubebrain"
		}
		if err := backupmetrics.WriteSuccess(metricsOutput, instance, status, info.Size(), time.Now()); err != nil {
			return fmt.Errorf("publish backup success metrics: %w", err)
		}
	}
	_, err = fmt.Fprintf(os.Stderr, "exported %d records and %d leases from %s at revision %d to %s (sha256 %s)\n",
		total, status.Leases, prefix, snapshotRevision, output, status.SHA256)
	return err
}

func validateExportPage(response *clientv3.GetResponse, snapshotRevision int64, start, end []byte, limit, expectedCount int64) (int64, error) {
	if response == nil {
		return 0, errors.New("range returned an empty response")
	}
	if response.Header == nil {
		return 0, errors.New("range response omitted its header")
	}
	if response.Header.Revision <= 0 {
		return 0, fmt.Errorf("range response returned invalid revision %d", response.Header.Revision)
	}
	if snapshotRevision > 0 && response.Header.Revision < snapshotRevision {
		return 0, fmt.Errorf("range response revision %d is behind snapshot revision %d",
			response.Header.Revision, snapshotRevision)
	}
	if response.Count < 0 || response.Count < int64(len(response.Kvs)) {
		return 0, fmt.Errorf("range response returned count %d for %d records", response.Count, len(response.Kvs))
	}
	if expectedCount >= 0 && response.Count != expectedCount {
		return 0, fmt.Errorf("range response count %d does not continue previous remaining count %d", response.Count, expectedCount)
	}
	if int64(len(response.Kvs)) > limit {
		return 0, fmt.Errorf("range response returned %d records above page limit %d", len(response.Kvs), limit)
	}
	if response.More != (response.Count > int64(len(response.Kvs))) {
		return 0, errors.New("range response returned inconsistent count/more metadata")
	}
	if response.More && int64(len(response.Kvs)) != limit {
		return 0, errors.New("range response indicated more records after a non-full page")
	}
	effectiveSnapshot := snapshotRevision
	if effectiveSnapshot == 0 {
		effectiveSnapshot = response.Header.Revision
	}
	for i, kv := range response.Kvs {
		if kv == nil {
			return 0, fmt.Errorf("range response returned a nil record at index %d", i)
		}
		if len(kv.Key) == 0 || bytes.Compare(kv.Key, start) < 0 || (!bytes.Equal(end, []byte{0}) && bytes.Compare(kv.Key, end) >= 0) {
			return 0, fmt.Errorf("range response returned key %q outside requested range [%q,%q)", kv.Key, start, end)
		}
		if i > 0 && bytes.Compare(response.Kvs[i-1].Key, kv.Key) >= 0 {
			return 0, errors.New("range response records are not in strict ascending key order")
		}
		rec := record.Record{CreateRevision: kv.CreateRevision, ModRevision: kv.ModRevision, Version: kv.Version}
		if err := backupfile.ValidateRecordMetadata(rec, effectiveSnapshot); err != nil {
			return 0, fmt.Errorf("range response record %q: %w", kv.Key, err)
		}
	}
	return response.Count - int64(len(response.Kvs)), nil
}

func exportLeaseRecord(id int64, ttl *clientv3.LeaseTimeToLiveResponse, snapshotRevision int64) (record.Lease, error) {
	if ttl == nil {
		return record.Lease{}, fmt.Errorf("lease %d returned an empty TTL response", id)
	}
	if int64(ttl.ID) != id {
		return record.Lease{}, fmt.Errorf("lease %d TTL response returned mismatched ID %d", id, ttl.ID)
	}
	if ttl.ResponseHeader == nil {
		return record.Lease{}, fmt.Errorf("lease %d TTL response omitted its header", id)
	}
	if ttl.ResponseHeader.Revision <= 0 {
		return record.Lease{}, fmt.Errorf("lease %d TTL response returned invalid revision %d", id, ttl.ResponseHeader.Revision)
	}
	if ttl.ResponseHeader.Revision < snapshotRevision {
		return record.Lease{}, fmt.Errorf("lease %d TTL response revision %d is behind snapshot revision %d",
			id, ttl.ResponseHeader.Revision, snapshotRevision)
	}
	if ttl.TTL <= 0 {
		return record.Lease{}, fmt.Errorf("lease %d expired while exporting snapshot revision %d", id, snapshotRevision)
	}
	if ttl.GrantedTTL <= 0 || ttl.GrantedTTL > clientv3.MaxLeaseTTL {
		return record.Lease{}, fmt.Errorf("lease %d returned invalid granted TTL %d", id, ttl.GrantedTTL)
	}
	if len(ttl.Keys) != 0 {
		return record.Lease{}, fmt.Errorf("lease %d TTL response returned %d attached keys when none were requested", id, len(ttl.Keys))
	}
	return record.Lease{ID: id, TTL: ttl.TTL, GrantedTTL: ttl.GrantedTTL}, nil
}
