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
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/keyrewrite"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/backup/internal/restorereceipt"
	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (retErr error) {
	input := os.Getenv("INPUT")
	rewriteFrom := os.Getenv("REWRITE_FROM")
	rewriteTo := os.Getenv("REWRITE_TO")
	receiptOutput := os.Getenv("RECEIPT_OUTPUT")
	if rewriteFrom == "" && rewriteTo != "" {
		return errors.New("REWRITE_TO requires REWRITE_FROM")
	}

	verified, err := backupfile.OpenVerified(input)
	if err != nil {
		return fmt.Errorf("backup integrity validation failed: %w", err)
	}
	verifiedClosed := false
	defer func() {
		if !verifiedClosed {
			retErr = errors.Join(retErr, verified.Close())
		}
	}()
	status := verified.Status()
	targetPrefix := status.Prefix
	if receiptOutput != "" {
		targetPrefix, err = receiptTargetPrefix(status.Prefix, rewriteFrom, rewriteTo)
		if err != nil {
			return err
		}
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

	total := 0
	seenTargetKeys := make(map[string]struct{})
	targetLeaseBySource := make(map[int64]int64)
	sourceLeaseByTarget := make(map[int64]int64)
	targetLeaseMinRevision := make(map[int64]int64)
	responseAdmission := &verifyResponseAdmission{}
	verificationRevision := int64(0)
	if receiptOutput != "" {
		count, err := cli.Get(ctx, targetPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			return fmt.Errorf("pin verified target prefix %q: %w", targetPrefix, err)
		}
		if err := validateTargetPrefixCount(count, targetPrefix, int64(status.Records)); err != nil {
			return err
		}
		if err := responseAdmission.admitGet(count, []byte(targetPrefix)); err != nil {
			return err
		}
		verificationRevision = count.Header.Revision
	}
	err = verified.Records(func(rec record.Record) error {
		key, err := base64.StdEncoding.DecodeString(rec.Key)
		if err != nil {
			return err
		}
		value, err := base64.StdEncoding.DecodeString(rec.Value)
		if err != nil {
			return err
		}
		key, err = keyrewrite.RewriteUnique(key, rewriteFrom, rewriteTo, seenTargetKeys)
		if err != nil {
			return err
		}

		resp, err := cli.Get(ctx, string(key), targetReadOptions(verificationRevision)...)
		if err != nil {
			return err
		}
		kv, responseRevision, err := validateTargetGetResponse(resp, key, verificationRevision)
		if err != nil {
			return err
		}
		if err := responseAdmission.admitGet(resp, key); err != nil {
			return err
		}
		if !bytes.Equal(kv.Value, value) {
			return fmt.Errorf("restored value mismatch for key %q", string(key))
		}
		targetLease := kv.Lease
		if rec.Lease == 0 {
			if targetLease != 0 {
				return fmt.Errorf("restored permanent key %q unexpectedly has lease %d", string(key), targetLease)
			}
		} else {
			if targetLease == 0 {
				return fmt.Errorf("restored leased key %q is permanent", string(key))
			}
			if existing, ok := targetLeaseBySource[rec.Lease]; ok && existing != targetLease {
				return fmt.Errorf("source lease %d restored as multiple target leases", rec.Lease)
			}
			if existing, ok := sourceLeaseByTarget[targetLease]; ok && existing != rec.Lease {
				return fmt.Errorf("source leases %d and %d merged into target lease %d", existing, rec.Lease, targetLease)
			}
			targetLeaseBySource[rec.Lease] = targetLease
			sourceLeaseByTarget[targetLease] = rec.Lease
			if responseRevision > targetLeaseMinRevision[targetLease] {
				targetLeaseMinRevision[targetLease] = responseRevision
			}
		}
		total++
		return nil
	})
	if err != nil {
		return err
	}
	for sourceID, targetID := range targetLeaseBySource {
		ttl, err := cli.TimeToLive(ctx, clientv3.LeaseID(targetID))
		if err != nil {
			return fmt.Errorf("read restored lease for source %d: %w", sourceID, err)
		}
		if err := validateTargetLeaseTTLResponse(sourceID, targetID, ttl, targetLeaseMinRevision[targetID]); err != nil {
			return err
		}
		if err := responseAdmission.admitLease(ttl, sourceID); err != nil {
			return err
		}
	}
	if total != status.Records {
		return fmt.Errorf("verified artifact record count changed from %d to %d", status.Records, total)
	}
	clientCloseErr := cli.Close()
	clientClosed = true
	verifiedCloseErr := verified.Close()
	verifiedClosed = true
	if err := errors.Join(clientCloseErr, verifiedCloseErr); err != nil {
		return err
	}
	if receiptOutput != "" {
		receipt := restorereceipt.Receipt{
			Format: restorereceipt.Format, ArtifactFormat: status.Format,
			ArtifactSHA256: status.SHA256, SnapshotRevision: status.Revision,
			ArtifactCreatedAtUnix: status.CreatedAtUnix,
			SourcePrefix:          status.Prefix, TargetPrefix: targetPrefix, Records: total,
			ArtifactLeases: status.Leases, VerifiedTargetLeases: len(targetLeaseBySource),
			VerifiedAtUnix: time.Now().UTC().Unix(),
		}
		if err := restorereceipt.WriteAtomic(receiptOutput, receipt); err != nil {
			return fmt.Errorf("publish restore verification receipt: %w", err)
		}
	}
	_, err = fmt.Fprintf(os.Stderr, "verified %d restored records and %d leases from %s (snapshot revision %d, sha256 %s)\n",
		total, len(targetLeaseBySource), input, status.Revision, status.SHA256)
	return err
}

func targetReadOptions(revision int64) []clientv3.OpOption {
	if revision <= 0 {
		return nil
	}
	return []clientv3.OpOption{clientv3.WithRev(revision)}
}

func validateTargetGetResponse(response *clientv3.GetResponse, key []byte, snapshotRevision int64) (*mvccpb.KeyValue, int64, error) {
	if response == nil {
		return nil, 0, fmt.Errorf("target key %q returned an empty range response", key)
	}
	if response.Header == nil || response.Header.Revision <= 0 {
		return nil, 0, fmt.Errorf("target key %q returned no valid response revision", key)
	}
	if snapshotRevision > 0 && response.Header.Revision < snapshotRevision {
		return nil, 0, fmt.Errorf("target key %q response revision %d is behind pinned verification revision %d", key, response.Header.Revision, snapshotRevision)
	}
	if response.More || response.Count != int64(len(response.Kvs)) {
		return nil, 0, fmt.Errorf("target key %q returned inconsistent count/more metadata", key)
	}
	if len(response.Kvs) != 1 {
		return nil, 0, fmt.Errorf("expected restored key %q exactly once, got %d", key, len(response.Kvs))
	}
	kv := response.Kvs[0]
	if kv == nil {
		return nil, 0, fmt.Errorf("target key %q returned a nil key-value", key)
	}
	if !bytes.Equal(kv.Key, key) {
		return nil, 0, fmt.Errorf("target key %q returned mismatched key %q", key, kv.Key)
	}
	rec := record.Record{CreateRevision: kv.CreateRevision, ModRevision: kv.ModRevision, Version: kv.Version}
	metadataRevision := response.Header.Revision
	if snapshotRevision > 0 {
		metadataRevision = snapshotRevision
	}
	if err := backupfile.ValidateRecordMetadata(rec, metadataRevision); err != nil {
		return nil, 0, fmt.Errorf("target key %q returned invalid MVCC metadata: %w", key, err)
	}
	return kv, response.Header.Revision, nil
}

func validateTargetLeaseTTLResponse(sourceID, targetID int64, response *clientv3.LeaseTimeToLiveResponse, minRevision int64) error {
	if response == nil {
		return fmt.Errorf("restored lease for source %d returned an empty TTL response", sourceID)
	}
	if int64(response.ID) != targetID {
		return fmt.Errorf("restored lease for source %d expected target ID %d, got %d", sourceID, targetID, response.ID)
	}
	if response.ResponseHeader == nil || response.ResponseHeader.Revision <= 0 {
		return fmt.Errorf("restored lease for source %d returned no valid response revision", sourceID)
	}
	if response.ResponseHeader.Revision < minRevision {
		return fmt.Errorf("restored lease for source %d returned revision %d behind key observation revision %d",
			sourceID, response.ResponseHeader.Revision, minRevision)
	}
	if response.TTL <= 0 {
		return fmt.Errorf("restored lease for source %d is expired", sourceID)
	}
	if response.GrantedTTL <= 0 || response.GrantedTTL > clientv3.MaxLeaseTTL {
		return fmt.Errorf("restored lease for source %d returned invalid granted TTL %d", sourceID, response.GrantedTTL)
	}
	if len(response.Keys) != 0 {
		return fmt.Errorf("restored lease for source %d returned %d attached keys when none were requested", sourceID, len(response.Keys))
	}
	return nil
}

func receiptTargetPrefix(sourcePrefix, rewriteFrom, rewriteTo string) (string, error) {
	if rewriteFrom == "" {
		return sourcePrefix, nil
	}
	if rewriteFrom != sourcePrefix {
		return "", fmt.Errorf("RECEIPT_OUTPUT requires REWRITE_FROM %q to equal artifact prefix %q",
			rewriteFrom, sourcePrefix)
	}
	if rewriteTo == "" {
		return "", fmt.Errorf("RECEIPT_OUTPUT requires a non-empty REWRITE_TO when REWRITE_FROM is set")
	}
	return rewriteTo, nil
}
