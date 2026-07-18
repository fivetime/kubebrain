package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/etcdutil"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/backup/internal/restorereceipt"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func rewriteKey(key []byte, from, to string) []byte {
	if from == "" {
		return key
	}
	keyText := string(key)
	if !strings.HasPrefix(keyText, from) {
		return key
	}
	return []byte(to + strings.TrimPrefix(keyText, from))
}

func main() {
	input := os.Getenv("INPUT")
	rewriteFrom := os.Getenv("REWRITE_FROM")
	rewriteTo := os.Getenv("REWRITE_TO")
	receiptOutput := os.Getenv("RECEIPT_OUTPUT")
	if rewriteFrom == "" && rewriteTo != "" {
		log.Fatal("REWRITE_TO requires REWRITE_FROM")
	}

	verified, err := backupfile.OpenVerified(input)
	if err != nil {
		log.Fatalf("backup integrity validation failed: %v", err)
	}
	defer verified.Close()
	status := verified.Status()
	targetPrefix := status.Prefix
	if receiptOutput != "" {
		targetPrefix, err = receiptTargetPrefix(status.Prefix, rewriteFrom, rewriteTo)
		if err != nil {
			log.Fatal(err)
		}
	}

	cli, err := etcdutil.NewClientFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	timeout, err := etcdutil.TimeoutFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	total := 0
	targetLeaseBySource := make(map[int64]int64)
	sourceLeaseByTarget := make(map[int64]int64)
	err = verified.Records(func(rec record.Record) error {
		key, err := base64.StdEncoding.DecodeString(rec.Key)
		if err != nil {
			return err
		}
		value, err := base64.StdEncoding.DecodeString(rec.Value)
		if err != nil {
			return err
		}
		key = rewriteKey(key, rewriteFrom, rewriteTo)

		resp, err := cli.Get(ctx, string(key))
		if err != nil {
			return err
		}
		if len(resp.Kvs) != 1 {
			return fmt.Errorf("expected restored key %q exactly once, got %d", string(key), len(resp.Kvs))
		}
		if !bytes.Equal(resp.Kvs[0].Value, value) {
			return fmt.Errorf("restored value mismatch for key %q", string(key))
		}
		targetLease := resp.Kvs[0].Lease
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
		}
		total++
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	for sourceID, targetID := range targetLeaseBySource {
		ttl, err := cli.TimeToLive(ctx, clientv3.LeaseID(targetID))
		if err != nil {
			log.Fatalf("read restored lease for source %d: %v", sourceID, err)
		}
		if ttl.TTL <= 0 {
			log.Fatalf("restored lease for source %d is expired", sourceID)
		}
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
			log.Fatalf("publish restore verification receipt: %v", err)
		}
	}
	fmt.Fprintf(os.Stderr, "verified %d restored records and %d leases from %s (snapshot revision %d, sha256 %s)\n",
		total, len(targetLeaseBySource), input, status.Revision, status.SHA256)
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
