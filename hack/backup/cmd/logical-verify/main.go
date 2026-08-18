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
		return err
	}
	for sourceID, targetID := range targetLeaseBySource {
		ttl, err := cli.TimeToLive(ctx, clientv3.LeaseID(targetID))
		if err != nil {
			return fmt.Errorf("read restored lease for source %d: %w", sourceID, err)
		}
		if ttl.TTL <= 0 {
			return fmt.Errorf("restored lease for source %d is expired", sourceID)
		}
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
