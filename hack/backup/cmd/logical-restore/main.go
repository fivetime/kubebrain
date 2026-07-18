package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/etcdutil"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
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

func envBool(name string) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return value == "1" || value == "true" || value == "yes"
}

func validateLeaseReference(rec record.Record, leaseSpecs map[int64]int64) error {
	if rec.Lease == 0 {
		return nil
	}
	if _, exists := leaseSpecs[rec.Lease]; !exists {
		return fmt.Errorf("backup record references unrestorable lease %d; re-export with %s", rec.Lease, backupfile.Format)
	}
	return nil
}

func main() {
	input := os.Getenv("INPUT")
	rewriteFrom := os.Getenv("REWRITE_FROM")
	rewriteTo := os.Getenv("REWRITE_TO")
	allowOverwrite := envBool("ALLOW_OVERWRITE")
	batchSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil || batchSize <= 0 {
		log.Fatalf("invalid BATCH_SIZE: %q", os.Getenv("BATCH_SIZE"))
	}
	if rewriteFrom == "" && rewriteTo != "" {
		log.Fatal("REWRITE_TO requires REWRITE_FROM")
	}

	verified, err := backupfile.OpenVerified(input)
	if err != nil {
		log.Fatalf("backup integrity validation failed: %v", err)
	}
	defer verified.Close()

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

	type kvPair struct {
		key   []byte
		value []byte
		lease int64
	}
	leaseSpecs := make(map[int64]int64)
	if err := verified.Leases(func(lease record.Lease) error {
		leaseSpecs[lease.ID] = lease.TTL
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := verified.Records(func(rec record.Record) error {
		return validateLeaseReference(rec, leaseSpecs)
	}); err != nil {
		log.Fatal(err)
	}

	targetLeases := make(map[int64]clientv3.LeaseID, len(leaseSpecs))
	cleanupTargetLeases := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, id := range targetLeases {
			_, _ = cli.Revoke(cleanupCtx, id)
		}
	}
	for sourceID, ttl := range leaseSpecs {
		granted, grantErr := cli.Grant(ctx, ttl)
		if grantErr != nil {
			cleanupTargetLeases()
			log.Fatalf("restore lease %d: %v", sourceID, grantErr)
		}
		targetLeases[sourceID] = granted.ID
	}
	ops := make([]kvPair, 0, batchSize)
	total := 0
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		txn := cli.Txn(ctx)
		compares := make([]clientv3.Cmp, 0, len(ops))
		puts := make([]clientv3.Op, 0, len(ops))
		for _, op := range ops {
			key := string(op.key)
			if !allowOverwrite {
				compares = append(compares, clientv3.Compare(clientv3.Version(key), "=", 0))
			}
			opts := make([]clientv3.OpOption, 0, 1)
			if op.lease != 0 {
				targetID, exists := targetLeases[op.lease]
				if !exists {
					return fmt.Errorf("backup record references unrestorable lease %d; re-export with %s", op.lease, backupfile.Format)
				}
				opts = append(opts, clientv3.WithLease(targetID))
			}
			puts = append(puts, clientv3.OpPut(key, string(op.value), opts...))
		}
		if len(compares) > 0 {
			txn = txn.If(compares...)
		}
		resp, err := txn.Then(puts...).Commit()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			return fmt.Errorf("refusing to overwrite one or more existing keys in restore batch; set ALLOW_OVERWRITE=true to replace existing records")
		}
		ops = ops[:0]
		return nil
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
		key = rewriteKey(key, rewriteFrom, rewriteTo)
		ops = append(ops, kvPair{key: key, value: value, lease: rec.Lease})
		total++
		if len(ops) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		cleanupTargetLeases()
		log.Fatal(err)
	}
	if err := flush(); err != nil {
		cleanupTargetLeases()
		log.Fatal(err)
	}
	status := verified.Status()
	fmt.Fprintf(os.Stderr, "restored %d records and %d leases from %s to %s (snapshot revision %d, sha256 %s)\n",
		total, status.Leases, input, os.Getenv("ENDPOINT"), status.Revision, status.SHA256)
}
