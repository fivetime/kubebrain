package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/keyrewrite"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func envBool(name string) (bool, error) {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch value {
	case "", "0", "false", "no":
		return false, nil
	case "1", "true", "yes":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be a boolean", name)
	}
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

func validateRestoredLeaseGrant(sourceID, requestedTTL int64, response *clientv3.LeaseGrantResponse, seen map[clientv3.LeaseID]int64) error {
	if response == nil {
		return errors.New("target returned an empty lease grant response")
	}
	if response.ID == clientv3.NoLease {
		return errors.New("target returned zero lease ID")
	}
	if response.ResponseHeader == nil || response.ResponseHeader.Revision <= 0 {
		return errors.New("target lease grant response omitted a valid header")
	}
	if response.Error != "" {
		return errors.New("target lease grant returned success with a legacy error")
	}
	if response.TTL < requestedTTL {
		return fmt.Errorf("target granted TTL %d below requested TTL %d", response.TTL, requestedTTL)
	}
	if response.TTL > clientv3.MaxLeaseTTL {
		return fmt.Errorf("target granted TTL %d above maximum %d", response.TTL, clientv3.MaxLeaseTTL)
	}
	if previousSource, exists := seen[response.ID]; exists {
		return fmt.Errorf("target lease ID %d reused for source leases %d and %d", response.ID, previousSource, sourceID)
	}
	seen[response.ID] = sourceID
	return nil
}

func validateRestorePutTxnResponse(response *clientv3.TxnResponse, expectedPuts int) (int64, error) {
	if response == nil {
		return 0, errors.New("restore batch returned an empty transaction response")
	}
	if !response.Succeeded {
		return 0, errors.New("refusing to overwrite one or more existing keys in restore batch; set ALLOW_OVERWRITE=true to replace existing records")
	}
	if response.Header == nil || response.Header.Revision <= 0 {
		return 0, errors.New("restore batch committed without a valid response revision")
	}
	if len(response.Responses) != expectedPuts {
		return 0, fmt.Errorf("restore batch returned %d responses for %d puts", len(response.Responses), expectedPuts)
	}
	for i, op := range response.Responses {
		if op == nil || op.GetResponsePut() == nil {
			return 0, fmt.Errorf("restore batch response %d is not a put response", i)
		}
		put := op.GetResponsePut()
		if put.Header == nil || put.Header.Revision != response.Header.Revision {
			return 0, fmt.Errorf("restore batch put response %d has revision %d, transaction revision is %d",
				i, put.GetHeader().GetRevision(), response.Header.Revision)
		}
		if put.PrevKv != nil {
			return 0, fmt.Errorf("restore batch put response %d returned an unrequested previous key", i)
		}
	}
	return response.Header.Revision, nil
}

func validateBatchSize(batchSize, maxTxnOps int) error {
	if maxTxnOps <= 0 {
		return fmt.Errorf("MAX_TXN_OPS must be positive")
	}
	if batchSize > maxTxnOps {
		return fmt.Errorf("BATCH_SIZE %d exceeds MAX_TXN_OPS %d", batchSize, maxTxnOps)
	}
	return nil
}

type committedBatch struct {
	keys     []string
	revision int64
}

func rollbackCommittedBatches(batches []committedBatch, rollback func(committedBatch) error) error {
	for i := len(batches) - 1; i >= 0; i-- {
		if err := rollback(batches[i]); err != nil {
			return fmt.Errorf("rollback batch %d at revision %d: %w", i, batches[i].revision, err)
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (retErr error) {
	input := os.Getenv("INPUT")
	rewriteFrom := os.Getenv("REWRITE_FROM")
	rewriteTo := os.Getenv("REWRITE_TO")
	allowOverwrite, err := envBool("ALLOW_OVERWRITE")
	if err != nil {
		return err
	}
	batchSize, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("invalid BATCH_SIZE: %q", os.Getenv("BATCH_SIZE"))
	}
	maxTxnOps, err := strconv.Atoi(os.Getenv("MAX_TXN_OPS"))
	if err != nil || maxTxnOps <= 0 {
		return fmt.Errorf("invalid MAX_TXN_OPS: %q", os.Getenv("MAX_TXN_OPS"))
	}
	if err := validateBatchSize(batchSize, maxTxnOps); err != nil {
		return err
	}
	failAfterBatches := 0
	if value := os.Getenv("FAIL_AFTER_BATCHES"); value != "" {
		failAfterBatches, err = strconv.Atoi(value)
		if err != nil || failAfterBatches <= 0 {
			return fmt.Errorf("invalid FAIL_AFTER_BATCHES: %q", value)
		}
	}
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

	type kvPair struct {
		key   []byte
		value []byte
		lease int64
	}
	leaseSpecs := make(map[int64]int64)
	if err := verified.Leases(func(lease record.Lease) error {
		leaseSpecs[lease.ID] = restorableLeaseTTL(lease)
		return nil
	}); err != nil {
		return err
	}
	seenTargetKeys := make(map[string]struct{})
	if err := verified.Records(func(rec record.Record) error {
		if err := validateLeaseReference(rec, leaseSpecs); err != nil {
			return err
		}
		key, err := base64.StdEncoding.DecodeString(rec.Key)
		if err != nil {
			return err
		}
		_, err = keyrewrite.RewriteUnique(key, rewriteFrom, rewriteTo, seenTargetKeys)
		return err
	}); err != nil {
		return err
	}

	if !allowOverwrite {
		keys := make([]string, 0, batchSize)
		preflight := func() error {
			if len(keys) == 0 {
				return nil
			}
			gets := make([]clientv3.Op, 0, len(keys))
			for _, key := range keys {
				gets = append(gets, clientv3.OpGet(key))
			}
			resp, err := cli.Txn(ctx).Then(gets...).Commit()
			if err != nil {
				return err
			}
			if len(resp.Responses) != len(keys) {
				return fmt.Errorf("target preflight returned %d responses for %d keys", len(resp.Responses), len(keys))
			}
			for i, response := range resp.Responses {
				ranged := response.GetResponseRange()
				if ranged == nil {
					return fmt.Errorf("target preflight response %d is not a range response", i)
				}
				if len(ranged.Kvs) != 0 {
					return fmt.Errorf("refusing to overwrite existing key %q; set ALLOW_OVERWRITE=true to replace existing records", keys[i])
				}
			}
			keys = keys[:0]
			return nil
		}
		err = verified.Records(func(rec record.Record) error {
			key, decodeErr := base64.StdEncoding.DecodeString(rec.Key)
			if decodeErr != nil {
				return decodeErr
			}
			keys = append(keys, string(keyrewrite.Rewrite(key, rewriteFrom, rewriteTo)))
			if len(keys) == batchSize {
				return preflight()
			}
			return nil
		})
		if err == nil {
			err = preflight()
		}
		if err != nil {
			return err
		}
	}

	targetLeases := make(map[int64]clientv3.LeaseID, len(leaseSpecs))
	cleanupTargetLeases := func() (cleanupErr error) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		revoked := make(map[clientv3.LeaseID]struct{}, len(targetLeases))
		for sourceID, id := range targetLeases {
			if _, exists := revoked[id]; exists {
				continue
			}
			revoked[id] = struct{}{}
			if _, err := cli.Revoke(cleanupCtx, id); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("revoke restored lease %d: %w", sourceID, err))
			}
		}
		return cleanupErr
	}
	seenTargetLeases := make(map[clientv3.LeaseID]int64, len(leaseSpecs))
	for sourceID, ttl := range leaseSpecs {
		granted, grantErr := cli.Grant(ctx, ttl)
		if grantErr != nil {
			return errors.Join(fmt.Errorf("restore lease %d: %w", sourceID, grantErr), cleanupTargetLeases())
		}
		if granted != nil && granted.ID != clientv3.NoLease {
			targetLeases[sourceID] = granted.ID
		}
		if err := validateRestoredLeaseGrant(sourceID, ttl, granted, seenTargetLeases); err != nil {
			return errors.Join(fmt.Errorf("restore lease %d: %w", sourceID, err), cleanupTargetLeases())
		}
	}
	ops := make([]kvPair, 0, batchSize)
	committed := make([]committedBatch, 0)
	committedCount := 0
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
		responseRevision, err := validateRestorePutTxnResponse(resp, len(puts))
		if err != nil {
			return err
		}
		if !allowOverwrite {
			keys := make([]string, 0, len(ops))
			for _, op := range ops {
				keys = append(keys, string(op.key))
			}
			committed = append(committed, committedBatch{keys: keys, revision: responseRevision})
		}
		committedCount++
		if failAfterBatches > 0 && committedCount >= failAfterBatches {
			return fmt.Errorf("injected failure after %d committed batches", committedCount)
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
		key = keyrewrite.Rewrite(key, rewriteFrom, rewriteTo)
		ops = append(ops, kvPair{key: key, value: value, lease: rec.Lease})
		total++
		if len(ops) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		restoreErr := err
		var rollbackErr error
		if !allowOverwrite {
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer rollbackCancel()
			rollbackErr = rollbackCommittedBatches(committed, func(batch committedBatch) error {
				compares := make([]clientv3.Cmp, 0, len(batch.keys))
				deletes := make([]clientv3.Op, 0, len(batch.keys))
				for _, key := range batch.keys {
					compares = append(compares, clientv3.Compare(clientv3.ModRevision(key), "=", batch.revision))
					deletes = append(deletes, clientv3.OpDelete(key))
				}
				resp, txnErr := cli.Txn(rollbackCtx).If(compares...).Then(deletes...).Commit()
				if txnErr != nil {
					return txnErr
				}
				if !resp.Succeeded {
					return errors.New("one or more restored keys changed after commit; refusing to delete concurrent data")
				}
				return nil
			})
		}
		cleanupErr := cleanupTargetLeases()
		if rollbackErr != nil {
			return errors.Join(fmt.Errorf("restore failed: %w", restoreErr), fmt.Errorf("rollback incomplete: %w", rollbackErr), cleanupErr)
		}
		if allowOverwrite {
			return errors.Join(fmt.Errorf("restore failed: %w; ALLOW_OVERWRITE=true prevents safe automatic rollback", restoreErr), cleanupErr)
		}
		return errors.Join(fmt.Errorf("restore failed and committed batches were rolled back: %w", restoreErr), cleanupErr)
	}
	status := verified.Status()
	clientCloseErr := cli.Close()
	clientClosed = true
	verifiedCloseErr := verified.Close()
	verifiedClosed = true
	if err := errors.Join(clientCloseErr, verifiedCloseErr); err != nil {
		return err
	}
	_, err = fmt.Fprintf(os.Stderr, "restored %d records and %d leases from %s to %s (snapshot revision %d, sha256 %s)\n",
		total, status.Leases, input, os.Getenv("ENDPOINT"), status.Revision, status.SHA256)
	return err
}

func restorableLeaseTTL(lease record.Lease) int64 {
	ttl := lease.TTL
	if lease.GrantedTTL > 0 && ttl > lease.GrantedTTL {
		ttl = lease.GrantedTTL
	}
	if ttl > clientv3.MaxLeaseTTL {
		ttl = clientv3.MaxLeaseTTL
	}
	return ttl
}
