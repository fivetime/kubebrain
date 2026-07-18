package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strconv"
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
	prefix := os.Getenv("PREFIX")
	output := os.Getenv("OUTPUT")
	batchSize, err := strconv.ParseInt(os.Getenv("BATCH_SIZE"), 10, 64)
	if err != nil || batchSize <= 0 {
		log.Fatalf("invalid BATCH_SIZE: %q", os.Getenv("BATCH_SIZE"))
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

	start := []byte(prefix)
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	total := 0
	var snapshotRevision int64
	var writer *backupfile.AtomicWriter
	exportedLeases := make(map[int64]struct{})
	defer func() {
		if writer != nil {
			writer.Abort()
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
			log.Fatal(err)
		}
		if snapshotRevision == 0 && resp.Header != nil {
			snapshotRevision = resp.Header.Revision
			writer, err = backupfile.NewAtomicWriter(output, prefix, snapshotRevision)
			if err != nil {
				log.Fatal(err)
			}
		}
		if writer == nil {
			log.Fatal("range response did not contain a revision")
		}
		for _, kv := range resp.Kvs {
			if kv.Lease != 0 {
				if _, exists := exportedLeases[kv.Lease]; !exists {
					ttl, ttlErr := cli.TimeToLive(ctx, clientv3.LeaseID(kv.Lease))
					if ttlErr != nil {
						log.Fatalf("read lease %d TTL: %v", kv.Lease, ttlErr)
					}
					if ttl.TTL <= 0 {
						log.Fatalf("lease %d expired while exporting snapshot revision %d", kv.Lease, snapshotRevision)
					}
					if err := writer.AddLease(record.Lease{ID: kv.Lease, TTL: ttl.TTL}); err != nil {
						log.Fatal(err)
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
				log.Fatal(err)
			}
			total++
		}
		if !resp.More || len(resp.Kvs) == 0 {
			break
		}
		start = nextKey(resp.Kvs[len(resp.Kvs)-1].Key)
	}
	status, err := writer.Commit()
	if err != nil {
		log.Fatal(err)
	}
	if metricsOutput := os.Getenv("METRICS_OUTPUT"); metricsOutput != "" {
		info, err := os.Stat(output)
		if err != nil {
			log.Fatalf("stat completed backup for metrics: %v", err)
		}
		instance := os.Getenv("BACKUP_INSTANCE")
		if instance == "" {
			instance = "kubebrain"
		}
		if err := backupmetrics.WriteSuccess(metricsOutput, instance, status, info.Size(), time.Now()); err != nil {
			log.Fatalf("publish backup success metrics: %v", err)
		}
	}
	fmt.Fprintf(os.Stderr, "exported %d records and %d leases from %s at revision %d to %s (sha256 %s)\n",
		total, status.Leases, prefix, snapshotRevision, output, status.SHA256)
}
