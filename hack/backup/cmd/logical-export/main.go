package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/kubewharf/kubebrain/hack/backup/internal/etcdutil"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
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

	out, err := os.Create(output)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := []byte(prefix)
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	total := 0
	var snapshotRevision int64
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
		}
		for _, kv := range resp.Kvs {
			rec := record.Record{
				Key:            base64.StdEncoding.EncodeToString(kv.Key),
				Value:          base64.StdEncoding.EncodeToString(kv.Value),
				ModRevision:    kv.ModRevision,
				CreateRevision: kv.CreateRevision,
				Version:        kv.Version,
				Lease:          kv.Lease,
			}
			if err := json.NewEncoder(out).Encode(&rec); err != nil {
				log.Fatal(err)
			}
			total++
		}
		if !resp.More || len(resp.Kvs) == 0 {
			break
		}
		start = nextKey(resp.Kvs[len(resp.Kvs)-1].Key)
	}
	fmt.Fprintf(os.Stderr, "exported %d records from %s at revision %d to %s\n", total, prefix, snapshotRevision, output)
}
