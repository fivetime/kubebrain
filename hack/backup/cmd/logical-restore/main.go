package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kubewharf/kubebrain/hack/backup/internal/etcdutil"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
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

	cli, err := etcdutil.NewClientFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	timeout, err := etcdutil.TimeoutFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(input)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	type kvPair struct {
		key   []byte
		value []byte
	}
	ops := make([]kvPair, 0, batchSize)
	total := 0
	flush := func() {
		if len(ops) == 0 {
			return
		}
		for _, op := range ops {
			key := string(op.key)
			value := string(op.value)
			if allowOverwrite {
				if _, err := cli.Put(ctx, key, value); err != nil {
					log.Fatal(err)
				}
				continue
			}
			resp, err := cli.Txn(ctx).
				If(clientv3.Compare(clientv3.Version(key), "=", 0)).
				Then(clientv3.OpPut(key, value)).
				Commit()
			if err != nil {
				log.Fatal(err)
			}
			if !resp.Succeeded {
				log.Fatalf("refusing to overwrite existing key %q; set ALLOW_OVERWRITE=true to replace existing records", key)
			}
		}
		ops = ops[:0]
	}

	for scanner.Scan() {
		var rec record.Record
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			log.Fatal(err)
		}
		key, err := base64.StdEncoding.DecodeString(rec.Key)
		if err != nil {
			log.Fatal(err)
		}
		value, err := base64.StdEncoding.DecodeString(rec.Value)
		if err != nil {
			log.Fatal(err)
		}
		key = rewriteKey(key, rewriteFrom, rewriteTo)
		ops = append(ops, kvPair{key: key, value: value})
		total++
		if len(ops) >= batchSize {
			flush()
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	flush()
	fmt.Fprintf(os.Stderr, "restored %d records from %s to %s\n", total, input, os.Getenv("ENDPOINT"))
}
