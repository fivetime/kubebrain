package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

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
			puts = append(puts, clientv3.OpPut(key, string(op.value)))
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
		ops = append(ops, kvPair{key: key, value: value})
		total++
		if len(ops) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := flush(); err != nil {
		log.Fatal(err)
	}
	status := verified.Status()
	fmt.Fprintf(os.Stderr, "restored %d records from %s to %s (snapshot revision %d, sha256 %s)\n",
		total, input, os.Getenv("ENDPOINT"), status.Revision, status.SHA256)
}
