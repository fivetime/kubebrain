package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
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

func main() {
	input := os.Getenv("INPUT")
	rewriteFrom := os.Getenv("REWRITE_FROM")
	rewriteTo := os.Getenv("REWRITE_TO")
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

	total := 0
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
		total++
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	status := verified.Status()
	fmt.Fprintf(os.Stderr, "verified %d restored records from %s (snapshot revision %d, sha256 %s)\n",
		total, input, status.Revision, status.SHA256)
}
