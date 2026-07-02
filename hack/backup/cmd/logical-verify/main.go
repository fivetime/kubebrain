package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

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
	total := 0
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

		resp, err := cli.Get(ctx, string(key))
		if err != nil {
			log.Fatal(err)
		}
		if len(resp.Kvs) != 1 {
			log.Fatalf("expected restored key %q exactly once, got %d", string(key), len(resp.Kvs))
		}
		if !bytes.Equal(resp.Kvs[0].Value, value) {
			log.Fatalf("restored value mismatch for key %q", string(key))
		}
		total++
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "verified %d restored records from %s\n", total, input)
}
