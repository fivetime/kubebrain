package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	prefix := os.Getenv("PREFIX")
	action := strings.TrimSpace(os.Getenv("ACTION"))
	keySuffix := os.Getenv("KEY_SUFFIX")
	if keySuffix == "" {
		keySuffix = "/key"
	}
	value := os.Getenv("VALUE")
	if value == "" {
		value = "original"
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

	switch action {
	case "count":
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(resp.Count)
	case "delete":
		resp, err := cli.Delete(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(resp.Deleted)
	case "put":
		_, err := cli.Put(ctx, prefix+keySuffix, value)
		if err != nil {
			log.Fatal(err)
		}
	case "lease-put":
		ttl, err := strconv.ParseInt(os.Getenv("LEASE_TTL"), 10, 64)
		if err != nil || ttl <= 0 {
			log.Fatalf("invalid LEASE_TTL %q", os.Getenv("LEASE_TTL"))
		}
		lease, err := cli.Grant(ctx, ttl)
		if err != nil {
			log.Fatal(err)
		}
		suffixes := strings.Split(os.Getenv("KEY_SUFFIXES"), ",")
		if len(suffixes) == 0 || (len(suffixes) == 1 && suffixes[0] == "") {
			log.Fatal("KEY_SUFFIXES is required")
		}
		ops := make([]clientv3.Op, 0, len(suffixes))
		for _, suffix := range suffixes {
			if suffix == "" {
				log.Fatal("KEY_SUFFIXES contains an empty suffix")
			}
			ops = append(ops, clientv3.OpPut(prefix+suffix, value, clientv3.WithLease(lease.ID)))
		}
		if _, err := cli.Txn(ctx).Then(ops...).Commit(); err != nil {
			log.Fatal(err)
		}
		fmt.Println(lease.ID)
	default:
		log.Fatalf("unknown ACTION %q", action)
	}
}
