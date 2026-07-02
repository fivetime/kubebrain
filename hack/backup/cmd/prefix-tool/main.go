package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kubewharf/kubebrain/hack/backup/internal/etcdutil"
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
	default:
		log.Fatalf("unknown ACTION %q", action)
	}
}
