package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if err := run(os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(stdout io.Writer) (retErr error) {
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
	if prefix == "" {
		return errors.New("PREFIX is required")
	}
	var leaseTTL int64
	var leaseSuffixes []string
	switch action {
	case "count", "delete", "put":
	case "lease-put":
		var err error
		leaseTTL, err = strconv.ParseInt(os.Getenv("LEASE_TTL"), 10, 64)
		if err != nil || leaseTTL <= 0 {
			return fmt.Errorf("invalid LEASE_TTL %q", os.Getenv("LEASE_TTL"))
		}
		leaseSuffixes = strings.Split(os.Getenv("KEY_SUFFIXES"), ",")
		if len(leaseSuffixes) == 0 || (len(leaseSuffixes) == 1 && leaseSuffixes[0] == "") {
			return errors.New("KEY_SUFFIXES is required")
		}
		for _, suffix := range leaseSuffixes {
			if suffix == "" {
				return errors.New("KEY_SUFFIXES contains an empty suffix")
			}
		}
	default:
		return fmt.Errorf("unknown ACTION %q", action)
	}

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

	var result any
	switch action {
	case "count":
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			return err
		}
		result = resp.Count
	case "delete":
		resp, err := cli.Delete(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			return err
		}
		result = resp.Deleted
	case "put":
		_, err := cli.Put(ctx, prefix+keySuffix, value)
		if err != nil {
			return err
		}
	case "lease-put":
		lease, err := cli.Grant(ctx, leaseTTL)
		if err != nil {
			return err
		}
		ops := make([]clientv3.Op, 0, len(leaseSuffixes))
		for _, suffix := range leaseSuffixes {
			ops = append(ops, clientv3.OpPut(prefix+suffix, value, clientv3.WithLease(lease.ID)))
		}
		if _, err := cli.Txn(ctx).Then(ops...).Commit(); err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, revokeErr := cli.Revoke(cleanupCtx, lease.ID)
			cleanupCancel()
			return errors.Join(err, revokeErr)
		}
		result = lease.ID
	}
	if err := cli.Close(); err != nil {
		clientClosed = true
		return err
	}
	clientClosed = true
	if result != nil {
		_, err = fmt.Fprintln(stdout, result)
		return err
	}
	return nil
}
