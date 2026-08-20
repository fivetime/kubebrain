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

	"github.com/kubewharf/kubebrain/hack/backup/internal/targetverify"
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
		if err != nil || leaseTTL <= 0 || leaseTTL > clientv3.MaxLeaseTTL {
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

	responseAdmission := &prefixResponseAdmission{}
	var result any
	switch action {
	case "count":
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			return err
		}
		if err := validateCountResponse(resp); err != nil {
			return err
		}
		if err := responseAdmission.admitCount(resp); err != nil {
			return err
		}
		result = resp.Count
	case "delete":
		resp, err := cli.Delete(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			return err
		}
		if err := validateDeleteResponse(resp); err != nil {
			return err
		}
		if err := responseAdmission.admitDelete(resp); err != nil {
			return err
		}
		result = resp.Deleted
	case "put":
		response, err := cli.Put(ctx, prefix+keySuffix, value)
		if err != nil {
			return err
		}
		if err := validatePutResponse(response); err != nil {
			return err
		}
		if err := responseAdmission.admitPut(response); err != nil {
			return err
		}
	case "lease-put":
		lease, err := cli.Grant(ctx, leaseTTL)
		if err != nil {
			return err
		}
		var cleanupLeaseID clientv3.LeaseID
		if lease != nil && lease.ID != 0 {
			cleanupLeaseID = lease.ID
		}
		leaseID, validationErr := targetverify.ValidateProbeGrant(lease, leaseTTL)
		if validationErr != nil {
			return errors.Join(validationErr, cleanupLease(cli, cleanupLeaseID, responseAdmission))
		}
		if err := responseAdmission.admitGrant(lease); err != nil {
			return errors.Join(err, cleanupLease(cli, cleanupLeaseID, responseAdmission))
		}
		ops := make([]clientv3.Op, 0, len(leaseSuffixes))
		for _, suffix := range leaseSuffixes {
			ops = append(ops, clientv3.OpPut(prefix+suffix, value, clientv3.WithLease(leaseID)))
		}
		response, err := cli.Txn(ctx).Then(ops...).Commit()
		if err == nil {
			err = responseAdmission.admitLeasePut(response)
		}
		if err == nil {
			err = validateLeasePutTxnResponse(response, len(ops))
		}
		if err != nil {
			return errors.Join(err, cleanupLease(cli, cleanupLeaseID, responseAdmission))
		}
		result = leaseID
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

func cleanupLease(cli *clientv3.Client, id clientv3.LeaseID, admission *prefixResponseAdmission) error {
	if id == 0 {
		return nil
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	response, err := cli.Revoke(cleanupCtx, id)
	if err != nil {
		return err
	}
	if err := targetverify.ValidateProbeRevoke(response, 0); err != nil {
		return err
	}
	return admission.admitRevoke(response)
}
