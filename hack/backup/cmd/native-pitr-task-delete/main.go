// Command native-pitr-task-delete conditionally removes one owned backup-stream
// task and records its final observed global checkpoint.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	pingcaplog "github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zapcore"
)

type options struct {
	taskCreate, taskReady, pd, ca, cert, key string
	timeout                                  time.Duration
}

func main() {
	pingcaplog.SetLevel(zapcore.ErrorLevel)
	var o options
	flag.StringVar(&o.taskCreate, "task-create", "", "exact native-pitr-task-create.v1 receipt")
	flag.StringVar(&o.taskReady, "task-ready", "", "exact native-pitr-task-ready.v1 receipt")
	flag.StringVar(&o.pd, "pd", "", "comma-separated PD endpoints for the task cluster")
	flag.StringVar(&o.ca, "ca", "", "PD CA file")
	flag.StringVar(&o.cert, "cert", "", "PD client certificate file")
	flag.StringVar(&o.key, "key", "", "PD client private key file")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "operation timeout")
	flag.Parse()
	if err := execute(context.Background(), o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR task delete:", err)
		os.Exit(1)
	}
}

func execute(parent context.Context, o options, out interface{ Write([]byte) (int, error) }) error {
	if o.taskCreate == "" || o.taskReady == "" || o.pd == "" {
		return fmt.Errorf("task-create, task-ready, and pd are required")
	}
	if o.timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if (o.cert == "") != (o.key == "") || (o.cert != "" && o.ca == "") {
		return fmt.Errorf("cert and key must be set together and require ca")
	}
	createBytes, err := os.ReadFile(o.taskCreate)
	if err != nil {
		return err
	}
	task, err := nativepitr.DecodeTaskCreate(bytes.NewReader(createBytes))
	if err != nil {
		return err
	}
	readyBytes, err := os.ReadFile(o.taskReady)
	if err != nil {
		return err
	}
	ready, err := nativepitr.DecodeTaskReady(bytes.NewReader(readyBytes))
	if err != nil {
		return err
	}
	var endpoints []string
	for _, endpoint := range strings.Split(o.pd, ",") {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("pd must contain an endpoint")
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	var tlsCfg *tls.Config
	if o.ca != "" {
		tlsCfg, err = (transport.TLSInfo{TrustedCAFile: o.ca, CertFile: o.cert, KeyFile: o.key}).ClientConfig()
		if err != nil {
			return fmt.Errorf("TLS config: %w", err)
		}
	}
	etcd, err := clientv3.New(clientv3.Config{Endpoints: endpoints, TLS: tlsCfg, DialTimeout: o.timeout})
	if err != nil {
		return fmt.Errorf("connect PD metadata: %w", err)
	}
	defer etcd.Close()
	pdc, err := pd.NewClientWithContext(ctx, endpoints, pd.SecurityOption{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return fmt.Errorf("connect PD: %w", err)
	}
	defer pdc.Close()
	metadata := nativepitr.EtcdMetadata{KV: etcd.KV}
	receipt, err := nativepitr.DeleteTask(ctx, metadata, pdc.GetClusterID(ctx), task, ready)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	_, err = out.Write(encoded)
	return err
}
