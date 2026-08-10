// Command native-pitr-task-create atomically installs one arbitrary-range
// TiKV backup-stream task after pinning a task-specific bootstrap GC safepoint.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	pingcaplog "github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zapcore"
)

type options struct {
	preflight, ca, cert, key                             string
	startTS, endTS                                       uint64
	s3Endpoint, s3Region, s3Bucket, s3Prefix, s3Provider string
	forcePathStyle                                       bool
	timeout                                              time.Duration
}

func main() {
	pingcaplog.SetLevel(zapcore.ErrorLevel)
	var o options
	flag.StringVar(&o.preflight, "preflight", "", "exact native-pitr-preflight.v1 receipt")
	flag.Uint64Var(&o.startTS, "start-ts", 0, "log task start TSO; zero obtains a fresh PD TSO")
	flag.Uint64Var(&o.endTS, "end-ts", ^uint64(0), "exclusive log task end TSO")
	flag.StringVar(&o.s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint")
	flag.StringVar(&o.s3Region, "s3-region", "", "S3 region")
	flag.StringVar(&o.s3Bucket, "s3-bucket", "", "S3 bucket")
	flag.StringVar(&o.s3Prefix, "s3-prefix", "", "immutable task prefix")
	flag.StringVar(&o.s3Provider, "s3-provider", "", "S3 provider")
	flag.BoolVar(&o.forcePathStyle, "s3-force-path-style", false, "use path-style S3 requests")
	flag.StringVar(&o.ca, "ca", "", "PD CA file")
	flag.StringVar(&o.cert, "cert", "", "PD client certificate file")
	flag.StringVar(&o.key, "key", "", "PD client private key file")
	flag.DurationVar(&o.timeout, "timeout", 30*time.Second, "operation timeout")
	flag.Parse()
	if err := execute(context.Background(), o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "native PITR task create:", err)
		os.Exit(1)
	}
}

func execute(parent context.Context, o options, out interface{ Write([]byte) (int, error) }) error {
	if o.preflight == "" {
		return fmt.Errorf("preflight is required")
	}
	if o.timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if (o.cert == "") != (o.key == "") {
		return fmt.Errorf("cert and key must be set together")
	}
	if o.cert != "" && o.ca == "" {
		return fmt.Errorf("client certificate authentication requires ca")
	}
	b, err := os.ReadFile(o.preflight)
	if err != nil {
		return err
	}
	p, err := nativepitr.DecodePreflight(bytes.NewReader(b))
	if err != nil {
		return err
	}
	digest := sha256.Sum256(b)
	storage := &backuppb.StorageBackend{Backend: &backuppb.StorageBackend_S3{S3: &backuppb.S3{Endpoint: o.s3Endpoint, Region: o.s3Region, Bucket: o.s3Bucket, Prefix: o.s3Prefix, Provider: o.s3Provider, ForcePathStyle: o.forcePathStyle}}}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	var tlsCfg *tls.Config
	if o.ca != "" {
		tlsCfg, err = (transport.TLSInfo{TrustedCAFile: o.ca, CertFile: o.cert, KeyFile: o.key}).ClientConfig()
		if err != nil {
			return fmt.Errorf("TLS config: %w", err)
		}
	}
	etcd, err := clientv3.New(clientv3.Config{Endpoints: p.PDAddrs, TLS: tlsCfg, DialTimeout: o.timeout})
	if err != nil {
		return fmt.Errorf("connect PD metadata: %w", err)
	}
	defer etcd.Close()
	pdc, err := pd.NewClientWithContext(ctx, p.PDAddrs, pd.SecurityOption{CAPath: o.ca, CertPath: o.cert, KeyPath: o.key})
	if err != nil {
		return fmt.Errorf("connect PD: %w", err)
	}
	defer pdc.Close()
	if o.startTS == 0 {
		physical, logical, err := pdc.GetTS(ctx)
		if err != nil {
			return fmt.Errorf("obtain task start TSO: %w", err)
		}
		o.startTS = oracle.ComposeTS(physical, logical)
	}
	receipt, err := nativepitr.CreateTask(ctx, pdc, nativepitr.EtcdMetadata{KV: etcd.KV}, nativepitr.TaskCreateInput{Preflight: p, PreflightSHA256: hex.EncodeToString(digest[:]), StartTS: o.startTS, EndTS: o.endTS, Storage: storage, OperationID: uuid.NewString()})
	if err != nil {
		return err
	}
	outBytes, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	outBytes = append(outBytes, '\n')
	_, err = out.Write(outBytes)
	return err
}
