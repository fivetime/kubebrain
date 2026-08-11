// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !badger
// +build !badger

package option

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/tikv/pd/client/tlsutil"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kubewharf/kubebrain/pkg/backend/admissionfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
)

const defaultTiKVClientNum = 16

type storageConfig struct {
	pdAddrs   []string
	clientNum int

	// mTLS for the KubeBrain->TiKV/PD data plane (#33). Empty = plaintext.
	caFile   string
	certFile string
	keyFile  string
	verifyCN []string
}

type tikvProcessAdmission struct {
	session *admissionfence.Session
	client  *clientv3.Client
}

func (a *tikvProcessAdmission) Fresh() bool { return a.session.Fresh() }
func (a *tikvProcessAdmission) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sessionErr := a.session.Close(ctx)
	clientErr := a.client.Close()
	if sessionErr != nil {
		return sessionErr
	}
	return clientErr
}

func (s *storageConfig) buildProcessAdmission(ctx context.Context, keyspace, identity string) (processAdmission, error) {
	tlsConfig, err := (tlsutil.TLSConfig{CAPath: s.caFile, CertPath: s.certFile, KeyPath: s.keyFile, CertAllowedCN: s.verifyCN}).ToTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("build PD admission TLS: %w", err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: s.pdAddrs, DialTimeout: 5 * time.Second, TLS: tlsConfig})
	if err != nil {
		return nil, fmt.Errorf("connect PD admission metadata: %w", err)
	}
	session, err := admissionfence.StartSession(ctx, client, keyspace, identity, 15*time.Second)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("register PD restore admission session: %w", err)
	}
	return &tikvProcessAdmission{session: session, client: client}, nil
}

func newStorageConfig() *storageConfig {
	return &storageConfig{clientNum: defaultTiKVClientNum}
}

func (s *storageConfig) addFlag(fs *pflag.FlagSet) {
	fs.StringSliceVar(&s.pdAddrs, "pd-addrs", s.pdAddrs, "addresses of TiKV PD servers")
	fs.IntVar(&s.clientNum, "tikv-client-num", s.clientNum, "number of round-robined TiKV txn clients; each has its own PD connections, region cache and TSO stream, so keep it modest")
	fs.StringVar(&s.caFile, "tikv-ca-file", s.caFile, "Path to the CA cert for TLS to TiKV/PD (empty = plaintext data plane).")
	fs.StringVar(&s.certFile, "tikv-cert-file", s.certFile, "Path to the client cert for mTLS to TiKV/PD.")
	fs.StringVar(&s.keyFile, "tikv-key-file", s.keyFile, "Path to the client key for mTLS to TiKV/PD.")
	fs.StringSliceVar(&s.verifyCN, "tikv-verify-cn", s.verifyCN, "Allowed CNs of the TiKV/PD server cert (empty = no CN restriction).")
}

func (s *storageConfig) validate() error {
	if len(s.pdAddrs) == 0 {
		return fmt.Errorf("invalid param --pd-addrs")
	}
	if s.clientNum <= 0 {
		return fmt.Errorf("invalid param --tikv-client-num: must be > 0")
	}
	// TiKV/PD cluster TLS is mutual: require CA + cert + key together, so a partial
	// config does not silently fall back to plaintext or fail deep in the client.
	if s.caFile != "" || s.certFile != "" || s.keyFile != "" {
		if s.caFile == "" || s.certFile == "" || s.keyFile == "" {
			return fmt.Errorf("TiKV TLS requires all of --tikv-ca-file, --tikv-cert-file, --tikv-key-file")
		}
	}
	return nil
}

func (s *storageConfig) buildStorage() (storage.KvStorage, error) {
	return storagetikv.NewKvStorage(s.pdAddrs, s.clientNum, storagetikv.Security{
		CAPath:   s.caFile,
		CertPath: s.certFile,
		KeyPath:  s.keyFile,
		VerifyCN: s.verifyCN,
	})
}
