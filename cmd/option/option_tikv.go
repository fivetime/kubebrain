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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"
	tikvcfg "github.com/tikv/client-go/v2/config"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kubewharf/kubebrain/pkg/backend/admissionfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
)

const defaultTiKVClientNum = 16

type storageConfig struct {
	pdAddrs                 []string
	clientNum               int
	experimental1PC         bool
	experimentalAsyncCommit bool

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
	return errors.Join(sessionErr, clientErr)
}

func (s *storageConfig) buildProcessAdmission(ctx context.Context, keyspace, identity string) (processAdmission, error) {
	tlsConfig, err := (storagetikv.Security{
		CAPath: s.caFile, CertPath: s.certFile, KeyPath: s.keyFile, VerifyCN: s.verifyCN,
	}).TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("build PD admission TLS: %w", err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: s.pdAddrs, DialTimeout: 5 * time.Second, TLS: tlsConfig})
	if err != nil {
		return nil, fmt.Errorf("connect PD admission metadata: %w", err)
	}
	session, err := admissionfence.StartSessionWithIdentityHandoff(ctx, client, keyspace, identity, 15*time.Second)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("register PD restore admission session: %w", err),
			client.Close(),
		)
	}
	return &tikvProcessAdmission{session: session, client: client}, nil
}

func newStorageConfig() *storageConfig {
	return &storageConfig{clientNum: defaultTiKVClientNum}
}

func (s *storageConfig) addFlag(fs *pflag.FlagSet) {
	fs.BoolVar(&s.experimental1PC, "experimental-tikv-enable-1pc", false, "Try TiKV 1PC with SDK 2PC fallback; dedicated test clusters only, disabled by default. Async commit remains disabled.")
	fs.BoolVar(&s.experimentalAsyncCommit, "experimental-tikv-enable-async-commit", false, "Try TiKV async commit with SDK 2PC fallback; dedicated test clusters only, disabled by default. Cannot be combined with experimental 1PC; restart required.")
	fs.StringSliceVar(&s.pdAddrs, "pd-addrs", s.pdAddrs, "addresses of TiKV PD servers")
	fs.IntVar(&s.clientNum, "tikv-client-num", s.clientNum, "number of round-robined TiKV txn clients; each has its own PD connections, region cache and TSO stream, so keep it modest")
	fs.StringVar(&s.caFile, "tikv-ca-file", s.caFile, "Path to the CA cert for TLS to TiKV/PD (empty = plaintext data plane).")
	fs.StringVar(&s.certFile, "tikv-cert-file", s.certFile, "Path to the client cert for mTLS to TiKV/PD.")
	fs.StringVar(&s.keyFile, "tikv-key-file", s.keyFile, "Path to the client key for mTLS to TiKV/PD.")
	fs.StringSliceVar(&s.verifyCN, "tikv-verify-cn", s.verifyCN, "Allowed CNs of the TiKV/PD server cert (empty = no CN restriction).")
}

func (s *storageConfig) validate() error {
	if err := s.validateCommitProtocol(); err != nil {
		return err
	}
	if len(s.pdAddrs) == 0 {
		return fmt.Errorf("invalid param --pd-addrs")
	}
	if s.clientNum <= 0 || s.clientNum > storagetikv.MaxClientNum {
		return fmt.Errorf("invalid param --tikv-client-num: must be between 1 and %d", storagetikv.MaxClientNum)
	}
	// TiKV/PD cluster TLS is mutual: require CA + cert + key together, so a partial
	// config does not silently fall back to plaintext or fail deep in the client.
	if s.caFile != "" || s.certFile != "" || s.keyFile != "" {
		if s.caFile == "" || s.certFile == "" || s.keyFile == "" {
			return fmt.Errorf("TiKV TLS requires all of --tikv-ca-file, --tikv-cert-file, --tikv-key-file")
		}
	}
	if len(s.verifyCN) != 0 {
		if s.caFile == "" {
			return fmt.Errorf("--tikv-verify-cn requires TiKV TLS to be configured")
		}
		for _, cn := range s.verifyCN {
			if strings.TrimSpace(cn) == "" {
				return fmt.Errorf("--tikv-verify-cn values must not be empty")
			}
		}
	}
	return nil
}

func (s *storageConfig) buildStorage(ctx context.Context) (storage.KvStorage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.configureCommitProtocol(); err != nil {
		return nil, err
	}
	return storagetikv.NewKvStorageWithContext(ctx, s.pdAddrs, s.clientNum, storagetikv.Security{
		CAPath:   s.caFile,
		CertPath: s.certFile,
		KeyPath:  s.keyFile,
		VerifyCN: s.verifyCN,
	})
}

// The SDK snapshots these process-wide settings when it creates a transaction.
// Apply once at process startup, before constructing any storage clients. This
// is not a runtime toggle; restoring the default requires restarting the process.
func (s *storageConfig) validateCommitProtocol() error {
	if s.experimental1PC && s.experimentalAsyncCommit {
		return fmt.Errorf("--experimental-tikv-enable-1pc and --experimental-tikv-enable-async-commit are mutually exclusive")
	}
	return nil
}

func (s *storageConfig) configureCommitProtocol() error {
	if err := s.validateCommitProtocol(); err != nil {
		return err
	}
	tikvcfg.UpdateGlobal(func(c *tikvcfg.Config) {
		c.Enable1PC = s.experimental1PC
		c.EnableAsyncCommit = s.experimentalAsyncCommit
	})
	return nil
}
