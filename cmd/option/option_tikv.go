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
	"fmt"

	"github.com/spf13/pflag"

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

func (s *storageConfig) buildStorage(keyspace string) (storage.KvStorage, error) {
	// keyspace scopes the PD GC service safepoint so co-tenants on a shared
	// PD/TiKV cluster do not GC each other's MVCC history (#76).
	return storagetikv.NewKvStorage(s.pdAddrs, s.clientNum, storagetikv.Security{
		CAPath:   s.caFile,
		CertPath: s.certFile,
		KeyPath:  s.keyFile,
		VerifyCN: s.verifyCN,
	}, storagetikv.WithKeyspace(keyspace))
}
