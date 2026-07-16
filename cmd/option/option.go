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

package option

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/endpoint"
	imetrics "github.com/kubewharf/kubebrain/pkg/metrics"
	metrics "github.com/kubewharf/kubebrain/pkg/metrics/prometheus"
	etcdserver "github.com/kubewharf/kubebrain/pkg/server/etcd"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	storagemetrics "github.com/kubewharf/kubebrain/pkg/storage/metrics"
	"github.com/kubewharf/kubebrain/pkg/util"
)

type KubeBrainOption struct {
	epsConf *endpoint.Config

	// key prefix
	Prefix string

	// skipped key prefix
	// if specified, compact will ignore keys with this prefix.
	// used when kube-brain cluster split by object type but share one underlying storage cluster

	// TODO validate key by prefix when read and write
	SkippedPrefixes []string

	ClusterName string

	// keyspace names this cluster's tenant on a shared storage cluster (#76).
	Keyspace string

	// advertiseHost, when non-empty, is the host advertised to peers as this
	// replica's identity (the leader-election holderIdentity and the address a
	// follower dials to reach the leader). Empty = auto-detect via util.GetHost,
	// which picks the lexicographically smallest private IPv4 and therefore
	// guesses wrong on multi-homed hosts — set this explicitly there. Listeners
	// still bind all interfaces (:port); this only affects the advertised address.
	advertiseHost  string
	initialCluster string

	storageConfig *storageConfig

	EnableStorageMetrics bool

	watchCacheSize    int
	watchFanoutBuffer int
	storageGCLifetime time.Duration

	enableCountIndex        bool
	countIndexMaxKeys       int
	autoCompactionRetention uint64
	historyScanRevBucket    uint64

	watchProgressNotifyInterval time.Duration
}

func NewOptions() *KubeBrainOption {
	return &KubeBrainOption{
		epsConf: &endpoint.Config{
			Port:                 2379,
			PeerPort:             2380,
			ClientSecurityConfig: &endpoint.SecurityConfig{},
			PeerSecurityConfig:   &endpoint.SecurityConfig{},
			InfoSecurityConfig:   &endpoint.SecurityConfig{},
			// Default true: this is what every kube-apiserver deployment needs
			// (etcd value/lease semantics, follower→leader write proxy, count
			// index eligibility). With it false, a multi-replica deployment whose
			// apiserver lists all endpoints has followers REJECT writes — and
			// clientv3 does not retry mutable RPCs on Unavailable, so ~2/3 of
			// writes fail while reads (retried) succeed. Opt out only for
			// non-etcd (native brain-client) consumers.
			EnableEtcdCompatibility: true,
			// Leader-election defaults (client-go). Overridable via flags below.
			LeaseDuration: 8 * time.Second,
			RenewDeadline: 5 * time.Second,
			RetryPeriod:   1 * time.Second,
			// Disabled by default for backward compatibility. Production DBaaS
			// manifests enable aging so TLS trust retirement has a finite bound.
			GRPCMaxConnectionAgeGrace: 5 * time.Minute,
		},
		// The namespace for KubeBrain-internal coordination keys (leader-election
		// lock, compact watermark) is a fixed constant, NOT configuration: it is
		// unrelated to client key prefixes (reads/writes, physical GC and the
		// count index always cover the whole keyspace), and its configurable
		// ancestor --key-prefix invited a P0 misconfiguration (pre-2026-07 builds
		// derived the physical-GC borders from it; a mismatch with the
		// apiserver's --etcd-prefix silently disabled GC).
		Prefix:               "/kubebrain-internal",
		ClusterName:          "default",
		storageConfig:        newStorageConfig(),
		watchCacheSize:       200 * 1000,
		watchFanoutBuffer:    10 * 1000,
		storageGCLifetime:    10 * time.Minute,
		countIndexMaxKeys:    5 * 1000 * 1000,
		historyScanRevBucket: 4096,

		watchProgressNotifyInterval: time.Second,
	}
}

// AddFlags adds flags to fs and binds them to options.
func (o *KubeBrainOption) AddFlags(fs *pflag.FlagSet) {
	// parse flags
	fs.IntVar(&o.epsConf.Port, "port", o.epsConf.Port, "the port kubebrain listen on for client")
	fs.IntVar(&o.epsConf.PeerPort, "peer-port", o.epsConf.PeerPort, "the port kubebrain listen on for peer communication")
	fs.IntVar(&o.epsConf.InfoPort, "info-port", o.epsConf.InfoPort, "the port kubebrain listen on for node info")
	fs.DurationVar(&o.epsConf.GRPCMaxConnectionAge, "grpc-max-connection-age", o.epsConf.GRPCMaxConnectionAge, "Maximum age of a client or peer gRPC connection before GOAWAY; 0 disables. Bounds how long pre-rotation TLS trust remains active.")
	fs.DurationVar(&o.epsConf.GRPCMaxConnectionAgeGrace, "grpc-max-connection-age-grace", o.epsConf.GRPCMaxConnectionAgeGrace, "Drain window after max connection age before active streams are closed. Must be positive when connection aging is enabled.")
	fs.StringSliceVar(&o.SkippedPrefixes, "skip-key-prefix", o.SkippedPrefixes, "skipped key prefix.")
	fs.StringVar(&o.ClusterName, "cluster-name", o.ClusterName, "cluster name; used ONLY as the metrics 'cluster' tag. For data isolation on a shared storage cluster use --keyspace")
	fs.StringVar(&o.Keyspace, "keyspace", o.Keyspace, "tenant keyspace on the shared storage cluster ([a-z0-9-], max 64). Every key family (objects, event log, internal metadata, coordination keys) is derived from it, so clusters with different keyspaces on one TiKV cannot see or garbage-collect each other's data. Empty (default) = the original single-tenant keyspace; existing deployments keep their data. All replicas of one cluster MUST agree")
	fs.StringVar(&o.advertiseHost, "advertise-host", o.advertiseHost, "IP/host advertised to peers as this replica's identity: the leader-election holderIdentity and the address followers dial to reach the leader. Empty = auto-detect (smallest private IPv4), which guesses wrong on multi-homed hosts — REQUIRED there. Listeners still bind all interfaces; this only sets the advertised address. IPv6 must be bracketed, e.g. [2001:db8::1].")
	fs.StringVar(&o.initialCluster, "initial-cluster", o.initialCluster, "Static KubeBrain service membership exposed by etcd MemberList, in etcd's name=http[s]://host:peerPort comma-separated form. DBaaS deployments should set every replica identically so clientv3 AutoSync retains all endpoints.")

	// security
	fs.StringVar(&o.epsConf.ClientSecurityConfig.CertFile, "cert-file",
		o.epsConf.ClientSecurityConfig.CertFile, "Path to the client server ClientTLS cert file.")
	fs.StringVar(&o.epsConf.ClientSecurityConfig.KeyFile, "key-file",
		o.epsConf.ClientSecurityConfig.KeyFile, "Path to the client server ClientTLS key file.")
	fs.StringVar(&o.epsConf.ClientSecurityConfig.CA, "trusted-ca-file",
		o.epsConf.ClientSecurityConfig.CA, "Path to the client server ClientTLS trusted CA cert file.")
	fs.StringVar(&o.epsConf.ClientSecurityConfig.ServerName, "tls-server-name",
		o.epsConf.ClientSecurityConfig.ServerName, "Server name used by client TLS verification.")
	fs.BoolVar(&o.epsConf.ClientSecurityConfig.ClientAuth, "client-cert-auth",
		o.epsConf.ClientSecurityConfig.ClientAuth, "Enable client cert authentication.")
	fs.BoolVar(&o.epsConf.ClientSecurityConfig.AllowInsecure, "allow-insecure",
		o.epsConf.ClientSecurityConfig.AllowInsecure, "Allow insecure access even if client TLS config is set.")
	fs.StringVar(&o.epsConf.PeerSecurityConfig.CertFile, "peer-cert-file",
		o.epsConf.PeerSecurityConfig.CertFile, "Path to the peer server ClientTLS cert file.")
	fs.StringVar(&o.epsConf.PeerSecurityConfig.KeyFile, "peer-key-file",
		o.epsConf.PeerSecurityConfig.KeyFile, "Path to the peer server ClientTLS key file.")
	fs.StringVar(&o.epsConf.PeerSecurityConfig.CA, "peer-trusted-ca-file",
		o.epsConf.PeerSecurityConfig.CA, "Path to the peer server ClientTLS trusted CA cert file.")
	fs.StringVar(&o.epsConf.PeerSecurityConfig.ServerName, "peer-tls-server-name",
		o.epsConf.PeerSecurityConfig.ServerName, "Server name used by peer client TLS verification.")
	fs.BoolVar(&o.epsConf.PeerSecurityConfig.ClientAuth, "peer-client-cert-auth",
		o.epsConf.PeerSecurityConfig.ClientAuth, "Enable client cert authentication.")
	fs.BoolVar(&o.epsConf.PeerSecurityConfig.AllowInsecure, "peer-allow-insecure",
		o.epsConf.PeerSecurityConfig.AllowInsecure, "Allow insecure access even if peer TLS config is set.")
	fs.BoolVar(&o.epsConf.EnableEtcdCompatibility, "compatible-with-etcd",
		o.epsConf.EnableEtcdCompatibility, "Enable full compatibility with usage of etcd3 in kube-apiserver (etcd value/lease semantics + follower write proxy). Default true; set false only for non-etcd brain-client consumers — with it false, followers reject etcd writes and multi-endpoint apiservers lose ~2/3 of writes")

	// info/metrics port TLS (#32): metrics and pprof are served on the info port,
	// not the client data port; these let the info port serve TLS instead of plaintext.
	fs.StringVar(&o.epsConf.InfoSecurityConfig.CertFile, "info-cert-file",
		o.epsConf.InfoSecurityConfig.CertFile, "Path to the info/metrics server TLS cert file (empty = plaintext).")
	fs.StringVar(&o.epsConf.InfoSecurityConfig.KeyFile, "info-key-file",
		o.epsConf.InfoSecurityConfig.KeyFile, "Path to the info/metrics server TLS key file.")
	fs.StringVar(&o.epsConf.InfoSecurityConfig.CA, "info-trusted-ca-file",
		o.epsConf.InfoSecurityConfig.CA, "Path to the info/metrics server trusted CA cert file.")
	fs.BoolVar(&o.epsConf.InfoSecurityConfig.ClientAuth, "info-client-cert-auth",
		o.epsConf.InfoSecurityConfig.ClientAuth, "Require client cert authentication on the info/metrics port.")
	fs.BoolVar(&o.epsConf.EnablePprof, "enable-pprof",
		o.epsConf.EnablePprof, "Expose net/http/pprof debug handlers on the info port (off by default; never on the client port).")

	fs.BoolVar(&o.EnableStorageMetrics, "enable-storage-metrics", o.EnableStorageMetrics, "enable storage metrics.")
	o.storageConfig.addFlag(fs)
	fs.IntVar(&o.watchCacheSize, "watch-cache-size", o.watchCacheSize, "size of global watch cache")
	fs.IntVar(&o.watchFanoutBuffer, "watch-fanout-buffer", o.watchFanoutBuffer, "per-watcher fan-out channel buffer in event batches; a watcher overrunning it is replayed from the watch cache ring instead of dropped")
	fs.DurationVar(&o.storageGCLifetime, "storage-gc-lifetime", o.storageGCLifetime, "MVCC history retention when the leader advances the storage engine GC safepoint (TiKV; the gc_worker role on bare PD+TiKV). 0 disables — required if nothing else (e.g. a TiDB instance) drives GC, or reads degrade as versions accumulate")
	fs.BoolVar(&o.enableCountIndex, "enable-count-index", o.enableCountIndex, "maintain an in-memory versioned key index on the leader for exact O(range) counts (approach A-index; requires --compatible-with-etcd)")
	fs.IntVar(&o.countIndexMaxKeys, "count-index-max-keys", o.countIndexMaxKeys, "cap on keys tracked by the count index; above it the index disables and counts fall back to a scan (0 = unlimited)")
	fs.Uint64Var(&o.autoCompactionRetention, "auto-compaction-retention-revisions", o.autoCompactionRetention, "SAFETY NET: if >0, the leader caps MVCC history to the last N revisions should the apiserver's own compaction stop (KubeBrain never auto-compacts otherwise). 0 = off. Set generously large so it only bites when the primary compactor is far behind.")
	fs.Uint64Var(&o.historyScanRevBucket, "watch-history-scan-rev-bucket", o.historyScanRevBucket, "reconnect herd (#30): watchers reconnecting to the same prefix within this many revisions share one watch-history storage scan (singleflight). Larger = more sharing across HA-apiserver replicas whose reconnect revisions are spread apart, at the cost of a shared scan window up to one bucket wider than requested. 1 = share only exact-revision reconnects. 0 = default.")
	fs.DurationVar(&o.watchProgressNotifyInterval, "watch-progress-notify-interval", o.watchProgressNotifyInterval, "how often watch progress notifications advance/emit (drives kube-apiserver ConsistentListFromCache convergence; smaller = fresher at more marker traffic). Must be < 2.5s: the apiserver blocks consistent reads on progress for only 3s before falling back to a full LIST against storage")
	fs.DurationVar(&o.epsConf.LeaseDuration, "leader-lease-duration", o.epsConf.LeaseDuration, "leader-election lease duration: how long a dead leader's lease is held before a successor can acquire it (dominates the failover leaderless window). Smaller = faster failover but more spurious failovers under load. Must satisfy retry < renew < lease.")
	fs.DurationVar(&o.epsConf.RenewDeadline, "leader-renew-deadline", o.epsConf.RenewDeadline, "leader-election renew deadline; also the write-fence self-fencing bound (#39). Must be < leader-lease-duration.")
	fs.DurationVar(&o.epsConf.RetryPeriod, "leader-retry-period", o.epsConf.RetryPeriod, "leader-election retry period; how often leadership is renewed/acquired. Must be < leader-renew-deadline.")
}

// Validate checks the option before running
func (o *KubeBrainOption) Validate() error {
	err := o.epsConf.Validate()
	if err != nil {
		return err
	}

	if err := (leader.Config{
		LeaseDuration: o.epsConf.LeaseDuration,
		RenewDeadline: o.epsConf.RenewDeadline,
		RetryPeriod:   o.epsConf.RetryPeriod,
	}).Validate(); err != nil {
		return err
	}

	if o.advertiseHost != "" {
		// It is combined with the peer port into a host:port identity downstream
		// (SplitHostPort'd by the etcd proxy); a bare IPv6 without brackets would
		// parse wrong. Fail loudly here instead of dialing a mangled address.
		if _, _, err := net.SplitHostPort(fmt.Sprintf("%s:%d", o.advertiseHost, o.epsConf.PeerPort)); err != nil {
			return fmt.Errorf("--advertise-host %q is invalid (IPv6 must be bracketed, e.g. [2001:db8::1]): %w", o.advertiseHost, err)
		}
	}
	if o.initialCluster != "" {
		members, err := etcdserver.ParseInitialCluster(o.initialCluster, o.epsConf.Port, o.epsConf.ClientSecurityConfig.CertFile != "")
		if err != nil {
			return fmt.Errorf("--initial-cluster: %w", err)
		}
		identity, err := o.buildIdentity()
		if err != nil {
			return err
		}
		localID := uint64(crc32.ChecksumIEEE([]byte(identity)))
		found := false
		for _, member := range members {
			if member.ID == localID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("--initial-cluster does not contain this replica identity %q", identity)
		}
	}

	// kube-apiserver's ConsistentListFromCache (GA in 1.37) blocks a consistent
	// read on watch progress for a hard-coded 3s before falling back to a full
	// LIST against storage — at 10M+ keys that fallback is an O(all-keys) scan
	// per consistent read. A progress cadence at or above the block timeout would
	// make EVERY consistent read on a quiet resource take that cliff, so fail
	// loudly instead. (<=0 keeps the "use default 1s" semantic.)
	if _, err := coder.NewKeyspace(o.Keyspace); err != nil {
		return err
	}

	const watchProgressNotifyIntervalMax = 2500 * time.Millisecond
	if o.watchProgressNotifyInterval > watchProgressNotifyIntervalMax {
		return fmt.Errorf("--watch-progress-notify-interval %v is too large: must be < %v (kube-apiserver blocks consistent reads on progress for only 3s before falling back to a full storage LIST)",
			o.watchProgressNotifyInterval, watchProgressNotifyIntervalMax)
	}

	for _, skippedPrefix := range o.SkippedPrefixes {
		if !strings.HasPrefix(skippedPrefix, o.Prefix) {
			return fmt.Errorf("skipped prefix %s has no prefix %s", skippedPrefix, o.Prefix)
		}
		if strings.HasSuffix(skippedPrefix, "/") {
			return fmt.Errorf("skipped prefix %s is invalid, make sure it has no / suffix", skippedPrefix)
		}
	}

	return o.storageConfig.validate()
}

// buildIdentity computes this replica's advertised identity (host:peerPort).
// It prefers the explicit --advertise-host; otherwise it auto-detects via
// util.GetHost (the legacy behavior). The host half is what a follower dials to
// reach the leader, so on a multi-homed host --advertise-host must be set.
func (o *KubeBrainOption) buildIdentity() (string, error) {
	host := o.advertiseHost
	if host == "" {
		host = util.GetHost()
	}
	if len(host) == 0 {
		return "", fmt.Errorf("local ip is empty")
	}
	return fmt.Sprintf("%s:%d", host, o.epsConf.PeerPort), nil
}

// Run runs the storage engine
func (o *KubeBrainOption) Run(ctx context.Context) error {
	// add cluster metric tag
	metricsCli := metrics.NewMetrics(imetrics.Tag("cluster", o.ClusterName))

	identity, err := o.buildIdentity()
	if err != nil {
		return err
	}
	klog.InfoS("build identity", "identity", identity)
	members, err := etcdserver.ParseInitialCluster(o.initialCluster, o.epsConf.Port, o.epsConf.ClientSecurityConfig.CertFile != "")
	if err != nil {
		return err
	}
	o.epsConf.ClusterMembers = members

	kv, err := o.storageConfig.buildStorage()
	if err != nil {
		return err
	}

	// Coordination keys (election lock, compact watermark) live at raw keys
	// under Prefix; namespace them per keyspace so tenants on one storage
	// cluster elect independent leaders and keep independent watermarks.
	prefix := o.Prefix
	if o.Keyspace != "" {
		prefix = prefix + "/ks-" + o.Keyspace
	}
	config := backend.Config{
		Prefix:                      prefix,
		Keyspace:                    o.Keyspace,
		Identity:                    identity,
		SkippedPrefixes:             o.SkippedPrefixes,
		EnableEtcdCompatibility:     o.epsConf.EnableEtcdCompatibility,
		WatchCacheSize:              o.watchCacheSize,
		WatchFanoutBuffer:           o.watchFanoutBuffer,
		StorageGCLifetime:           o.storageGCLifetime,
		EnableCountIndex:            o.enableCountIndex,
		CountIndexMaxKeys:           o.countIndexMaxKeys,
		AutoCompactionRetention:     o.autoCompactionRetention,
		HistoryScanRevBucket:        o.historyScanRevBucket,
		WatchProgressNotifyInterval: o.watchProgressNotifyInterval,
	}

	if o.EnableStorageMetrics {
		kv = storagemetrics.NewKvStorage(kv, metricsCli)
	}

	b := backend.NewBackend(kv, config, metricsCli)
	return endpoint.NewEndpoint(b, metricsCli, o.epsConf).Run(ctx)
}
