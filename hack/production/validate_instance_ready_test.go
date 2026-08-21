package production_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const fakeInitialCluster = "kb-0=peer-0,kb-1=peer-1,kb-2=peer-2"
const productionCipherSuites = "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"

func fakeKubeBrainArgs(advertisedURLs, initialCluster string) string {
	return "--port=3379\n" +
		"--peer-port=3380\n" +
		"--info-port=8080\n" +
		"--keyspace=instance-a\n" +
		"--pd-addrs=kb-pd.storage.svc:2379\n" +
		"--tikv-client-num=16\n" +
		"--quota-backend-bytes=429496729600\n" +
		"--leader-lease-duration=30s\n" +
		"--leader-renew-deadline=25s\n" +
		"--leader-retry-period=500ms\n" +
		"--advertise-client-urls=" + advertisedURLs + "\n" +
		"--initial-cluster=" + initialCluster + "\n" +
		"--compatible-with-etcd=true\n" +
		"--enable-count-index=true\n" +
		"--count-index-max-keys=5000000\n" +
		"--enable-storage-metrics=true\n" +
		"--enable-grpc-gateway=true\n" +
		"--allow-insecure=false\n" +
		"--peer-allow-insecure=false\n" +
		"--enable-pprof=false\n" +
		"--max-txn-ops=128\n" +
		"--max-request-bytes=1572864\n" +
		"--max-concurrent-streams=4294967295\n" +
		"--max-requests-inflight=1024\n" +
		"--max-request-rate=2000\n" +
		"--request-rate-burst=4000\n" +
		"--max-delete-range-keys=1024\n" +
		"--max-watches=10000\n" +
		"--grpc-keepalive-min-time=5s\n" +
		"--grpc-keepalive-interval=2h\n" +
		"--grpc-keepalive-timeout=20s\n" +
		"--auth-token=simple\n" +
		"--bcrypt-cost=10\n" +
		"--auth-token-ttl=300\n" +
		"--v=2"
}

func fakeTLSKubeBrainArgs(advertisedURLs, initialCluster string) string {
	return fakeKubeBrainArgs(advertisedURLs, initialCluster) + "\n" +
		"--grpc-max-connection-age=1h\n" +
		"--grpc-max-connection-age-grace=5m\n" +
		"--tls-min-version=TLS1.2\n" +
		"--tls-max-version=TLS1.3\n" +
		"--cipher-suites=" + productionCipherSuites + "\n" +
		"--cors=https://instance.example:2379\n" +
		"--cert-file=/etc/kubebrain/client-tls/tls.crt\n" +
		"--key-file=/etc/kubebrain/client-tls/tls.key\n" +
		"--trusted-ca-file=/etc/kubebrain/client-tls/ca.crt\n" +
		"--tls-server-name=kubebrain-client.kubebrain-system.svc\n" +
		"--client-cert-auth=true\n" +
		"--peer-cert-file=/etc/kubebrain/peer-tls/tls.crt\n" +
		"--peer-key-file=/etc/kubebrain/peer-tls/tls.key\n" +
		"--peer-trusted-ca-file=/etc/kubebrain/peer-tls/ca.crt\n" +
		"--peer-tls-server-name=kubebrain-peer.kubebrain-system.svc.cluster.local\n" +
		"--peer-client-cert-auth=true"
}

func expectedTLSGateEnv() []string {
	return []string{
		"EXPECTED_GRPC_MAX_CONNECTION_AGE=1h",
		"EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE=5m",
		"EXPECTED_TLS_MIN_VERSION=TLS1.2",
		"EXPECTED_TLS_MAX_VERSION=TLS1.3",
		"EXPECTED_CIPHER_SUITES=" + productionCipherSuites,
		"EXPECTED_CORS=https://instance.example:2379",
		"EXPECTED_CERT_FILE=/etc/kubebrain/client-tls/tls.crt",
		"EXPECTED_KEY_FILE=/etc/kubebrain/client-tls/tls.key",
		"EXPECTED_TRUSTED_CA_FILE=/etc/kubebrain/client-tls/ca.crt",
		"EXPECTED_TLS_SERVER_NAME=kubebrain-client.kubebrain-system.svc",
		"EXPECTED_CLIENT_CERT_AUTH=true",
		"EXPECTED_PEER_CERT_FILE=/etc/kubebrain/peer-tls/tls.crt",
		"EXPECTED_PEER_KEY_FILE=/etc/kubebrain/peer-tls/tls.key",
		"EXPECTED_PEER_TRUSTED_CA_FILE=/etc/kubebrain/peer-tls/ca.crt",
		"EXPECTED_PEER_TLS_SERVER_NAME=kubebrain-peer.kubebrain-system.svc.cluster.local",
		"EXPECTED_PEER_CLIENT_CERT_AUTH=true",
	}
}

func TestValidateInstanceReady(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		image                    string
		kubeStatus               string
		finalKubeStatus          string
		kubeArgs                 string
		topology                 string
		tidbVersion              string
		pdImage                  string
		tikvImage                string
		tikvImageID              string
		tikvPodPrefix            string
		tikvOwnerUID             string
		tikvTidbOwnerUID         string
		tikvMaxKeySize           string
		tikvCurrentRevision      string
		pdSTSJSON                string
		finalTikvSTSJSON         string
		tidbSnapshotReady        *bool
		healthOK                 bool
		advertisedURLs           string
		unreachableAdvertisedURL string
		memberListJSON           string
		podsJSON                 string
		endpointSlicesJSON       string
		finalEndpointSlicesJSON  string
		serviceJSON              string
		initialCluster           string
		extraEnv                 []string
		etcdctlExecPod           string
		wantExec                 bool
		wantClientService        string
		wantNoKubectl            bool
		wantKubectl              bool
		wantOK                   bool
		wantOutput               string
	}{
		{
			name:          "watch progress interval is negative",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL=-1s"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL must be empty, 0, or a positive integer ms/s duration below 2.5s",
		},
		{
			name:          "watch progress interval reaches safety cap",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL=2500ms"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL must be empty, 0, or a positive integer ms/s duration below 2.5s",
		},
		{
			name:        "default watch progress interval reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL=0"},
			wantKubectl: true,
			wantOutput:  "watch progress notify interval configuration mismatch",
		},
		{
			name:        "maximum production watch progress interval reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL=2499ms"},
			wantKubectl: true,
			wantOutput:  "watch progress notify interval configuration mismatch",
		},
		{
			name:          "storage GC lifetime is negative",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_STORAGE_GC_LIFETIME=-1s"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_STORAGE_GC_LIFETIME must be empty, 0, or a positive production Go duration",
		},
		{
			name:          "storage GC lifetime overflows Go duration",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_STORAGE_GC_LIFETIME=2562048h"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_STORAGE_GC_LIFETIME must be empty, 0, or a positive production Go duration",
		},
		{
			name:        "disabled storage GC lifetime reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_STORAGE_GC_LIFETIME=0"},
			wantKubectl: true,
			wantOutput:  "storage GC lifetime configuration mismatch",
		},
		{
			name:        "maximum whole-hour storage GC lifetime reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_STORAGE_GC_LIFETIME=2562047h"},
			wantKubectl: true,
			wantOutput:  "storage GC lifetime configuration mismatch",
		},
		{
			name:          "auto compaction retention is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS must be empty or a canonical non-negative uint64",
		},
		{
			name:          "auto compaction retention overflows uint64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS=18446744073709551616"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS must be empty or a canonical non-negative uint64",
		},
		{
			name:          "watch history scan bucket overflows uint64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET=18446744073709551616"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET must be empty or a canonical non-negative uint64",
		},
		{
			name:        "maximum uint64 revision controls reach Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS=18446744073709551615", "EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET=18446744073709551615"},
			wantKubectl: true,
			wantOutput:  "auto-compaction retention configuration mismatch",
		},
		{
			name:          "watch cache size is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_WATCH_CACHE_SIZE=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_WATCH_CACHE_SIZE must be empty or a canonical non-negative Go int",
		},
		{
			name:          "watch cache size overflows Go int",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_WATCH_CACHE_SIZE=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_WATCH_CACHE_SIZE must be empty or a canonical non-negative Go int",
		},
		{
			name:          "watch fanout buffer overflows Go int",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_WATCH_FANOUT_BUFFER=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_WATCH_FANOUT_BUFFER must be empty or a canonical non-negative Go int",
		},
		{
			name:        "maximum Go int watch buffers reach Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_WATCH_CACHE_SIZE=9223372036854775807", "EXPECTED_WATCH_FANOUT_BUFFER=9223372036854775807"},
			wantKubectl: true,
			wantOutput:  "watch cache size configuration mismatch",
		},
		{
			name:          "gRPC max connection age is malformed",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_GRPC_MAX_CONNECTION_AGE=-1s"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_GRPC_MAX_CONNECTION_AGE must be empty, 0, or a positive production Go duration",
		},
		{
			name:          "gRPC max connection age grace overflows Go duration",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE=2562048h"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE must be empty, 0, or a positive production Go duration",
		},
		{
			name:          "gRPC connection aging lacks positive grace",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_GRPC_MAX_CONNECTION_AGE=1h", "EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE=0"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE must be positive when connection aging is enabled",
		},
		{
			name:        "disabled gRPC connection aging reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_GRPC_MAX_CONNECTION_AGE=0", "EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE=0"},
			wantKubectl: true,
			wantOutput:  "gRPC max connection age configuration mismatch",
		},
		{
			name:        "maximum whole-hour gRPC connection aging reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_GRPC_MAX_CONNECTION_AGE=2562047h", "EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE=2562047h"},
			wantKubectl: true,
			wantOutput:  "gRPC max connection age configuration mismatch",
		},
		{
			name:          "auth token provider is unsupported",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_AUTH_TOKEN=bearer"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_AUTH_TOKEN must be a supported simple or structurally valid jwt provider",
		},
		{
			name:          "auth token option is malformed",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_AUTH_TOKEN=simple,foo"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_AUTH_TOKEN must be a supported simple or structurally valid jwt provider",
		},
		{
			name:          "auth token option is duplicated",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_AUTH_TOKEN=simple,foo=bar,foo=baz"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_AUTH_TOKEN must be a supported simple or structurally valid jwt provider",
		},
		{
			name:          "JWT auth token provider lacks key option",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_AUTH_TOKEN=jwt,sign-method=RS256"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_AUTH_TOKEN must be a supported simple or structurally valid jwt provider",
		},
		{
			name:        "structurally valid JWT auth provider reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_AUTH_TOKEN=jwt,sign-method=RS256,pub-key=/etc/kubebrain/auth/public.pem"},
			wantKubectl: true,
			wantOutput:  "auth token provider configuration mismatch",
		},
		{
			name:          "gRPC keepalive min time is malformed",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_GRPC_KEEPALIVE_MIN_TIME=-1s"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_GRPC_KEEPALIVE_MIN_TIME must be 0 or a positive production Go duration",
		},
		{
			name:          "gRPC keepalive interval overflows Go duration",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_GRPC_KEEPALIVE_INTERVAL=2562048h"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_GRPC_KEEPALIVE_INTERVAL must be 0 or a positive production Go duration",
		},
		{
			name:          "gRPC keepalive timeout is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_GRPC_KEEPALIVE_TIMEOUT=020s"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_GRPC_KEEPALIVE_TIMEOUT must be 0 or a positive production Go duration",
		},
		{
			name:        "disabled gRPC keepalive durations reach Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_GRPC_KEEPALIVE_MIN_TIME=0", "EXPECTED_GRPC_KEEPALIVE_INTERVAL=0", "EXPECTED_GRPC_KEEPALIVE_TIMEOUT=0"},
			wantKubectl: true,
			wantOutput:  "gRPC keepalive min time configuration mismatch",
		},
		{
			name:        "maximum whole-hour gRPC keepalive interval reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_GRPC_KEEPALIVE_INTERVAL=2562047h"},
			wantKubectl: true,
			wantOutput:  "gRPC keepalive interval configuration mismatch",
		},
		{
			name:          "leader lease duration is malformed",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_LEADER_LEASE_DURATION=forever"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_LEADER_LEASE_DURATION must be a positive production Go duration",
		},
		{
			name:          "leader renew deadline overflows Go duration",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_LEADER_RENEW_DEADLINE=2562048h"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_LEADER_RENEW_DEADLINE must be a positive production Go duration",
		},
		{
			name:          "leader retry period is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_LEADER_RETRY_PERIOD=0500ms"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_LEADER_RETRY_PERIOD must be a positive production Go duration",
		},
		{
			name:        "maximum whole-hour leader lease reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_LEADER_LEASE_DURATION=2562047h"},
			wantKubectl: true,
			wantOutput:  "leader election lease duration configuration mismatch",
		},
		{
			name:          "log verbosity is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_LOG_VERBOSITY=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_LOG_VERBOSITY must be a canonical non-negative klog int32 level",
		},
		{
			name:          "log verbosity overflows int32",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_LOG_VERBOSITY=2147483648"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_LOG_VERBOSITY must be a canonical non-negative klog int32 level",
		},
		{
			name:        "maximum int32 log verbosity reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_LOG_VERBOSITY=2147483647"},
			wantKubectl: true,
			wantOutput:  "log verbosity configuration mismatch",
		},
		{
			name:          "backend quota is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_QUOTA_BACKEND_BYTES=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_QUOTA_BACKEND_BYTES is required and must be a canonical positive int64",
		},
		{
			name:          "backend quota overflows int64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_QUOTA_BACKEND_BYTES=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_QUOTA_BACKEND_BYTES is required and must be a canonical positive int64",
		},
		{
			name:        "maximum int64 backend quota reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_QUOTA_BACKEND_BYTES=9223372036854775807"},
			wantKubectl: true,
			wantOutput:  "KubeBrain quota configuration mismatch",
		},
		{
			name:          "cluster ID is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CLUSTER_ID=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_CLUSTER_ID is required and must be a canonical positive uint64",
		},
		{
			name:          "cluster ID overflows uint64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CLUSTER_ID=18446744073709551616"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_CLUSTER_ID is required and must be a canonical positive uint64",
		},
		{
			name:        "maximum uint64 cluster ID reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_CLUSTER_ID=18446744073709551615"},
			wantKubectl: true,
			wantOutput:  "TidbCluster storage identity mismatch",
		},
		{
			name:          "bcrypt cost is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_BCRYPT_COST=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_BCRYPT_COST must be a canonical non-negative Go uint",
		},
		{
			name:          "bcrypt cost overflows uint64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_BCRYPT_COST=18446744073709551616"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_BCRYPT_COST must be a canonical non-negative Go uint",
		},
		{
			name:          "auth token TTL overflows uint64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_AUTH_TOKEN_TTL=18446744073709551616"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_AUTH_TOKEN_TTL must be a canonical non-negative Go uint",
		},
		{
			name:        "maximum Go uint auth controls reach Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_BCRYPT_COST=18446744073709551615", "EXPECTED_AUTH_TOKEN_TTL=18446744073709551615"},
			wantKubectl: true,
			wantOutput:  "bcrypt cost",
		},
		{
			name:          "client TLS cert expectation lacks key",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CERT_FILE=/tls/tls.crt"},
			wantNoKubectl: true,
			wantOutput:    "client TLS cert and key expectations must both be present or both be empty",
		},
		{
			name:          "peer TLS server name lacks key pair",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_PEER_TLS_SERVER_NAME=peer.example"},
			wantNoKubectl: true,
			wantOutput:    "peer TLS CA, server name, or client auth expectation requires both cert and key",
		},
		{
			name:          "client certificate auth lacks trusted CA",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CERT_FILE=/tls/tls.crt", "EXPECTED_KEY_FILE=/tls/tls.key", "EXPECTED_CLIENT_CERT_AUTH=true"},
			wantNoKubectl: true,
			wantOutput:    "client TLS client certificate auth requires a trusted CA",
		},
		{
			name:          "peer insecure access is not boolean",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_PEER_ALLOW_INSECURE=0"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_PEER_ALLOW_INSECURE must be true or false",
		},
		{
			name:        "client insecure access enabled reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_ALLOW_INSECURE=true"},
			wantKubectl: true,
			wantOutput:  "client insecure access",
		},
		{
			name:        "pprof enabled reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_ENABLE_PPROF=true"},
			wantKubectl: true,
			wantOutput:  "pprof enablement",
		},
		{
			name:          "CORS allowlist cannot use wildcard",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CORS=*"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_CORS must be empty or a unique comma-separated HTTP(S) origin allowlist",
		},
		{
			name:          "CORS allowlist cannot include a path",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CORS=https://instance.example/v3"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_CORS must be empty or a unique comma-separated HTTP(S) origin allowlist",
		},
		{
			name:          "Host allowlist cannot use wildcard",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_HOST_WHITELIST=*"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_HOST_WHITELIST must be empty or a unique comma-separated hostname/IP allowlist",
		},
		{
			name:        "plaintext Host allowlist reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_HOST_WHITELIST=kubebrain-client.kubebrain-system.svc"},
			wantKubectl: true,
			wantOutput:  "HTTP Host allowlist",
		},
		{
			name:          "gRPC gateway enablement is not boolean",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_ENABLE_GRPC_GATEWAY=1"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_ENABLE_GRPC_GATEWAY must be true or false",
		},
		{
			name:        "disabled gRPC gateway reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_ENABLE_GRPC_GATEWAY=false"},
			wantKubectl: true,
			wantOutput:  "gRPC gateway enablement",
		},
		{
			name:          "TLS cipher suite is unsupported",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CIPHER_SUITES=TLS_RSA_WITH_AES_128_CBC_SHA"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_CIPHER_SUITES must be empty or a unique comma-separated list",
		},
		{
			name:          "TLS cipher suites contain a duplicate",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_CIPHER_SUITES=TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_CIPHER_SUITES must be empty or a unique comma-separated list",
		},
		{
			name:          "TLS 1.3 only window cannot configure cipher suites",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_TLS_MIN_VERSION=TLS1.3", "EXPECTED_TLS_MAX_VERSION=TLS1.3", "EXPECTED_CIPHER_SUITES=TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_CIPHER_SUITES must be empty when only TLS1.3 is enabled",
		},
		{
			name:          "TLS maximum version is unsupported",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_TLS_MAX_VERSION=TLS1.4"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_TLS_MAX_VERSION must be empty, TLS1.2, or TLS1.3",
		},
		{
			name:          "TLS version window is inverted",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_TLS_MIN_VERSION=TLS1.3", "EXPECTED_TLS_MAX_VERSION=TLS1.2"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_TLS_MIN_VERSION must not exceed EXPECTED_TLS_MAX_VERSION",
		},
		{
			name:          "TiKV client pool size is not positive",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_TIKV_CLIENT_NUM=0"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_TIKV_CLIENT_NUM must be a canonical integer between 1 and 128",
		},
		{
			name:          "TiKV client pool size exceeds resource bound",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_TIKV_CLIENT_NUM=129"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_TIKV_CLIENT_NUM must be a canonical integer between 1 and 128",
		},
		{
			name:        "maximum safe TiKV client pool size reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_TIKV_CLIENT_NUM=128"},
			wantKubectl: true,
			wantOutput:  "TiKV client pool size",
		},
		{
			name:          "count index cap is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_COUNT_INDEX_MAX_KEYS=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_COUNT_INDEX_MAX_KEYS must be a canonical non-negative Go int",
		},
		{
			name:          "count index cap overflows int64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_COUNT_INDEX_MAX_KEYS=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_COUNT_INDEX_MAX_KEYS must be a canonical non-negative Go int",
		},
		{
			name:          "max transaction operations exceed Go platform limit",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_TXN_OPS=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_TXN_OPS must be a canonical non-negative Go int",
		},
		{
			name:        "maximum Go int controls reach Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_COUNT_INDEX_MAX_KEYS=9223372036854775807", "EXPECTED_MAX_TXN_OPS=9223372036854775807"},
			wantKubectl: true,
			wantOutput:  "count index key cap",
		},
		{
			name:          "concurrent streams exceed uint32",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_CONCURRENT_STREAMS=4294967296"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_CONCURRENT_STREAMS must be a canonical non-negative uint32",
		},
		{
			name:          "inflight limit is not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_REQUESTS_INFLIGHT=01"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_REQUESTS_INFLIGHT must be a canonical non-negative uint32",
		},
		{
			name:          "delete range limit overflows int64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_DELETE_RANGE_KEYS=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_DELETE_RANGE_KEYS must be a canonical non-negative uint32",
		},
		{
			name:          "watch limit exceeds uint32",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_WATCHES=4294967296"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_WATCHES must be a canonical non-negative uint32",
		},
		{
			name:          "request rate enabled without burst",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_REQUEST_RATE=1", "EXPECTED_REQUEST_RATE_BURST=0"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_REQUEST_RATE and EXPECTED_REQUEST_RATE_BURST must both be zero or both be positive",
		},
		{
			name:          "request burst enabled without rate",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_REQUEST_RATE=0", "EXPECTED_REQUEST_RATE_BURST=1"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_REQUEST_RATE and EXPECTED_REQUEST_RATE_BURST must both be zero or both be positive",
		},
		{
			name:          "client port is zero",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_PORT=0"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_PORT must be a canonical TCP port between 1 and 65535",
		},
		{
			name:          "peer port exceeds TCP range",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_PEER_PORT=65536"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_PEER_PORT must be a canonical TCP port between 1 and 65535",
		},
		{
			name:          "info port exceeds TCP range",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_INFO_PORT=65536"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_INFO_PORT must be a canonical TCP port between 0 and 65535",
		},
		{
			name:          "client and peer ports collide",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_PORT=3380"},
			wantNoKubectl: true,
			wantOutput:    "expected client, peer, and enabled info ports must be distinct",
		},
		{
			name:        "maximum client port reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_PORT=65535"},
			wantKubectl: true,
			wantOutput:  "client Service release mismatch",
		},
		{
			name:        "disabled info port reaches Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_INFO_PORT=0"},
			wantKubectl: true,
			wantOutput:  "info listener port",
		},
		{
			name:          "max request bytes are not numeric",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_REQUEST_BYTES=invalid"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_REQUEST_BYTES must be a canonical non-negative Go int",
		},
		{
			name:          "max request bytes exceed Go platform limit",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_REQUEST_BYTES=9223372036854251520"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_REQUEST_BYTES must be a canonical non-negative Go int",
		},
		{
			name:          "max request bytes overflow int64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_MAX_REQUEST_BYTES=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_MAX_REQUEST_BYTES must be a canonical non-negative Go int",
		},
		{
			name:          "TiKV max key size overflows int64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_TIKV_MAX_KEY_SIZE=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_TIKV_MAX_KEY_SIZE must cover max-request-bytes plus physical-key overhead",
		},
		{
			name:           "maximum Go request bytes reach Kubernetes",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			tikvMaxKeySize: "9223372036854251583",
			extraEnv:       []string{"EXPECTED_MAX_REQUEST_BYTES=9223372036854251519", "EXPECTED_TIKV_MAX_KEY_SIZE=9223372036854251583"},
			wantKubectl:    true,
			wantOutput:     "request byte limit",
		},
		{
			name:          "KubeBrain replicas exceed int32",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_KUBEBRAIN_REPLICAS=2147483648"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_KUBEBRAIN_REPLICAS must be a canonical positive int32",
		},
		{
			name:          "PD replicas are not canonical",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_PD_REPLICAS=03"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_PD_REPLICAS must be a canonical positive int32",
		},
		{
			name:          "TiKV replicas overflow int64",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			extraEnv:      []string{"EXPECTED_TIKV_REPLICAS=9223372036854775808"},
			wantNoKubectl: true,
			wantOutput:    "EXPECTED_TIKV_REPLICAS must be a canonical positive int32",
		},
		{
			name:        "maximum int32 KubeBrain replicas reach Kubernetes",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			extraEnv:    []string{"EXPECTED_KUBEBRAIN_REPLICAS=2147483647"},
			wantKubectl: true,
			wantOutput:  "KubeBrain StatefulSet is not the expected converged release",
		},
		{
			name:              "converged release",
			image:             "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:        "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:          "3\t3",
			healthOK:          true,
			wantOK:            true,
			wantOutput:        "release gate passed",
			wantClientService: "kubebrain-client",
		},
		{
			name:       "KubeBrain approved revision drift",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   []string{"EXPECTED_KUBEBRAIN_STATEFULSET_REVISION=kb-approved"},
			wantOutput: "KubeBrain StatefulSet revision mismatch",
		},
		{
			name:            "KubeBrain rollout starts during validation",
			image:           "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:      "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			finalKubeStatus: "9\t8\t3\t3\t2\tkb-new\tkb-next\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tuid-kubebrain",
			topology:        "3\t3",
			healthOK:        true,
			wantOutput:      "KubeBrain StatefulSet changed during validation",
		},
		{
			name:        "wrong TiDB storage version",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			tidbVersion: "v8.5.2",
			healthOK:    true,
			wantOutput:  "TidbCluster storage release mismatch",
		},
		{
			name:           "TiKV key limit cannot cover etcd API",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			tikvMaxKeySize: "8192",
			healthOK:       true,
			wantOutput:     "TiKV storage.max-key-size mismatch",
		},
		{
			name:       "mixed TiKV storage image",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			tikvImage:  "pingcap/tikv:v8.5.2",
			healthOK:   true,
			wantOutput: "TiKV StatefulSet release snapshot mismatch",
		},
		{
			name:        "TiKV runtime digest drift",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			tikvImageID: "docker-pullable://pingcap/tikv@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			healthOK:    true,
			wantOutput:  "TiKV Pod runtime release mismatch",
		},
		{
			name:          "TiKV selector returns foreign Pods",
			image:         "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:    "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:      "3\t3",
			tikvPodPrefix: "foreign-tikv",
			healthOK:      true,
			wantOutput:    "TiKV Pod runtime release mismatch",
		},
		{
			name:         "TiKV Pod owner drift",
			image:        "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:   "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:     "3\t3",
			tikvOwnerUID: "uid-foreign-sts",
			healthOK:     true,
			wantOutput:   "TiKV Pod runtime release mismatch",
		},
		{
			name:             "TiKV StatefulSet TidbCluster owner drift",
			image:            "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:       "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:         "3\t3",
			tikvTidbOwnerUID: "uid-foreign-tidb",
			healthOK:         true,
			wantOutput:       "TiKV StatefulSet owner identity mismatch",
		},
		{
			name:                "TiKV rollout starts after convergence wait",
			image:               "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:          "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:            "3\t3",
			tikvCurrentRevision: "tikv-old",
			healthOK:            true,
			wantOutput:          "TiKV StatefulSet release snapshot mismatch",
		},
		{
			name:       "TiKV approved revision drift",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   []string{"EXPECTED_TIKV_STATEFULSET_REVISION=tikv-approved"},
			wantOutput: "TiKV StatefulSet revision mismatch",
		},
		{
			name:             "TiKV rollout starts during validation",
			image:            "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:       "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:         "3\t3",
			healthOK:         true,
			finalTikvSTSJSON: fakeStorageStatefulSetJSON("tikv", "uid-tikv-sts", "uid-tidb", "tikv-new", "tikv-next", "pingcap/tikv:v8.5.3"),
			wantOutput:       "TiKV StatefulSet release snapshot mismatch",
		},
		{
			name:              "TidbCluster becomes unready after convergence wait",
			image:             "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:        "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:          "3\t3",
			tidbSnapshotReady: boolPointer(false),
			healthOK:          true,
			wantOutput:        "TidbCluster release snapshot mismatch",
		},
		{
			name:       "TidbCluster compute resource drift",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   []string{"EXPECTED_PD_CPU_REQUEST=1500m"},
			wantOutput: "TidbCluster compute resource contract mismatch",
		},
		{
			name:       "rendered PD compute resource drift",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			pdSTSJSON:  fakeStorageStatefulSetJSONWithResources("pd", "uid-pd-sts", "uid-tidb", "pd-new", "pd-new", "pingcap/pd:v8.5.3", "500m", "2Gi", "2", "4Gi"),
			wantOutput: "PD StatefulSet compute resource mismatch",
		},
		{
			name:       "tls release baseline",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeTLSKubeBrainArgs("https://instance.example:2379", fakeInitialCluster),
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   expectedTLSGateEnv(),
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:       "dual-stack EndpointSlices",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON: fakeDualStackKubeBrainPodsJSON(
				"registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			endpointSlicesJSON: fakeDualStackEndpointSlicesJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2",
			}),
			serviceJSON: fakeClientServiceWithFamiliesJSON(3379, []string{"IPv4", "IPv6"}, "RequireDualStack"),
			wantOK:      true,
			wantOutput:  "release gate passed",
		},
		{
			name:       "dual-stack Pods behind single-stack Service",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON: fakeDualStackKubeBrainPodsJSON(
				"registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			endpointSlicesJSON: fakeEndpointSlicesJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2",
			}, true),
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:           "runs etcdctl from selected cluster Pod",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			etcdctlExecPod: "release-gate-runner",
			wantExec:       true,
			wantOK:         true,
			wantOutput:     "release gate passed",
		},
		{
			name:           "multiple reachable advertised client URLs",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance-a.example:2379,https://instance-b.example:2379",
			wantOK:         true,
			wantOutput:     "--endpoints=https://instance-b.example:2379",
		},
		{
			name:       "wrong quota",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--port=3379\n--quota-backend-bytes=1073741824\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "quota configuration mismatch",
		},
		{
			name:       "duplicate quota",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "quota configuration mismatch",
		},
		{
			name:       "missing advertised client URL argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--port=3379\n--quota-backend-bytes=429496729600",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "wrong advertised client URL",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://internal.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "duplicate advertised client URL argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "missing keyspace argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "wrong keyspace",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-b\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "duplicate keyspace argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--keyspace=instance-a\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "missing PD address argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "wrong PD address",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--pd-addrs=wrong-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "duplicate PD address argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "wrong image",
			image:      "registry/kubebrain@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "stale rollout",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "9\t8\t3\t3\t2\tkb-old\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "wrong KubeBrain StatefulSet resource identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tuid-other",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "StatefulSet resource identity mismatch",
		},
		{
			name:       "Pod has stale StatefulSet owner",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-old", "kb-new", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, false),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "Pod has stale controller revision",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-kubebrain", "kb-old", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, false),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "Pod is terminating",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, true),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "Pod is not Ready",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON:   fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false, false),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "Pod runtime digest drift",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			podsJSON: fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true, false,
				"docker-pullable://registry/kubebrain@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
			wantOutput: "Pod set does not match",
		},
		{
			name:       "client Service has stale endpoint Pod identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			endpointSlicesJSON: fakeEndpointSlicesJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-old",
			}, true),
			wantOutput: "EndpointSlices do not match",
		},
		{
			name:       "client EndpointSlice has stale Service owner",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			endpointSlicesJSON: fakeEndpointSlicesJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2",
			}, true, "uid-old-service"),
			wantOutput: "EndpointSlices do not match",
		},
		{
			name:        "client Service port drift",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			healthOK:    true,
			serviceJSON: fakeClientServiceJSON(3380),
			wantOutput:  "client Service release mismatch",
		},
		{
			name:       "client EndpointSlice port drift",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			endpointSlicesJSON: fakeEndpointSlicesWithPortJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2",
			}, true, 3380, "uid-client-service"),
			wantOutput: "EndpointSlices do not match",
		},
		{
			name:       "client EndpointSlice address drift",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			endpointSlicesJSON: fakeEndpointSlicesWithRouteJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2",
			}, true, 3379, "uid-client-service", []string{"10.0.0.99", "10.0.0.11", "10.0.0.12"}),
			wantOutput: "EndpointSlices do not match",
		},
		{
			name:        "client EndpointSlice address family drift",
			image:       "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:  "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:    "3\t3",
			healthOK:    true,
			serviceJSON: fakeClientServiceWithFamiliesJSON(3379, []string{"IPv4", "IPv6"}, "RequireDualStack"),
			endpointSlicesJSON: fakeEndpointSlicesWithDeclaredAddressTypeJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2",
			}, "IPv6"),
			wantOutput: "EndpointSlices do not match",
		},
		{
			name:       "client EndpointSlice changes during validation",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			healthOK:   true,
			finalEndpointSlicesJSON: fakeEndpointSlicesWithRouteJSON([]string{
				"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2",
			}, true, 3379, "uid-client-service", []string{"10.0.0.99", "10.0.0.11", "10.0.0.12"}),
			wantOutput: "EndpointSlices changed during validation",
		},
		{
			name:       "wrong storage topology",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "1\t3",
			healthOK:   true,
			wantOutput: "topology mismatch",
		},
		{
			name:       "wrong storage cluster identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3\t2",
			healthOK:   true,
			wantOutput: "storage identity mismatch",
		},
		{
			name:       "wrong TidbCluster resource identity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3\t1\tuid-other",
			healthOK:   true,
			wantOutput: "resource identity mismatch",
		},
		{
			name:       "unhealthy endpoint",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:   "3\t3",
			wantOutput: "endpoint health failed",
		},
		{
			name:                     "unreachable advertised client URL",
			image:                    "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:               "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:                 "3\t3",
			healthOK:                 true,
			advertisedURLs:           "https://instance.example:2379,https://unreachable.example:2379",
			unreachableAdvertisedURL: "https://unreachable.example:2379",
			wantOutput:               "advertised client URL is unreachable",
		},
		{
			name:           "empty advertised client URL entry",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance.example:2379,,https://other.example:2379",
			wantOutput:     "empty entry",
		},
		{
			name:           "duplicate advertised client URL entry",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance.example:2379,https://instance.example:2379",
			wantOutput:     "duplicate entry",
		},
		{
			name:           "runtime MemberList has wrong advertised URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: fakeRuntimeMemberListJSON("https://internal.example:2379"),
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList repeats advertised URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: fakeRuntimeMemberListJSON("https://instance.example:2379,https://instance.example:2379"),
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList repeats peer URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["peer-0","peer-0"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["peer-1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["peer-2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has wrong storage cluster identity",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":2},"members":[]}`,
			wantOutput:     "runtime storage identity mismatch",
		},
		{
			name:           "runtime MemberList repeats member identity",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["p0"],"clientURLs":["https://instance.example:2379"]},{"ID":11,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["p2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList is missing a member",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["p0"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has an incomplete member",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":[],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["p2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has wrong peer mapping",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["wrong-peer"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["peer-1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["peer-2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:       "missing initial cluster argument",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   "--port=3379\n--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "initial cluster configuration mismatch",
		},
		{
			name:           "wrong initial cluster argument",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0,kb-1=peer-1,kb-2=wrong-peer",
			kubeArgs:       "--port=3379\n--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379\n--initial-cluster=" + fakeInitialCluster,
			wantOutput:     "initial cluster configuration mismatch",
		},
		{
			name:           "repeated initial cluster member peer URLs",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0a,kb-0=peer-0b,kb-1=peer-1,kb-2=peer-2",
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["peer-0a","peer-0b"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["peer-1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["peer-2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOK:         true,
			wantOutput:     "release gate passed",
		},
		{
			name:           "duplicate initial cluster peer URL",
			image:          "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0,kb-1=peer-0,kb-2=peer-2",
			wantOutput:     "duplicate peer URL",
		},
		{
			name:       "wrong client listener port",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--port=3379", "--port=12379"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "client listener port configuration mismatch",
		},
		{
			name:       "missing peer listener port",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--peer-port=3380", ""),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "peer listener port configuration mismatch",
		},
		{
			name:       "duplicate info listener port",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--info-port=8080",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "info listener port configuration mismatch",
		},
		{
			name:       "advertise host baseline",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs: fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) +
				"\n--advertise-host=$(POD_NAME).kubebrain-peer.kubebrain-system.svc.cluster.local",
			topology: "3\t3",
			healthOK: true,
			extraEnv: []string{
				"EXPECTED_ADVERTISE_HOST=$(POD_NAME).kubebrain-peer.kubebrain-system.svc.cluster.local",
			},
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:       "wrong advertise host",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--advertise-host=wrong-peer.example",
			topology:   "3\t3",
			healthOK:   true,
			extraEnv: []string{
				"EXPECTED_ADVERTISE_HOST=$(POD_NAME).kubebrain-peer.kubebrain-system.svc.cluster.local",
			},
			wantOutput: "advertise host configuration mismatch",
		},
		{
			name:       "unexpected advertise host",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--advertise-host=unexpected.example",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertise host configuration mismatch",
		},
		{
			name:       "wrong max request rate",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--max-request-rate=2000", "--max-request-rate=100"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "max request rate configuration mismatch",
		},
		{
			name:       "missing request rate burst",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--request-rate-burst=4000", ""),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "request rate burst configuration mismatch",
		},
		{
			name:       "duplicate max delete range keys",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--max-delete-range-keys=1024",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "max delete range keys configuration mismatch",
		},
		{
			name:       "wrong max watches",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--max-watches=10000", "--max-watches=1000"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "max watches configuration mismatch",
		},
		{
			name:       "missing etcd compatibility",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--compatible-with-etcd=true", ""),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "etcd compatibility configuration mismatch",
		},
		{
			name:       "naked etcd compatibility flag",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--compatible-with-etcd=true", "--compatible-with-etcd"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "etcd compatibility configuration mismatch",
		},
		{
			name:       "disabled count index",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--enable-count-index=true", "--enable-count-index=false"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "count index enablement configuration mismatch",
		},
		{
			name:       "wrong count index key cap",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--count-index-max-keys=5000000", "--count-index-max-keys=1000000"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "count index key cap configuration mismatch",
		},
		{
			name:       "duplicate storage metrics enablement",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--enable-storage-metrics=true",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "storage metrics enablement configuration mismatch",
		},
		{
			name:       "unexpected storage gc lifetime",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--storage-gc-lifetime=0",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "storage GC lifetime configuration mismatch",
		},
		{
			name:       "storage gc lifetime override baseline",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--storage-gc-lifetime=10m",
			topology:   "3\t3",
			healthOK:   true,
			extraEnv: []string{
				"EXPECTED_STORAGE_GC_LIFETIME=10m",
			},
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:       "unexpected watch progress notify interval",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--watch-progress-notify-interval=2s",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "watch progress notify interval configuration mismatch",
		},
		{
			name:       "watch progress notify interval override baseline",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--watch-progress-notify-interval=1s",
			topology:   "3\t3",
			healthOK:   true,
			extraEnv: []string{
				"EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL=1s",
			},
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:       "unexpected watch cache size",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--watch-cache-size=1000",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "watch cache size configuration mismatch",
		},
		{
			name:       "unexpected watch fanout buffer",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--watch-fanout-buffer=1",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "watch fanout buffer configuration mismatch",
		},
		{
			name:       "watch buffer override baseline",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs: fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) +
				"\n--watch-cache-size=200000\n--watch-fanout-buffer=10000",
			topology: "3\t3",
			healthOK: true,
			extraEnv: []string{
				"EXPECTED_WATCH_CACHE_SIZE=200000",
				"EXPECTED_WATCH_FANOUT_BUFFER=10000",
			},
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:       "unexpected auto compaction retention",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--auto-compaction-retention-revisions=1",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "auto-compaction retention configuration mismatch",
		},
		{
			name:       "unexpected watch history scan bucket",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--watch-history-scan-rev-bucket=1",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "watch history scan bucket configuration mismatch",
		},
		{
			name:       "compaction and history scan override baseline",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs: fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) +
				"\n--auto-compaction-retention-revisions=0\n--watch-history-scan-rev-bucket=4096",
			topology: "3\t3",
			healthOK: true,
			extraEnv: []string{
				"EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS=0",
				"EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET=4096",
			},
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:       "wrong transaction operation limit",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--max-txn-ops=128", "--max-txn-ops=64"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "transaction operation limit configuration mismatch",
		},
		{
			name:       "missing request byte limit",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--max-request-bytes=1572864", ""),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "request byte limit configuration mismatch",
		},
		{
			name:       "duplicate concurrent stream limit",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--max-concurrent-streams=4294967295",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "concurrent stream limit configuration mismatch",
		},
		{
			name:       "wrong inflight request limit",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--max-requests-inflight=1024", "--max-requests-inflight=256"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "inflight request limit configuration mismatch",
		},
		{
			name:       "wrong grpc keepalive min time",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--grpc-keepalive-min-time=5s", "--grpc-keepalive-min-time=1s"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "gRPC keepalive min time configuration mismatch",
		},
		{
			name:       "missing grpc keepalive interval",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--grpc-keepalive-interval=2h", ""),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "gRPC keepalive interval configuration mismatch",
		},
		{
			name:       "duplicate auth token provider",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--auth-token=simple",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "auth token provider configuration mismatch",
		},
		{
			name:       "wrong bcrypt cost",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--bcrypt-cost=10", "--bcrypt-cost=4"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "bcrypt cost configuration mismatch",
		},
		{
			name:       "missing auth token ttl",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--auth-token-ttl=300", ""),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "auth token TTL configuration mismatch",
		},
		{
			name:       "wrong log verbosity",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--v=2", "--v=4"),
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "log verbosity configuration mismatch",
		},
		{
			name:       "unexpected tls min version",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   fakeKubeBrainArgs("https://instance.example:2379", fakeInitialCluster) + "\n--tls-min-version=TLS1.2",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "TLS min version configuration mismatch",
		},
		{
			name:       "missing expected client tls cert file",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeTLSKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--cert-file=/etc/kubebrain/client-tls/tls.crt", ""),
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   expectedTLSGateEnv(),
			wantOutput: "client TLS cert file configuration mismatch",
		},
		{
			name:       "wrong peer tls ca file",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeTLSKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--peer-trusted-ca-file=/etc/kubebrain/peer-tls/ca.crt", "--peer-trusted-ca-file=/etc/kubebrain/peer-tls/wrong-ca.crt"),
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   expectedTLSGateEnv(),
			wantOutput: "peer TLS CA file configuration mismatch",
		},
		{
			name:       "missing expected peer tls server name",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeTLSKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "\n--peer-tls-server-name=kubebrain-peer.kubebrain-system.svc.cluster.local", ""),
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   expectedTLSGateEnv(),
			wantOutput: "peer TLS server name configuration mismatch",
		},
		{
			name:       "wrong peer client certificate auth",
			image:      "registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			kubeArgs:   strings.ReplaceAll(fakeTLSKubeBrainArgs("https://instance.example:2379", fakeInitialCluster), "--peer-client-cert-auth=true", "--peer-client-cert-auth=false"),
			topology:   "3\t3",
			healthOK:   true,
			extraEnv:   expectedTLSGateEnv(),
			wantOutput: "peer client certificate auth configuration mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_KUBECTL_LOG"
if [[ "$*" == *"get pods"* && "$*" == *"component=tikv"* ]]; then
  if [[ "$*" == *" -o json" ]]; then printf '%s' "$FAKE_TIKV_PODS_JSON"; else printf 'kb-tikv-0\nkb-tikv-1\nkb-tikv-2\n'; fi
elif [[ "$*" == *"get pods"* && "$*" == *"component=pd"* ]]; then
  printf '%s' "$FAKE_PD_PODS_JSON"
elif [[ "$*" == *" exec "* && "$*" == *"/tikv-ctl --host 127.0.0.1:20160 metrics"* ]]; then
  exit 0
elif [[ "$*" == *" exec "* ]]; then
  printf '%s\n' "$*" >>"${FAKE_EXEC_LOG:?}"
  while [[ "$1" != "--" ]]; do
    shift
  done
  shift
  exec "$@"
elif [[ "$*" == *"get tidbcluster"* && "$*" == *".status.conditions"* ]]; then
  printf 'True'
elif [[ "$*" == *"get tidbcluster"* && "$*" == *" -o json" ]]; then
  printf '%s' "$FAKE_TIDB_CLUSTER_JSON"
elif [[ "$*" == *"get tidbcluster"* && "$*" == *".spec.version"* ]]; then
  printf '%s' "$FAKE_TIDB_VERSION"
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *".image}"* ]]; then
  printf '%s' "$FAKE_PD_IMAGE"
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *".image}"* ]]; then
  printf '%s' "$FAKE_TIKV_IMAGE"
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *" -o json" ]]; then
  printf '%s' "$FAKE_PD_STS_JSON"
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *" -o json" ]]; then
  calls=0
  [[ ! -f "$FAKE_TIKV_STS_CALLS" ]] || calls="$(<"$FAKE_TIKV_STS_CALLS")"
  calls=$((calls + 1))
  printf '%s' "$calls" >"$FAKE_TIKV_STS_CALLS"
  if [[ -n "$FAKE_FINAL_TIKV_STS_JSON" && "$calls" -ge 2 ]]; then printf '%s' "$FAKE_FINAL_TIKV_STS_JSON"; else printf '%s' "$FAKE_TIKV_STS_JSON"; fi
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *"jsonpath={.metadata.uid}"* ]]; then
  printf 'uid-pd-sts'
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *"jsonpath={.metadata.uid}"* ]]; then
  printf 'uid-tikv-sts'
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *"jsonpath={.status.updateRevision}"* ]]; then
  printf 'pd-new'
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *"jsonpath={.status.updateRevision}"* ]]; then
  printf 'tikv-new'
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *"jsonpath="* ]]; then
  printf '5\t5\t3\t3\t3\tpd-new\tpd-new'
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *"jsonpath="* ]]; then
  if [[ "$*" == *"readinessProbe.tcpSocket.port"* ]]; then
    printf '20160\t10\t5'
  else
    printf '7\t7\t3\t3\t3\ttikv-new\ttikv-new'
  fi
elif [[ "$*" == *"get tidbcluster"* && "$*" == *".spec.pd.replicas"* ]]; then
  printf '%s' "$FAKE_TOPOLOGY"
elif [[ "$*" == *"get statefulset kubebrain"* && "$*" == *".args"* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_ARGS"
elif [[ "$*" == *"get statefulset kubebrain"* && "$*" == *"jsonpath="* ]]; then
  calls=0
  [[ ! -f "$FAKE_KUBEBRAIN_STATUS_CALLS" ]] || calls="$(<"$FAKE_KUBEBRAIN_STATUS_CALLS")"
  calls=$((calls + 1))
  printf '%s' "$calls" >"$FAKE_KUBEBRAIN_STATUS_CALLS"
  if [[ -n "$FAKE_FINAL_KUBEBRAIN_STATUS" && "$calls" -ge 2 ]]; then printf '%s' "$FAKE_FINAL_KUBEBRAIN_STATUS"; else printf '%s' "$FAKE_KUBEBRAIN_STATUS"; fi
elif [[ "$*" == *"get service kubebrain-client"* && "$*" == *" -o json" ]]; then
  printf '%s' "$FAKE_CLIENT_SERVICE_JSON"
elif [[ "$*" == *"get service kubebrain-client"* && "$*" == *"jsonpath="* ]]; then
  printf 'uid-client-service'
elif [[ "$*" == *"get endpointslice"* && "$*" == *"kubernetes.io/service-name=kubebrain"* ]]; then
  calls=0
  [[ ! -f "$FAKE_ENDPOINT_SLICES_CALLS" ]] || calls="$(<"$FAKE_ENDPOINT_SLICES_CALLS")"
  calls=$((calls + 1))
  printf '%s' "$calls" >"$FAKE_ENDPOINT_SLICES_CALLS"
  if [[ -n "$FAKE_FINAL_ENDPOINT_SLICES_JSON" && "$calls" -ge 2 ]]; then printf '%s' "$FAKE_FINAL_ENDPOINT_SLICES_JSON"; else printf '%s' "$FAKE_ENDPOINT_SLICES_JSON"; fi
elif [[ "$*" == *"get pods"* && "$*" == *"app.kubernetes.io/name=kubebrain"* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_PODS_JSON"
else
  printf 'diagnostic output\n'
fi
`), 0o755))
			fakeEtcdctl := filepath.Join(dir, "etcdctl")
			require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ -n "$FAKE_UNREACHABLE_ADVERTISED_URL" && "$*" == *"--endpoints=$FAKE_UNREACHABLE_ADVERTISED_URL"* ]]; then
  printf 'unreachable\n' >&2
  exit 1
fi
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' "$FAKE_MEMBER_LIST_JSON"
  exit 0
fi
if [[ "$FAKE_HEALTH_OK" == "true" && "$*" == *"endpoint health"* ]]; then
  printf 'healthy %s\n' "$*"
  exit 0
fi
printf 'unhealthy\n' >&2
exit 1
`), 0o755))

			kubeArgs := tc.kubeArgs
			advertisedURLs := tc.advertisedURLs
			if advertisedURLs == "" {
				advertisedURLs = "https://instance.example:2379"
			}
			topology := tc.topology
			switch strings.Count(topology, "\t") {
			case 1:
				topology += "\t1\tuid-tidb"
			case 2:
				topology += "\tuid-tidb"
			}
			initialCluster := tc.initialCluster
			if initialCluster == "" {
				initialCluster = fakeInitialCluster
			}
			if kubeArgs == "" {
				kubeArgs = fakeKubeBrainArgs(advertisedURLs, initialCluster)
			}
			memberListJSON := tc.memberListJSON
			if memberListJSON == "" {
				memberListJSON = fakeRuntimeMemberListJSON(advertisedURLs)
			}
			podsJSON := tc.podsJSON
			if podsJSON == "" {
				podsJSON = fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", tc.image, true, false)
			}
			endpointSlicesJSON := tc.endpointSlicesJSON
			if endpointSlicesJSON == "" {
				endpointSlicesJSON = fakeEndpointSlicesJSON([]string{"uid-kubebrain-0", "uid-kubebrain-1", "uid-kubebrain-2"}, true)
			}
			serviceJSON := tc.serviceJSON
			if serviceJSON == "" {
				serviceJSON = fakeClientServiceJSON(3379)
			}
			kubeStatus := tc.kubeStatus
			if strings.Count(kubeStatus, "\t") == 7 {
				kubeStatus += "\tuid-kubebrain"
			}
			tidbVersion := tc.tidbVersion
			if tidbVersion == "" {
				tidbVersion = "v8.5.3"
			}
			pdImage := tc.pdImage
			if pdImage == "" {
				pdImage = "pingcap/pd:v8.5.3"
			}
			tikvImage := tc.tikvImage
			if tikvImage == "" {
				tikvImage = "pingcap/tikv:v8.5.3"
			}
			tikvImageID := tc.tikvImageID
			if tikvImageID == "" {
				tikvImageID = "docker-pullable://pingcap/tikv@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			}
			tikvPodPrefix := tc.tikvPodPrefix
			if tikvPodPrefix == "" {
				tikvPodPrefix = "kb-tikv"
			}
			tikvOwnerUID := tc.tikvOwnerUID
			if tikvOwnerUID == "" {
				tikvOwnerUID = "uid-tikv-sts"
			}
			tikvTidbOwnerUID := tc.tikvTidbOwnerUID
			if tikvTidbOwnerUID == "" {
				tikvTidbOwnerUID = "uid-tidb"
			}
			tikvCurrentRevision := tc.tikvCurrentRevision
			if tikvCurrentRevision == "" {
				tikvCurrentRevision = "tikv-new"
			}
			pdSTSJSON := tc.pdSTSJSON
			if pdSTSJSON == "" {
				pdSTSJSON = fakeStorageStatefulSetJSON("pd", "uid-pd-sts", "uid-tidb", "pd-new", "pd-new", "pingcap/pd:v8.5.3")
			}
			tidbSnapshotReady := true
			if tc.tidbSnapshotReady != nil {
				tidbSnapshotReady = *tc.tidbSnapshotReady
			}
			tikvMaxKeySize := tc.tikvMaxKeySize
			if tikvMaxKeySize == "" {
				tikvMaxKeySize = "2621440"
			}
			env := []string{
				"KUBECTL=" + fakeKubectl,
				"ETCDCTL=" + fakeEtcdctl,
				"EXPECTED_IMAGE=" + tc.image,
				"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
				"EXPECTED_KUBEBRAIN_STATEFULSET_REVISION=kb-new",
				"EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID=uid-client-service",
				"EXPECTED_KEYSPACE=instance-a",
				"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
				"EXPECTED_CLUSTER_ID=1",
				"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
				"EXPECTED_TIDB_VERSION=v8.5.3",
				"EXPECTED_PD_IMAGE=pingcap/pd:v8.5.3",
				"EXPECTED_TIKV_IMAGE=pingcap/tikv:v8.5.3",
				"EXPECTED_PD_IMAGE_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"EXPECTED_TIKV_IMAGE_DIGEST=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"EXPECTED_PD_STATEFULSET_REVISION=pd-new",
				"EXPECTED_TIKV_STATEFULSET_REVISION=tikv-new",
				"EXPECTED_INITIAL_CLUSTER=" + initialCluster,
				"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
				"EXPECTED_ADVERTISE_CLIENT_URLS=" + advertisedURLs,
				"EXPECTED_MAX_REQUEST_RATE=2000",
				"EXPECTED_REQUEST_RATE_BURST=4000",
				"EXPECTED_MAX_DELETE_RANGE_KEYS=1024",
				"EXPECTED_MAX_WATCHES=10000",
				"ENDPOINT=https://instance.example:2379",
				"TIMEOUT_SECONDS=1",
				"POLL_INTERVAL_SECONDS=0",
				"FAKE_KUBEBRAIN_STATUS=" + kubeStatus,
				"FAKE_FINAL_KUBEBRAIN_STATUS=" + tc.finalKubeStatus,
				"FAKE_KUBEBRAIN_STATUS_CALLS=" + filepath.Join(dir, "kubebrain-status-calls"),
				"FAKE_KUBECTL_LOG=" + filepath.Join(dir, "kubectl.log"),
				"FAKE_KUBEBRAIN_ARGS=" + kubeArgs,
				"FAKE_KUBEBRAIN_PODS_JSON=" + podsJSON,
				"FAKE_ENDPOINT_SLICES_JSON=" + endpointSlicesJSON,
				"FAKE_FINAL_ENDPOINT_SLICES_JSON=" + tc.finalEndpointSlicesJSON,
				"FAKE_ENDPOINT_SLICES_CALLS=" + filepath.Join(dir, "endpoint-slices-calls"),
				"FAKE_CLIENT_SERVICE_JSON=" + serviceJSON,
				"FAKE_TOPOLOGY=" + topology,
				"FAKE_TIDB_VERSION=" + tidbVersion,
				"FAKE_TIDB_CLUSTER_JSON=" + fakeTidbClusterJSON(tidbVersion, topology, tidbSnapshotReady, tikvMaxKeySize),
				"FAKE_PD_IMAGE=" + pdImage,
				"FAKE_TIKV_IMAGE=" + tikvImage,
				"FAKE_PD_STS_JSON=" + pdSTSJSON,
				"FAKE_TIKV_STS_JSON=" + fakeStorageStatefulSetJSON("tikv", "uid-tikv-sts", tikvTidbOwnerUID, tikvCurrentRevision, "tikv-new", tikvImage),
				"FAKE_FINAL_TIKV_STS_JSON=" + tc.finalTikvSTSJSON,
				"FAKE_TIKV_STS_CALLS=" + filepath.Join(dir, "tikv-sts-calls"),
				"FAKE_PD_PODS_JSON=" + fakeStoragePodsJSON("pd", "kb-pd", "uid-pd-sts", "pd-new", "pingcap/pd:v8.5.3", "docker-pullable://pingcap/pd@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				"FAKE_TIKV_PODS_JSON=" + fakeStoragePodsJSON("tikv", tikvPodPrefix, tikvOwnerUID, "tikv-new", tikvImage, tikvImageID),
				"FAKE_HEALTH_OK=" + boolString(tc.healthOK),
				"FAKE_UNREACHABLE_ADVERTISED_URL=" + tc.unreachableAdvertisedURL,
				"FAKE_MEMBER_LIST_JSON=" + memberListJSON,
			}
			env = append(env, tc.extraEnv...)
			execLog := filepath.Join(dir, "etcdctl-exec.log")
			if tc.etcdctlExecPod != "" {
				env = append(env,
					"ETCDCTL_EXEC_POD="+tc.etcdctlExecPod,
					"FAKE_EXEC_LOG="+execLog,
				)
			}
			output, err := runValidateInstanceReady(t, env)
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, strings.TrimSpace(string(output)), tc.wantOutput)
			if tc.wantExec {
				executions, readErr := os.ReadFile(execLog)
				require.NoError(t, readErr)
				require.Contains(t, string(executions), "exec "+tc.etcdctlExecPod+" -- "+fakeEtcdctl)
			}
			if tc.wantClientService != "" {
				calls, readErr := os.ReadFile(filepath.Join(dir, "kubectl.log"))
				require.NoError(t, readErr)
				require.Contains(t, string(calls), "get service "+tc.wantClientService+" ")
			}
			if tc.wantNoKubectl {
				require.NoFileExists(t, filepath.Join(dir, "kubectl.log"))
			}
			if tc.wantKubectl {
				require.FileExists(t, filepath.Join(dir, "kubectl.log"))
			}
		})
	}
}

func fakeTidbClusterJSON(version, topology string, ready bool, tikvMaxKeySize string) string {
	status := "False"
	if ready {
		status = "True"
	}
	parts := strings.Split(topology, "\t")
	encoded, err := json.Marshal(map[string]any{
		"apiVersion": "pingcap.com/v1alpha1", "kind": "TidbCluster",
		"metadata": map[string]any{"name": "kb", "uid": parts[3], "generation": 8},
		"spec": map[string]any{
			"version": version,
			"pd": map[string]any{
				"replicas": json.Number(parts[0]),
				"requests": map[string]any{"cpu": "1", "memory": "2Gi"},
				"limits":   map[string]any{"cpu": "2", "memory": "4Gi"},
			},
			"tikv": map[string]any{
				"replicas": json.Number(parts[1]),
				"config":   "[storage]\nmax-key-size = " + tikvMaxKeySize + "\n",
				"requests": map[string]any{"cpu": "4", "memory": "8Gi"},
				"limits":   map[string]any{"cpu": "8", "memory": "16Gi"},
			},
		},
		"status": map[string]any{"clusterID": parts[2], "conditions": []map[string]any{{"type": "Ready", "status": status}}},
	})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeStorageStatefulSetJSON(component, uid, tidbOwnerUID, currentRevision, updateRevision, image string) string {
	if component == "pd" {
		return fakeStorageStatefulSetJSONWithResources(component, uid, tidbOwnerUID, currentRevision, updateRevision, image, "1", "2Gi", "2", "4Gi")
	}
	return fakeStorageStatefulSetJSONWithResources(component, uid, tidbOwnerUID, currentRevision, updateRevision, image, "4", "8Gi", "8", "16Gi")
}

func fakeStorageStatefulSetJSONWithResources(component, uid, tidbOwnerUID, currentRevision, updateRevision, image, cpuRequest, memoryRequest, cpuLimit, memoryLimit string) string {
	encoded, err := json.Marshal(map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "StatefulSet",
		"metadata": map[string]any{
			"generation": 8,
			"name":       "kb-" + component,
			"uid":        uid,
			"ownerReferences": []map[string]any{{
				"apiVersion": "pingcap.com/v1alpha1", "kind": "TidbCluster", "name": "kb", "uid": tidbOwnerUID, "controller": true,
			}},
		},
		"spec": map[string]any{
			"replicas": 3,
			"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{
				"name": component, "image": image,
				"resources": map[string]any{
					"requests": map[string]any{"cpu": cpuRequest, "memory": memoryRequest},
					"limits":   map[string]any{"cpu": cpuLimit, "memory": memoryLimit},
				},
			}}}},
		},
		"status": map[string]any{"observedGeneration": 8, "readyReplicas": 3, "updatedReplicas": 3, "currentRevision": currentRevision, "updateRevision": updateRevision},
	})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeStoragePodsJSON(component, podPrefix, ownerUID, revision, image, imageID string) string {
	items := make([]map[string]any, 3)
	for index := range items {
		items[index] = map[string]any{
			"metadata": map[string]any{
				"name":            podPrefix + "-" + string(rune('0'+index)),
				"labels":          map[string]any{"controller-revision-hash": revision},
				"ownerReferences": []map[string]any{{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kb-" + component, "uid": ownerUID, "controller": true}},
			},
			"spec": map[string]any{"containers": []map[string]any{{"name": component, "image": image}}},
			"status": map[string]any{
				"phase":             "Running",
				"conditions":        []map[string]any{{"type": "Ready", "status": "True"}},
				"containerStatuses": []map[string]any{{"name": component, "ready": true, "imageID": imageID}},
			},
		}
	}
	encoded, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeKubeBrainPodsJSON(ownerUID, revision, image string, ready, terminating bool, runtimeImageIDs ...string) string {
	runtimeImageID := "docker-pullable://" + image
	if len(runtimeImageIDs) > 0 {
		runtimeImageID = runtimeImageIDs[0]
	}
	items := make([]map[string]any, 3)
	for index := range items {
		metadata := map[string]any{
			"name":   "kubebrain-" + string(rune('0'+index)),
			"uid":    "uid-kubebrain-" + string(rune('0'+index)),
			"labels": map[string]any{"controller-revision-hash": revision},
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": ownerUID, "controller": true,
			}},
		}
		if terminating && index == 0 {
			metadata["deletionTimestamp"] = "2026-07-22T00:00:00Z"
		}
		readyStatus := "True"
		if !ready && index == 0 {
			readyStatus = "False"
		}
		items[index] = map[string]any{
			"metadata": metadata,
			"spec":     map[string]any{"containers": []map[string]any{{"name": "kubebrain", "image": image}}},
			"status": map[string]any{
				"phase":             "Running",
				"podIP":             fmt.Sprintf("10.0.0.%d", index+10),
				"podIPs":            []map[string]any{{"ip": fmt.Sprintf("10.0.0.%d", index+10)}},
				"conditions":        []map[string]any{{"type": "Ready", "status": readyStatus}},
				"containerStatuses": []map[string]any{{"name": "kubebrain", "ready": ready, "imageID": runtimeImageID}},
			},
		}
	}
	encoded, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeDualStackKubeBrainPodsJSON(image string) string {
	var document map[string]any
	if err := json.Unmarshal([]byte(fakeKubeBrainPodsJSON("uid-kubebrain", "kb-new", image, true, false)), &document); err != nil {
		panic(err)
	}
	for index, rawItem := range document["items"].([]any) {
		item := rawItem.(map[string]any)
		status := item["status"].(map[string]any)
		status["podIPs"] = []any{
			map[string]any{"ip": fmt.Sprintf("10.0.0.%d", index+10)},
			map[string]any{"ip": fmt.Sprintf("fd00::%d", index+10)},
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeClientServiceJSON(port int) string {
	return fakeClientServiceWithFamiliesJSON(port, []string{"IPv4"}, "SingleStack")
}

func fakeClientServiceWithFamiliesJSON(port int, families []string, policy string) string {
	clusterIPs := []string{"10.96.0.42"}
	if len(families) == 2 {
		clusterIPs = append(clusterIPs, "fd00::42")
	}
	encoded, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]any{"name": "kubebrain-client", "uid": "uid-client-service"},
		"spec": map[string]any{
			"type":           "ClusterIP",
			"clusterIP":      clusterIPs[0],
			"clusterIPs":     clusterIPs,
			"ipFamilies":     families,
			"ipFamilyPolicy": policy,
			"selector": map[string]any{
				"app.kubernetes.io/name": "kubebrain", "app.kubernetes.io/instance": "kubebrain",
			},
			"ports": []map[string]any{{"name": "client", "protocol": "TCP", "port": port, "targetPort": "client"}},
		},
	})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeEndpointSlicesJSON(podUIDs []string, ready bool, serviceUIDs ...string) string {
	serviceUID := "uid-client-service"
	if len(serviceUIDs) > 0 {
		serviceUID = serviceUIDs[0]
	}
	return fakeEndpointSlicesWithPortJSON(podUIDs, ready, 3379, serviceUID)
}

func fakeEndpointSlicesWithPortJSON(podUIDs []string, ready bool, port int, serviceUID string) string {
	addresses := make([]string, len(podUIDs))
	for index := range addresses {
		addresses[index] = fmt.Sprintf("10.0.0.%d", index+10)
	}
	return fakeEndpointSlicesWithRouteJSON(podUIDs, ready, port, serviceUID, addresses)
}

func fakeEndpointSlicesWithRouteJSON(podUIDs []string, ready bool, port int, serviceUID string, addresses []string) string {
	addressType := "IPv4"
	if len(addresses) > 0 && strings.Contains(addresses[0], ":") {
		addressType = "IPv6"
	}
	endpoints := make([]map[string]any, len(podUIDs))
	for index, podUID := range podUIDs {
		endpoints[index] = map[string]any{
			"conditions": map[string]any{"ready": ready, "serving": ready, "terminating": false},
			"targetRef":  map[string]any{"kind": "Pod", "name": "kubebrain-" + string(rune('0'+index)), "uid": podUID},
			"addresses":  []string{addresses[index]},
		}
	}
	encoded, err := json.Marshal(map[string]any{"items": []map[string]any{{
		"addressType": addressType,
		"metadata": map[string]any{
			"name":   "kubebrain-client-slice",
			"uid":    "uid-client-slice",
			"labels": map[string]any{"kubernetes.io/service-name": "kubebrain-client"},
			"ownerReferences": []map[string]any{{
				"apiVersion": "v1", "kind": "Service", "name": "kubebrain-client", "uid": serviceUID, "controller": true,
			}},
		},
		"ports":     []map[string]any{{"name": "client", "protocol": "TCP", "port": port}},
		"endpoints": endpoints,
	}}})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeDualStackEndpointSlicesJSON(podUIDs []string) string {
	var ipv4Document, ipv6Document map[string]any
	ipv4 := []string{"10.0.0.10", "10.0.0.11", "10.0.0.12"}
	ipv6 := []string{"fd00::10", "fd00::11", "fd00::12"}
	if err := json.Unmarshal([]byte(fakeEndpointSlicesWithRouteJSON(podUIDs, true, 3379, "uid-client-service", ipv4)), &ipv4Document); err != nil {
		panic(err)
	}
	if err := json.Unmarshal([]byte(fakeEndpointSlicesWithRouteJSON(podUIDs, true, 3379, "uid-client-service", ipv6)), &ipv6Document); err != nil {
		panic(err)
	}
	ipv4Document["items"] = append(ipv4Document["items"].([]any), ipv6Document["items"].([]any)...)
	encoded, err := json.Marshal(ipv4Document)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeEndpointSlicesWithDeclaredAddressTypeJSON(podUIDs []string, addressType string) string {
	var document map[string]any
	if err := json.Unmarshal([]byte(fakeEndpointSlicesJSON(podUIDs, true)), &document); err != nil {
		panic(err)
	}
	document["items"].([]any)[0].(map[string]any)["addressType"] = addressType
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func fakeRuntimeMemberListJSON(advertisedURLs string) string {
	clientURLs := strings.Split(advertisedURLs, ",")
	members := make([]map[string]any, 3)
	for index := range members {
		members[index] = map[string]any{
			"ID":         index + 11,
			"name":       "kb-" + string(rune('0'+index)),
			"peerURLs":   []string{"peer-" + string(rune('0'+index))},
			"clientURLs": clientURLs,
		}
	}
	encoded, err := json.Marshal(map[string]any{
		"header":  map[string]any{"cluster_id": 1},
		"members": members,
	})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func TestValidateInstanceReadyRequiresImmutableInputs(t *testing.T) {
	baseEnv := func() []string {
		return []string{
			"EXPECTED_IMAGE=registry/kubebrain@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"EXPECTED_KUBEBRAIN_STATEFULSET_UID=uid-kubebrain",
			"EXPECTED_KUBEBRAIN_STATEFULSET_REVISION=kb-new",
			"EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID=uid-client-service",
			"ENDPOINT=https://instance.example:2379",
			"EXPECTED_KEYSPACE=instance-a",
			"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
			"EXPECTED_CLUSTER_ID=1",
			"EXPECTED_TIDB_CLUSTER_UID=uid-tidb",
			"EXPECTED_TIDB_VERSION=v8.5.3",
			"EXPECTED_PD_IMAGE=pingcap/pd:v8.5.3",
			"EXPECTED_TIKV_IMAGE=pingcap/tikv:v8.5.3",
			"EXPECTED_PD_IMAGE_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"EXPECTED_TIKV_IMAGE_DIGEST=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"EXPECTED_PD_STATEFULSET_REVISION=pd-new",
			"EXPECTED_TIKV_STATEFULSET_REVISION=tikv-new",
			"EXPECTED_INITIAL_CLUSTER=" + fakeInitialCluster,
			"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
			"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
		}
	}
	replace := func(env []string, name, value string) []string {
		prefix := name + "="
		for index, item := range env {
			if strings.HasPrefix(item, prefix) {
				env[index] = prefix + value
				return env
			}
		}
		return append(env, prefix+value)
	}
	for _, tc := range []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "image required", key: "EXPECTED_IMAGE", want: "EXPECTED_IMAGE is required"},
		{name: "immutable image", key: "EXPECTED_IMAGE", value: "kubebrain:dev", want: "EXPECTED_IMAGE must be an immutable image reference"},
		{name: "endpoint control character", key: "ENDPOINT", value: "https://instance.example:2379\nother", want: "ENDPOINT contains unsupported characters"},
		{name: "endpoint DEL", key: "ENDPOINT", value: "https://instance.example:2379\x7fother", want: "ENDPOINT contains unsupported characters"},
		{name: "endpoint quote", key: "ENDPOINT", value: `https://instance.example:2379"other`, want: "ENDPOINT contains unsupported characters"},
		{name: "endpoint backslash", key: "ENDPOINT", value: `https://instance.example:2379\other`, want: "ENDPOINT contains unsupported characters"},
		{name: "quota required", key: "EXPECTED_QUOTA_BACKEND_BYTES", want: "EXPECTED_QUOTA_BACKEND_BYTES is required"},
		{name: "advertise client urls required", key: "EXPECTED_ADVERTISE_CLIENT_URLS", want: "EXPECTED_ADVERTISE_CLIENT_URLS is required"},
		{name: "keyspace required", key: "EXPECTED_KEYSPACE", want: "EXPECTED_KEYSPACE is required"},
		{name: "pd addrs required", key: "EXPECTED_PD_ADDRS", want: "EXPECTED_PD_ADDRS is required"},
		{name: "initial cluster required", key: "EXPECTED_INITIAL_CLUSTER", want: "EXPECTED_INITIAL_CLUSTER is required"},
		{name: "cluster id required", key: "EXPECTED_CLUSTER_ID", want: "EXPECTED_CLUSTER_ID is required"},
		{name: "tidb cluster uid required", key: "EXPECTED_TIDB_CLUSTER_UID", want: "EXPECTED_TIDB_CLUSTER_UID is required"},
		{name: "tidb version required", key: "EXPECTED_TIDB_VERSION", want: "EXPECTED_TIDB_VERSION is required"},
		{name: "tidb version exact", key: "EXPECTED_TIDB_VERSION", value: "8.5", want: "must be an exact vMAJOR.MINOR.PATCH version"},
		{name: "pd image required", key: "EXPECTED_PD_IMAGE", want: "EXPECTED_PD_IMAGE is required"},
		{name: "pd image whitespace", key: "EXPECTED_PD_IMAGE", value: "pingcap/pd: v8.5.3", want: "must be an exact image reference without whitespace"},
		{name: "tikv image required", key: "EXPECTED_TIKV_IMAGE", want: "EXPECTED_TIKV_IMAGE is required"},
		{name: "pd digest required", key: "EXPECTED_PD_IMAGE_DIGEST", want: "EXPECTED_PD_IMAGE_DIGEST is required"},
		{name: "pd digest exact", key: "EXPECTED_PD_IMAGE_DIGEST", value: "sha256:abcd", want: "must be sha256:<64 lowercase hex>"},
		{name: "tikv digest required", key: "EXPECTED_TIKV_IMAGE_DIGEST", want: "EXPECTED_TIKV_IMAGE_DIGEST is required"},
		{name: "pd revision required", key: "EXPECTED_PD_STATEFULSET_REVISION", want: "EXPECTED_PD_STATEFULSET_REVISION is required"},
		{name: "pd revision exact", key: "EXPECTED_PD_STATEFULSET_REVISION", value: "PD revision", want: "must be a DNS subdomain"},
		{name: "tikv revision required", key: "EXPECTED_TIKV_STATEFULSET_REVISION", want: "EXPECTED_TIKV_STATEFULSET_REVISION is required"},
		{name: "kubebrain statefulset uid required", key: "EXPECTED_KUBEBRAIN_STATEFULSET_UID", want: "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required"},
		{name: "kubebrain revision required", key: "EXPECTED_KUBEBRAIN_STATEFULSET_REVISION", want: "EXPECTED_KUBEBRAIN_STATEFULSET_REVISION is required"},
		{name: "kubebrain revision exact", key: "EXPECTED_KUBEBRAIN_STATEFULSET_REVISION", value: "kb revision", want: "must be a DNS subdomain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := runValidateInstanceReady(t, replace(baseEnv(), tc.key, tc.value))
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
		})
	}
}

func runValidateInstanceReady(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, "validate-instance-ready.sh", env)
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func boolPointer(value bool) *bool { return &value }
