package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectMoveLeaderDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-direct-moveleader-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "must contain exactly three non-empty")
	require.Contains(t, string(script), "must contain three distinct endpoints")
	require.Contains(t, string(script), "reference direct client and peer endpoints must be mutually distinct")
	require.Contains(t, string(script), "one healthy three-member topology with an in-set leader")
	require.Contains(t, string(script), "TEST_COUNT must be a positive integer")
	require.Contains(t, string(script), "TEST_SCOPE must be all, lease-response-loss, lease-revoke-cross-replica, or lease-revoke-tls-passthrough")
	require.Contains(t, string(script), "for index in 0 1 2")
	require.Contains(t, string(script), `--name "reference-${index}"`)
	require.Contains(t, string(script), "127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379")
	require.Contains(t, string(script), "ResponseLossReplayAcrossReplicas")
	require.Contains(t, string(script), "ReplayAfterSameIDRegrant")
	require.Contains(t, string(script), "GrantResponseLossReplayAcrossReplicas")
	require.Contains(t, string(script), "ExplicitLeaseGrantResponseLossRetryAcrossReplicas")
	require.Contains(t, string(script), "OrphanLeaseExpiresAfterGrantResponseLoss")
	require.Contains(t, string(script), "GrantResponseLossAcrossExternalL4Proxy")
	require.Contains(t, string(script), "RevokeResponseLossAcrossExternalL4TLSPassthrough")
	require.Contains(t, string(script), "ResponseLossAcrossExternalL7Proxy")
	require.Contains(t, string(script), "WatchResumesAcrossExternalL7Reset")
	require.Contains(t, string(script), `cd "$ROOT_DIR/hack/etcd-client-compat"`)
	require.Contains(t, string(script), `go build -o "$data_dir/tcp-switch-proxy" ./cmd/tcp-switch-proxy`)
	require.Contains(t, string(script), `go build -o "$data_dir/grpc-switch-proxy" ./cmd/grpc-switch-proxy`)
	require.Contains(t, string(script), `EXTERNAL_TCP_SWITCH_PROXY_BINARY="$data_dir/tcp-switch-proxy"`)
	require.Contains(t, string(script), `EXTERNAL_GRPC_SWITCH_PROXY_BINARY="$data_dir/grpc-switch-proxy"`)
}

func TestDirectMoveLeaderDifferentialRunnerRejectsInvalidScopeBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-moveleader-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
		"TEST_SCOPE=typo-that-would-run-zero-tests",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "TEST_SCOPE must be all, lease-response-loss, lease-revoke-cross-replica, or lease-revoke-tls-passthrough")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectMoveLeaderDifferentialRunnerRequiresTLSFilesBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-moveleader-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
		"TEST_SCOPE=lease-revoke-tls-passthrough",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "TLS_CA_FILE, TLS_CERT_FILE, and TLS_KEY_FILE must name readable files")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectMoveLeaderDifferentialRunnerRejectsInvalidCountBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-moveleader-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
		"TEST_COUNT=0",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "TEST_COUNT must be a positive integer")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectMoveLeaderDifferentialRunnerRequiresEndpointsBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-moveleader-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_DIRECT_ENDPOINTS")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectMoveLeaderDifferentialRunnerRejectsEndpointCountBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-moveleader-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "must contain exactly three non-empty")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectMoveLeaderDifferentialRunnerRejectsDuplicateEndpointsBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-moveleader-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:1,127.0.0.1:2",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "must contain three distinct endpoints")
	require.NotContains(t, string(output), "missing required command")
}

func TestDirectMoveLeaderDifferentialRunnerRejectsReferencePortCollisionBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-direct-moveleader-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_DIRECT_ENDPOINTS=127.0.0.1:1,127.0.0.1:2,127.0.0.1:3",
		"REFERENCE_DIRECT_CLIENT_ENDPOINTS=127.0.0.1:4,127.0.0.1:5,127.0.0.1:6",
		"REFERENCE_DIRECT_PEER_ENDPOINTS=127.0.0.1:6,127.0.0.1:7,127.0.0.1:8",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "reference direct client and peer endpoints must be mutually distinct")
	require.NotContains(t, string(output), "missing required command")
}
