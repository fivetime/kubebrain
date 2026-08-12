package compat

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type externalL7ProxyResponse struct {
	OK               bool           `json:"ok"`
	Error            string         `json:"error"`
	Endpoint         string         `json:"endpoint"`
	DroppedResponses int            `json:"droppedResponses"`
	Dials            map[string]int `json:"dials"`
}

type externalL7Proxy struct {
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	cmd    *exec.Cmd
	stderr lockedBuffer
}

type leaseRevokeExternalL7ReplayOutcome struct {
	ResponseDiscarded   bool
	SecondReplicaDialed bool
	LeaseNotFound       bool
	KeyDeleted          bool
	LeaseMissing        bool
	LeaseUnlisted       bool
	RevisionDelta       int64
}

// TestLeaseRevokeResponseLossAcrossExternalL7ProxyDifferential proves the
// replay contract through a standalone gRPC-aware intermediary. Unlike the L4
// proxy, this process observes RPC boundaries, withholds the committed success
// response, switches its upstream, and emits an L7 Unavailable status.
func TestLeaseRevokeResponseLossAcrossExternalL7ProxyDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_GRPC_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_GRPC_SWITCH_PROXY_BINARY to run the external L7 response-loss differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseRevokeExternalL7ReplayOutcome{
		ResponseDiscarded:   true,
		SecondReplicaDialed: true,
		LeaseNotFound:       true,
		KeyDeleted:          true,
		LeaseMissing:        true,
		LeaseUnlisted:       true,
		RevisionDelta:       1,
	}
	reference := runExternalL7LeaseRevokeResponseLossScenario(t, binary, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runExternalL7LeaseRevokeResponseLossScenario(t, binary, kubeBrainEndpoints, "kubebrain"))
}

func runExternalL7LeaseRevokeResponseLossScenario(
	t *testing.T,
	binary string,
	endpoints []string,
	instance string,
) leaseRevokeExternalL7ReplayOutcome {
	t.Helper()
	proxy, endpoint := startExternalL7Proxy(t, binary, endpoints[0])
	throughProxy, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughProxy.Close()) })
	observer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[2]}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-external-l7-revoke-response-loss/%s/%d", instance, time.Now().UnixNano())
	grant, err := throughProxy.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = throughProxy.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	before, err := observer.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, before.Kvs, 1)
	initialStats := proxy.command(t, "stats")
	require.Positive(t, initialStats.Dials[grpcTarget(endpoints[0])])

	require.True(t, proxy.command(t, "block-responses").OK)
	revokeDone := make(chan error, 1)
	go func() {
		_, revokeErr := throughProxy.Revoke(ctx, grant.ID)
		revokeDone <- revokeErr
	}()
	require.Eventually(t, func() bool {
		response, ttlErr := observer.TimeToLive(ctx, grant.ID)
		return ttlErr == nil && response.TTL == -1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return proxy.command(t, "stats").DroppedResponses > initialStats.DroppedResponses
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, proxy.command(t, "target "+grpcTarget(endpoints[1])).OK)
	require.True(t, proxy.command(t, "fail-blocked-responses").OK)

	var revokeErr error
	select {
	case revokeErr = <-revokeDone:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.Eventually(t, func() bool {
		return proxy.command(t, "stats").Dials[grpcTarget(endpoints[1])] > 0
	}, 5*time.Second, 10*time.Millisecond)
	after, err := observer.Get(ctx, key)
	require.NoError(t, err)
	ttl, err := observer.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	listed, err := observer.Leases(ctx)
	require.NoError(t, err)
	leaseUnlisted := true
	for _, lease := range listed.Leases {
		if lease.ID == grant.ID {
			leaseUnlisted = false
		}
	}
	stats := proxy.command(t, "stats")
	return leaseRevokeExternalL7ReplayOutcome{
		ResponseDiscarded:   stats.DroppedResponses > initialStats.DroppedResponses,
		SecondReplicaDialed: stats.Dials[grpcTarget(endpoints[1])] > 0,
		LeaseNotFound:       errors.Is(revokeErr, rpctypes.ErrLeaseNotFound),
		KeyDeleted:          len(after.Kvs) == 0,
		LeaseMissing:        ttl.TTL == -1,
		LeaseUnlisted:       leaseUnlisted,
		RevisionDelta:       after.Header.Revision - before.Header.Revision,
	}
}

func startExternalL7Proxy(t *testing.T, binary, target string) (*externalL7Proxy, string) {
	t.Helper()
	cmd := exec.Command(binary, "--listen=127.0.0.1:0", "--target="+grpcTarget(target))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	proxy := &externalL7Proxy{stdin: stdin, stdout: bufio.NewScanner(stdout), cmd: cmd}
	cmd.Stderr = &proxy.stderr
	require.NoError(t, cmd.Start())
	require.True(t, proxy.stdout.Scan(), "external L7 proxy exited before ready: %s", proxy.stderr.String())
	var ready externalL7ProxyResponse
	require.NoError(t, json.Unmarshal(proxy.stdout.Bytes(), &ready))
	require.True(t, ready.OK, ready.Error)
	require.NotEmpty(t, ready.Endpoint)
	t.Cleanup(func() {
		_ = proxy.stdin.Close()
		done := make(chan error, 1)
		go func() { done <- proxy.cmd.Wait() }()
		select {
		case waitErr := <-done:
			require.NoError(t, waitErr, proxy.stderr.String())
		case <-time.After(5 * time.Second):
			_ = proxy.cmd.Process.Kill()
			<-done
			t.Errorf("external L7 proxy did not exit after stdin close: %s", proxy.stderr.String())
		}
	})
	return proxy, ready.Endpoint
}

func (p *externalL7Proxy) command(t *testing.T, command string) externalL7ProxyResponse {
	t.Helper()
	_, err := io.WriteString(p.stdin, command+"\n")
	require.NoError(t, err, p.stderr.String())
	require.True(t, p.stdout.Scan(), "external L7 proxy exited after %q: %s", command, p.stderr.String())
	var response externalL7ProxyResponse
	require.NoError(t, json.Unmarshal(p.stdout.Bytes(), &response))
	require.Empty(t, response.Error)
	return response
}
