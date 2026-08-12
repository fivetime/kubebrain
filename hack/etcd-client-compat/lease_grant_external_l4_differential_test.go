package compat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type externalL4ProxyResponse struct {
	OK           bool           `json:"ok"`
	Error        string         `json:"error"`
	Endpoint     string         `json:"endpoint"`
	DroppedBytes int64          `json:"droppedBytes"`
	Dials        map[string]int `json:"dials"`
}

type externalL4Proxy struct {
	stdin  io.WriteCloser
	stdout *bufio.Scanner
	cmd    *exec.Cmd
	stderr bytes.Buffer
}

// TestLeaseGrantResponseLossAcrossExternalL4ProxyDifferential repeats the
// automatic-ID ambiguity through a standalone proxy process. This proves that
// the result is not an artifact of the in-process tcpBridge or shared Go state.
func TestLeaseGrantResponseLossAcrossExternalL4ProxyDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_TCP_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_TCP_SWITCH_PROXY_BINARY to run the external L4 response-loss differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := leaseGrantResponseLossReplayOutcome{
		ResponseDiscarded:    true,
		SecondReplicaDialed:  true,
		GrantReturnedSuccess: true,
		NewLeaseCount:        2,
		ReturnedLeasePresent: true,
		OrphanLeaseCount:     1,
		AllNewLeasesLive:     true,
		RevisionDelta:        0,
	}
	reference := runExternalL4LeaseGrantResponseLossScenario(t, binary, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runExternalL4LeaseGrantResponseLossScenario(t, binary, kubeBrainEndpoints, "kubebrain"))
}

func runExternalL4LeaseGrantResponseLossScenario(
	t *testing.T,
	binary string,
	endpoints []string,
	instance string,
) leaseGrantResponseLossReplayOutcome {
	t.Helper()
	proxy, endpoint := startExternalL4Proxy(t, binary, endpoints[0])
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
	baseline, err := observer.Leases(ctx)
	require.NoError(t, err)
	baselineIDs := leaseIDSet(baseline)
	warm, err := throughProxy.Leases(ctx)
	require.NoError(t, err)
	require.Equal(t, baselineIDs, leaseIDSet(warm))
	initialStats := proxy.command(t, "stats")
	require.Positive(t, initialStats.Dials[grpcTarget(endpoints[0])])
	probeKey := fmt.Sprintf("/dbaas-external-l4-grant-response-loss/%s/%d", instance, time.Now().UnixNano())
	before, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)

	require.True(t, proxy.command(t, "blackhole-responses").OK)
	grantDone := make(chan *clientv3.LeaseGrantResponse, 1)
	grantErrDone := make(chan error, 1)
	go func() {
		response, grantErr := throughProxy.Grant(ctx, 60)
		grantDone <- response
		grantErrDone <- grantErr
	}()
	require.Eventually(t, func() bool {
		current, listErr := observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(current, baselineIDs) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return proxy.command(t, "stats").DroppedBytes > initialStats.DroppedBytes
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, proxy.command(t, "target "+grpcTarget(endpoints[1])).OK)
	require.True(t, proxy.command(t, "drop-connections").OK)
	require.True(t, proxy.command(t, "resume").OK)

	var grant *clientv3.LeaseGrantResponse
	var grantErr error
	select {
	case grant = <-grantDone:
		grantErr = <-grantErrDone
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	var final *clientv3.LeaseLeasesResponse
	require.Eventually(t, func() bool {
		var listErr error
		final, listErr = observer.Leases(ctx)
		return listErr == nil && countNewLeaseIDs(final, baselineIDs) == 2
	}, 5*time.Second, 10*time.Millisecond)
	stats := proxy.command(t, "stats")
	newIDs := newLeaseIDs(final, baselineIDs)
	returnedPresent := false
	allLive := len(newIDs) == 2
	for _, id := range newIDs {
		returnedPresent = returnedPresent || grant != nil && id == grant.ID
		ttl, ttlErr := observer.TimeToLive(ctx, id)
		allLive = allLive && ttlErr == nil && ttl.TTL > 0
	}
	after, err := observer.Get(ctx, probeKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, id := range newIDs {
			_, _ = observer.Revoke(cleanupCtx, id)
		}
	})
	orphans := len(newIDs)
	if returnedPresent {
		orphans--
	}
	return leaseGrantResponseLossReplayOutcome{
		ResponseDiscarded:    stats.DroppedBytes > initialStats.DroppedBytes,
		SecondReplicaDialed:  stats.Dials[grpcTarget(endpoints[1])] > 0,
		GrantReturnedSuccess: grantErr == nil && grant != nil,
		NewLeaseCount:        len(newIDs),
		ReturnedLeasePresent: returnedPresent,
		OrphanLeaseCount:     orphans,
		AllNewLeasesLive:     allLive,
		RevisionDelta:        after.Header.Revision - before.Header.Revision,
	}
}

func startExternalL4Proxy(t *testing.T, binary, target string) (*externalL4Proxy, string) {
	t.Helper()
	cmd := exec.Command(binary, "--listen=127.0.0.1:0", "--target="+grpcTarget(target))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	proxy := &externalL4Proxy{stdin: stdin, stdout: bufio.NewScanner(stdout), cmd: cmd}
	cmd.Stderr = &proxy.stderr
	require.NoError(t, cmd.Start())
	require.True(t, proxy.stdout.Scan(), "external proxy exited before ready: %s", proxy.stderr.String())
	var ready externalL4ProxyResponse
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
			t.Errorf("external proxy did not exit after stdin close: %s", proxy.stderr.String())
		}
	})
	return proxy, ready.Endpoint
}

func (p *externalL4Proxy) command(t *testing.T, command string) externalL4ProxyResponse {
	t.Helper()
	_, err := io.WriteString(p.stdin, command+"\n")
	require.NoError(t, err, p.stderr.String())
	require.True(t, p.stdout.Scan(), "external proxy exited after %q: %s", command, p.stderr.String())
	var response externalL4ProxyResponse
	require.NoError(t, json.Unmarshal(p.stdout.Bytes(), &response))
	require.Empty(t, response.Error)
	return response
}
