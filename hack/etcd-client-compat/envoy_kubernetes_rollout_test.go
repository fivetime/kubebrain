package compat

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type envoyRolloutSample struct {
	readyUIDs   map[string]struct{}
	presentUIDs map[string]struct{}
}

type envoyKeepAliveObservation struct {
	response *clientv3.LeaseKeepAliveResponse
	at       time.Time
}

type envoyWatchCohortMember struct {
	client           *clientv3.Client
	watch            clientv3.WatchChan
	key              string
	previousRevision int64
	leaseID          clientv3.LeaseID
	leaseKey         string
	keepAlive        <-chan envoyKeepAliveObservation
	keepAliveClosed  <-chan struct{}
	keepAliveState   *envoyKeepAliveState
}

type envoyKeepAliveState struct {
	mu     sync.Mutex
	latest envoyKeepAliveObservation
}

const envoyKubernetesRolloutLeaseTTL = int64(3)

func TestEnvoyKubernetesRollout(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("KUBEBRAIN_ENVOY_ROLLOUT_ENDPOINT"))
	contextName := strings.TrimSpace(os.Getenv("KUBEBRAIN_ENVOY_ROLLOUT_CONTEXT"))
	namespace := strings.TrimSpace(os.Getenv("KUBEBRAIN_ENVOY_ROLLOUT_NAMESPACE"))
	if endpoint == "" || contextName == "" || namespace == "" {
		t.Skip("set KUBEBRAIN_ENVOY_ROLLOUT_ENDPOINT, KUBEBRAIN_ENVOY_ROLLOUT_CONTEXT, and KUBEBRAIN_ENVOY_ROLLOUT_NAMESPACE")
	}
	rolloutCycles, err := strconv.Atoi(strings.TrimSpace(os.Getenv("KUBEBRAIN_ENVOY_ROLLOUT_CYCLES")))
	require.NoError(t, err)
	require.GreaterOrEqual(t, rolloutCycles, 1)
	require.LessOrEqual(t, rolloutCycles, 10)
	tlsEnabled := strings.TrimSpace(os.Getenv("KUBEBRAIN_ENVOY_ROLLOUT_TLS")) == "true"
	tlsRotationCommand := strings.TrimSpace(os.Getenv("KUBEBRAIN_ENVOY_ROLLOUT_TLS_ROTATION_COMMAND"))
	var clientTLS *tls.Config
	var rotatedClientTLS *tls.Config
	var retiredCAClientTLS *tls.Config
	var initialServerCertificate string
	activeDownstreamStat := "http.kubebrain_downstream.downstream_cx_active"
	if tlsEnabled {
		clientTLS = loadExternalL4ClientTLS(t, "KUBERNETES_ENVOY_ROLLOUT")
		activeDownstreamStat = "listener.0.0.0.0_2380.downstream_cx_active"
		initialServerCertificate = envoyServerCertificateFingerprint(t, endpoint, clientTLS)
		if strings.TrimSpace(os.Getenv("KUBERNETES_ENVOY_ROTATED_TLS_CA_FILE")) != "" {
			require.NotEmpty(t, tlsRotationCommand, "rotated TLS credentials require the rotation overlap command")
			rotatedClientTLS = loadExternalL4ClientTLS(t, "KUBERNETES_ENVOY_ROTATED")
			retiredCAClientTLS = clientTLS.Clone()
			retiredCAClientTLS.RootCAs = loadEnvoyCertificatePool(t,
				strings.TrimSpace(os.Getenv("KUBERNETES_ENVOY_RETIRED_TLS_CA_FILE")))
		}
	} else {
		require.Empty(t, tlsRotationCommand, "TLS rotation overlap requires the TLS passthrough profile")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	t.Cleanup(cancel)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, TLS: clientTLS})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/dbaas-envoy-kubernetes-rollout/%d", time.Now().UnixNano())
	watchKey, leaseKey := prefix+"/watch", prefix+"/lease"
	watch := client.Watch(ctx, watchKey, clientv3.WithCreatedNotify())
	created := receiveExternalL7WatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	grant, err := client.Grant(ctx, envoyKubernetesRolloutLeaseTTL)
	require.NoError(t, err)
	_, err = client.Put(ctx, leaseKey, "kept-alive", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	keepAlive, err := client.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initial := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	require.Positive(t, initial.TTL)
	keepAliveObserved, keepAliveClosed, keepAliveState := observeEnvoyKeepAlive(ctx, keepAlive)

	cohort := []*envoyWatchCohortMember{{
		client: client, watch: watch, key: watchKey,
		leaseID: grant.ID, leaseKey: leaseKey,
		keepAlive: keepAliveObserved, keepAliveClosed: keepAliveClosed, keepAliveState: keepAliveState,
	}}
	for member := 1; member < 30 && !allEnvoyPodsHaveActiveDownstreams(t, contextName, namespace, activeDownstreamStat); member++ {
		cohortClient, createErr := clientv3.New(clientv3.Config{
			Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, TLS: clientTLS,
		})
		require.NoError(t, createErr)
		t.Cleanup(func() { require.NoError(t, cohortClient.Close()) })
		cohortKey := fmt.Sprintf("%s/watch-cohort-%d", prefix, member)
		cohortWatch := cohortClient.Watch(ctx, cohortKey, clientv3.WithCreatedNotify())
		cohortCreated := receiveExternalL7WatchResponse(t, ctx, cohortWatch)
		require.True(t, cohortCreated.Created)
		cohortGrant, grantErr := cohortClient.Grant(ctx, envoyKubernetesRolloutLeaseTTL)
		require.NoError(t, grantErr)
		cohortLeaseKey := fmt.Sprintf("%s/lease-cohort-%d", prefix, member)
		_, putErr := cohortClient.Put(ctx, cohortLeaseKey, "kept-alive", clientv3.WithLease(cohortGrant.ID))
		require.NoError(t, putErr)
		cohortKeepAlive, keepAliveErr := cohortClient.KeepAlive(ctx, cohortGrant.ID)
		require.NoError(t, keepAliveErr)
		cohortInitial := receiveExternalL7KeepAlive(t, ctx, cohortKeepAlive, cohortGrant.ID)
		require.Positive(t, cohortInitial.TTL)
		cohortObserved, cohortClosed, cohortState := observeEnvoyKeepAlive(ctx, cohortKeepAlive)
		cohort = append(cohort, &envoyWatchCohortMember{
			client: cohortClient, watch: cohortWatch, key: cohortKey,
			leaseID: cohortGrant.ID, leaseKey: cohortLeaseKey,
			keepAlive: cohortObserved, keepAliveClosed: cohortClosed, keepAliveState: cohortState,
		})
	}
	require.True(t, allEnvoyPodsHaveActiveDownstreams(t, contextName, namespace, activeDownstreamStat),
		"client cohort must place at least one active downstream connection on every Envoy Pod")
	require.GreaterOrEqual(t, len(cohort), 3)
	t.Logf("established %d long-lived Watch clients across all three Envoy Pods", len(cohort))
	if tlsEnabled {
		assertEnvoyKubernetesTLSRejectsInvalidClients(t, endpoint, clientTLS, "before-rollout")
		require.Eventually(t, func() bool {
			return allEnvoyPodsHaveHealthyUpstreams(contextName, namespace)
		}, 75*time.Second, 2*time.Second,
			"all Envoy Pods must recover three healthy upstreams after rejected TLS probes")
	}

	for cycle := 1; cycle <= rolloutCycles; cycle++ {
		oldUIDs := envoyPodUIDs(t, contextName, namespace)
		require.Len(t, oldUIDs, 3)
		require.Equal(t, oldUIDs, envoyReadyEndpointUIDs(t, contextName, namespace))

		monitorCtx, stopMonitor := context.WithCancel(ctx)
		var samplesMu sync.Mutex
		var samples []envoyRolloutSample
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			// The preStop hook holds every drained Pod for ten seconds. A 250ms
			// cadence gives forty observation opportunities per replica without
			// making race-enabled clients compete with twenty kubectl processes/s.
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-monitorCtx.Done():
					return
				case <-ticker.C:
					ready := envoyReadyEndpointUIDsNoFail(contextName, namespace)
					present := envoyPresentPodUIDsNoFail(contextName, namespace)
					if ready != nil && present != nil {
						samplesMu.Lock()
						samples = append(samples, envoyRolloutSample{readyUIDs: ready, presentUIDs: present})
						samplesMu.Unlock()
					}
				}
			}
		}()

		kubectl(t, contextName, namespace, "rollout", "restart", "deployment/kubebrain-envoy")
		if cycle == 1 && tlsRotationCommand != "" {
			command := exec.CommandContext(ctx, tlsRotationCommand)
			command.Env = append(os.Environ(),
				"KUBEBRAIN_ENVOY_ROLLOUT_HOOK_CONTEXT="+contextName,
				"KUBEBRAIN_ENVOY_ROLLOUT_HOOK_NAMESPACE="+namespace,
				"KUBEBRAIN_ENVOY_ROLLOUT_HOOK_ENDPOINT="+endpoint,
			)
			output, commandErr := command.CombinedOutput()
			require.NoErrorf(t, commandErr, "TLS rotation overlap command failed: %s", strings.TrimSpace(string(output)))
			overlapUIDs := envoyPodUIDs(t, contextName, namespace)
			require.NotEmpty(t, intersectStrings(oldUIDs, overlapUIDs),
				"TLS rotation command must finish while at least one old Envoy Pod remains")
			require.Greater(t, len(overlapUIDs), len(intersectStrings(oldUIDs, overlapUIDs)),
				"TLS rotation command must finish after at least one new Envoy Pod has appeared")
			t.Logf("TLS rotation overlap command completed during rollout cycle 1: %s", strings.TrimSpace(string(output)))
		}
		kubectl(t, contextName, namespace, "rollout", "status", "deployment/kubebrain-envoy", "--timeout=150s")
		require.Eventually(t, func() bool {
			present := envoyPresentPodUIDsNoFail(contextName, namespace)
			return present != nil && len(intersectStrings(oldUIDs, sortedSet(present))) == 0
		}, 60*time.Second, 250*time.Millisecond,
			"all old Envoy Pod UIDs must finish preStop and be deleted in rollout cycle %d", cycle)
		rolloutCompletedAt := time.Now()
		stopMonitor()
		<-monitorDone

		newUIDs := envoyPodUIDs(t, contextName, namespace)
		require.Len(t, newUIDs, 3)
		require.Equal(t, newUIDs, envoyReadyEndpointUIDs(t, contextName, namespace))
		require.Empty(t, intersectStrings(oldUIDs, newUIDs), "rollout cycle %d must replace every Envoy Pod identity", cycle)

		samplesMu.Lock()
		drainedOldUIDs := observedOldPodsDrainedBeforeDeletion(oldUIDs, samples)
		minimumReady := minimumReadyEndpointCount(samples)
		samplesMu.Unlock()
		require.Equal(t, oldUIDs, drainedOldUIDs,
			"every old Envoy Pod UID in rollout cycle %d must drain before deletion", cycle)
		require.GreaterOrEqual(t, minimumReady, 2,
			"rollout cycle %d must retain at least two ready Envoy EndpointSlice targets", cycle)

		for memberIndex, member := range cohort {
			watchValue := fmt.Sprintf("after-rollout-%d-member-%d", cycle, memberIndex)
			_, err = member.client.Put(ctx, member.key, watchValue)
			require.NoError(t, err)
			watchResponse := receiveExternalL7WatchEvent(t, ctx, member.watch)
			require.Len(t, watchResponse.Events, 1)
			require.Equal(t, watchValue, string(watchResponse.Events[0].Kv.Value))
			require.Greater(t, watchResponse.Header.Revision, member.previousRevision)
			member.previousRevision = watchResponse.Header.Revision
		}

		for memberIndex, member := range cohort {
			recovered := receiveEnvoyKeepAliveAfter(t, ctx, member, rolloutCompletedAt, cycle, memberIndex)
			require.Equal(t, member.leaseID, recovered.ID)
			require.Positive(t, recovered.TTL)
		}
	}
	for _, member := range cohort {
		leaseGet, getErr := member.client.Get(ctx, member.leaseKey)
		require.NoError(t, getErr)
		require.Len(t, leaseGet.Kvs, 1)
		ttl, ttlErr := member.client.TimeToLive(ctx, member.leaseID)
		require.NoError(t, ttlErr)
		require.Positive(t, ttl.TTL)
		require.True(t, channelStillOpenWatch(t, member.watch))
		select {
		case <-member.keepAliveClosed:
			t.Fatal("cohort keepalive channel closed after Envoy rollout")
		default:
		}
	}
	if tlsEnabled {
		if tlsRotationCommand != "" {
			probeTLS := clientTLS
			if rotatedClientTLS != nil {
				probeTLS = rotatedClientTLS
			}
			rotatedServerCertificate := envoyServerCertificateFingerprint(t, endpoint, probeTLS)
			require.NotEqual(t, initialServerCertificate, rotatedServerCertificate,
				"TLS rotation command must replace the server leaf certificate observed through Envoy")
		}
		if rotatedClientTLS != nil {
			assertEnvoyKubernetesTLSClientWorks(t, endpoint, rotatedClientTLS, "/dbaas-envoy-kubernetes-rotated-ca")
			assertEnvoyKubernetesTLSHandshakeRejected(t, endpoint, retiredCAClientTLS)
			assertEnvoyKubernetesTLSClientWorks(t, endpoint, rotatedClientTLS, "/dbaas-envoy-kubernetes-rotated-ca-after-rejection")
		}
		assertEnvoyKubernetesTLSRejectsInvalidClients(t, endpoint, clientTLS, "after-rollout")
	}
}

func loadEnvoyCertificatePool(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	require.NotEmpty(t, path)
	pem, err := os.ReadFile(path)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pem))
	return pool
}

func assertEnvoyKubernetesTLSClientWorks(t *testing.T, endpoint string, clientTLS *tls.Config, key string) {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, TLS: clientTLS})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(probeCtx, key, "accepted")
	require.NoError(t, err)
}

func assertEnvoyKubernetesTLSHandshakeRejected(t *testing.T, endpoint string, clientTLS *tls.Config) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := (&tls.Dialer{Config: clientTLS}).DialContext(probeCtx, "tcp", endpoint)
	if connection != nil {
		_ = connection.Close()
	}
	require.Error(t, err, "a client trusting only the retired CA must reject the rotated server leaf")
}

func envoyServerCertificateFingerprint(t *testing.T, endpoint string, clientTLS *tls.Config) string {
	t.Helper()
	probeTLS := clientTLS.Clone()
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := (&tls.Dialer{Config: probeTLS}).DialContext(probeCtx, "tcp", endpoint)
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.Close()) }()
	tlsConnection, ok := connection.(*tls.Conn)
	require.True(t, ok)
	certificates := tlsConnection.ConnectionState().PeerCertificates
	require.NotEmpty(t, certificates)
	digest := sha256.Sum256(certificates[0].Raw)
	return hex.EncodeToString(digest[:])
}

func allEnvoyPodsHaveHealthyUpstreams(contextName, namespace string) bool {
	podsOutput, err := kubectlOutput(contextName, namespace, "get", "pods", "-l",
		"app.kubernetes.io/name=kubebrain-envoy,app.kubernetes.io/instance=kubebrain",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}")
	if err != nil {
		return false
	}
	pods := sortedNonEmptyLines(podsOutput)
	if len(pods) != 3 {
		return false
	}
	for _, pod := range pods {
		output, outputErr := kubectlOutput(contextName, namespace, "exec", pod, "--", "/bin/bash", "-ec",
			"exec 3<>/dev/tcp/127.0.0.1/9901; printf 'GET /stats HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3; cat <&3")
		if outputErr != nil || parseExactEnvoyStat(output, "cluster.kubebrain.membership_healthy") != 3 ||
			parseExactEnvoyStat(output, "cluster.kubebrain.outlier_detection.ejections_active") != 0 {
			return false
		}
	}
	return true
}

func parseExactEnvoyStat(output, statName string) int {
	return parseEnvoyActiveDownstreams(output, statName)
}

func assertEnvoyKubernetesTLSRejectsInvalidClients(t *testing.T, endpoint string, validTLS *tls.Config, phase string) {
	t.Helper()
	withoutCertificate := validTLS.Clone()
	withoutCertificate.Certificates = nil
	for name, tlsConfig := range map[string]*tls.Config{
		"missing-client-certificate": withoutCertificate,
		"plaintext":                  nil,
	} {
		t.Run(phase+"/"+name, func(t *testing.T) {
			client, err := clientv3.New(clientv3.Config{
				Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second, TLS: tlsConfig,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			probeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = client.Get(probeCtx, "/dbaas-envoy-kubernetes-tls/rejected-"+phase+"-"+name)
			require.Error(t, err, "%s client must be rejected through Envoy 2380 during %s", name, phase)
		})
	}
}

func observeEnvoyKeepAlive(ctx context.Context, keepAlive <-chan *clientv3.LeaseKeepAliveResponse) (
	<-chan envoyKeepAliveObservation, <-chan struct{}, *envoyKeepAliveState,
) {
	observed := make(chan envoyKeepAliveObservation, 256)
	closed := make(chan struct{})
	state := &envoyKeepAliveState{}
	go func() {
		defer close(closed)
		for response := range keepAlive {
			observation := envoyKeepAliveObservation{response: response, at: time.Now()}
			state.mu.Lock()
			state.latest = observation
			state.mu.Unlock()
			select {
			case observed <- observation:
			case <-ctx.Done():
				return
			}
		}
	}()
	return observed, closed, state
}

func receiveEnvoyKeepAliveAfter(t *testing.T, ctx context.Context, cohortMember *envoyWatchCohortMember,
	after time.Time, cycle, member int,
) *clientv3.LeaseKeepAliveResponse {
	t.Helper()
	for {
		select {
		case observation := <-cohortMember.keepAlive:
			if !observation.at.Before(after) {
				require.NotNil(t, observation.response)
				return observation.response
			}
		case <-cohortMember.keepAliveClosed:
			cohortMember.keepAliveState.mu.Lock()
			latest := cohortMember.keepAliveState.latest
			cohortMember.keepAliveState.mu.Unlock()
			ttlCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			ttl, ttlErr := cohortMember.client.TimeToLive(ttlCtx, cohortMember.leaseID)
			cancel()
			t.Fatalf("keepalive channel for cohort member %d closed during Envoy rollout cycle %d; latest response at %s ttl=%v; server TTL=%v error=%v",
				member, cycle, latest.at.Format(time.RFC3339Nano), keepAliveResponseTTL(latest.response), leaseTTL(ttl), ttlErr)
		case <-ctx.Done():
			t.Fatalf("waiting for cohort member %d keepalive after Envoy rollout cycle %d: %v", member, cycle, ctx.Err())
		}
	}
}

func keepAliveResponseTTL(response *clientv3.LeaseKeepAliveResponse) any {
	if response == nil {
		return nil
	}
	return response.TTL
}

func leaseTTL(response *clientv3.LeaseTimeToLiveResponse) any {
	if response == nil {
		return nil
	}
	return response.TTL
}

func allEnvoyPodsHaveActiveDownstreams(t *testing.T, contextName, namespace, statName string) bool {
	t.Helper()
	pods := envoyPodNames(t, contextName, namespace)
	require.Len(t, pods, 3)
	for _, pod := range pods {
		output := kubectl(t, contextName, namespace, "exec", pod, "--", "/bin/bash", "-ec",
			fmt.Sprintf("exec 3<>/dev/tcp/127.0.0.1/9901; printf 'GET /stats?filter=%s HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3; cat <&3", statName))
		if parseEnvoyActiveDownstreams(output, statName) < 1 {
			return false
		}
	}
	return true
}

func parseEnvoyActiveDownstreams(output, statName string) int {
	prefix := statName + ": "
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			value, err := strconv.Atoi(strings.TrimPrefix(line, prefix))
			if err == nil {
				return value
			}
		}
	}
	return 0
}

func observedOldPodsDrainedBeforeDeletion(oldUIDs []string, samples []envoyRolloutSample) []string {
	drained := map[string]struct{}{}
	for _, sample := range samples {
		for _, uid := range oldUIDs {
			_, present := sample.presentUIDs[uid]
			_, ready := sample.readyUIDs[uid]
			if present && !ready {
				drained[uid] = struct{}{}
			}
		}
	}
	return sortedSet(drained)
}

func minimumReadyEndpointCount(samples []envoyRolloutSample) int {
	if len(samples) == 0 {
		return 0
	}
	minimum := len(samples[0].readyUIDs)
	for _, sample := range samples[1:] {
		if count := len(sample.readyUIDs); count < minimum {
			minimum = count
		}
	}
	return minimum
}

func envoyPodUIDs(t *testing.T, contextName, namespace string) []string {
	t.Helper()
	output := kubectl(t, contextName, namespace, "get", "pods", "-l",
		"app.kubernetes.io/name=kubebrain-envoy,app.kubernetes.io/instance=kubebrain",
		"-o", "jsonpath={range .items[*]}{.metadata.uid}{'\\n'}{end}")
	return sortedNonEmptyLines(output)
}

func envoyPodNames(t *testing.T, contextName, namespace string) []string {
	t.Helper()
	output := kubectl(t, contextName, namespace, "get", "pods", "-l",
		"app.kubernetes.io/name=kubebrain-envoy,app.kubernetes.io/instance=kubebrain",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}")
	return sortedNonEmptyLines(output)
}

func envoyReadyEndpointUIDs(t *testing.T, contextName, namespace string) []string {
	t.Helper()
	return sortedSet(envoyReadyEndpointUIDsNoFail(contextName, namespace))
}

func envoyReadyEndpointUIDsNoFail(contextName, namespace string) map[string]struct{} {
	output, err := kubectlOutput(contextName, namespace, "get", "endpointslice", "-l",
		"kubernetes.io/service-name=kubebrain-envoy",
		"-o", "jsonpath={range .items[*].endpoints[*]}{.targetRef.uid}{'\\t'}{.conditions.ready}{'\\n'}{end}")
	if err != nil {
		return nil
	}
	result := map[string]struct{}{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == "true" {
			result[fields[0]] = struct{}{}
		}
	}
	return result
}

func envoyPresentPodUIDsNoFail(contextName, namespace string) map[string]struct{} {
	output, err := kubectlOutput(contextName, namespace, "get", "pods", "-l",
		"app.kubernetes.io/name=kubebrain-envoy,app.kubernetes.io/instance=kubebrain",
		"-o", "jsonpath={range .items[*]}{.metadata.uid}{'\\n'}{end}")
	if err != nil {
		return nil
	}
	result := map[string]struct{}{}
	for _, uid := range sortedNonEmptyLines(output) {
		result[uid] = struct{}{}
	}
	return result
}

func kubectl(t *testing.T, contextName, namespace string, args ...string) string {
	t.Helper()
	output, err := kubectlOutput(contextName, namespace, args...)
	require.NoError(t, err, "kubectl output: %s", output)
	return output
}

func kubectlOutput(contextName, namespace string, args ...string) (string, error) {
	commandArgs := []string{"--context", contextName, "--namespace", namespace}
	commandArgs = append(commandArgs, args...)
	output, err := exec.Command("kubectl", commandArgs...).CombinedOutput()
	return string(output), err
}

func sortedNonEmptyLines(output string) []string {
	var result []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}
	sort.Strings(result)
	return result
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func intersectStrings(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightSet[value] = struct{}{}
	}
	var result []string
	for _, value := range left {
		if _, ok := rightSet[value]; ok {
			result = append(result, value)
		}
	}
	return result
}
