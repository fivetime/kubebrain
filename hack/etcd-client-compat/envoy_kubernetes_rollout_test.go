package compat

import (
	"context"
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

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	t.Cleanup(cancel)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/dbaas-envoy-kubernetes-rollout/%d", time.Now().UnixNano())
	watchKey, leaseKey := prefix+"/watch", prefix+"/lease"
	watch := client.Watch(ctx, watchKey, clientv3.WithCreatedNotify())
	created := receiveExternalL7WatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	grant, err := client.Grant(ctx, 3)
	require.NoError(t, err)
	_, err = client.Put(ctx, leaseKey, "kept-alive", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	keepAlive, err := client.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	initial := receiveExternalL7KeepAlive(t, ctx, keepAlive, grant.ID)
	require.Positive(t, initial.TTL)
	keepAliveObserved := make(chan envoyKeepAliveObservation, 256)
	keepAliveClosed := make(chan struct{})
	go func() {
		defer close(keepAliveClosed)
		for response := range keepAlive {
			select {
			case keepAliveObserved <- envoyKeepAliveObservation{response: response, at: time.Now()}:
			case <-ctx.Done():
				return
			}
		}
	}()

	var previousWatchRevision int64
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
		kubectl(t, contextName, namespace, "rollout", "status", "deployment/kubebrain-envoy", "--timeout=150s")
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

		watchValue := fmt.Sprintf("after-rollout-%d", cycle)
		_, err = client.Put(ctx, watchKey, watchValue)
		require.NoError(t, err)
		watchResponse := receiveExternalL7WatchEvent(t, ctx, watch)
		require.Len(t, watchResponse.Events, 1)
		require.Equal(t, watchValue, string(watchResponse.Events[0].Kv.Value))
		require.Greater(t, watchResponse.Header.Revision, previousWatchRevision)
		previousWatchRevision = watchResponse.Header.Revision

		var recovered *clientv3.LeaseKeepAliveResponse
		for recovered == nil {
			select {
			case observation := <-keepAliveObserved:
				if !observation.at.Before(rolloutCompletedAt) {
					recovered = observation.response
				}
			case <-keepAliveClosed:
				t.Fatalf("keepalive channel closed during Envoy rollout cycle %d", cycle)
			case <-ctx.Done():
				t.Fatalf("waiting for keepalive after Envoy rollout cycle %d: %v", cycle, ctx.Err())
			}
		}
		require.NotNil(t, recovered)
		require.Equal(t, grant.ID, recovered.ID)
		require.Positive(t, recovered.TTL)
	}
	leaseGet, err := client.Get(ctx, leaseKey)
	require.NoError(t, err)
	require.Len(t, leaseGet.Kvs, 1)
	ttl, err := client.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	require.True(t, channelStillOpenWatch(t, watch))
	select {
	case <-keepAliveClosed:
		t.Fatal("keepalive channel closed after Envoy rollout")
	default:
	}
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
