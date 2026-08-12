package compat

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObservedOldPodsDrainedBeforeDeletionRequiresEveryUID(t *testing.T) {
	oldUIDs := []string{"old-a", "old-b", "old-c"}
	samples := []envoyRolloutSample{
		{
			readyUIDs:   stringSet("old-b", "old-c", "new-a"),
			presentUIDs: stringSet("old-a", "old-b", "old-c", "new-a"),
		},
		{
			readyUIDs:   stringSet("old-c", "new-a", "new-b"),
			presentUIDs: stringSet("old-b", "old-c", "new-a", "new-b"),
		},
		{
			readyUIDs:   stringSet("new-a", "new-b", "new-c"),
			presentUIDs: stringSet("old-c", "new-a", "new-b", "new-c"),
		},
	}
	require.Equal(t, oldUIDs, observedOldPodsDrainedBeforeDeletion(oldUIDs, samples))
	require.Equal(t, 3, minimumReadyEndpointCount(samples))

	missingLastDrain := samples[:2]
	require.Equal(t, []string{"old-a", "old-b"},
		observedOldPodsDrainedBeforeDeletion(oldUIDs, missingLastDrain))
}

func TestMinimumReadyEndpointCountFailsClosedWithoutSamples(t *testing.T) {
	require.Zero(t, minimumReadyEndpointCount(nil))
	require.Equal(t, 1, minimumReadyEndpointCount([]envoyRolloutSample{
		{readyUIDs: stringSet("a", "b")},
		{readyUIDs: stringSet("b")},
	}))
}

func TestParseEnvoyActiveDownstreams(t *testing.T) {
	const stat = "http.kubebrain_downstream.downstream_cx_active"
	require.Equal(t, 7, parseEnvoyActiveDownstreams("HTTP/1.1 200 OK\r\n\r\n"+stat+": 7\n", stat))
	require.Zero(t, parseEnvoyActiveDownstreams(stat+": invalid\n", stat))
	require.Zero(t, parseEnvoyActiveDownstreams("cluster.kubebrain.upstream_cx_active: 7\n", stat))
	const tlsStat = "listener.0.0.0.0_2380.downstream_cx_active"
	require.Equal(t, 3, parseEnvoyActiveDownstreams(tlsStat+": 3\n", tlsStat))
}

func TestParseExactEnvoyStat(t *testing.T) {
	require.Equal(t, 3, parseExactEnvoyStat("cluster.kubebrain.membership_healthy: 3\n",
		"cluster.kubebrain.membership_healthy"))
	require.Zero(t, parseExactEnvoyStat("cluster.kubebrain.membership_healthy: invalid\n",
		"cluster.kubebrain.membership_healthy"))
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
