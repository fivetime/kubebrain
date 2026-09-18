package testcluster_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

func protocolTrustInput(t *testing.T) map[string]any {
	t.Helper()
	in := memberTrustInput(t)
	p, err := trustPlan(t, in)
	require.NoError(t, err, string(p))
	in["current"] = applyTrustPatch(t, in["current"], p)
	in["phase"] = "protocol"
	in["candidate_image"] = "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("a", 64)
	return in
}

func TestPeerProtocolPlanChangesOnlyImageAndOptInAndRestoresAdjacent(t *testing.T) {
	in := protocolTrustInput(t)
	before := cloneTrustObject(t, trustMap(in, "current"))
	p, err := trustPlan(t, in)
	require.NoError(t, err, string(p))
	require.NotContains(t, string(p), "member-0-tls.key")
	actual := applyTrustPatch(t, in["current"], p)
	expected := cloneTrustObject(t, before)
	c := trustMap(expected, "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)
	c["image"] = in["candidate_image"]
	c["args"] = append(c["args"].([]any), "--experimental-peer-retirement-config=/etc/kubebrain/peer-tls/policy.json")
	require.Equal(t, expected, actual)
	in["current"] = actual
	p, err = trustPlan(t, in)
	require.NoError(t, err, string(p))
	require.JSONEq(t, "[]", string(p))
	// Rollback must work after an incomplete candidate rollout, preserving
	// member private-key isolation and dual roots until later adjacent phases.
	trustMap(actual, "status")["readyReplicas"] = 1
	in["mode"] = "restore"
	p, err = trustPlan(t, in)
	require.NoError(t, err, string(p))
	restored := applyTrustPatch(t, actual, p)
	require.Equal(t, before["spec"], restored["spec"])
	require.NotEqual(t, trustMap(in, "baseline")["spec"], restored["spec"])
	in["current"] = restored
	p, err = trustPlan(t, in)
	require.NoError(t, err, string(p))
	require.JSONEq(t, "[]", string(p))
}

func TestPeerProtocolPlanRejectsUnsafeInputs(t *testing.T) {
	cases := map[string]func(map[string]any){
		"single CA baseline": func(in map[string]any) { in["current"] = cloneTrustObject(t, trustMap(in, "baseline")) },
		"shared old leaf":    func(in map[string]any) { in["current"] = memberTrustInput(t)["current"] },
		"not ready":          func(in map[string]any) { trustMap(in, "current", "status")["readyReplicas"] = 2 },
		"unobserved":         func(in map[string]any) { trustMap(in, "current", "status")["observedGeneration"] = 0 },
		"mixed revision":     func(in map[string]any) { trustMap(in, "current", "status")["updateRevision"] = "other" },
		"missing image":      func(in map[string]any) { delete(in, "candidate_image") },
		"mutable tag":        func(in map[string]any) { in["candidate_image"] = "ghcr.io/fivetime/kubebrain:dbaas" },
		"other registry": func(in map[string]any) {
			in["candidate_image"] = "example.org/kubebrain@sha256:" + strings.Repeat("a", 64)
		},
		"invalid digest": func(in map[string]any) { in["candidate_image"] = "ghcr.io/fivetime/kubebrain@sha256:abcd" },
		"original image": func(in map[string]any) {
			in["candidate_image"] = trustMap(in, "baseline", "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)["image"]
		},
		"recreated member secret": func(in map[string]any) { trustMap(in, "live_member_secret", "metadata")["uid"] = "other" },
		"changed election args": func(in map[string]any) {
			c := trustMap(in, "current", "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)
			c["args"] = append(c["args"].([]any), "--leader-lease-duration=1s")
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := protocolTrustInput(t)
			mutate(in)
			p, err := trustPlan(t, in)
			require.Error(t, err, string(p))
		})
	}
}

func TestPeerProtocolPlanCannotSkipRollbackOrConcurrentGuards(t *testing.T) {
	for _, phase := range []string{"roots", "members"} {
		t.Run("skip rollback to "+phase, func(t *testing.T) {
			in := protocolTrustInput(t)
			p, err := trustPlan(t, in)
			require.NoError(t, err, string(p))
			in["current"] = applyTrustPatch(t, in["current"], p)
			in["phase"], in["mode"] = phase, "restore"
			p, err = trustPlan(t, in)
			require.Error(t, err, string(p))
		})
	}
	for _, field := range []string{"uid", "resourceVersion", "spec"} {
		t.Run(field, func(t *testing.T) {
			in := protocolTrustInput(t)
			p, err := trustPlan(t, in)
			require.NoError(t, err, string(p))
			patch, err := jsonpatch.DecodePatch(p)
			require.NoError(t, err)
			if field == "spec" {
				trustMap(in, "current", "spec")["replicas"] = 2
			} else {
				trustMap(in, "current", "metadata")[field] = "changed"
			}
			raw, err := json.Marshal(in["current"])
			require.NoError(t, err)
			_, err = patch.Apply(raw)
			require.Error(t, err)
		})
	}
}
