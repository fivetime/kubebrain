package testcluster_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

func memberTrustInput(t *testing.T) map[string]any {
	t.Helper()
	in := trustPlanInput(t)
	patch, err := trustPlan(t, in)
	require.NoError(t, err, string(patch))
	in["current"] = applyTrustPatch(t, in["current"], patch)
	in["phase"] = "members"
	data := map[string]any{}
	for i := 0; i < 3; i++ {
		member := fmt.Sprintf("kubebrain-local-%d", i)
		for _, file := range []string{"tls.crt", "tls.key", "policy.json"} {
			data[member+"."+file] = fmt.Sprintf("member-%d-%s", i, file)
		}
		data[member+".ca.crt"] = trustMap(in, "expanded_secret", "data")["ca.crt"]
	}
	secret := map[string]any{"kind": "Secret", "type": "Opaque", "immutable": true, "metadata": map[string]any{"name": "peer-members-dual-test", "namespace": "kubebrain-dbaas-test", "uid": "members-uid", "resourceVersion": "33"}, "data": data}
	in["member_secret"] = secret
	in["live_member_secret"] = cloneTrustObject(t, secret)
	return in
}

func applyTrustPatch(t *testing.T, object any, patch []byte) map[string]any {
	t.Helper()
	p, err := jsonpatch.DecodePatch(patch)
	require.NoError(t, err)
	raw, err := json.Marshal(object)
	require.NoError(t, err)
	result, err := p.Apply(raw)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(result, &decoded))
	return decoded
}

func TestPeerTrustMembersOnlyChangesApprovedIsolatedMounts(t *testing.T) {
	in := memberTrustInput(t)
	from := cloneTrustObject(t, trustMap(in, "current"))
	out, err := trustPlan(t, in)
	require.NoError(t, err, string(out))
	require.NotContains(t, string(out), "member-0-tls.key")
	actual := applyTrustPatch(t, in["current"], out)
	fragment, err := os.ReadFile("peer-retirement-mounts.patch.json")
	require.NoError(t, err)
	var change map[string]any
	require.NoError(t, json.Unmarshal(fragment, &change))
	volume := trustMap(change, "spec", "template", "spec")["volumes"].([]any)[0].(map[string]any)
	trustMap(volume, "secret")["secretName"] = "peer-members-dual-test"
	fragment, err = json.Marshal(change)
	require.NoError(t, err)
	raw, err := json.Marshal(from)
	require.NoError(t, err)
	merged, err := strategicpatch.StrategicMergePatch(raw, fragment, appsv1.StatefulSet{})
	require.NoError(t, err)
	var expected map[string]any
	require.NoError(t, json.Unmarshal(merged, &expected))
	require.Equal(t, expected, actual, "member phase must change exactly the already-tested mount fragment, never image or args")
	in["current"] = actual
	out, err = trustPlan(t, in)
	require.NoError(t, err, string(out))
	require.JSONEq(t, "[]", string(out))
	// A failed member rollout still permits adjacent rollback to shared/dual.
	trustMap(actual, "status")["readyReplicas"] = 1
	in["mode"] = "restore"
	out, err = trustPlan(t, in)
	require.NoError(t, err, string(out))
	restored := applyTrustPatch(t, actual, out)
	require.Equal(t, from["spec"], restored["spec"])
	require.NotEqual(t, trustMap(in, "baseline")["spec"], restored["spec"], "rollback must keep dual roots")
}

func TestPeerTrustMembersRejectsSkippedPhasesAndIdentityDrift(t *testing.T) {
	cases := map[string]func(map[string]any){
		"skip dual roots":  func(in map[string]any) { in["current"] = cloneTrustObject(t, trustMap(in, "baseline")) },
		"not all ready":    func(in map[string]any) { trustMap(in, "current", "status")["readyReplicas"] = 2 },
		"recreated secret": func(in map[string]any) { trustMap(in, "live_member_secret", "metadata")["uid"] = "other" },
		"single new root": func(in map[string]any) {
			for _, key := range []string{"member_secret", "live_member_secret"} {
				trustMap(in, key, "data")["kubebrain-local-1.ca.crt"] = "new-root-only"
			}
		},
		"duplicate key": func(in map[string]any) {
			for _, key := range []string{"member_secret", "live_member_secret"} {
				d := trustMap(in, key, "data")
				d["kubebrain-local-1.tls.key"] = d["kubebrain-local-0.tls.key"]
			}
		},
		"ca private key": func(in map[string]any) {
			for _, key := range []string{"member_secret", "live_member_secret"} {
				trustMap(in, key, "data")["ca.key"] = "private"
			}
		},
		"missing policy": func(in map[string]any) {
			for _, key := range []string{"member_secret", "live_member_secret"} {
				delete(trustMap(in, key, "data"), "kubebrain-local-1.policy.json")
			}
		},
		"mutable secret": func(in map[string]any) {
			for _, key := range []string{"member_secret", "live_member_secret"} {
				trustMap(in, key)["immutable"] = false
			}
		},
		"image drift": func(in map[string]any) {
			trustMap(in, "current", "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)["image"] = "other"
		},
		"invalid phase type": func(in map[string]any) { in["phase"] = false },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := memberTrustInput(t)
			mutate(in)
			out, err := trustPlan(t, in)
			require.Error(t, err, string(out))
		})
	}
	// The roots planner refuses to jump from member keys to original single CA.
	in := memberTrustInput(t)
	out, err := trustPlan(t, in)
	require.NoError(t, err, string(out))
	in["current"] = applyTrustPatch(t, in["current"], out)
	in["phase"] = "roots"
	in["mode"] = "restore"
	out, err = trustPlan(t, in)
	require.Error(t, err, string(out))
}

func TestPeerTrustMemberPatchRejectsConcurrentChanges(t *testing.T) {
	for _, field := range []string{"uid", "resourceVersion", "spec"} {
		t.Run(field, func(t *testing.T) {
			in := memberTrustInput(t)
			out, err := trustPlan(t, in)
			require.NoError(t, err, string(out))
			p, err := jsonpatch.DecodePatch(out)
			require.NoError(t, err)
			if field == "spec" {
				trustMap(in, "current", "spec")["replicas"] = 2
			} else {
				trustMap(in, "current", "metadata")[field] = "changed"
			}
			data, err := json.Marshal(in["current"])
			require.NoError(t, err)
			_, err = p.Apply(data)
			require.Error(t, err)
		})
	}
}
