package testcluster_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

func trustPlanInput(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile("kubebrain-local.json")
	require.NoError(t, err)
	var list struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(data, &list))
	var base map[string]any
	for _, item := range list.Items {
		if item["kind"] == "StatefulSet" {
			base = item
		}
	}
	require.NotNil(t, base)
	meta := base["metadata"].(map[string]any)
	meta["uid"], meta["resourceVersion"], meta["generation"] = "sts-uid", "100", 38
	base["status"] = map[string]any{"observedGeneration": 38, "readyReplicas": 3, "updatedReplicas": 3, "currentRevision": "revision", "updateRevision": "revision"}
	secret := func(name, uid, roots string, immutable bool) map[string]any {
		return map[string]any{"kind": "Secret", "type": "Opaque", "immutable": immutable,
			"metadata": map[string]any{"name": name, "namespace": meta["namespace"], "uid": uid, "resourceVersion": "22"},
			"data":     map[string]any{"tls.crt": "fake-leaf", "tls.key": "fake-key", "ca.crt": roots}}
	}
	return cloneTrustObject(t, map[string]any{
		"mode": "expand", "namespace_uid": "namespace-uid",
		"namespace": map[string]any{"kind": "Namespace", "metadata": map[string]any{"name": meta["namespace"], "uid": "namespace-uid"}},
		"baseline":  base, "current": base,
		"original_secret":      secret("kubebrain-local-peer-tls", "old-uid", "old-roots", false),
		"live_original_secret": secret("kubebrain-local-peer-tls", "old-uid", "old-roots", false),
		"expanded_secret":      secret("peer-old-dual-test", "dual-uid", "dual-roots", true),
		"live_expanded_secret": secret("peer-old-dual-test", "dual-uid", "dual-roots", true),
	})
}

func cloneTrustObject(t *testing.T, in map[string]any) map[string]any {
	t.Helper()
	data, err := json.Marshal(in)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(data, &out))
	return out
}

func trustPlan(t *testing.T, in map[string]any) ([]byte, error) {
	t.Helper()
	data, err := json.Marshal(in)
	require.NoError(t, err)
	cmd := exec.Command("jq", "-er", "-f", "peer-trust-expand-plan.jq")
	cmd.Stdin = bytes.NewReader(data)
	return cmd.CombinedOutput()
}

func trustMap(in map[string]any, path ...string) map[string]any {
	for _, key := range path {
		in = in[key].(map[string]any)
	}
	return in
}

func TestPeerTrustExpansionAndFailedRolloutRestoration(t *testing.T) {
	in := trustPlanInput(t)
	output, err := trustPlan(t, in)
	require.NoError(t, err, string(output))
	require.NotContains(t, string(output), "fake-key")
	patch, err := jsonpatch.DecodePatch(output)
	require.NoError(t, err)
	original, err := json.Marshal(in["current"])
	require.NoError(t, err)
	expanded, err := patch.Apply(original)
	require.NoError(t, err)
	var current map[string]any
	require.NoError(t, json.Unmarshal(expanded, &current))
	// Exactly one volume Secret name changes. Everything else is retained.
	spec := trustMap(current, "spec", "template", "spec")
	count := 0
	for _, volume := range spec["volumes"].([]any) {
		v := volume.(map[string]any)
		if v["name"] == "peer-tls" {
			require.Equal(t, "peer-old-dual-test", trustMap(v, "secret")["secretName"])
			trustMap(v, "secret")["secretName"] = "kubebrain-local-peer-tls"
			count++
		}
	}
	require.Equal(t, 1, count)
	require.Equal(t, in["current"], current)
	require.NoError(t, json.Unmarshal(expanded, &current))
	in["current"] = current
	output, err = trustPlan(t, in)
	require.NoError(t, err, string(output))
	require.JSONEq(t, "[]", string(output))
	// Non-ready first-stage rollback is permitted; it changes no later phase.
	trustMap(current, "status")["readyReplicas"] = 1
	trustMap(current, "metadata")["resourceVersion"] = "101"
	in["mode"] = "restore"
	output, err = trustPlan(t, in)
	require.NoError(t, err, string(output))
	patch, err = jsonpatch.DecodePatch(output)
	require.NoError(t, err)
	data, err := json.Marshal(current)
	require.NoError(t, err)
	restored, err := patch.Apply(data)
	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal(restored, &result))
	require.Equal(t, trustMap(in, "baseline")["spec"], result["spec"])
	in["current"] = result
	output, err = trustPlan(t, in)
	require.NoError(t, err, string(output))
	require.JSONEq(t, "[]", string(output))
}

func TestPeerTrustExpansionRejectsDrift(t *testing.T) {
	cases := map[string]func(map[string]any){
		"namespace recreated":   func(in map[string]any) { trustMap(in, "namespace", "metadata")["uid"] = "other" },
		"sts recreated":         func(in map[string]any) { trustMap(in, "current", "metadata")["uid"] = "other" },
		"sts terminating":       func(in map[string]any) { trustMap(in, "current", "metadata")["deletionTimestamp"] = "now" },
		"replicas changed":      func(in map[string]any) { trustMap(in, "current", "spec")["replicas"] = 2 },
		"not ready":             func(in map[string]any) { trustMap(in, "current", "status")["readyReplicas"] = 2 },
		"old secret changed":    func(in map[string]any) { trustMap(in, "live_original_secret", "data")["ca.crt"] = "different" },
		"new secret recreated":  func(in map[string]any) { trustMap(in, "live_expanded_secret", "metadata")["uid"] = "other" },
		"new secret changed rv": func(in map[string]any) { trustMap(in, "live_expanded_secret", "metadata")["resourceVersion"] = "23" },
		"new leaf too early": func(in map[string]any) {
			for _, k := range []string{"expanded_secret", "live_expanded_secret"} {
				trustMap(in, k, "data")["tls.crt"] = "new-leaf"
			}
		},
		"mutable candidate": func(in map[string]any) {
			for _, k := range []string{"expanded_secret", "live_expanded_secret"} {
				trustMap(in, k)["immutable"] = false
			}
		},
		"extra ca key": func(in map[string]any) {
			for _, k := range []string{"expanded_secret", "live_expanded_secret"} {
				trustMap(in, k, "data")["ca.key"] = "private"
			}
		},
		"later phase rollback": func(in map[string]any) {
			in["mode"] = "restore"
			trustMap(in, "current", "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)["args"] = []any{"--experimental-peer-retirement-config=/policy.json"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := trustPlanInput(t)
			mutate(in)
			out, err := trustPlan(t, in)
			require.Error(t, err, string(out))
			require.NotContains(t, string(out), "fake-key")
		})
	}
}

func TestPeerTrustExpansionPatchRejectsConcurrentChanges(t *testing.T) {
	for _, target := range []string{"uid", "resourceVersion", "spec"} {
		t.Run(target, func(t *testing.T) {
			in := trustPlanInput(t)
			out, err := trustPlan(t, in)
			require.NoError(t, err, string(out))
			patch, err := jsonpatch.DecodePatch(out)
			require.NoError(t, err)
			if target == "spec" {
				trustMap(in, "current", "spec")["replicas"] = 2
			} else {
				trustMap(in, "current", "metadata")[target] = "changed"
			}
			data, err := json.Marshal(in["current"])
			require.NoError(t, err)
			_, err = patch.Apply(data)
			require.Error(t, err)
		})
	}
}

func TestPeerTrustExpansionPreservesEscapedCommandAndAnnotationStrings(t *testing.T) {
	in := trustPlanInput(t)
	for _, name := range []string{"baseline", "current"} {
		trustMap(in, name, "spec", "template", "metadata")["annotations"] = map[string]any{
			"test": "<script>&\u2028\u2029 literal \\u0026",
		}
	}
	out, err := trustPlan(t, in)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), `\u0026`)
	patch, err := jsonpatch.DecodePatch(out)
	require.NoError(t, err)
	data, err := json.Marshal(in["current"])
	require.NoError(t, err)
	result, err := patch.Apply(data)
	require.NoError(t, err)
	var changed map[string]any
	require.NoError(t, json.Unmarshal(result, &changed))
	require.Equal(t, trustMap(in, "current", "spec", "template", "metadata"), trustMap(changed, "spec", "template", "metadata"))
}
