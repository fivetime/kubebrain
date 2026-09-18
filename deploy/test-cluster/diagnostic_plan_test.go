package testcluster_test

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	"os/exec"
	"testing"
)

func diagnosticInput(t *testing.T) map[string]any {
	in := trustPlanInput(t)
	in["mode"] = "enable"
	in["info_dns"] = "kubebrain-local-info.kubebrain-dbaas-test.svc"
	return in
}
func diagnosticPlan(t *testing.T, in map[string]any) ([]byte, error) {
	t.Helper()
	data, err := json.Marshal(in)
	require.NoError(t, err)
	cmd := exec.Command("jq", "-er", "-f", "diagnostic-plan.jq")
	cmd.Stdin = bytes.NewReader(data)
	return cmd.CombinedOutput()
}

func TestDiagnosticPlanProtectsInfoAndPreservesEverythingElse(t *testing.T) {
	for _, protocol := range []bool{false, true} {
		t.Run(map[bool]string{false: "baseline", true: "protocol"}[protocol], func(t *testing.T) {
			in := diagnosticInput(t)
			if protocol {
				p := protocolTrustInput(t)
				patch, err := trustPlan(t, p)
				require.NoError(t, err, string(patch))
				active := applyTrustPatch(t, p["current"], patch)
				in["baseline"] = active
				in["current"] = cloneTrustObject(t, active)
			}
			patch, err := diagnosticPlan(t, in)
			require.NoError(t, err, string(patch))
			enabled := applyTrustPatch(t, in["current"], patch)
			c := trustMap(enabled, "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)
			args := c["args"].([]any)
			require.Contains(t, args, "--enable-pprof=true")
			require.Contains(t, args, "--info-client-cert-auth=true")
			require.NotContains(t, args, "--enable-pprof=false")
			original := trustMap(in, "baseline", "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)
			for _, name := range []string{"livenessProbe", "startupProbe", "readinessProbe"} {
				probe := c[name].(map[string]any)
				require.Nil(t, probe["httpGet"])
				argv := probe["exec"].(map[string]any)["command"].([]any)
				require.Equal(t, []any{"curl", "--disable"}, argv[:2])
				require.Contains(t, argv, "--cert")
				require.Contains(t, argv, "--cacert")
				require.NotContains(t, argv, "-k")
				copy := cloneTrustObject(t, probe)
				delete(copy, "exec")
				expected := cloneTrustObject(t, original[name].(map[string]any))
				delete(expected, "httpGet")
				require.Equal(t, expected, copy)
			}
			// Revert only allowed fields, then compare the entire spec.
			compare := cloneTrustObject(t, enabled)
			cc := trustMap(compare, "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)
			for _, key := range []string{"args", "livenessProbe", "startupProbe", "readinessProbe"} {
				cc[key] = original[key]
			}
			require.Equal(t, trustMap(in, "baseline")["spec"], compare["spec"])
			in["current"] = enabled
			patch, err = diagnosticPlan(t, in)
			require.NoError(t, err, string(patch))
			require.JSONEq(t, "[]", string(patch))
			trustMap(enabled, "status")["readyReplicas"] = 1
			in["mode"] = "restore"
			patch, err = diagnosticPlan(t, in)
			require.NoError(t, err, string(patch))
			restored := applyTrustPatch(t, enabled, patch)
			require.Equal(t, trustMap(in, "baseline")["spec"], restored["spec"])
		})
	}
}

func TestDiagnosticPlanRejectsDriftAndConcurrentUpdates(t *testing.T) {
	for _, scenario := range []string{"namespace", "uid", "not-ready", "drift", "insecure", "duplicate-flag", "unprotected-mount", "wrong-probe"} {
		t.Run(scenario, func(t *testing.T) {
			in := diagnosticInput(t)
			switch scenario {
			case "namespace":
				trustMap(in, "namespace", "metadata")["uid"] = "changed"
			case "uid":
				trustMap(in, "current", "metadata")["uid"] = "changed"
			case "not-ready":
				trustMap(in, "current", "status")["readyReplicas"] = 2
			case "drift":
				trustMap(in, "current", "spec")["replicas"] = 2
			default:
				c := trustMap(in, "baseline", "spec", "template", "spec")["containers"].([]any)[0].(map[string]any)
				switch scenario {
				case "insecure":
					c["args"] = append(c["args"].([]any), "--info-allow-insecure=true")
				case "duplicate-flag":
					c["args"] = append(c["args"].([]any), "--enable-pprof=false")
				case "unprotected-mount":
					c["volumeMounts"] = []any{}
				case "wrong-probe":
					c["readinessProbe"].(map[string]any)["httpGet"] = map[string]any{"path": "/wrong"}
				}
				in["current"] = cloneTrustObject(t, trustMap(in, "baseline"))
			}
			output, err := diagnosticPlan(t, in)
			require.Error(t, err, string(output))
		})
	}
	for _, field := range []string{"uid", "resourceVersion", "spec"} {
		t.Run("concurrent-"+field, func(t *testing.T) {
			in := diagnosticInput(t)
			output, err := diagnosticPlan(t, in)
			require.NoError(t, err, string(output))
			p, err := jsonpatch.DecodePatch(output)
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
