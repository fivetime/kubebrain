package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControlPlaneWriterAudit(t *testing.T) {
	for _, mode := range []string{"valid-generated-name", "missing-pod", "duplicate-pod", "admin-created", "wrong-uid", "wrong-binding", "failed-create", "metadata-only"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, data string) string {
				path := filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(path, []byte(data), 0600))
				return path
			}
			rs := write("rs.json", `{"items":[{"metadata":{"name":"rs","uid":"rs-uid"}}]}`)
			pods := write("pods.json", `{"items":[{"metadata":{"name":"p1","uid":"uid1"}},{"metadata":{"name":"p2","uid":"uid2"}},{"metadata":{"name":"p3","uid":"uid3"}}]}`)
			events := []string{}
			appendEvent := func(resource, subresource, user, name, uid string, code int, response bool) {
				ref := map[string]any{"namespace": "controlplane-smoke", "resource": resource}
				if subresource != "" {
					ref["subresource"], ref["name"], ref["uid"] = subresource, name, uid
				}
				event := map[string]any{"stage": "ResponseComplete", "verb": "create", "objectRef": ref,
					"responseStatus": map[string]any{"code": code}, "user": map[string]any{"username": user}}
				if response {
					event["responseObject"] = map[string]any{"metadata": map[string]any{"name": name, "uid": uid}}
				}
				data, err := json.Marshal(event)
				require.NoError(t, err)
				events = append(events, string(data))
			}
			appendEvent("replicasets", "", "system:serviceaccount:kube-system:deployment-controller", "rs", "rs-uid", 201, true)
			for i := 1; i <= 3; i++ {
				name, uid := fmt.Sprintf("p%d", i), fmt.Sprintf("uid%d", i)
				user, code := "system:serviceaccount:kube-system:replicaset-controller", 201
				if mode == "admin-created" {
					user = "kubebrain-test-admin"
				}
				if mode == "wrong-uid" {
					uid = "replacement"
				}
				if mode == "failed-create" {
					code = 500
				}
				if mode != "missing-pod" || i != 3 {
					appendEvent("pods", "", user, name, uid, code, mode != "metadata-only")
					if mode == "duplicate-pod" && i == 3 {
						events = append(events, events[len(events)-1])
					}
				}
				binder := "system:kube-scheduler"
				if mode == "wrong-binding" {
					binder = "kubebrain-test-admin"
				}
				appendEvent("pods", "binding", binder, name, uid, 201, false)
			}
			audit := write("audit.jsonl", strings.Join(events, "\n"))
			out, err := runCompatCommandContext(t, context.Background(), "jq", []string{"-s", "-e", "--slurpfile", "rs", rs, "--slurpfile", "pods", pods,
				"-f", filepath.Join("..", "scale-lab", "verify-controlplane-audit.jq"), audit}, nil)
			if mode == "valid-generated-name" {
				require.NoError(t, err, "%s", out)
			} else {
				require.Error(t, err, "%s", out)
			}
		})
	}
}
