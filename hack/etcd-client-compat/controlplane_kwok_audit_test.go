package compat

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControlPlaneKWOKAudit(t *testing.T) {
	for _, mode := range []string{"valid", "missing-node", "missing-pod", "admin-writer", "wrong-uid", "failed-update", "missing-renewal", "no-renewal", "missing-node-uid"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name string, value any) string {
				data, err := json.Marshal(value)
				require.NoError(t, err)
				path := filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(path, data, 0600))
				return path
			}
			nodeUID := "node-uid"
			if mode == "missing-node-uid" {
				nodeUID = ""
			}
			node := write("node.json", map[string]any{"metadata": map[string]string{"uid": nodeUID}})
			pods := write("pods.json", map[string]any{"items": []any{
				map[string]any{"metadata": map[string]string{"uid": "p1"}}, map[string]any{"metadata": map[string]string{"uid": "p2"}}, map[string]any{"metadata": map[string]string{"uid": "p3"}},
			}})
			lease := func(timestamp string) map[string]any {
				return map[string]any{"metadata": map[string]string{"uid": "lease-uid"}, "spec": map[string]string{"renewTime": timestamp, "holderIdentity": "kwok-owner"}}
			}
			firstTime, lastTime := "2026-09-17T08:00:00.000000Z", "2026-09-17T08:00:10.000000Z"
			if mode == "no-renewal" {
				lastTime = firstTime
			}
			first, last := write("first.json", lease(firstTime)), write("last.json", lease(lastTime))
			events := []any{}
			appendEvent := func(resource, namespace, subresource string, response map[string]any) {
				user, code := "kubebrain-test-kwok", 200
				if mode == "admin-writer" {
					user = "kubebrain-test-admin"
				}
				if mode == "failed-update" {
					code = 403
				}
				events = append(events, map[string]any{"stage": "ResponseComplete", "verb": "patch", "user": map[string]string{"username": user},
					"responseStatus": map[string]int{"code": code}, "objectRef": map[string]string{"resource": resource, "namespace": namespace, "subresource": subresource}, "responseObject": response})
			}
			ready := func(uid string) map[string]any {
				if mode == "wrong-uid" {
					uid = "replacement"
				}
				return map[string]any{"metadata": map[string]string{"uid": uid}, "status": map[string]any{"phase": "Running", "conditions": []any{map[string]string{"type": "Ready", "status": "True"}}}}
			}
			if mode != "missing-node" {
				appendEvent("nodes", "", "status", ready(nodeUID))
			}
			for _, uid := range []string{"p1", "p2", "p3"} {
				if mode != "missing-pod" || uid != "p3" {
					appendEvent("pods", "controlplane-smoke", "status", ready(uid))
				}
			}
			appendEvent("leases", "kube-node-lease", "", lease(firstTime))
			if mode != "missing-renewal" {
				appendEvent("leases", "kube-node-lease", "", lease(lastTime))
			}
			audit := write("audit.json", events)
			out, err := runCompatCommandContext(t, context.Background(), "jq", []string{"-e", "--slurpfile", "node", node, "--slurpfile", "pods", pods,
				"--slurpfile", "first", first, "--slurpfile", "last", last, "-f", filepath.Join("..", "scale-lab", "verify-controlplane-kwok-audit.jq"), audit}, nil)
			if mode == "valid" {
				require.NoError(t, err, "%s", out)
			} else {
				require.Error(t, err, "%s", out)
			}
		})
	}
}
