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

func TestControlPlanePodReplacement(t *testing.T) {
	for _, mode := range []string{"valid", "old-survives", "two-replaced", "wrong-owner", "not-ready", "not-scheduled", "changed-rs", "changed-deployment", "unobserved-generation", "admin-create", "admin-binding", "missing-delete", "wrong-delete-uid", "wrong-delete-rv", "missing-status", "wrong-status-writer", "failed-create", "duplicate-create"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, data string) string {
				require.True(t, json.Valid([]byte(data)), "%s", name)
				path := filepath.Join(dir, name+".json")
				require.NoError(t, os.WriteFile(path, []byte(data), 0600))
				return path
			}
			pod := func(uid string) string {
				return fmt.Sprintf(`{"metadata":{"name":%q,"uid":%q,"resourceVersion":"100","ownerReferences":[{"controller":true,"kind":"ReplicaSet","uid":"rs"}]},"spec":{"nodeName":"reference-node"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"},{"type":"PodScheduled","status":"True"}]}}`, uid, uid)
			}
			before := write("before", `{"items":[`+pod("gone")+`,`+pod("keep1")+`,`+pod("keep2")+`]}`)
			victim := write("victim", pod("gone"))
			after := `{"items":[` + pod("new") + `,` + pod("keep1") + `,` + pod("keep2") + `]}`
			rsJSON := `{"items":[{"metadata":{"uid":"rs","ownerReferences":[{"controller":true,"kind":"Deployment","uid":"deployment"}]},"spec":{"replicas":3}}]}`
			originalRS := write("originalRS", rsJSON)
			deployment := `{"metadata":{"uid":"deployment","generation":1},"spec":{"replicas":3},"status":{"observedGeneration":1,"readyReplicas":3,"availableReplicas":3}}`
			created := write("created", deployment)
			stateFail := true
			switch mode {
			case "old-survives":
				after = strings.ReplaceAll(after, `"new"`, `"gone"`)
			case "two-replaced":
				after = strings.ReplaceAll(after, `"keep1"`, `"other"`)
			case "wrong-owner":
				after = strings.ReplaceAll(after, `"uid":"rs"`, `"uid":"other"`)
			case "not-ready":
				after = strings.ReplaceAll(after, `"type":"Ready","status":"True"`, `"type":"Ready","status":"False"`)
			case "not-scheduled":
				after = strings.ReplaceAll(after, `"type":"PodScheduled","status":"True"`, `"type":"PodScheduled","status":"False"`)
			case "changed-rs":
				rsJSON = strings.ReplaceAll(rsJSON, `"uid":"rs"`, `"uid":"other"`)
			case "changed-deployment":
				deployment = strings.ReplaceAll(deployment, `"uid":"deployment"`, `"uid":"other"`)
			case "unobserved-generation":
				deployment = strings.ReplaceAll(deployment, `"generation":1`, `"generation":2`)
			default:
				stateFail = false
			}
			pods := write("pods", after)
			args := []string{"-n", "-e", "--slurpfile", "before", before, "--slurpfile", "victim", victim, "--slurpfile", "pods", pods, "--slurpfile", "created", created, "--slurpfile", "deployment", write("deployment", deployment), "--slurpfile", "originalRS", originalRS, "--slurpfile", "rs", write("rs", rsJSON), "-f", filepath.Join("..", "scale-lab", "verify-controlplane-replacement.jq")}
			out, err := runCompatCommandContext(t, context.Background(), "jq", args, nil)
			if stateFail {
				require.Error(t, err, "%s", out)
				return
			}
			require.NoError(t, err, "%s", out)
			events := []string{}
			for _, uid := range []string{"gone", "keep1", "keep2", "new"} {
				for _, binding := range []bool{false, true} {
					user, sub := "system:serviceaccount:kube-system:replicaset-controller", ""
					if binding {
						user, sub = "system:kube-scheduler", "binding"
					}
					if (mode == "admin-create" && !binding || mode == "admin-binding" && binding) && uid == "new" {
						user = "kubebrain-test-admin"
					}
					code := 201
					if mode == "failed-create" && uid == "new" && !binding {
						code = 500
					}
					subJSON := "null"
					if sub != "" {
						subJSON = `"binding"`
					}
					event := fmt.Sprintf(`{"stage":"ResponseComplete","verb":"create","responseStatus":{"code":%d},"user":{"username":%q},"objectRef":{"resource":"pods","namespace":"controlplane-smoke","subresource":%s,"name":%q,"uid":%q},"responseObject":%s}`, code, user, subJSON, uid, uid, pod(uid))
					events = append(events, event)
					if mode == "duplicate-create" && uid == "new" && !binding {
						events = append(events, event)
					}
				}
			}
			deleteUID, deleteRV := "gone", "100"
			if mode == "wrong-delete-uid" {
				deleteUID = "other"
			}
			if mode == "wrong-delete-rv" {
				deleteRV = "99"
			}
			if mode != "missing-delete" {
				events = append(events, fmt.Sprintf(`{"stage":"ResponseComplete","verb":"delete","responseStatus":{"code":200},"user":{"username":"kubebrain-test-admin"},"objectRef":{"resource":"pods","namespace":"controlplane-smoke","name":"gone"},"requestObject":{"preconditions":{"uid":%q,"resourceVersion":%q},"gracePeriodSeconds":0}}`, deleteUID, deleteRV))
			}
			statusUser := "kubebrain-test-kwok"
			if mode == "wrong-status-writer" {
				statusUser = "kubebrain-test-admin"
			}
			if mode != "missing-status" {
				events = append(events, fmt.Sprintf(`{"stage":"ResponseComplete","verb":"patch","responseStatus":{"code":200},"user":{"username":%q},"objectRef":{"resource":"pods","namespace":"controlplane-smoke","subresource":"status"},"responseObject":%s}`, statusUser, pod("new")))
			}
			audit := write("audit", `[`+strings.Join(events, ",")+`]`)
			out, err = runCompatCommandContext(t, context.Background(), "jq", []string{"-e", "--slurpfile", "before", before, "--slurpfile", "victim", victim, "--slurpfile", "pods", pods, "-f", filepath.Join("..", "scale-lab", "verify-controlplane-replacement-audit.jq"), audit}, nil)
			if mode == "valid" {
				require.NoError(t, err, "%s", out)
			} else {
				require.Error(t, err, "%s", out)
			}
		})
	}
}
