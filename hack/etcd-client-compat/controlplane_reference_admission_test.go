package compat

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControlPlaneReferenceAdmission(t *testing.T) {
	for _, mode := range []string{"no-consent", "bad-hash", "duplicate-ports", "reserved-api-port"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			binary := filepath.Join(dir, "must-not-execute")
			body := []byte("#!/bin/sh\ntouch \"$TEST_EXECUTED\"\nexit 90\n")
			require.NoError(t, os.WriteFile(binary, body, 0700))
			digest := fmt.Sprintf("%x", sha256.Sum256(body))
			consent, api, client := "true", "18453", "13579"
			switch mode {
			case "no-consent":
				consent = "false"
			case "bad-hash":
				digest = "0000000000000000000000000000000000000000000000000000000000000000"
			case "duplicate-ports":
				client = api
			case "reserved-api-port":
				api = "6443"
			}
			env := []string{"ALLOW_LOCAL_CONTROLPLANE_TEST=" + consent, "WORK_PARENT=" + dir,
				"API_PORT=" + api, "ETCD_CLIENT_PORT=" + client, "ETCD_PEER_PORT=13580", "TEST_EXECUTED=" + filepath.Join(dir, "executed")}
			for _, name := range []string{"APISERVER_BIN", "CONTROLLER_MANAGER_BIN", "SCHEDULER_BIN", "REFERENCE_ETCD_BIN"} {
				env = append(env, name+"="+binary, name+"_SHA256="+digest)
			}
			out, err := runCompatCommandContext(t, context.Background(), "bash", []string{filepath.Join("..", "scale-lab", "controlplane-reference-smoke.sh")}, env)
			require.Error(t, err, "%s", out)
			require.NoFileExists(t, filepath.Join(dir, "executed"))
			work, err := filepath.Glob(filepath.Join(dir, "controlplane-reference.*"))
			require.NoError(t, err)
			require.Empty(t, work, "refusal must precede work-directory creation")
		})
	}
}

func TestControlPlaneSharedBackendNeedsSeparateConsent(t *testing.T) {
	for _, entry := range []string{"controlplane-reference-smoke.sh", "controlplane-smoke.sh"} {
		out, err := runCompatCommandContext(t, context.Background(), "bash", []string{filepath.Join("..", "scale-lab", entry)}, []string{
			"ALLOW_LOCAL_CONTROLPLANE_TEST=true", "CONTROLPLANE_BACKEND=kubebrain", "ALLOW_MUTATING_CONTROLPLANE_BACKEND=false",
		})
		require.Error(t, err, "%s", out)
		if entry == "controlplane-reference-smoke.sh" {
			require.Contains(t, string(out), "reference entrypoint refuses shared backend")
		} else {
			require.Contains(t, string(out), "shared backend mutation not authorized")
		}
	}
}
