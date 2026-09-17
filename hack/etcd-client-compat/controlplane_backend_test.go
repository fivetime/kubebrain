package compat

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestControlPlaneBackendOwnership(t *testing.T) {
	for _, mode := range []string{"success", "adjacent-cluster-id", "duplicate-status", "nonempty", "missing-header", "failed-range", "existing-lease", "cleanup-drift", "delete-failed", "dirty-after-delete", "lease-natural-expiry", "lease-timeout", "lease-list-failed"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, content string) string {
				path := filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(path, []byte(content), 0700))
				return path
			}
			body := `#!/bin/bash
set -eu
printf '%s\n' "$*" >> "$CALLS"
while [[ "$1" == --* ]]; do shift; done
case "$1" in
  endpoint)
    cluster=9007199254740993
    if [[ "$MODE" == adjacent-cluster-id || ( "$MODE" == cleanup-drift && -f "$AFTER" ) ]]; then cluster=9007199254740992; fi
    printf '"ClusterID" : %s\n"MemberID" : 18446744073709551615\n' "$cluster"
    if [[ "$MODE" == duplicate-status ]]; then printf '"ClusterID" : %s\n' "$cluster"; fi ;;
  get)
    if [[ "$MODE" == missing-header ]]; then echo '{}'
    elif [[ "$MODE" == nonempty || ( "$MODE" == dirty-after-delete && -f "$AFTER" ) ]]; then echo '{"header":{},"count":1,"kvs":[{}]}'
    else echo '{"header":{},"count":0}'; fi
    if [[ "$MODE" == failed-range ]]; then exit 9; fi ;;
  lease)
    [[ "$2" == list ]] || exit 91
    if [[ "$MODE" == lease-list-failed && -f "$AFTER" ]]; then exit 9; fi
    if [[ "$MODE" == existing-lease || ( -f "$AFTER" && "$MODE" == lease-timeout ) ||
          ( -f "$AFTER" && "$MODE" == lease-natural-expiry && ! -f "$AFTER.expired" ) ]]; then
      printf 'found 1 leases\n123\n'
    else echo 'found 0 leases'; fi ;;
  del)
    [[ "$MODE" != delete-failed ]] || exit 9
    echo '{"header":{},"deleted":5}' ;;
  *) exit 90 ;;
esac
`
			binary := write("etcdctl", body)
			help, err := filepath.Abs(filepath.Join("..", "scale-lab", "controlplane-backend.sh"))
			require.NoError(t, err)
			wrapper := write("run", `#!/bin/bash
set -euo pipefail
source "$HELPER"
# Accelerate only this test shell's clock, retaining the helper's real 60s
# comparison. The mock lease disappears only after a poll/sleep cycle.
sleep() {
  [[ "$1" == 1 ]]
  if [[ "$MODE" == lease-timeout ]]; then SECONDS=$((SECONDS+61)); fi
  if [[ "$MODE" == lease-natural-expiry ]]; then touch "$AFTER.expired"; fi
}
controlplane_backend_prepare "$DIRECTORY" test-run-1234567890
touch "$AFTER"
controlplane_backend_cleanup
[[ "$controlplane_backend_owned" == false ]]
`)
			calls := filepath.Join(dir, "calls")
			env := []string{"HELPER=" + help, "DIRECTORY=" + dir, "AFTER=" + filepath.Join(dir, "after"), "CALLS=" + calls, "MODE=" + mode,
				"ALLOW_MUTATING_CONTROLPLANE_BACKEND=true", "CONTROLPLANE_ENDPOINT=https://backend.test:2379",
				"CONTROLPLANE_CLUSTER_ID=9007199254740993", "CONTROLPLANE_MEMBER_ID=18446744073709551615",
				"CONTROLPLANE_CA=" + write("ca", "fixture"), "CONTROLPLANE_CERT=" + write("cert", "fixture"), "CONTROLPLANE_KEY=" + write("key", "fixture"),
				"CONTROLPLANE_ETCDCTL=" + binary, "CONTROLPLANE_ETCDCTL_SHA256=" + fmt.Sprintf("%x", sha256.Sum256([]byte(body)))}
			out, err := runCompatCommandContext(t, context.Background(), "bash", []string{wrapper}, env)
			if mode == "success" || mode == "lease-natural-expiry" {
				require.NoError(t, err, "%s", out)
			} else {
				require.Error(t, err, "%s", out)
			}
			data, err := os.ReadFile(calls)
			require.NoError(t, err)
			log := string(data)
			require.NotContains(t, log, "revoke")
			require.NotContains(t, log, "keep-alive")
			deletes := strings.Count(log, " del ")
			if mode == "success" || mode == "delete-failed" || mode == "dirty-after-delete" || strings.HasPrefix(mode, "lease-") {
				require.Equal(t, 1, deletes)
				require.Contains(t, log, " del /registry-kubebrain-controlplane-test-run-1234567890/ --prefix")
			} else {
				require.Zero(t, deletes, "failed admission or identity drift must never delete")
			}
			require.Contains(t, log, "--insecure-transport=false --insecure-skip-tls-verify=false")
			if mode == "lease-timeout" {
				require.Contains(t, string(out), "leases did not naturally expire; none revoked")
				require.Equal(t, 3, strings.Count(log, " lease list"), "baseline and two cleanup polls")
			}
			if mode == "lease-natural-expiry" {
				require.FileExists(t, filepath.Join(dir, "after.expired"))
				require.Equal(t, 3, strings.Count(log, " lease list"))
			}
		})
	}
}
