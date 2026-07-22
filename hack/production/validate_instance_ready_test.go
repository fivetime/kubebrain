package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const fakeInitialCluster = "kb-0=peer-0,kb-1=peer-1,kb-2=peer-2"

func TestValidateInstanceReady(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		image                    string
		kubeStatus               string
		kubeArgs                 string
		topology                 string
		healthOK                 bool
		advertisedURLs           string
		unreachableAdvertisedURL string
		memberListJSON           string
		initialCluster           string
		wantOK                   bool
		wantOutput               string
	}{
		{
			name:       "converged release",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "3\t3",
			healthOK:   true,
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:           "multiple reachable advertised client URLs",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance-a.example:2379,https://instance-b.example:2379",
			wantOK:         true,
			wantOutput:     "--endpoints=https://instance-b.example:2379",
		},
		{
			name:       "wrong quota",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--port=3379\n--quota-backend-bytes=1073741824\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "quota configuration mismatch",
		},
		{
			name:       "duplicate quota",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "quota configuration mismatch",
		},
		{
			name:       "missing advertised client URL argument",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--port=3379\n--quota-backend-bytes=429496729600",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "wrong advertised client URL",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://internal.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "duplicate advertised client URL argument",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "advertised client URL mismatch",
		},
		{
			name:       "missing keyspace argument",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "wrong keyspace",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--keyspace=instance-b\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "duplicate keyspace argument",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--keyspace=instance-a\n--keyspace=instance-a\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "keyspace configuration mismatch",
		},
		{
			name:       "missing PD address argument",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--keyspace=instance-a\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "wrong PD address",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--keyspace=instance-a\n--pd-addrs=wrong-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "duplicate PD address argument",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "PD address configuration mismatch",
		},
		{
			name:       "wrong image",
			image:      "registry/kubebrain@sha256:wanted",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:old",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "stale rollout",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "9\t8\t3\t3\t2\tkb-old\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "wrong storage topology",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "1\t3",
			healthOK:   true,
			wantOutput: "topology mismatch",
		},
		{
			name:       "unhealthy endpoint",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "3\t3",
			wantOutput: "endpoint health failed",
		},
		{
			name:                     "unreachable advertised client URL",
			image:                    "registry/kubebrain@sha256:abc",
			kubeStatus:               "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:                 "3\t3",
			healthOK:                 true,
			advertisedURLs:           "https://instance.example:2379,https://unreachable.example:2379",
			unreachableAdvertisedURL: "https://unreachable.example:2379",
			wantOutput:               "advertised client URL is unreachable",
		},
		{
			name:           "empty advertised client URL entry",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			advertisedURLs: "https://instance.example:2379,,https://other.example:2379",
			wantOutput:     "empty entry",
		},
		{
			name:           "runtime MemberList has wrong advertised URL",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: fakeRuntimeMemberListJSON("https://internal.example:2379"),
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList repeats member identity",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["p0"],"clientURLs":["https://instance.example:2379"]},{"ID":11,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["p2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList is missing a member",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["p0"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has an incomplete member",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":[],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["p1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["p2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:           "runtime MemberList has wrong peer mapping",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			memberListJSON: `{"header":{"cluster_id":1},"members":[{"ID":11,"name":"kb-0","peerURLs":["wrong-peer"],"clientURLs":["https://instance.example:2379"]},{"ID":12,"name":"kb-1","peerURLs":["peer-1"],"clientURLs":["https://instance.example:2379"]},{"ID":13,"name":"kb-2","peerURLs":["peer-2"],"clientURLs":["https://instance.example:2379"]}]}`,
			wantOutput:     "MemberList does not match",
		},
		{
			name:       "missing initial cluster argument",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			kubeArgs:   "--port=3379\n--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "initial cluster configuration mismatch",
		},
		{
			name:           "wrong initial cluster argument",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0,kb-1=peer-1,kb-2=wrong-peer",
			kubeArgs:       "--port=3379\n--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=https://instance.example:2379\n--initial-cluster=" + fakeInitialCluster,
			wantOutput:     "initial cluster configuration mismatch",
		},
		{
			name:           "duplicate initial cluster peer URL",
			image:          "registry/kubebrain@sha256:abc",
			kubeStatus:     "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:       "3\t3",
			healthOK:       true,
			initialCluster: "kb-0=peer-0,kb-1=peer-0,kb-2=peer-2",
			wantOutput:     "duplicate peer URL",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"get tidbcluster"* && "$*" == *".status.conditions"* ]]; then
  printf 'True'
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *"jsonpath="* ]]; then
  printf '5\t5\t3\t3\t3\tpd-new\tpd-new'
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *"jsonpath="* ]]; then
  printf '7\t7\t3\t3\t3\ttikv-new\ttikv-new'
elif [[ "$*" == *"get tidbcluster"* && "$*" == *".spec.pd.replicas"* ]]; then
  printf '%s' "$FAKE_TOPOLOGY"
elif [[ "$*" == *"get statefulset kubebrain"* && "$*" == *".args"* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_ARGS"
elif [[ "$*" == *"get statefulset kubebrain"* && "$*" == *"jsonpath="* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_STATUS"
else
  printf 'diagnostic output\n'
fi
`), 0o755))
			fakeEtcdctl := filepath.Join(dir, "etcdctl")
			require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ -n "$FAKE_UNREACHABLE_ADVERTISED_URL" && "$*" == *"--endpoints=$FAKE_UNREACHABLE_ADVERTISED_URL"* ]]; then
  printf 'unreachable\n' >&2
  exit 1
fi
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' "$FAKE_MEMBER_LIST_JSON"
  exit 0
fi
if [[ "$FAKE_HEALTH_OK" == "true" && "$*" == *"endpoint health"* ]]; then
  printf 'healthy %s\n' "$*"
  exit 0
fi
printf 'unhealthy\n' >&2
exit 1
`), 0o755))

			kubeArgs := tc.kubeArgs
			advertisedURLs := tc.advertisedURLs
			if advertisedURLs == "" {
				advertisedURLs = "https://instance.example:2379"
			}
			initialCluster := tc.initialCluster
			if initialCluster == "" {
				initialCluster = fakeInitialCluster
			}
			if kubeArgs == "" {
				kubeArgs = "--port=3379\n--keyspace=instance-a\n--pd-addrs=kb-pd.storage.svc:2379\n--quota-backend-bytes=429496729600\n--advertise-client-urls=" + advertisedURLs + "\n--initial-cluster=" + initialCluster
			}
			memberListJSON := tc.memberListJSON
			if memberListJSON == "" {
				memberListJSON = fakeRuntimeMemberListJSON(advertisedURLs)
			}
			command := exec.Command("bash", "validate-instance-ready.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"ETCDCTL="+fakeEtcdctl,
				"EXPECTED_IMAGE="+tc.image,
				"EXPECTED_KEYSPACE=instance-a",
				"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
				"EXPECTED_INITIAL_CLUSTER="+initialCluster,
				"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
				"EXPECTED_ADVERTISE_CLIENT_URLS="+advertisedURLs,
				"ENDPOINT=https://instance.example:2379",
				"TIMEOUT_SECONDS=1",
				"POLL_INTERVAL_SECONDS=0",
				"FAKE_KUBEBRAIN_STATUS="+tc.kubeStatus,
				"FAKE_KUBEBRAIN_ARGS="+kubeArgs,
				"FAKE_TOPOLOGY="+tc.topology,
				"FAKE_HEALTH_OK="+boolString(tc.healthOK),
				"FAKE_UNREACHABLE_ADVERTISED_URL="+tc.unreachableAdvertisedURL,
				"FAKE_MEMBER_LIST_JSON="+memberListJSON,
			)
			output, err := command.CombinedOutput()
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, strings.TrimSpace(string(output)), tc.wantOutput)
		})
	}
}

func fakeRuntimeMemberListJSON(advertisedURLs string) string {
	clientURLs := strings.Split(advertisedURLs, ",")
	members := make([]map[string]any, 3)
	for index := range members {
		members[index] = map[string]any{
			"ID":         index + 11,
			"name":       "kb-" + string(rune('0'+index)),
			"peerURLs":   []string{"peer-" + string(rune('0'+index))},
			"clientURLs": clientURLs,
		}
	}
	encoded, err := json.Marshal(map[string]any{
		"header":  map[string]any{"cluster_id": 1},
		"members": members,
	})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func TestValidateInstanceReadyRequiresImmutableInputs(t *testing.T) {
	command := exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(), "EXPECTED_IMAGE=", "ENDPOINT=", "EXPECTED_KEYSPACE=", "EXPECTED_PD_ADDRS=", "EXPECTED_INITIAL_CLUSTER=", "EXPECTED_QUOTA_BACKEND_BYTES=", "EXPECTED_ADVERTISE_CLIENT_URLS=")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_IMAGE is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:abc",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_QUOTA_BACKEND_BYTES is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:abc",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_ADVERTISE_CLIENT_URLS is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:abc",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_KEYSPACE is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:abc",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=",
		"EXPECTED_INITIAL_CLUSTER="+fakeInitialCluster,
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_PD_ADDRS is required")

	command = exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(),
		"EXPECTED_IMAGE=registry/kubebrain@sha256:abc",
		"ENDPOINT=https://instance.example:2379",
		"EXPECTED_KEYSPACE=instance-a",
		"EXPECTED_PD_ADDRS=kb-pd.storage.svc:2379",
		"EXPECTED_INITIAL_CLUSTER=",
		"EXPECTED_QUOTA_BACKEND_BYTES=429496729600",
		"EXPECTED_ADVERTISE_CLIENT_URLS=https://instance.example:2379",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_INITIAL_CLUSTER is required")
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
