package compat

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDifferentialRunnerRejectsUnreachableAdvertisedClientURL(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"--endpoints=http://internal.invalid:3379"* ]]; then
  exit 1
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	command := exec.Command("bash", "run-differential.sh")
	command.Env = append(os.Environ(),
		"PATH="+dir+":"+os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN="+fakeEtcdctl,
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "http://internal.invalid:3379")
}

func TestDifferentialRunnerRejectsMissingAdvertisedClientURLs(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":[]}]}'
  exit 0
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	command := exec.Command("bash", "run-differential.sh")
	command.Env = append(os.Environ(),
		"PATH="+dir+":"+os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN="+fakeEtcdctl,
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "returned no advertised client URLs")
}
