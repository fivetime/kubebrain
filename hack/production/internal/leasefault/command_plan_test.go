package leasefault

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
)

func serializedCommandFixture(t *testing.T) NativeCommandPlan {
	t.Helper()
	o := commandPlan(t)
	c, _ := connectionFixture(t)
	image, release := commandReleaseFixture(t)
	o.Bindings.Image = image
	pod := func(name, uid, ip string) json.RawMessage {
		return json.RawMessage(fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"test-ns","name":%q,"uid":%q,"resourceVersion":"1"},"spec":{"nodeName":"worker1","containers":[{"name":"brain","image":%q}]},"status":{"podIP":%q,"containerStatuses":[{"name":"brain","imageID":%q,"containerID":"containerd://fixture","state":{"running":{"startedAt":"2026-09-20T00:00:00Z"}}}]}}`, name, uid, image, ip, release.Reviewed["linux/amd64"]))
	}
	o.Bindings.Network.PodBefore = pod("brain-0", "pod-uid", "127.0.0.1")
	dir := t.TempDir()
	tool := func(name string) string {
		path := filepath.Join(dir, name)
		data := []byte("#!/bin/sh\nexit 99\n")
		require.NoError(t, os.WriteFile(path, data, 0700))
		c.Files[path] = planinput.SHA256(data)
		return path
	}
	p := NativeCommandPlan{Version: 1, Bindings: o.Bindings, OwnerDirectory: o.OwnerDirectory, StatefulSetName: "brain", ProbeExecutable: tool("probe"), StackExecutable: tool("stack"), MetricExecutable: tool("metrics"), JoinScript: tool("join"), Endpoint: c.Endpoint, ServerName: c.ServerName, ObserverEndpoint: "127.0.0.2:2379", ObserverServerName: c.ServerName, CA: c.CA, Certificate: c.Certificate, Key: c.Key, Kubeconfig: c.Kubeconfig, KubeContext: c.Context, APIServer: c.APIServer, BeforeDirectory: o.BeforeDirectory, AfterDirectory: o.AfterDirectory, Duration: "2m", RecoveryTimeout: "30s", CaptureSeconds: 1, TargetsSHA256: strings.Repeat("a", 64), Env: o.Env, Release: release, Files: c.Files, Targets: []CommandPlanTarget{{PodName: "brain-0", PodUID: "pod-uid", InfoPort: 18600, AnonymousPort: 18601}}}
	p.Processes = CommandProcessInputs{JQ: tool("jq"), Predicate: tool("predicate"), Observer: pod("brain-1", "observer-uid", "127.0.0.2"), Metrics: []json.RawMessage{o.Bindings.Network.PodBefore}}
	return p
}

func TestNativeCommandPlanLoad(t *testing.T) {
	for _, mode := range []string{"valid", "unknown", "duplicate", "numeric-duration", "wrong-digest", "public", "changed-tool", "missing-pin", "bad-binding", "port-overlap", "metric-uid", "metric-snapshot", "endpoint", "recovery-budget", "environment", "descriptor", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			p := serializedCommandFixture(t)
			switch mode {
			case "missing-pin":
				delete(p.Files, p.JoinScript)
			case "bad-binding":
				p.Bindings.ObserverMemberID = p.Bindings.Protocol.AlarmMemberID
			case "port-overlap":
				p.Targets[0].InfoPort = p.Bindings.StackPorts[0]
			case "metric-uid":
				p.Targets[0].PodUID = "other"
			case "metric-snapshot":
				p.Processes.Metrics[0] = p.Processes.Observer
			case "endpoint":
				p.ObserverEndpoint = "127.0.0.3:2379"
			case "recovery-budget":
				p.RecoveryTimeout = "6m"
			case "environment":
				p.Env = append(p.Env, "BASH_ENV=/tmp/startup")
			}
			data, err := json.Marshal(p)
			require.NoError(t, err)
			switch mode {
			case "unknown":
				data = append(data[:len(data)-1], []byte(`,"execute":true}`)...)
			case "duplicate":
				data = bytes.Replace(data, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "numeric-duration":
				data = bytes.Replace(data, []byte(`"duration":"2m"`), []byte(`"duration":120`), 1)
			case "descriptor":
				data = bytes.Replace(data, []byte(`"pod_name":"brain-0"`), []byte(`"pod_name":"brain-0","stderr":3`), 1)
			case "changed-tool":
				require.NoError(t, os.WriteFile(p.ProbeExecutable, []byte("changed"), 0700))
			}
			path := filepath.Join(t.TempDir(), "plan.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			digest := planinput.SHA256(data)
			if mode == "wrong-digest" {
				digest = strings.Repeat("f", 64)
			}
			if mode == "public" {
				require.NoError(t, os.Chmod(path, 0644))
			}
			if mode == "symlink" {
				require.NoError(t, os.Symlink(path, path+".link"))
				path += ".link"
			}
			before, err := os.ReadDir(p.OwnerDirectory)
			require.NoError(t, err)
			loaded, err := LoadNativeCommandPlan(path, digest)
			if mode == "valid" {
				require.NoError(t, err)
				o, err := loaded.ObservationPlan()
				require.NoError(t, err)
				require.Equal(t, p.Endpoint, o.Endpoint)
				require.Nil(t, o.ProbeLog)
				require.Nil(t, loaded.MetricTargets()[0].Stderr)
				require.Equal(t, p.Bindings.Protocol.ClusterID, o.Bindings.Protocol.ClusterID)
			} else {
				require.Error(t, err)
			}
			after, err := os.ReadDir(p.OwnerDirectory)
			require.NoError(t, err)
			require.Equal(t, before, after, "loading must not create artifacts")
		})
	}
}
