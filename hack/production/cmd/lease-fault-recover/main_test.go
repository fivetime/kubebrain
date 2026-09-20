package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
)

// Static admission fixture only. Digests deliberately do not describe real host
// credentials or tools; verifyFiles is tested separately using temporary files.
func planFixture(t *testing.T) plan {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	n := leasefault.NetworkRecovery{Owner: "test-owner", Namespace: "kubebrain-dbaas-test", NamespaceUID: "6c57c242-912b-41bb-9020-f4fdb3225ef3", StatefulSetUID: "7d760f53-5bb5-4429-a2f8-651b89665616", PodName: "kubebrain-local-0", PodUID: "pod-uid", PolicyName: "kb-term-test", Nonce: "term-test", ReservedNonce: "reserved-test",
		PodBefore:      json.RawMessage(`{"apiVersion":"v1","kind":"Pod","metadata":{"namespace":"kubebrain-dbaas-test","name":"kubebrain-local-0","uid":"pod-uid","resourceVersion":"1"}}`),
		ApprovedPolicy: json.RawMessage(`{"apiVersion":"cilium.io/v2","kind":"CiliumNetworkPolicy","metadata":{"namespace":"kubebrain-dbaas-test","name":"kb-term-test"},"spec":{"endpointSelector":{"matchLabels":{"kubebrain.io/fault-owner":"term-test"}}}}`)}
	p := plan{Directory: dir, Network: n, Protocol: leasefault.ProtocolRecovery{Owner: n.Owner, NamespaceUID: n.NamespaceUID, StatefulSetUID: n.StatefulSetUID, ClusterID: 1, AlarmMemberID: 2, LeaseID: 3, Key: "/acceptance/test"}, APIServer: "https://10.224.33.1:6443", Endpoint: "127.0.0.1:18681", ServerName: "fixture.invalid", CA: filepath.Join(dir, "ca.crt"), Certificate: filepath.Join(dir, "client.crt"), Key: filepath.Join(dir, "client.key"), ScriptDirectory: filepath.Join(dir, "repo/deploy/test-cluster"), JoinScript: filepath.Join(dir, "join.sh"), TargetsSHA256: strings.Repeat("a", 64), TimeoutSeconds: 5, Files: map[string]string{}}
	for _, name := range []string{testKubeconfig, p.CA, p.Certificate, p.Key, p.JoinScript, "/bin/bash"} {
		p.Files[name] = strings.Repeat("a", 64)
	}
	for _, name := range observerFiles {
		p.Files[filepath.Join(p.ScriptDirectory, name)] = strings.Repeat("a", 64)
	}
	p.Files[filepath.Join(dir, "observer-pod.json")] = digest(n.PodBefore)
	p.Files[filepath.Join(dir, "observer-targets.json")] = p.TargetsSHA256
	return p
}
func savePlan(t *testing.T, p plan) (string, string) {
	t.Helper()
	data, err := json.Marshal(p)
	require.NoError(t, err)
	path := filepath.Join(p.Directory, "plan.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	return path, digest(data)
}

func TestPlanRejectsUnboundInputs(t *testing.T) {
	for _, mode := range []string{"valid", "wrong-hash", "duplicate", "unknown", "binding", "missing-script", "timeout", "public", "symlink", "invalid-protocol"} {
		t.Run(mode, func(t *testing.T) {
			p := planFixture(t)
			switch mode {
			case "binding":
				p.Protocol.Owner = "other"
			case "missing-script":
				delete(p.Files, p.JoinScript)
			case "timeout":
				p.TimeoutSeconds = 301
			case "invalid-protocol":
				p.Protocol.ClusterID = 0
			}
			path, approved := savePlan(t, p)
			switch mode {
			case "wrong-hash":
				approved = strings.Repeat("b", 64)
			case "public":
				require.NoError(t, os.Chmod(path, 0644))
			case "symlink":
				link := path + ".link"
				require.NoError(t, os.Symlink(path, link))
				path = link
			case "duplicate", "unknown":
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				if mode == "duplicate" {
					data = bytes.Replace(data, []byte(`"directory":`), []byte(`"directory":`+strconv.Quote(p.Directory)+`,"directory":`), 1)
				} else {
					data = append(data[:len(data)-1], []byte(`,"unexpected":true}`)...)
				}
				require.NoError(t, os.WriteFile(path, data, 0600))
				approved = digest(data)
			}
			_, err := loadPlan(path, approved)
			if mode == "valid" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestFilesRequireExactPrivateRegularInputs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	data := []byte("non-secret fixture")
	require.NoError(t, os.WriteFile(path, data, 0600))
	p := plan{Key: path, Files: map[string]string{path: digest(data)}}
	require.NoError(t, p.verifyFiles(context.Background()))
	require.NoError(t, os.Chmod(path, 0644))
	require.Error(t, p.verifyFiles(context.Background()))
	require.NoError(t, os.Chmod(path, 0600))
	require.NoError(t, os.WriteFile(path, []byte("changed"), 0600))
	require.Error(t, p.verifyFiles(context.Background()))
	require.NoError(t, os.Rename(path, path+".original"))
	require.NoError(t, os.Symlink(path+".original", path))
	require.Error(t, p.verifyFiles(context.Background()))
}

func TestRecoveryPinsIsolatedJoinIdentity(t *testing.T) {
	p := planFixture(t)
	delete(p.Files, p.JoinScript)
	p.JoinScript = filepath.Join(p.ScriptDirectory, leasefault.IsolatedJoinScript)
	p.Files[p.JoinScript] = strings.Repeat("a", 64)
	path, approved := savePlan(t, p)
	_, err := loadPlan(path, approved)
	require.ErrorContains(t, err, "requires pinned namespace identity")
	identity := filepath.Join(p.Directory, leasefault.IsolatedJoinIdentity)
	data := []byte("v1\t4:12345\t11111111-2222-3333-4444-555555555555\t12345\t6:789\n")
	require.NoError(t, os.WriteFile(identity, data, 0600))
	p.Files[identity] = digest(data)
	path, approved = savePlan(t, p)
	_, err = loadPlan(path, approved)
	require.NoError(t, err, "static check does not claim live PID namespace admission")
	// Exercise the actual repeated recovery file gate using only this input;
	// the static fixture's other pins deliberately are not host credentials.
	p.Files = map[string]string{identity: digest(data)}
	require.NoError(t, p.verifyFiles(context.Background()))
	require.NoError(t, os.Chmod(identity, 0644))
	require.Error(t, p.verifyFiles(context.Background()))
	require.NoError(t, os.Chmod(identity, 0600))
	require.NoError(t, os.WriteFile(identity, append(data, '\n'), 0600))
	require.ErrorContains(t, p.verifyFiles(context.Background()), "identity differs")
	delete(p.Files, identity)
	require.ErrorContains(t, p.verifyFiles(context.Background()), "requires pinned namespace identity")
}

func TestDefaultModeDoesNotConstructClients(t *testing.T) {
	p := planFixture(t)
	path, approved := savePlan(t, p)
	var out bytes.Buffer
	verified, connected := 0, 0
	connect := func(plan) (dynamic.Interface, *grpc.ClientConn, error) {
		connected++
		return nil, nil, errors.New("unexpected cluster client")
	}
	err := runWith(context.Background(), []string{"--plan", path, "--approve-sha256", approved}, &out, connect, func(context.Context, plan) error { verified++; return nil })
	require.NoError(t, err)
	require.Equal(t, 1, verified)
	require.Zero(t, connected)
	require.Contains(t, out.String(), "NO_CLUSTER_ACCESS")
	for _, args := range [][]string{{"-execute"}, {"--execute=true"}, {"--execute", "--execute"}, {"--plan", path, "--plan", path}, {"--plan"}, {"--release"}, {"--execute", "--release", "--release"}, {"--release=true"}} {
		out.Reset()
		require.Error(t, runWith(context.Background(), args, &out, connect, func(context.Context, plan) error { return nil }))
		require.Empty(t, out.String())
		require.Zero(t, connected)
	}
}

func TestExecuteJoinsBeforeJournalOrAPIAccess(t *testing.T) {
	for _, failJoin := range []bool{false, true} {
		t.Run(strconv.FormatBool(failJoin), func(t *testing.T) {
			p := planFixture(t)
			script := "printf 'join fixture ran\\n'\n"
			if failJoin {
				script += "exit 7\n"
			}
			require.NoError(t, os.WriteFile(p.JoinScript, []byte(script), 0600))
			path, approved := savePlan(t, p)
			client := fake.NewSimpleDynamicClient(runtime.NewScheme())
			connected := 0
			connect := func(plan) (dynamic.Interface, *grpc.ClientConn, error) {
				connected++
				// NewClient is lazy; no RPC or network dial is expected here.
				conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
				return client, conn, err
			}
			var out bytes.Buffer
			err := runWith(context.Background(), []string{"--plan", path, "--approve-sha256", approved, "--execute"}, &out, connect, func(context.Context, plan) error { return nil })
			require.Error(t, err)
			if failJoin {
				require.ErrorContains(t, err, "join fault workers")
			} else {
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			require.Equal(t, 1, connected)
			require.Empty(t, out.String())
			require.Empty(t, client.Actions())
			files, err := filepath.Glob(filepath.Join(p.Directory, "recovery-join.*.json"))
			require.NoError(t, err)
			require.Len(t, files, 1)
			data, err := os.ReadFile(files[0])
			require.NoError(t, err)
			var record struct {
				Output []byte
				Error  string
			}
			require.NoError(t, json.Unmarshal(data, &record))
			require.Equal(t, "join fixture ran\n", string(record.Output))
			if failJoin {
				require.Contains(t, record.Error, "exit status 7")
			}
			st, err := os.Stat(files[0])
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0600), st.Mode().Perm())
		})
	}
}
