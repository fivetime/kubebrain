package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

func TestNetworkAdmissionDoesNotRepeatOwnScope(t *testing.T) {
	client := fake.NewSimpleDynamicClient(runtime.NewScheme())
	g := &gates{c: &leasefault.CommandConnections{Client: client}, credential: &x509.Certificate{NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}}
	a := g.admission()
	// NetworkFaultRuntime invokes Own immediately before Network. Missing live
	// scope must still fail Own; Network must not repeat that API read.
	require.Error(t, a.Own(context.Background()))
	require.Len(t, client.Actions(), 1)
	client.ClearActions()
	require.NoError(t, a.Network(context.Background()))
	require.Empty(t, client.Actions())
	g.credential.NotAfter = time.Now().Add(-time.Second)
	require.ErrorContains(t, a.Network(context.Background()), "no longer valid")
	g.credential.NotAfter = time.Now().Add(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, a.Network(ctx), context.Canceled)
	require.Empty(t, client.Actions())
}

func TestExecutionRequestRequiresIndependentDigests(t *testing.T) {
	raw := `{"command_path":"/private/command.json","command_sha256":"` + strings.Repeat("a", 64) + `","release_path":"/private/release.json","release_sha256":"` + strings.Repeat("b", 64) + `"}`
	_, err := readRequest(strings.NewReader(raw))
	require.NoError(t, err)
	for _, input := range []string{"", "{}", raw + raw, raw[:len(raw)-1] + `,"execute":true}`, raw[:len(raw)-1] + `,"command_path":"/other"}`, strings.Replace(raw, "/private/command.json", "relative", 1), strings.Replace(raw, strings.Repeat("a", 64), "", 1), strings.Repeat(" ", 16385)} {
		_, err := readRequest(strings.NewReader(input))
		require.Error(t, err)
	}
}

func TestExecutionRequestDeadlineInterruptsBlockedHandshake(t *testing.T) {
	reader, writer := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err := readRequestBeforeDeadline(ctx, reader)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// The production caller exits PID 1 after this result. Close the fixture
	// writer here so the deliberately detached read goroutine can also finish.
	require.NoError(t, writer.Close())
	require.NoError(t, reader.Close())
}

func TestCommandReviewWindowLeavesTimeForHashApproval(t *testing.T) {
	require.Equal(t, 15*time.Minute, commandReviewWindow)
}

func executorFixture(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	p := &unstructured.Unstructured{}
	require.NoError(t, p.UnmarshalJSON([]byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"kb-lease-fault-fixture","namespace":"kubebrain-dbaas-test","uid":"fixture"},"spec":{"automountServiceAccountToken":false,"containers":[{"name":"driver","image":"ghcr.io/fivetime/kubebrain@sha256:`+strings.Repeat("a", 64)+`","securityContext":{"readOnlyRootFilesystem":true,"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}}]}}`)))
	return p
}

func TestExecutorRejectsUnsafeNamespaceOrContainer(t *testing.T) {
	require.NoError(t, checkExecutor(executorFixture(t)))
	for _, field := range []string{"hostPID", "hostIPC", "hostNetwork", "shareProcessNamespace"} {
		p := executorFixture(t)
		require.NoError(t, unstructured.SetNestedField(p.Object, true, "spec", field))
		require.Error(t, checkExecutor(p), field)
	}
	for _, mode := range []string{"foreign", "automount", "init", "ephemeral", "host-volume", "sidecar", "writable-root", "privileged", "escalation", "capability", "mutable-image"} {
		t.Run(mode, func(t *testing.T) {
			p := executorFixture(t)
			spec := p.Object["spec"].(map[string]any)
			container := spec["containers"].([]any)[0].(map[string]any)
			security := container["securityContext"].(map[string]any)
			switch mode {
			case "foreign":
				p.SetNamespace("business")
			case "automount":
				delete(spec, "automountServiceAccountToken")
			case "init":
				spec["initContainers"] = []any{container}
			case "ephemeral":
				spec["ephemeralContainers"] = []any{container}
			case "host-volume":
				spec["volumes"] = []any{map[string]any{"hostPath": map[string]any{"path": "/"}}}
			case "sidecar":
				spec["containers"] = []any{container, container}
			case "writable-root":
				security["readOnlyRootFilesystem"] = false
			case "privileged":
				security["privileged"] = true
			case "escalation":
				security["allowPrivilegeEscalation"] = true
			case "capability":
				security["capabilities"] = map[string]any{"drop": []any{"ALL"}, "add": []any{"SYS_ADMIN"}}
			case "mutable-image":
				container["image"] = "ghcr.io/fivetime/kubebrain:dbaas"
			}
			require.Error(t, checkExecutor(p))
		})
	}
}

func TestCaseScopeRefusesDifferentCandidateBeforeAdmission(t *testing.T) {
	p := leasefault.NativeCommandPlan{}
	require.ErrorContains(t, checkScope(p, "/private/owner", strings.Repeat("a", 64)), "restricted")
	data, err := os.ReadFile("../../../../pkg/server/etcd/lease.go")
	require.NoError(t, err)
	require.Equal(t, leaseSourceHash, planinput.SHA256(data), "re-review wait-site binding after a product change")
}

func TestCaseScopeBindsCurrentReviewedProductCandidate(t *testing.T) {
	const owner = "/private/attempt"
	identity := strings.Repeat("a", 64)
	p := leasefault.NativeCommandPlan{}
	p.OwnerDirectory = owner
	p.Files = map[string]string{filepath.Join(owner, leasefault.IsolatedJoinIdentity): identity}
	p.JoinScript = filepath.Join(owner, leasefault.IsolatedJoinScript)
	p.StatefulSetName = "kubebrain-local"
	p.Bindings.Network.Namespace = namespace
	p.Bindings.Network.NamespaceUID = namespaceUID
	p.Bindings.Network.StatefulSetUID = stsUID
	p.Bindings.Network.PodName = "kubebrain-local-0"
	p.Bindings.Source = candidateSource
	p.Bindings.Image = candidateImage
	p.Bindings.SourceHash = leaseSourceHash
	p.Kubeconfig = "/root/.kube/kubebrain-test-10.32.32.66.conf"
	p.KubeContext = "kubebrain-test-10.32.32.66"
	p.APIServer = "https://10.224.33.1:6443"
	// Identity admission succeeds, but missing pinned inputs still refuse execution.
	require.ErrorContains(t, checkScope(p, owner, identity), "exact reviewed observer environment")
	p.Bindings.Source = "6c295888a8ef5ea0763eb8d0b1c0597b83211a3e"
	require.ErrorContains(t, checkScope(p, owner, identity), "restricted")
	p.Bindings.Source = candidateSource
	p.Bindings.Image = "ghcr.io/fivetime/kubebrain@sha256:76e25a1e6dea2fcd9e4bffdc01317d1c74ef43dcd0d6e797f40a257a3c48b1b8"
	require.ErrorContains(t, checkScope(p, owner, identity), "restricted")
}

// Exercise the actual main program as PID 1. A bad external approval must stop
// before opening a cluster connection or creating any attempt/claim artifact.
func TestDriverStartupAndRefusalInRealPIDNamespace(t *testing.T) {
	if out, err := exec.Command("unshare", "--fork", "--pid", "--mount-proc", "/bin/true").CombinedOutput(); err != nil {
		if strings.Contains(string(out), "Operation not permitted") {
			t.Skip("PID namespace unavailable; not executed")
		}
		require.NoError(t, err, string(out))
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	owner := t.TempDir()
	require.NoError(t, os.Chmod(owner, 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "unshare", "--fork", "--pid", "--mount-proc", "--kill-child=KILL", executable, "-test.run=^TestDriverMainHelper$")
	cmd.Env = append(os.Environ(), "KB_RUN_HELPER_OWNER="+owner)
	cmd.Stdin = strings.NewReader("{}")
	out, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.NoError(t, ctx.Err(), string(out))
	require.Contains(t, string(out), "require exact command and release approvals")
	var startup map[string]string
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "{") {
			require.NoError(t, json.Unmarshal([]byte(line), &startup))
		}
	}
	require.Equal(t, owner, startup["owner"])
	identity, err := os.ReadFile(filepath.Join(owner, leasefault.IsolatedJoinIdentity))
	require.NoError(t, err)
	require.Equal(t, planinput.SHA256(identity), startup["startup_identity_sha256"])
	entries, err := os.ReadDir(owner)
	require.NoError(t, err)
	require.Len(t, entries, 1, "invalid approval must not start an attempt")
}

func TestDriverMainHelper(t *testing.T) {
	owner := os.Getenv("KB_RUN_HELPER_OWNER")
	if owner == "" {
		return
	}
	os.Args = []string{"lease-fault-run", "--owner", owner, "--execute"}
	main()
	t.Fatal("invalid request unexpectedly completed")
}
