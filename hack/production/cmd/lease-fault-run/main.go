// lease-fault-run is the one-shot, PID-1 driver for the dedicated-cluster lease
// acceptance case. It is not a general chaos runner. Plans are supplied only
// after the caller has reviewed the startup identity printed on stdout.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	strictjson "sigs.k8s.io/json"
)

// Image workflow 36739259406, attempt 1; release artifact 11111945808.
const candidateSource = "4906a5f87b4282d860c5fd748be69ca68d42e898"
const candidateImage = "ghcr.io/fivetime/kubebrain@sha256:611caa13c3403edd98c92e3fa605ebe31edeb62b52b4172004b5698527c7fe51"
const leaseSourceHash = "3c98f802359a5f185dc6e618691ad6098641a54afa528668c6dfcaf8091ccd88"
const namespace = "kubebrain-dbaas-test"
const namespaceUID = "6c57c242-912b-41bb-9020-f4fdb3225ef3"
const stsUID = "7d760f53-5bb5-4429-a2f8-651b89665616"

// The controller must finish staging and exit all exec sessions before sending
// this single record followed by EOF on stdin. A digest discovered by the
// driver is not substituted for either independently approved digest.
type request struct {
	CommandPath string `json:"command_path"`
	CommandSHA  string `json:"command_sha256"`
	ReleasePath string `json:"release_path"`
	ReleaseSHA  string `json:"release_sha256"`
}

func readRequest(in io.Reader) (request, error) {
	var r request
	data, err := io.ReadAll(io.LimitReader(in, 16385))
	if err != nil || len(data) > 16384 {
		return r, errors.New("invalid or oversized execution request")
	}
	strict, err := strictjson.UnmarshalStrict(data, &r, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 || !planinput.ValidSHA256(r.CommandSHA) || !planinput.ValidSHA256(r.ReleaseSHA) {
		return request{}, errors.New("require exact command and release approvals")
	}
	for _, path := range []string{r.CommandPath, r.ReleasePath} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
			return request{}, errors.New("invalid approved plan path")
		}
	}
	return r, nil
}

func checkScope(p leasefault.NativeCommandPlan, owner, identity string) error {
	n := p.Bindings.Network
	if p.OwnerDirectory != owner || p.Files[filepath.Join(owner, leasefault.IsolatedJoinIdentity)] != identity ||
		filepath.Base(p.JoinScript) != leasefault.IsolatedJoinScript || p.StatefulSetName != "kubebrain-local" ||
		n.Namespace != namespace || n.NamespaceUID != namespaceUID || n.StatefulSetUID != stsUID ||
		p.Bindings.Source != candidateSource || p.Bindings.Image != candidateImage || p.Bindings.SourceHash != leaseSourceHash ||
		p.Kubeconfig != "/root/.kube/kubebrain-test-10.32.32.66.conf" || p.KubeContext != "kubebrain-test-10.32.32.66" || p.APIServer != "https://10.224.33.1:6443" {
		return errors.New("execution is restricted to the reviewed candidate, startup identity and dedicated cluster")
	}
	if n.PodName != "kubebrain-local-0" && n.PodName != "kubebrain-local-1" && n.PodName != "kubebrain-local-2" {
		return errors.New("unsupported fault target")
	}
	return checkInputs(p)
}

// This case uses the existing observer layout. Reject arbitrary shell startup
// environment and require pins for its transitive scripts/data, not just argv[0].
func checkInputs(p leasefault.NativeCommandPlan) error {
	env := map[string]string{}
	for _, entry := range p.Env {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || env[name] != "" {
			return errors.New("invalid execution environment")
		}
		env[name] = value
	}
	want := map[string]string{"PATH": "/usr/local/bin:/usr/bin:/bin", "stack_owner": p.OwnerDirectory,
		"stack_kubeconfig": p.Kubeconfig, "stack_context": p.KubeContext, "stack_namespace": namespace,
		"stack_namespace_uid": namespaceUID, "stack_sts": "kubebrain-local", "stack_sts_uid": stsUID,
		"stack_tls": filepath.Join(p.OwnerDirectory, "tls"), "stack_server_name": "kubebrain-local-info.kubebrain-dbaas-test.svc"}
	if len(env) != len(want) {
		return errors.New("execution environment must be the exact reviewed observer environment")
	}
	for name, value := range want {
		if env[name] != value {
			return fmt.Errorf("unexpected execution environment: %s", name)
		}
	}
	if p.CA != filepath.Join(want["stack_tls"], "ca.crt") || p.Certificate != filepath.Join(want["stack_tls"], "probe.crt") || p.Key != filepath.Join(want["stack_tls"], "probe.key") {
		return errors.New("RPC and diagnostic TLS inputs must use the same pinned identity")
	}
	required := []string{"executor-pod.json", "observer-pod.json", "observer-targets.json", "diagnostic-spec.json", "diagnostic-inputs.sha256", "tools.sha256", "info.crt", "bin/info-diagnostic-probe"}
	for _, name := range required {
		if !planinput.ValidSHA256(p.Files[filepath.Join(p.OwnerDirectory, name)]) {
			return fmt.Errorf("missing pinned observer input: %s", name)
		}
	}
	dir := filepath.Dir(p.StackExecutable)
	for _, name := range []string{"protected-wait-worker.sh", "protected-metrics-worker.sh", "protected-stack-session.sh", "join-isolated-fault-workers.sh", "same-pod-process.jq", "expired-lease-wait-frames.jq", "capture-local-cilium-drops.sh", "capture-local-cilium-endpoint.sh", "observe-local-backend-drops.sh", "observe-local-backend-tcp.sh", "observe-local-fault-label.sh", "observe-local-network-restored.sh", "observe-local-nonces.sh", "observe-local-policy-state.sh", "local-label-identity-transition.jq", "local-policy-observation.jq", "local-nonce-endpoints.jq", "../../hack/production/same-pod-process.jq", "../../hack/production/monitor-stream.jq", "../../hack/production/backend-drops.jq"} {
		if !planinput.ValidSHA256(p.Files[filepath.Join(dir, name)]) {
			return fmt.Errorf("missing pinned observer dependency: %s", name)
		}
	}
	// Shared libraries are supplied by the independently admitted, immutable
	// execution image; executable files used through PATH are pinned as well.
	for _, name := range []string{"bash", "jq", "kubectl", "timeout", "date", "sha256sum", "grep", "ss", "find", "mktemp", "stat", "openssl", "cut", "sed", "dirname", "basename", "sleep", "mkdir"} {
		found := false
		for _, directory := range []string{"/usr/local/bin", "/usr/bin", "/bin"} {
			path := filepath.Join(directory, name)
			if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
				continue
			} else if err != nil {
				return err
			}
			if !planinput.ValidSHA256(p.Files[path]) {
				return fmt.Errorf("missing pinned PATH dependency: %s", path)
			}
			found = true
			break
		}
		if !found {
			return fmt.Errorf("missing PATH dependency: %s", name)
		}
	}
	if !planinput.ValidSHA256(p.Files["/bin/bash"]) {
		return errors.New("missing Join interpreter pin")
	}
	return nil
}

type gates struct {
	p          leasefault.NativeCommandPlan
	c          *leasefault.CommandConnections
	self       []byte
	credential *x509.Certificate
}

func newGates(p leasefault.NativeCommandPlan) (*gates, error) {
	executable, err := os.Executable()
	if err != nil || !planinput.ValidSHA256(p.Files[executable]) {
		return nil, errors.New("running driver executable must be independently pinned")
	}
	path := filepath.Join(p.OwnerDirectory, "executor-pod.json")
	raw, err := planinput.ReadFile(path, true, 256<<10)
	if err != nil || planinput.SHA256(raw) != p.Files[path] {
		return nil, errors.New("missing independently pinned execution Pod")
	}
	var pod unstructured.Unstructured
	if err := pod.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	if err := checkExecutor(&pod); err != nil {
		return nil, err
	}
	pair, err := tls.LoadX509KeyPair(p.Certificate, p.Key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, err
	}
	o, err := p.ObservationPlan()
	if err != nil {
		return nil, err
	}
	c, err := o.OpenConnections(p.Kubeconfig, p.KubeContext, p.APIServer, p.ObserverEndpoint, p.ObserverServerName, p.Files)
	if err != nil {
		return nil, err
	}
	return &gates{p: p, c: c, self: raw, credential: cert}, nil
}

func checkExecutor(pod *unstructured.Unstructured) error {
	if pod.GetNamespace() != namespace || !strings.HasPrefix(pod.GetName(), "kb-lease-fault-") || pod.GetUID() == "" || pod.GetDeletionTimestamp() != nil {
		return errors.New("invalid dedicated execution Pod identity")
	}
	for _, field := range []string{"hostPID", "hostIPC", "hostNetwork", "shareProcessNamespace"} {
		v, _, err := unstructured.NestedBool(pod.Object, "spec", field)
		if err != nil || v {
			return errors.New("execution Pod must have private namespaces")
		}
	}
	automount, found, err := unstructured.NestedBool(pod.Object, "spec", "automountServiceAccountToken")
	if err != nil || !found || automount {
		return errors.New("execution Pod must disable automatic credentials")
	}
	containers, _, err := unstructured.NestedSlice(pod.Object, "spec", "containers")
	ephemeral, _, ephemeralErr := unstructured.NestedSlice(pod.Object, "spec", "ephemeralContainers")
	if err != nil || ephemeralErr != nil || len(containers) != 1 || len(ephemeral) != 0 {
		return errors.New("execution Pod must have one container and no exec sidecars")
	}
	container, ok := containers[0].(map[string]interface{})
	if !ok {
		return errors.New("invalid execution container")
	}
	image, _, _ := unstructured.NestedString(container, "image")
	if !regexp.MustCompile(`^ghcr\.io/fivetime/kubebrain@sha256:[a-f0-9]{64}$`).MatchString(image) {
		return errors.New("execution image must be independently admitted by digest")
	}
	init, _, initErr := unstructured.NestedSlice(pod.Object, "spec", "initContainers")
	if initErr != nil || len(init) != 0 {
		return errors.New("this execution layout does not admit init containers")
	}
	drop, _, dropErr := unstructured.NestedStringSlice(container, "securityContext", "capabilities", "drop")
	add, _, addErr := unstructured.NestedStringSlice(container, "securityContext", "capabilities", "add")
	if dropErr != nil || addErr != nil || len(drop) != 1 || drop[0] != "ALL" || len(add) != 0 {
		return errors.New("execution container must drop all capabilities")
	}
	readOnly, _, readErr := unstructured.NestedBool(container, "securityContext", "readOnlyRootFilesystem")
	privileged, _, privErr := unstructured.NestedBool(container, "securityContext", "privileged")
	escalate, found, escalateErr := unstructured.NestedBool(container, "securityContext", "allowPrivilegeEscalation")
	if readErr != nil || !readOnly || privErr != nil || privileged || escalateErr != nil || !found || escalate {
		return errors.New("execution container security differs from required isolation")
	}
	volumes, _, err := unstructured.NestedSlice(pod.Object, "spec", "volumes")
	if err != nil {
		return err
	}
	for _, volume := range volumes {
		v, ok := volume.(map[string]interface{})
		if !ok || v["hostPath"] != nil {
			return errors.New("execution Pod must not mount host paths")
		}
	}
	return nil
}

func (g *gates) local(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	if now.Before(g.credential.NotBefore) || !now.Before(g.credential.NotAfter) {
		return errors.New("admitted client credential is no longer valid")
	}
	return ctx.Err()
}

func (g *gates) scope(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ns, err := g.c.Client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}).Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(ns.GetUID()) != namespaceUID || ns.GetDeletionTimestamp() != nil {
		return errors.New("dedicated namespace changed")
	}
	o, err := g.p.ObservationPlan()
	if err != nil {
		return err
	}
	return o.VerifyCommandDeployment(ctx, g.c.Client, "kubebrain-local", g.local)
}

func (g *gates) tools(ctx context.Context) error {
	if err := g.p.VerifyFiles(ctx); err != nil {
		return err
	}
	if err := g.local(ctx); err != nil {
		return err
	}
	if err := g.scope(ctx); err != nil {
		return err
	}
	return leasefault.CheckLivePodProcess(ctx, g.c.Client, g.self, g.p.Processes.JQ, g.p.Processes.Predicate, g.local,
		func(data []byte, observed error) error {
			return leasefault.RetainRecoveryObserver(g.p.OwnerDirectory, "executor", data, observed)
		})
}

func (g *gates) member(ctx context.Context, conn grpc.ClientConnInterface, id uint64) error {
	if err := g.scope(ctx); err != nil {
		return err
	}
	response, err := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
	if err != nil {
		return err
	}
	if response.GetHeader().GetClusterId() != g.p.Bindings.Protocol.ClusterID || response.GetHeader().GetMemberId() != id {
		return errors.New("live TLS endpoint has the wrong member identity")
	}
	return ctx.Err()
}

func (g *gates) admission() leasefault.CommandAdmission {
	original := func(ctx context.Context) error {
		return g.member(ctx, g.c.Original, g.p.Bindings.Protocol.AlarmMemberID)
	}
	successor := func(ctx context.Context) error { return g.member(ctx, g.c.Successor, g.p.Bindings.ObserverMemberID) }
	return leasefault.CommandAdmission{Tools: g.tools, Own: g.scope, Original: original, Network: g.scope, Successor: successor, Metrics: g.scope, Outcome: successor,
		Stack: func(ctx context.Context, stage string) error {
			if stage != "before" && stage != "after" {
				return errors.New("invalid stack stage")
			}
			return original(ctx)
		}}
}

func execute(ctx context.Context, owner, identity string, r request) (err error) {
	p, err := leasefault.LoadNativeCommandPlan(r.CommandPath, r.CommandSHA)
	if err != nil {
		return err
	}
	if err := checkScope(p, owner, identity); err != nil {
		return err
	}
	// Authenticate CI/release before starting the fault clock. During the run,
	// immutable plan/file pins and fresh live image/process checks preserve this
	// exact candidate binding; no network provenance lookup extends the clock.
	if err := leasefault.AuthenticateCommandRelease(ctx, r.CommandPath, r.CommandSHA, r.ReleasePath, r.ReleaseSHA); err != nil {
		return err
	}
	g, err := newGates(p)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, g.c.Close()) }()
	// Refuse leftover staging/exec processes before acquiring any cluster claim.
	if err := leasefault.RunFaultJoin(ctx, owner, p.JoinScript, g.tools); err != nil {
		return err
	}
	result, runErr := leasefault.RunNativeCommand(ctx, r.CommandPath, r.CommandSHA, g.admission())
	if result.Owner == nil || !result.Lifecycle.RecoveryAttempted || result.Lifecycle.RecoveryError != nil {
		return errors.Join(runErr, errors.New("claim release not authorized by completed recovery; inspect retained evidence"))
	}
	// Release after fresh proof even when the acceptance result failed. Retain
	// that failure independently; successful recovery must not convert it to PASS.
	recovery, err := time.ParseDuration(p.RecoveryTimeout)
	if err != nil {
		return errors.Join(runErr, err)
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), recovery)
	defer cancel()
	observer := leasefault.NetworkObserver{Directory: owner, StatefulSetName: p.StatefulSetName, ScriptDirectory: filepath.Dir(p.StackExecutable), TargetsSHA256: p.TargetsSHA256, Network: p.Bindings.Network, Client: g.c.Client, Env: p.Env, Admit: g.tools,
		Retain: func(stage string, data []byte, observed error) error {
			return leasefault.RetainRecoveryObserver(owner, stage, data, observed)
		}}
	releaseErr := result.Owner.ReleaseRecovered(releaseCtx, leasefault.RecoveryReleaseProof{Network: p.Bindings.Network, Protocol: p.Bindings.Protocol, Connection: g.c.Successor, Admit: g.tools,
		Join: func(ctx context.Context) error { return leasefault.RunFaultJoin(ctx, owner, p.JoinScript, g.tools) }, NetworkRestored: observer.Restored, IdentityRestored: observer.Unlabelled})
	return errors.Join(runErr, releaseErr)
}

func main() {
	f := flag.NewFlagSet("lease-fault-run", flag.ContinueOnError)
	owner := f.String("owner", "", "fresh private owner directory in the dedicated execution Pod")
	executeFlag := f.Bool("execute", false, "execute one independently approved fault attempt and restore it")
	if err := f.Parse(os.Args[1:]); err != nil || f.NArg() != 0 || !*executeFlag {
		fmt.Fprintln(os.Stderr, "require --owner and explicit --execute")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	startup, cancelStartup := context.WithTimeout(ctx, 5*time.Second)
	identity, err := leasefault.CaptureIsolatedJoinIdentity(startup, *owner)
	cancelStartup()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"startup_identity_sha256": identity, "owner": *owner}); err != nil {
		os.Exit(1)
	}
	// Bound the external staging handshake; close stdin to unblock ReadAll on
	// cancellation. The watcher is joined before any execution goroutine starts.
	staging, cancelStaging := context.WithTimeout(ctx, 5*time.Minute)
	joined := make(chan struct{})
	go func() { defer close(joined); <-staging.Done(); _ = os.Stdin.Close() }()
	r, err := readRequest(os.Stdin)
	stagingErr := staging.Err()
	cancelStaging()
	<-joined
	if err = errors.Join(err, stagingErr); err == nil {
		runCtx, cancelRun := context.WithTimeout(ctx, 3*time.Minute)
		err = execute(runCtx, *owner, identity, r)
		cancelRun()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stdout, "FAULT_AND_RECOVERY_PASS")
}
