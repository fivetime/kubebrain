package compat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const compatScriptCommandTimeout = 30 * time.Second
const compatScriptOutputLimitBytes = 1 << 20
const compatCommandWaitDelay = 5 * time.Second

func runDifferentialScript(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runCompatScriptCommand(t, "run-differential.sh", env)
}

func runCompatSuiteScript(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runCompatScriptCommand(t, "run.sh", env)
}

func runCompatScriptCommand(t *testing.T, script string, env []string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), compatScriptCommandTimeout)
	defer cancel()

	output, err := runCompatCommandContext(t, ctx, "bash", []string{script}, env)
	if ctx.Err() == context.DeadlineExceeded {
		require.Failf(t, "compat script timed out", "script=%s timeout=%s output:\n%s", script, compatScriptCommandTimeout, string(output))
	}
	return output, err
}

func requireRunnerRejectsUnreachableAdvertisedClientURLBeforeMutation(
	t *testing.T,
	script, endpointEnv, expectedMessage string,
) {
	t.Helper()
	dir := t.TempDir()
	fakeEtcd := writeFakeReferenceEtcd(t, dir)
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"--endpoints=http://internal.invalid:3379"* ]]; then
  exit 1
fi
if [[ "$*" == *"auth status"* || "$*" == *"endpoint status"* || "$*" == *"get '' --from-key"* || "$*" == *"user list"* || "$*" == *"role list"* || "$*" == *"lease list"* || "$*" == *"alarm list"* ]]; then
  echo "mutation-adjacent preflight reached" >&2
  exit 9
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "curl"), []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	output, err := runCompatScriptCommand(t, script, []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		endpointEnv + "=127.0.0.1:24379",
		"ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA=true",
		"ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION=true",
		"ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), expectedMessage)
	require.Contains(t, string(output), "http://internal.invalid:3379")
	require.NotContains(t, string(output), "mutation-adjacent preflight reached")
}

func runCompatShellCommandContext(t *testing.T, ctx context.Context, command string) ([]byte, error) {
	t.Helper()
	return runCompatCommandContext(t, ctx, "bash", []string{"-c", command}, nil)
}

func runCompatKubectlContext(t *testing.T, ctx context.Context, args ...string) ([]byte, error) {
	t.Helper()
	return runCompatCommandContext(t, ctx, "kubectl", args, nil)
}

func runCompatCommandContext(t *testing.T, ctx context.Context, commandName string, args []string, env []string) ([]byte, error) {
	t.Helper()
	return runCompatCommand(ctx, commandName, args, env)
}

func runCompatCommandInputContext(t *testing.T, ctx context.Context, commandName string, args []string, env []string, input []byte) ([]byte, error) {
	t.Helper()
	command := exec.CommandContext(ctx, commandName, args...)
	configureCompatProcessGroup(command)
	command.WaitDelay = compatCommandWaitDelay
	command.Env = append(os.Environ(), env...)
	command.Stdin = bytes.NewReader(input)
	return compatCombinedOutput(command, compatScriptOutputLimitBytes)
}

func runCompatCommand(ctx context.Context, commandName string, args []string, env []string) ([]byte, error) {
	command := exec.CommandContext(ctx, commandName, args...)
	configureCompatProcessGroup(command)
	command.WaitDelay = compatCommandWaitDelay
	command.Env = append(os.Environ(), env...)
	return compatCombinedOutput(command, compatScriptOutputLimitBytes)
}

func startCompatCommand(t *testing.T, commandName string, args ...string) func() {
	t.Helper()
	if referenceBinary := os.Getenv("REFERENCE_ETCD_BINARY"); referenceBinary != "" && commandName == referenceBinary {
		requireReferenceEtcdProvenance(t, referenceBinary)
	}
	processCtx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(processCtx, commandName, args...)
	configureCompatProcessGroup(command)
	command.WaitDelay = compatCommandWaitDelay
	require.NoError(t, command.Start())
	processDone := make(chan struct{})
	go func() {
		_ = command.Wait()
		close(processDone)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-processDone
		})
	}
	t.Cleanup(stop)
	return stop
}

func requireReferenceEtcdProvenance(t *testing.T, binary string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), compatScriptCommandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", "verify-reference-etcd-provenance.sh")
	command.Env = append(os.Environ(), "REFERENCE_ETCD_BIN="+binary)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func configureCompatProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		// Give shell fault injectors a chance to run their EXIT/TERM cleanup
		// traps before Cmd.WaitDelay escalates termination. Killing the entire
		// process group immediately can strand iptables rules owned by children.
		err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
}

func compatCombinedOutput(command *exec.Cmd, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("process output limit must be positive")
	}
	if command.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	if command.Stderr != nil {
		return nil, errors.New("exec: Stderr already set")
	}
	if usesCompatProcessGroup(command) && command.Cancel != nil {
		defer func() {
			_ = command.Cancel()
		}()
	}
	output := &compatBoundedOutput{limit: limit, cancel: command.Cancel}
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	if output.exceeded() {
		return output.bytes(), fmt.Errorf("process output exceeds %d bytes", limit)
	}
	return output.bytes(), err
}

func usesCompatProcessGroup(command *exec.Cmd) bool {
	return command.SysProcAttr != nil && command.SysProcAttr.Setpgid
}

type compatBoundedOutput struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	limit  int64
	cancel func() error
	over   bool
}

func (o *compatBoundedOutput) Write(data []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.over {
		return 0, fmt.Errorf("process output exceeds %d bytes", o.limit)
	}
	remaining := o.limit - int64(o.buffer.Len())
	if remaining <= 0 {
		o.over = true
		if o.cancel != nil {
			_ = o.cancel()
		}
		return 0, fmt.Errorf("process output exceeds %d bytes", o.limit)
	}
	if int64(len(data)) > remaining {
		o.buffer.Write(data[:remaining])
		o.over = true
		if o.cancel != nil {
			_ = o.cancel()
		}
		return int(remaining), fmt.Errorf("process output exceeds %d bytes", o.limit)
	}
	return o.buffer.Write(data)
}

func (o *compatBoundedOutput) bytes() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]byte(nil), o.buffer.Bytes()...)
}

func (o *compatBoundedOutput) exceeded() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.over
}

func TestCompatCombinedOutputCleansProcessGroupDescendantsAfterParentExit(t *testing.T) {
	command := compatHelperCommand("daemonize")
	configureCompatProcessGroup(command)
	output, err := compatCombinedOutput(command, compatScriptOutputLimitBytes)
	require.NoError(t, err, "output=%q", string(output))
	pid, err := strconv.Atoi(strings.TrimSpace(string(output)))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	})
	require.Eventually(t, func() bool {
		return !compatProcessExists(pid)
	}, 2*time.Second, 10*time.Millisecond, "descendant process %d still exists after compatCombinedOutput returned", pid)
}

func compatProcessExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || !errors.Is(err, syscall.ESRCH)
}

func compatHelperCommand(mode string) *exec.Cmd {
	command := exec.CommandContext(context.Background(), os.Args[0], "-test.run=TestCompatCommandHelper", "--", mode)
	command.Env = append(os.Environ(), "KUBEBRAIN_COMPAT_COMMAND_HELPER=1")
	return command
}

func TestCompatCommandHelper(t *testing.T) {
	if os.Getenv("KUBEBRAIN_COMPAT_COMMAND_HELPER") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	switch mode {
	case "daemonize":
		child := exec.Command(os.Args[0], "-test.run=TestCompatCommandHelper", "--", "park")
		child.Env = append(os.Environ(), "KUBEBRAIN_COMPAT_COMMAND_HELPER=1")
		if err := child.Start(); err != nil {
			_, _ = os.Stderr.WriteString("start child: " + err.Error() + "\n")
			os.Exit(3)
		}
		_, _ = os.Stdout.WriteString(strconv.Itoa(child.Process.Pid) + "\n")
	case "park":
		time.Sleep(30 * time.Second)
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func TestCompatFailoverCommandsUseBoundedHelpers(t *testing.T) {
	for _, testFile := range []string{
		"auth_watch_failover_test.go",
		"backend_quorum_failover_test.go",
		"http_gateway_concurrency_zero_lease_failover_test.go",
		"lease_checkpoint_failover_test.go",
		"lease_expiry_spread_failover_test.go",
		"lease_read_failover_test.go",
		"lease_renewal_soak_failover_test.go",
		"mutation_failover_test.go",
		"revision_monotonic_failover_test.go",
	} {
		t.Run(testFile, func(t *testing.T) {
			data, err := os.ReadFile(testFile)
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, "runCompatShellCommandContext(t, ctx,")
			require.NotContains(t, text, `exec.CommandContext(ctx, "bash", "-c"`)
		})
	}

	helper, err := os.ReadFile("failover_helpers_test.go")
	require.NoError(t, err)
	helperText := string(helper)
	require.Contains(t, helperText, "runCompatKubectlContext(")
	require.NotContains(t, helperText, `exec.CommandContext(`)
	require.NotContains(t, helperText, ".CombinedOutput()")
}

func TestLeaseRenewalNetworkPartitionHelperIsRecoverable(t *testing.T) {
	data, err := os.ReadFile("../dev/lease-renewal-failover-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `FAILOVER_MODE="${FAILOVER_MODE:-pod-delete}"`)
	require.Contains(t, script, `KIND_NODE_CONTAINER="${KIND_NODE_CONTAINER:-}"`)
	require.Contains(t, script, `jsonpath='{.spec.nodeName}'`)
	require.Contains(t, script, `cannot resolve a valid node for leader Pod`)
	require.Contains(t, script, `current_context="$(kubectl config current-context)"`)
	require.Contains(t, script, `candidate="${KIND_NODE_CONTAINER:-$pod_node}"`)
	require.Contains(t, script, `"$candidate" != "$pod_node"`)
	require.Contains(t, script, `io.x-k8s.kind.cluster`)
	require.Contains(t, script, `"$container_cluster" != "$cluster_name"`)
	require.Contains(t, script, `cannot resolve leader node container`)
	require.Contains(t, script, `cannot resolve explicit node container`)
	require.Contains(t, script, `KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL`)
	require.Contains(t, script, `KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE`)
	require.Contains(t, script, `KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE`)
	require.Contains(t, script, `network-partition)`)
	require.Contains(t, script, `PARTITION_DIRECTION="${PARTITION_DIRECTION:-both}"`)
	require.Contains(t, script, `PARTITION_DROP_PERCENT="${PARTITION_DROP_PERCENT:-100}"`)
	require.Contains(t, script, `PARTITION_DROP_PERCENT must be an integer in [1,100]`)
	require.Contains(t, script, `partition_probability_args=(-m statistic --mode random --probability "$drop_probability")`)
	require.Contains(t, script, `"${partition_probability_args[@]}" -m comment`)
	require.Contains(t, script, `both|ingress|egress)`)
	require.Contains(t, script, `PARTITION_DIRECTION must be both, ingress, or egress`)
	require.Contains(t, script, `"$PARTITION_DIRECTION" == "both" || "$PARTITION_DIRECTION" == "egress"`)
	require.Contains(t, script, `"$PARTITION_DIRECTION" == "both" || "$PARTITION_DIRECTION" == "ingress"`)
	require.Contains(t, script, `KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL="${KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL:-120}"`)
	require.Contains(t, script, `KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION="${KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION:-2m}"`)
	require.Contains(t, script, `KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE="${KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE:-90s}"`)
	require.Contains(t, script, `{{.HostConfig.Privileged}}`)
	require.Contains(t, script, `partition_tag="kubebrain-lease-partition-${leader_name}-$$"`)
	require.Contains(t, script, `trap cleanup_partition EXIT`)
	require.Contains(t, script, `trap 'cleanup_partition; exit 130' INT`)
	require.Contains(t, script, `trap 'cleanup_partition; exit 143' TERM`)
	require.Contains(t, script, `trap - EXIT INT TERM`)
	require.Contains(t, script, `iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip"`)
	require.Contains(t, script, `iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip"`)
	require.Contains(t, script, `iptables -w 5 -L FORWARD -n -v -x`)
	require.Contains(t, script, `network partition rules did not drop packets`)
	require.Contains(t, script, `direction=$PARTITION_DIRECTION`)
	require.Contains(t, script, `drop_percent=$PARTITION_DROP_PERCENT`)
	require.Contains(t, script, `node=$KIND_NODE_CONTAINER`)
	require.Contains(t, script, `observed_uid="$(kubectl -n "$NAMESPACE" get pod "$leader_name"`)
	require.Contains(t, script, `"$observed_uid" != "$partition_pod_uid"`)
	require.Contains(t, script, `deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))`)
	require.Contains(t, script, `while (( SECONDS < deadline ))`)
	require.NotContains(t, script, `attempts=$((PARTITION_FAILOVER_TIMEOUT_SECONDS * 2))`)
	require.Contains(t, script, `lease renewal failover smoke requires a healthy endpoint`)
	require.Contains(t, script, `iptables -w 5 -D FORWARD -s "$partition_pod_ip"`)
	require.Contains(t, script, `iptables -w 5 -D FORWARD -d "$partition_pod_ip"`)
	require.Contains(t, script, `[[ "$new_leader" =~ ^[1-9][0-9]*$ ]]`)
	require.NotContains(t, script, "eval ")
}

func TestLeasePartitionWorkerNodeSmokeIsIsolatedAndRecoverable(t *testing.T) {
	data, err := os.ReadFile("../dev/lease-partition-worker-node-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `refusing to reuse existing kind cluster`)
	require.Contains(t, script, `original_context="$(kubectl config current-context`)
	require.Contains(t, script, `kind delete cluster --name "$CLUSTER_NAME"`)
	require.Contains(t, script, `kubectl config use-context "$original_context"`)
	require.Contains(t, script, `--overrides="{\"spec\":{\"nodeName\":\"$WORKER_NODE\"}}"`)
	require.Contains(t, script, `docker exec "$CONTROL_PLANE_NODE"`)
	require.Contains(t, script, `docker exec "$WORKER_NODE" iptables-save`)
	require.Contains(t, script, `node=$WORKER_NODE`)
	require.Contains(t, script, `worker partition drill left an iptables rule behind`)
	require.NotContains(t, script, `iptables-save | grep -q`)
	require.Contains(t, script, `trap cleanup EXIT`)
	require.Contains(t, script, `trap 'exit 130' INT`)
	require.Contains(t, script, `trap 'exit 143' TERM`)
	require.NotContains(t, script, `kind delete cluster --name "$original_context"`)

	verifyData, err := os.ReadFile("../dev/verify.sh")
	require.NoError(t, err)
	verifyScript := string(verifyData)
	require.Contains(t, verifyScript, `RUN_LEASE_PARTITION_WORKER_NODE_SMOKE="${RUN_LEASE_PARTITION_WORKER_NODE_SMOKE:-false}"`)
	require.Equal(t, 4, strings.Count(verifyScript, "RUN_LEASE_PARTITION_WORKER_NODE_SMOKE"))
	require.Contains(t, verifyScript, `if [ "$RUN_LEASE_PARTITION_WORKER_NODE_SMOKE" = "true" ]; then`)
	require.Contains(t, verifyScript, `run_step "lease partition worker-node smoke" hack/dev/lease-partition-worker-node-smoke.sh`)
}

func TestMemberListSyncSmokeUsesAdvertisedEndpoints(t *testing.T) {
	data, err := os.ReadFile("../dev/memberlist-sync-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `ENDPOINT="${KUBEBRAIN_ETCD_ENDPOINT:-${ENDPOINT:-}}"`)
	require.Contains(t, script, `memberlist sync smoke requires KUBEBRAIN_ETCD_ENDPOINT (or ENDPOINT)`)
	require.Contains(t, script, `KUBEBRAIN_MEMBERLIST_ENDPOINTS_DIALABLE=1`)
	require.Contains(t, script, `go test -count=1 -run '^TestMemberListSupportsOfficialClientSync$' .`)
	require.NotContains(t, script, `KUBEBRAIN_MEMBERLIST_ENDPOINTS_DIRECT`)

	verifyData, err := os.ReadFile("../dev/verify.sh")
	require.NoError(t, err)
	verifyScript := string(verifyData)
	require.Contains(t, verifyScript, `RUN_MEMBERLIST_SYNC_SMOKE="${RUN_MEMBERLIST_SYNC_SMOKE:-false}"`)
	require.Equal(t, 4, strings.Count(verifyScript, "RUN_MEMBERLIST_SYNC_SMOKE"))
	require.Contains(t, verifyScript, `if [ "$RUN_MEMBERLIST_SYNC_SMOKE" = "true" ]; then`)
	require.Contains(t, verifyScript, `run_step "MemberList clientv3 Sync smoke" env ENDPOINT="$ENDPOINT" hack/dev/memberlist-sync-smoke.sh`)
}

func TestVerifyExposesImmutableCandidateCanaryBehindExplicitGate(t *testing.T) {
	verifyData, err := os.ReadFile("../dev/verify.sh")
	require.NoError(t, err)
	verifyScript := string(verifyData)
	require.Contains(t, verifyScript, `RUN_KUBEBRAIN_CANDIDATE_CANARY="${RUN_KUBEBRAIN_CANDIDATE_CANARY:-false}"`)
	require.Equal(t, 5, strings.Count(verifyScript, "RUN_KUBEBRAIN_CANDIDATE_CANARY"))
	require.Contains(t, verifyScript, `if [ "$RUN_KUBEBRAIN_CANDIDATE_CANARY" = "true" ]; then`)
	require.Contains(t, verifyScript, `candidate canary requires TARGET_IMAGE and TARGET_RUNTIME_DIGESTS`)
	require.Contains(t, verifyScript, `run_step "immutable KubeBrain candidate canary" env PREFLIGHT_ONLY=false hack/production/run-kubebrain-rollout-availability.sh`)
	require.Contains(t, verifyScript, `PREFLIGHT_ONLY=true "${ROOT_DIR}/hack/production/run-kubebrain-rollout-availability.sh"`)
	require.Less(t,
		strings.Index(verifyScript, `candidate canary requires TARGET_IMAGE and TARGET_RUNTIME_DIGESTS`),
		strings.Index(verifyScript, `cd "$ROOT_DIR"`))
	require.Less(t,
		strings.Index(verifyScript, `PREFLIGHT_ONLY=true "${ROOT_DIR}/hack/production/run-kubebrain-rollout-availability.sh"`),
		strings.Index(verifyScript, `cd "$ROOT_DIR"`))
	require.Less(t,
		strings.Index(verifyScript, `run_step "Kubernetes version matrix"`),
		strings.Index(verifyScript, `run_step "immutable KubeBrain candidate canary"`))
	require.Equal(t,
		strings.LastIndex(verifyScript, `run_step "immutable KubeBrain candidate canary"`),
		strings.LastIndex(verifyScript, "run_step "),
		"candidate mutation must remain the final verification step")

	flagsBlock := verifyScript[strings.Index(verifyScript, "RUN_FLAGS=("):]
	flagsBlock = flagsBlock[:strings.Index(flagsBlock, "\n)")]
	env := make([]string, 0, len(os.Environ()))
	for _, item := range os.Environ() {
		name := strings.SplitN(item, "=", 2)[0]
		if !strings.HasPrefix(name, "RUN_") && name != "TARGET_IMAGE" && name != "TARGET_RUNTIME_DIGESTS" &&
			name != "PREFLIGHT_ONLY" && name != "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT" && name != "KUBECTL_BIN" {
			env = append(env, item)
		}
	}
	for _, line := range strings.Split(flagsBlock, "\n") {
		flag := strings.TrimSpace(line)
		if strings.HasPrefix(flag, "RUN_") {
			env = append(env, flag+"=false")
		}
	}
	env = append(env, "RUN_KUBEBRAIN_CANDIDATE_CANARY=true")
	command := exec.Command("bash", "../dev/verify.sh")
	command.Env = env
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "candidate canary requires TARGET_IMAGE and TARGET_RUNTIME_DIGESTS\n", string(output))

	preflightEnv := append(append([]string{}, env...),
		"TARGET_IMAGE=registry.example/kubebrain:latest",
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("a", 64),
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"KUBECTL_BIN=/bin/true",
	)
	command = exec.Command("bash", "../dev/verify.sh")
	command.Env = preflightEnv
	output, err = command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "TARGET_IMAGE must be an immutable image reference with @sha256:<64 lowercase hex digest>\n", string(output))
}

func TestInClusterBalancerSmokeDiscoversFailoverEndpoints(t *testing.T) {
	scriptData, err := os.ReadFile("../dev/incluster-balancer-smoke.sh")
	require.NoError(t, err)
	script := string(scriptData)
	require.Contains(t, script, "COPY third_party/tikv-client-go ./third_party/tikv-client-go")
	require.Less(t, strings.Index(script, "COPY third_party/tikv-client-go"), strings.Index(script, "RUN go mod download"))
	require.Contains(t, script, `trap 'status=\$?; printf "%s\\n" "\$status" >/state/client-exited; exit "\$status"' EXIT`)
	require.Contains(t, script, `if [[ -f /state/client-exited ]]; then`)
	require.Contains(t, script, `status=\$(cat /state/client-exited)`)
	require.Contains(t, script, `client exited before fault injection: status=\$status`)
	require.Less(t, strings.Index(script, `/state/client-exited`), strings.Index(script, `kubectl -n '${NAMESPACE}' delete pod`))
	require.Contains(t, script, `victim_node="$(kubectl -n "$NAMESPACE" get pod "$VICTIM_POD" -o jsonpath='{.spec.nodeName}')"`)
	require.Contains(t, script, `probe_goos="$(kubectl get node "$victim_node" -o jsonpath='{.status.nodeInfo.operatingSystem}')"`)
	require.Contains(t, script, `probe_goarch="$(kubectl get node "$victim_node" -o jsonpath='{.status.nodeInfo.architecture}')"`)
	require.Contains(t, script, `amd64|arm64)`)
	require.Contains(t, script, `FROM --platform=\$BUILDPLATFORM \${GO_IMAGE} AS build`)
	require.Contains(t, script, `ARG TARGETARCH`)
	require.Contains(t, script, `GOARCH=\${TARGETARCH}`)
	require.Contains(t, script, `--platform "linux/$probe_goarch"`)
	require.Contains(t, script, `built_platform="$(docker image inspect "$PROBE_IMAGE" --format '{{.Os}}/{{.Architecture}}')"`)
	require.Contains(t, script, `probe image platform mismatch: got ${built_platform}, want ${probe_goos}/${probe_goarch}`)
	require.Less(t, strings.Index(script, `probe image platform mismatch`), strings.Index(script, `kind load docker-image`))
	require.Contains(t, script, `PROBE_IMAGE="${PROBE_IMAGE:-}"`)
	require.Contains(t, script, `sha256sum "$ROOT_DIR/hack/dev/incluster-balancer-smoke.sh"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/cmd/balancer-smoke/main.go" "$ROOT_DIR/go.mod" "$ROOT_DIR/go.sum"`)
	require.Contains(t, script, `find "$ROOT_DIR/third_party/tikv-client-go" -type f -print0 | sort -z`)
	require.Contains(t, script, `printf '%s\0' "$GO_IMAGE" "$RUNTIME_IMAGE" "$probe_goos" "$probe_goarch"`)
	require.Contains(t, script, `PROBE_IMAGE="kubebrain:balancer-smoke-${probe_goarch}-${probe_source_digest}"`)
	require.Contains(t, script, `PROBE_IMAGE must be a safe tag reference (digests are not accepted)`)
	require.NotContains(t, script, `PROBE_IMAGE="${PROBE_IMAGE:-kubebrain:balancer-smoke}"`)
	require.Contains(t, script, `kubernetes.io/os: ${probe_goos}`)
	require.Contains(t, script, `kubernetes.io/arch: ${probe_goarch}`)
	require.NotContains(t, script, "GOARCH=amd64")

	data, err := os.ReadFile("../dev/cmd/balancer-smoke/main.go")
	require.NoError(t, err)
	probe := string(data)
	require.Equal(t, 1, strings.Count(probe, "syncAndRequireEndpoints(ctx,"))
	require.Contains(t, probe, "client.Sync(ctx)")
	require.Contains(t, probe, "client.Endpoints()")
	require.Contains(t, probe, `log.Fatalf("synced endpoint count mismatch:`)
	require.Contains(t, probe, `log.Fatalf("synced endpoints are not unique:`)
	require.NotContains(t, probe, "SetEndpoints(endpoints...)")
	require.Contains(t, probe, "newAutoSyncPinnedClient(victimEndpoint)")
	require.Contains(t, probe, "AutoSyncInterval: 100 * time.Millisecond")
	require.Contains(t, probe, "waitAndRequireAutoSyncedEndpoints(ctx, watchClient, endpoints)")
	require.Contains(t, probe, "auto-synced endpoints did not converge")
	require.Contains(t, probe, "autosynced_endpoints=3")
	require.Contains(t, probe, `waitMarker(ctx, stateDir, "replaced")`)
	require.Contains(t, probe, "serializableGetEventually(ctx,")
	require.Contains(t, probe, "clientv3.WithSerializable()")
	require.Contains(t, probe, "stale_serializable_reads=%d")
}

func TestBackendQuorumPDNetworkPartitionHelperIsRecoverable(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `BACKEND_FAULT_MODE="${BACKEND_FAULT_MODE:-pod-replacement}"`)
	require.Contains(t, script, `pd-network-partition)`)
	require.Contains(t, script, `{{.HostConfig.Privileged}}`)
	require.Contains(t, script, `partition_tag="kubebrain-pd-partition-${old_leader}-$$"`)
	require.Contains(t, script, `trap cleanup_partition EXIT`)
	require.Contains(t, script, `trap 'cleanup_partition; exit 130' INT`)
	require.Contains(t, script, `trap 'cleanup_partition; exit 143' TERM`)
	require.Contains(t, script, `trap - EXIT INT TERM`)
	require.Contains(t, script, `iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip"`)
	require.Contains(t, script, `iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip"`)
	require.Contains(t, script, `iptables -w 5 -D FORWARD -s "$partition_pod_ip"`)
	require.Contains(t, script, `iptables -w 5 -D FORWARD -d "$partition_pod_ip"`)
	require.Contains(t, script, `[[ "$new_leader" == "$TIDB_CLUSTER-pd-"*`)
	require.NotContains(t, script, "eval ")
}

func TestBackendQuorumPDAsymmetricPartitionHelperIsRecoverable(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-asymmetric-partition)`)
	require.Contains(t, script, `--partition-pd-leader-outbound`)
	require.Contains(t, script, `partition_pd_leader outbound`)
	require.Contains(t, script, `local direction="${1:-symmetric}"`)
	require.Contains(t, script, `if [[ "$direction" == "symmetric" ]]`)
	require.Contains(t, script, `iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip"`)
	require.Contains(t, script, `iptables -w 5 -C FORWARD -s "$partition_pod_ip"`)
	require.Contains(t, script, `PD outbound partition unexpectedly installed an inbound DROP rule`)
	require.Contains(t, script, `cleanup_partition`)
	require.Contains(t, script, `PD $direction network partition changed leader`)
	require.NotContains(t, script, "eval ")
}

func TestBackendQuorumPDCrossNodeAsymmetricPartitionTargetsLeaderNode(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-asymmetric-partition)`)
	require.Contains(t, script, `--partition-pd-leader-cross-node-outbound`)
	require.Contains(t, script, `partition_pd_leader outbound cross-node`)
	require.Contains(t, script, `local placement="${2:-any}"`)
	require.Contains(t, script, `expected 3 distinct PD nodes`)
	require.Contains(t, script, `KIND_NODE_CONTAINER="$old_leader_node"`)
	require.Contains(t, script, `Cross-node PD partition targets $old_leader on $KIND_NODE_CONTAINER`)
	require.Contains(t, script, `docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip"`)
	require.NotContains(t, script, "eval ")
}

func TestDevStackSupportsIsolatedHostPortsAndLowDiskTestHosts(t *testing.T) {
	up, err := os.ReadFile("../dev/up.sh")
	require.NoError(t, err)
	upScript := string(up)
	require.Contains(t, upScript, `KIND_CLIENT_HOST_PORT="${KIND_CLIENT_HOST_PORT:-3379}"`)
	require.Contains(t, upScript, `KIND_PEER_HOST_PORT="${KIND_PEER_HOST_PORT:-3380}"`)
	require.Contains(t, upScript, `KIND_WORKER_NODES="${KIND_WORKER_NODES:-0}"`)
	require.Contains(t, upScript, `KIND_CLIENT_HOST_PORT and KIND_PEER_HOST_PORT must differ`)
	require.Contains(t, upScript, `KIND_WORKER_NODES must be 0 or an integer in [3,10]`)
	require.Contains(t, upScript, `-v client_port="$KIND_CLIENT_HOST_PORT"`)
	require.Contains(t, upScript, `-v node_image="$KIND_NODE_IMAGE" -v workers="$KIND_WORKER_NODES"`)
	require.Contains(t, upScript, `topology.kubernetes.io/zone: dev-zone-`)
	require.Contains(t, upScript, `--patch-file deploy/dev/tidb-cluster-multinode-patch.yaml`)
	require.Contains(t, upScript, `sub(/hostPort:[[:space:]]*3379/, "hostPort: " client_port)`)
	require.Contains(t, upScript, `127.0.0.1:${KIND_CLIENT_HOST_PORT}`)
	require.Contains(t, upScript, `kubectl scale statefulset/kubebrain`)
	require.Contains(t, upScript, `kubectl rollout status statefulset/kubebrain`)
	require.NotContains(t, upScript, `deployment/kubebrain --namespace kubebrain-dev`)

	tidb, err := os.ReadFile("../../deploy/dev/tidb-cluster.yaml")
	require.NoError(t, err)
	manifest := string(tidb)
	require.Contains(t, manifest, `max-key-size = 2621440`)
	require.Contains(t, manifest, `reserve-space = "0MiB"`)
	require.Contains(t, manifest, `reserve-raft-space = "0MiB"`)
	require.Contains(t, manifest, "limits:\n      cpu: \"8\"\n      memory: \"16Gi\"\n      storage: 20Gi")
	require.Contains(t, manifest, `maps limits.storage to tikv-server --capacity`)
	require.Contains(t, manifest, `CLI value wins`)
	require.NotContains(t, manifest, `capacity = "5GiB"`)
	require.Contains(t, manifest, `Production must keep TiKV's reserve-space protection`)
	require.Contains(t, manifest, `hostPath filesystem is`)
	require.Contains(t, manifest, `still not a capacity-isolated CSI volume`)
	require.Contains(t, manifest, `storage-safety gate`)

	multinodePatch, err := os.ReadFile("../../deploy/dev/tidb-cluster-multinode-patch.yaml")
	require.NoError(t, err)
	patch := string(multinodePatch)
	require.Equal(t, 2, strings.Count(patch, `requiredDuringSchedulingIgnoredDuringExecution:`))
	require.Contains(t, patch, `app.kubernetes.io/component: pd`)
	require.Contains(t, patch, `app.kubernetes.io/component: tikv`)
	require.Equal(t, 2, strings.Count(patch, `topologyKey: kubernetes.io/hostname`))
}

func TestBackendPDQuorumLossHelperIsRecoverable(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-loss)`)
	require.Contains(t, script, `partition_pd_quorum`)
	require.Contains(t, script, `need exactly three healthy PD Pods`)
	require.Contains(t, script, `node container lacks curl`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_HOLD_SECONDS="${PD_QUORUM_PARTITION_HOLD_SECONDS:-15}"`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_HOLD_SECONDS must be an integer in [1,300]`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_CYCLES must be an integer in [1,20]`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_INTERVAL_SECONDS must be an integer in [0,300]`)
	require.Contains(t, script, `for cycle in $(seq 1 "$PD_QUORUM_PARTITION_CYCLES")`)
	require.Contains(t, script, `partition_pd_quorum_soak`)
	require.Contains(t, script, `hold_fault_window "$PD_QUORUM_PARTITION_HOLD_SECONDS"`)
	require.Contains(t, script, `pd_pods=("$leader")`)
	require.Contains(t, script, `http://${ip}:2379/health`)
	require.Contains(t, script, `member_healthy=false`)
	require.Contains(t, script, `preflight health did not become true within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s`)
	require.Contains(t, script, `.items[] | select(.metadata.name == $pod) | .spec.nodeName`)
	require.Contains(t, script, `expected all PD Pods on one node`)
	require.Contains(t, script, `PD quorum loss observed`)
	require.Contains(t, script, `dual_partition_tags+=("kubebrain-pd-quorum-${pod}-$$")`)
	require.Contains(t, script, `trap cleanup_dual_partition EXIT`)
	require.Contains(t, script, `PD quorum partition recovered members`)
	require.Contains(t, script, `KUBEBRAIN_WATCH_BACKEND_FAILOVER_COMMAND="$command"`)
	require.Contains(t, script, `KUBEBRAIN_LEASE_BACKEND_FAILOVER_COMMAND="$command"`)
	require.Contains(t, script, `TestLeaseKeepAliveRequireLeaderAcrossBackendFailover`)
	require.Contains(t, script, `KUBEBRAIN_REPEATED_REQUIRE_LEADER_COMMAND="$command"`)
	require.Contains(t, script, `KUBEBRAIN_REPEATED_REQUIRE_LEADER_CYCLES="$PD_QUORUM_PARTITION_CYCLES"`)
	require.Contains(t, script, `TestRequireLeaderStreamsAcrossRepeatedBackendFailover`)
	require.Contains(t, script, `KUBEBRAIN_MEMBERLIST_QUORUM_FAILOVER_COMMAND="$command"`)
	require.Contains(t, script, `TestMemberListSerializableSurvivesBackendQuorumLoss`)
	require.Contains(t, script, `KUBEBRAIN_SNAPSHOT_BACKEND_FAILOVER_COMMAND="$command"`)
	require.Contains(t, script, `COMBINED_FAULT_HOLD_SECONDS=60`)
	require.Contains(t, script, `TestSnapshotSurvivesBackendFailoverFromProtectedCheckpoint`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVQuorumLossRunsProtectedSnapshot(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-quorum-loss-snapshot)`)
	require.Contains(t, script, `"TiKV quorum-loss Snapshot"`)
	require.Contains(t, script, `"PARTITION_HOLD_SECONDS=60 $self --partition-tikv-quorum"`)
	require.Contains(t, script, `run_snapshot_failover_test`)
	require.Contains(t, script, `TestSnapshotSurvivesBackendFailoverFromProtectedCheckpoint`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeQuorumLossUsesObserverAndTargetNodes(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-quorum-loss)`)
	require.Contains(t, script, `--partition-pd-quorum-cross-node`)
	require.Contains(t, script, `--partition-pd-quorum-cross-node-soak`)
	require.Contains(t, script, `partition_pd_quorum cross-node`)
	require.Contains(t, script, `partition_pd_quorum_soak cross-node`)
	require.Contains(t, script, `local placement="${1:-any}"`)
	require.Contains(t, script, `expected 3 distinct PD nodes`)
	require.Contains(t, script, `dual_partition_node_containers+=("$node")`)
	require.Contains(t, script, `docker exec "$node" curl`)
	require.Contains(t, script, `docker exec "$node" iptables -w 5 -I FORWARD 1 -s "$ip"`)
	require.Contains(t, script, `on nodes: ${dual_partition_node_containers[*]}`)
	require.Contains(t, script, `cross-node PD quorum-loss MemberList consistency modes`)
	require.Contains(t, script, `cross-node PD quorum-loss Snapshot`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossTargetsEveryMember(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss)`)
	require.Contains(t, script, `--partition-pd-all-cross-node`)
	require.Contains(t, script, `--partition-pd-all-cross-node-soak`)
	require.Contains(t, script, `partition_pd_quorum cross-node-all`)
	require.Contains(t, script, `partition_pd_quorum_soak cross-node-all`)
	require.Contains(t, script, `target_count=3`)
	require.Contains(t, script, `PD total loss observed`)
	require.Contains(t, script, `cross-node PD total-loss MemberList consistency modes`)
	require.Contains(t, script, `cross-node PD total-loss Snapshot`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanRestartEveryKubeBrainReplica(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-restart)`)
	require.Contains(t, script, `--partition-pd-all-cross-node-restart-kubebrain`)
	require.Contains(t, script, `partition_pd_quorum cross-node-all restart-kubebrain`)
	require.Contains(t, script, `app.kubernetes.io/name=kubebrain`)
	require.Contains(t, script, `KubeBrain replicas replaced during PD total loss`)
	require.Contains(t, script, `TestKubeBrainColdRestartFailsClosedAndRecoversAcrossPDTotalLoss`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeStagedRecoveryRestoresOneMemberThenQuorum(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-staged-recovery-restart)`)
	require.Contains(t, script, `--partition-pd-all-cross-node-staged-restart-kubebrain`)
	require.Contains(t, script, `partition_pd_quorum cross-node-all restart-kubebrain-staged`)
	require.Contains(t, script, `cleanup_dual_partition_member 0`)
	require.Contains(t, script, `cleanup_dual_partition_member 1`)
	require.Contains(t, script, `PD staged recovery has one reachable member`)
	require.Contains(t, script, `PD staged recovery has quorum candidates`)
	require.Contains(t, script, `TestKubeBrainColdRestartRequiresRecoveredPDQuorum`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanOutlastLeaseTTL(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-lease-expiry)`)
	require.Contains(t, script, `run_pd_total_loss_lease_expiry_test`)
	require.Contains(t, script, `KUBEBRAIN_PD_LONG_LOSS_LEASE_COMMAND`)
	require.Contains(t, script, `TestLeaseExpiresAfterPDTotalLossOutlastsTTL`)
	require.Contains(t, script, `"PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanExpireLeaseBurst(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-lease-expiry-burst)`)
	require.Contains(t, script, `run_pd_total_loss_lease_expiry_burst_test`)
	require.Contains(t, script, `KUBEBRAIN_PD_LONG_LOSS_LEASE_BURST_COMMAND`)
	require.Contains(t, script, `TestLeaseExpiryBurstAfterPDTotalLoss`)
	require.Contains(t, script, `"PD_QUORUM_PARTITION_HOLD_SECONDS=90 $self --partition-pd-all-cross-node"`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanOverlapConcurrencySessions(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-session-overlap)`)
	require.Contains(t, script, `run_pd_total_loss_session_overlap_test`)
	require.Contains(t, script, `KUBEBRAIN_PD_SESSION_OVERLAP_COMMAND`)
	require.Contains(t, script, `TestConcurrencySessionsOverlapPDTotalLoss`)
	require.Contains(t, script, `"PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanRecoverLongDeadlineSessions(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-session-long-deadline)`)
	require.Contains(t, script, `run_pd_total_loss_session_long_deadline_test`)
	require.Contains(t, script, `KUBEBRAIN_PD_SESSION_LONG_DEADLINE_COMMAND`)
	require.Contains(t, script, `TestConcurrencySessionsWithLongDeadlineSurvivePDTotalLoss`)
	require.Contains(t, script, `"PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"`)
	require.NotContains(t, script, "eval ")
}

func TestBackendQuorumTiKVNetworkPartitionHelperIsRecoverable(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-network-partition)`)
	require.Contains(t, script, `TIKV_PARTITION_POD="${TIKV_PARTITION_POD:-}"`)
	require.Contains(t, script, `TIKV_PARTITION_POD must name exactly one Up TiKV store`)
	require.Contains(t, script, `max_by(.value.leaderCount).value.podName`)
	require.Contains(t, script, `partition_tag="kubebrain-tikv-partition-${tikv_pod}-$$"`)
	require.Contains(t, script, `[[ -n "$store_state" && "$store_state" != "Up" ]]`)
	require.Contains(t, script, `[[ "$store_state" == "Up" ]]`)
	require.Contains(t, script, `cleanup_partition`)
	require.Contains(t, script, `trap - EXIT INT TERM`)
	require.NotContains(t, script, "eval ")
}

func TestBackendQuorumTiKVCrossNodePartitionTargetsSelectedStoreNode(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-partition)`)
	require.Contains(t, script, `--partition-tikv-member-cross-node`)
	require.Contains(t, script, `partition_tikv_member cross-node`)
	require.Contains(t, script, `local placement="${1:-any}"`)
	require.Contains(t, script, `expected 3 distinct TiKV nodes`)
	require.Contains(t, script, `docker inspect "$tikv_node"`)
	require.Contains(t, script, `Cross-node TiKV partition targets $tikv_pod on $tikv_node`)
	require.Contains(t, script, `partition_tag="kubebrain-tikv-partition-${tikv_pod}-$$"`)
	require.Contains(t, script, `TiKV network partition changed store state`)
	require.Contains(t, script, `TiKV network partition recovered store`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVQuorumLossHelperIsRecoverable(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-quorum-loss)`)
	require.Contains(t, script, `sort_by(.value.leaderCount) | reverse | .[0:2][]`)
	require.Contains(t, script, `IFS=',' read -r -a tikv_pods <<<"$TIKV_QUORUM_PARTITION_PODS"`)
	require.Contains(t, script, `TIKV_QUORUM_PARTITION_PODS must name two Up TiKV stores`)
	require.Contains(t, script, `dual_partition_pod_ips+=("$ip")`)
	require.Contains(t, script, `node="$(kubectl -n "$TIDB_NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}')"`)
	require.Contains(t, script, `trap cleanup_dual_partition EXIT`)
	require.Contains(t, script, `all($selected[]; .state != "Up")`)
	require.Contains(t, script, `all($selected[]; .state == "Up")`)
	require.Contains(t, script, `deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))`)
	require.Contains(t, script, `select((.podName == $first) or (.podName == $second))`)
	require.Contains(t, script, `TIKV_QUORUM_PARTITION_CYCLES must be an integer in [1,20]`)
	require.Contains(t, script, `TIKV_QUORUM_PARTITION_INTERVAL_SECONDS must be an integer in [0,300]`)
	require.Contains(t, script, `for cycle in $(seq 1 "$TIKV_QUORUM_PARTITION_CYCLES")`)
	require.Contains(t, script, `partition_tikv_quorum_soak`)
	require.Contains(t, script, `KUBEBRAIN_WATCH_BACKEND_FAILOVER_COMMAND="$command"`)
	require.Contains(t, script, `TestWatchDeliversCommittedWritesAcrossBackendFailover`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVCrossNodeQuorumLossBindsEachStoreToItsNode(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-quorum-loss)`)
	require.Contains(t, script, `--partition-tikv-quorum-cross-node`)
	require.Contains(t, script, `partition_tikv_quorum cross-node`)
	require.Contains(t, script, `local placement="${1:-any}"`)
	require.Contains(t, script, `expected 3 distinct TiKV nodes`)
	require.Contains(t, script, `dual_partition_node_containers+=("$node")`)
	require.Contains(t, script, `node="${dual_partition_node_containers[$index]}"`)
	require.Contains(t, script, `docker exec "$node" iptables -w 5 -I FORWARD 1 -s "$ip"`)
	require.Contains(t, script, `docker exec "$node" iptables -w 5 -D FORWARD -s "$ip"`)
	require.Contains(t, script, `on nodes: ${dual_partition_node_containers[*]}`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVCrossNodeQuorumLossRestartsKubeBrainForTxnWitnessRecovery(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-quorum-loss-restart)`)
	require.Contains(t, script, `--partition-tikv-quorum-cross-node-restart-kubebrain`)
	require.Contains(t, script, `partition_tikv_quorum cross-node restart-kubebrain`)
	require.Contains(t, script, `KUBEBRAIN_TIKV_TXN_RESTART_FAULT_COMMAND="$command"`)
	require.Contains(t, script, `TestMultiKeyTxnWitnessSurvivesTiKVLossAndKubeBrainRestart`)
	require.Contains(t, script, `KubeBrain replicas replaced while backend quorum was unavailable`)
	require.Contains(t, script, `PARTITION_HOLD_SECONDS=20`)
	require.NotContains(t, script, "eval ")
}

func TestTiKVTxnRestartWitnessGateRequiresAmbiguityAtomicPairsAndNoCorruptAlarm(t *testing.T) {
	data, err := os.ReadFile("tikv_txn_restart_witness_test.go")
	require.NoError(t, err)
	source := string(data)
	require.Contains(t, source, `require.Positive(t, ambiguous.Load()`)
	require.Contains(t, source, `pairs := make(map[string]pairState)`)
	require.Contains(t, source, `state.left && state.right`)
	require.Contains(t, source, `transaction pair %q has mismatched values`)
	require.Contains(t, source, `assertNoCorruptAlarm(t, ctx, cli)`)
}

func TestWatchBackendFailoverHasBoundedConfigurableTimeout(t *testing.T) {
	data, err := os.ReadFile("watch_backend_failover_test.go")
	require.NoError(t, err)
	source := string(data)
	require.Contains(t, source, `testTimeout := 6 * time.Minute`)
	require.Contains(t, source, `os.Getenv("KUBEBRAIN_WATCH_BACKEND_FAILOVER_TIMEOUT")`)
	require.Contains(t, source, `time.ParseDuration(configured)`)
	require.Contains(t, source, `parsed, 30*time.Second`)
	require.Contains(t, source, `require.ErrorIs(t, requireErr, rpctypes.ErrNoLeader)`)
	require.Contains(t, source, `require-leader watch did not close with ErrNoLeader during backend quorum loss`)
}

func TestLeaseRequireLeaderBackendFailoverCoversWireAndClientContracts(t *testing.T) {
	data, err := os.ReadFile("lease_require_leader_backend_failover_test.go")
	require.NoError(t, err)
	source := string(data)
	require.Contains(t, source, `context.WithTimeout(context.Background(), 3*time.Minute)`)
	require.Contains(t, source, `rawLease.LeaseKeepAlive(clientv3.WithRequireLeader(ctx))`)
	require.Contains(t, source, `require.Equal(t, codes.Unavailable, status.Code(requireErr))`)
	require.Contains(t, source, `require.Equal(t, rpctypes.ErrNoLeader.Error(), status.Convert(requireErr).Message())`)
	require.Contains(t, source, `ordinary KeepAlive channel closed after require-leader stream failure`)
	require.Contains(t, source, `ordinary KeepAliveOnce must recover after backend quorum loss`)
}

func TestBackendPDCrossNodeTotalLossCanRecoverLongDeadlineLeaseRevokes(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-lease-revoke-long-deadline)`)
	require.Contains(t, script, `KUBEBRAIN_PD_LEASE_REVOKE_LONG_DEADLINE_COMMAND="$command"`)
	require.Contains(t, script, `TestLeaseRevokesWithLongDeadlineSurvivePDTotalLoss`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_HOLD_SECONDS=45`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanRecoverLongDeadlineLeaseKeepAlives(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-lease-keepalive-long-deadline)`)
	require.Contains(t, script, `KUBEBRAIN_PD_LEASE_KEEPALIVE_LONG_DEADLINE_COMMAND="$command"`)
	require.Contains(t, script, `TestLeaseKeepAlivesSurvivePDTotalLoss`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_HOLD_SECONDS=45`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanRecoverLongDeadlineKVWrites(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-kv-write-long-deadline)`)
	require.Contains(t, script, `KUBEBRAIN_PD_KV_WRITE_LONG_DEADLINE_COMMAND="$command"`)
	require.Contains(t, script, `TestKVWritesWithLongDeadlineSurvivePDTotalLoss`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_HOLD_SECONDS=45`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeTotalLossCanRecoverCompactWatermark(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-total-loss-compact-long-deadline)`)
	require.Contains(t, script, `KUBEBRAIN_PD_COMPACT_LONG_DEADLINE_COMMAND="$command"`)
	require.Contains(t, script, `TestCompactAcrossPDTotalLoss`)
	require.Contains(t, script, `PD_QUORUM_PARTITION_HOLD_SECONDS=45`)
	require.NotContains(t, script, "eval ")
}

func TestBackendPDCrossNodeDegradedNetworkUsesPodNamespacesAndCleansUp(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-degraded-network)`)
	require.Contains(t, script, `--degrade-pd-all-cross-node`)
	require.Contains(t, script, `crictl inspectp "$sandbox"`)
	require.Contains(t, script, `tc qdisc replace dev eth0 root netem delay`)
	require.Contains(t, script, `loss "${loss_percent}%" rate "$rate"`)
	require.Contains(t, script, `tc qdisc del dev eth0 root`)
	require.Contains(t, script, `run_quorum_test "cross-node PD latency, loss, and bandwidth degradation"`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVCrossNodeDegradedNetworkUsesEveryStore(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-degraded-network)`)
	require.Contains(t, script, `--degrade-tikv-all-cross-node`)
	require.Contains(t, script, `degrade_backend_cross_node_network tikv "$TIKV_NETEM_HOLD_SECONDS"`)
	require.Contains(t, script, `app.kubernetes.io/component=${component}`)
	require.Contains(t, script, `need three healthy ${component_label} Pods on three distinct nodes`)
	require.Contains(t, script, `run_quorum_test "cross-node TiKV latency, loss, and bandwidth degradation"`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVDegradedNetworkRunsPorcupineHistories(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-degraded-network-linearizability)`)
	require.Contains(t, script, `KUBEBRAIN_LINEARIZABILITY_FAULT_COMMAND="$command"`)
	require.Contains(t, script, `TestClientV3RegisterHistoryIsLinearizable|TestClientV3MultiKeyTxnHistoryIsLinearizable`)
	require.Contains(t, script, `"$self --degrade-tikv-all-cross-node"`)
	require.NotContains(t, script, "eval ")

	for _, testFile := range []string{"linearizability_test.go", "multikey_linearizability_test.go"} {
		t.Run(testFile, func(t *testing.T) {
			source, readErr := os.ReadFile(testFile)
			require.NoError(t, readErr)
			text := string(source)
			require.Contains(t, text, `os.Getenv("KUBEBRAIN_LINEARIZABILITY_FAULT_COMMAND")`)
			require.Contains(t, text, `faultCommand != ""`)
			require.Contains(t, text, `require.Positive(t, failedOperations.Load()`)
		})
	}
}

func TestBackendPDDegradedNetworkRunsPorcupineHistories(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-degraded-network-linearizability)`)
	require.Contains(t, script, `"Porcupine histories across cross-node PD network degradation"`)
	require.Contains(t, script, `"$self --degrade-pd-all-cross-node"`)
	require.Contains(t, script, `run_degraded_network_linearizability_test`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsPorcupineHistories(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-linearizability)`)
	require.Contains(t, script, `"Porcupine histories across concurrent PD quorum and TiKV member partition"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_degraded_network_linearizability_test`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsPorcupineLeaseHistories(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-lease-linearizability)`)
	require.Contains(t, script, `"Porcupine lease histories across concurrent PD quorum and TiKV member partition"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_degraded_network_lease_linearizability_test`)
	require.Contains(t, script, `TestClientV3LeaseGenerationHistoryIsLinearizable|TestClientV3LeaseLifecycleHistoryIsLinearizable`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsPorcupineLeaseExpiryHistory(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-lease-expiry-linearizability)`)
	require.Contains(t, script, `"Porcupine lease expiry history across concurrent PD quorum and TiKV member partition"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_degraded_network_lease_expiry_linearizability_test`)
	require.Contains(t, script, `TestClientV3LeaseNaturalExpiryHistoryIsLinearizable`)
	require.NotContains(t, script, "eval ")

	leaseSource, readErr := os.ReadFile("lease_linearizability_test.go")
	require.NoError(t, readErr)
	require.Contains(t, string(leaseSource), `testTimeout = 2 * time.Minute`)
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsStreamingKeepAlive(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-streaming-keepalive)`)
	require.Contains(t, script, `"streaming LeaseKeepAlive across concurrent PD quorum and TiKV member partition"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_tikv_degraded_network_streaming_keepalive_test`)
	require.Contains(t, script, `TestStreamingLeaseKeepAlivesRecoverAcrossTiKVDegradation`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsRevokeStreamConvergence(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-revoke-stream)`)
	require.Contains(t, script, `"LeaseRevoke/KeepAlive stream convergence across concurrent PD quorum and TiKV member partition"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_tikv_degraded_network_revoke_stream_test`)
	require.Contains(t, script, `TestLeaseRevokeClosesKeepAliveStreamsAcrossTiKVDegradation`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsWatchRecovery(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-watch-recovery)`)
	require.Contains(t, script, `"concurrent PD quorum and TiKV member partition"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_watch_recovery_test`)
	require.Contains(t, script, `TestWatchDeliversCommittedWritesAcrossBackendFailover`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsProtectedSnapshot(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-snapshot)`)
	require.Contains(t, script, `"concurrent PD quorum and TiKV member partition Snapshot"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_snapshot_failover_test`)
	require.Contains(t, script, `TestSnapshotSurvivesBackendFailoverFromProtectedCheckpoint`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDAndTiKVQuorumPartitionRunsProtectedSnapshot(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-quorum-snapshot)`)
	require.Contains(t, script, `TIKV_COMBINED_FAULT_MODE=quorum`)
	require.Contains(t, script, `run_snapshot_failover_test`)

	data, err = os.ReadFile("../dev/partition-pd-quorum-and-tikv-member.sh")
	require.NoError(t, err)
	wrapper := string(data)
	require.Contains(t, wrapper, `quorum) tikv_fault_argument="--partition-tikv-quorum"`)
	require.Contains(t, wrapper, `"$FAULT_RUNNER" "$tikv_fault_argument"`)
	require.Contains(t, wrapper, `while [[ ! -e "$pd_ready" || ! -e "$tikv_ready" ]]`)
	require.Contains(t, wrapper, `combined PD quorum and TiKV $TIKV_COMBINED_FAULT_MODE partition ready`)
	require.Contains(t, wrapper, `: >"$pd_release"`)
	require.NotContains(t, wrapper, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsMemberListConsistency(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-memberlist)`)
	require.Contains(t, script, `"concurrent PD quorum and TiKV member partition MemberList"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_memberlist_quorum_test`)
	require.Contains(t, script, `TestMemberListSerializableSurvivesBackendQuorumLoss`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDAndTiKVQuorumPartitionRunsMemberListConsistency(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-quorum-memberlist)`)
	require.Contains(t, script, `"serializable MemberList across concurrent PD quorum and TiKV quorum partition"`)
	require.Contains(t, script, `COMBINED_FAULT_READY_FILE=\$KUBEBRAIN_COMBINED_FAULT_READY_FILE TIKV_COMBINED_FAULT_MODE=quorum`)
	require.Contains(t, script, `run_memberlist_quorum_test`)

	testSource, readErr := os.ReadFile("memberlist_quorum_failover_test.go")
	require.NoError(t, readErr)
	text := string(testSource)
	require.Contains(t, text, `strings.Contains(command, "KUBEBRAIN_COMBINED_FAULT_READY_FILE")`)
	require.Contains(t, text, `combined backend fault must signal both quorums ready`)
	require.Contains(t, text, `MemberListRequest{Linearizable: true}`)
	require.Contains(t, text, `require.Equal(t, baseline.Members, serializable.Members)`)
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsLeaseRequireLeader(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-lease-require-leader)`)
	require.Contains(t, script, `"concurrent PD quorum and TiKV member partition LeaseKeepAlive"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_lease_require_leader_test`)
	require.Contains(t, script, `TestLeaseKeepAliveRequireLeaderAcrossBackendFailover`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsRepeatedRequireLeader(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-repeated-require-leader)`)
	require.Contains(t, script, `"repeated concurrent PD quorum and TiKV member partition require-leader streams"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `run_repeated_require_leader_test`)
	require.Contains(t, script, `TestRequireLeaderStreamsAcrossRepeatedBackendFailover`)
	require.NotContains(t, script, "eval ")
	testSource, err := os.ReadFile("repeated_require_leader_backend_failover_test.go")
	require.NoError(t, err)
	require.Contains(t, string(testSource), `time.Duration(cycles)*2*time.Minute`)
}

func TestBackendPDQuorumPartitionRunsFixedHashKV(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-hashkv)`)
	require.Contains(t, script, `"fixed-revision HashKV across PD quorum loss"`)
	require.Contains(t, script, `"$self --partition-pd-quorum"`)
	require.Contains(t, script, `KUBEBRAIN_HASHKV_PD_QUORUM_FAULT_COMMAND="$command"`)
	require.Contains(t, script, `TestFixedRevisionHashKVSurvivesPDQuorumFault`)
	require.NotContains(t, script, "eval ")
}

func TestBackendCombinedPDQuorumAndTiKVMemberPartitionRunsFixedHashKV(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-member-hashkv)`)
	require.Contains(t, script, `"fixed-revision HashKV across concurrent PD quorum and TiKV member partition"`)
	require.Contains(t, script, `"$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"`)
	require.Contains(t, script, `KUBEBRAIN_HASHKV_COMBINED_FAULT_COMMAND="$command"`)
	require.Contains(t, script, `TestFixedRevisionHashKVSurvivesCombinedBackendFault`)
	require.NotContains(t, script, "eval ")
	testSource, err := os.ReadFile("maintenance_hashkv_combined_fault_test.go")
	require.NoError(t, err)
	require.Contains(t, string(testSource), `clientv3.WithSerializable()`)
	require.Contains(t, string(testSource), `serializable Range must use the protected member checkpoint`)
	require.Contains(t, string(testSource), `NewKVClient(client.ActiveConnection()).RangeStream`)
	require.Contains(t, string(testSource), `serializable RangeStream must fall back before its first live frame`)
	require.Contains(t, string(testSource), `only the terminal RangeStream frame may carry metadata`)
}

func TestBackendCombinedPDAndTiKVQuorumPartitionRunsProtectedSerializableReads(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-quorum-serializable)`)
	require.Contains(t, script, `"protected serializable reads across concurrent PD quorum and TiKV quorum partition"`)
	require.Contains(t, script, `COMBINED_FAULT_READY_FILE=\$KUBEBRAIN_COMBINED_FAULT_READY_FILE TIKV_COMBINED_FAULT_MODE=quorum`)
	require.Contains(t, script, `run_combined_hashkv_test`)
	require.Contains(t, script, `before_restarts="$(kubectl -n "$KUBEBRAIN_NAMESPACE" get pods`)
	require.Contains(t, script, `KubeBrain Pods restarted or were replaced during the combined backend fault`)
	require.Contains(t, script, `KubeBrain Pod identities and restart counts remained stable`)

	testSource, readErr := os.ReadFile("maintenance_hashkv_combined_fault_test.go")
	require.NoError(t, readErr)
	text := string(testSource)
	require.Contains(t, text, `strings.Contains(command, "KUBEBRAIN_COMBINED_FAULT_READY_FILE")`)
	require.Contains(t, text, `case result := <-commandDone:`)
	require.Contains(t, text, `command exited before readiness`)
	require.Contains(t, text, `did not signal both quorums ready within 3m`)
	require.Contains(t, text, `serializable RangeStream must fall back before its first live frame`)
	require.Contains(t, text, `serializable Range must use the protected member checkpoint`)
	require.Contains(t, text, `serializable read-only Txn must use the protected member checkpoint`)
}

func TestBackendCombinedPDAndTiKVQuorumPartitionRunsProtectedStatus(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-quorum-status)`)
	require.Contains(t, script, `"Maintenance Status across concurrent PD quorum and TiKV quorum partition"`)
	require.Contains(t, script, `COMBINED_FAULT_READY_FILE=\$KUBEBRAIN_COMBINED_FAULT_READY_FILE TIKV_COMBINED_FAULT_MODE=quorum`)
	require.Contains(t, script, `run_combined_status_test`)

	testSource, readErr := os.ReadFile("maintenance_status_combined_fault_test.go")
	require.NoError(t, readErr)
	text := string(testSource)
	require.Contains(t, text, `client.Status(statusCtx, endpoint)`)
	require.Contains(t, text, `Maintenance Status must use the protected member checkpoint`)
	require.Contains(t, text, `duringFault.RaftAppliedIndex`)
}

func TestBackendCombinedPDAndTiKVQuorumPartitionRunsProtectedHash(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-quorum-hash)`)
	require.Contains(t, script, `"Maintenance Hash across concurrent PD quorum and TiKV quorum partition"`)
	require.Contains(t, script, `COMBINED_FAULT_READY_FILE=\$KUBEBRAIN_COMBINED_FAULT_READY_FILE TIKV_COMBINED_FAULT_MODE=quorum`)
	require.Contains(t, script, `run_combined_hash_test`)

	testSource, readErr := os.ReadFile("maintenance_hash_combined_fault_test.go")
	require.NoError(t, readErr)
	text := string(testSource)
	require.Contains(t, text, `NewMaintenanceClient(client.ActiveConnection()).Hash`)
	require.Contains(t, text, `Maintenance Hash must use the protected member checkpoint`)
	require.Contains(t, text, `duringFault.Header.Revision`)
}

func TestBackendCombinedPDAndTiKVQuorumPartitionRunsDefragment(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-quorum-defragment)`)
	require.Contains(t, script, `"Maintenance Defragment across concurrent PD quorum and TiKV quorum partition"`)
	require.Contains(t, script, `COMBINED_FAULT_READY_FILE=\$KUBEBRAIN_COMBINED_FAULT_READY_FILE TIKV_COMBINED_FAULT_MODE=quorum`)
	require.Contains(t, script, `run_combined_defragment_test`)

	testSource, readErr := os.ReadFile("maintenance_defragment_combined_fault_test.go")
	require.NoError(t, readErr)
	text := string(testSource)
	require.Contains(t, text, `client.Defragment(callCtx, endpoint)`)
	require.Contains(t, text, `Maintenance Defragment must remain a member-local compatibility no-op`)
	require.Contains(t, text, `duringFault.Header`)
}

func TestBackendCombinedPDAndTiKVQuorumPartitionRunsHTTPHealth(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-quorum-tikv-quorum-http-health)`)
	require.Contains(t, script, `"HTTP health probes across concurrent PD quorum and TiKV quorum partition"`)
	require.Contains(t, script, `COMBINED_FAULT_READY_FILE=\$KUBEBRAIN_COMBINED_FAULT_READY_FILE TIKV_COMBINED_FAULT_MODE=quorum`)
	require.Contains(t, script, `run_combined_http_health_test`)
	require.Contains(t, script, `INFO_ENDPOINT must name the info/metrics endpoint for the HTTP health profile`)
	require.Contains(t, script, `KUBEBRAIN_INFO_ENDPOINT="$INFO_ENDPOINT"`)

	testSource, readErr := os.ReadFile("http_health_combined_fault_test.go")
	require.NoError(t, readErr)
	text := string(testSource)
	require.Contains(t, text, `request("/health?serializable=true")`)
	require.Contains(t, text, `request("/livez?verbose")`)
	require.Contains(t, text, `request("/readyz?verbose")`)
	require.Contains(t, text, `healthpb.NewHealthClient(etcdClient.ActiveConnection()).Check`)
	require.Contains(t, text, `healthpb.NewHealthClient(etcdClient.ActiveConnection()).List`)
	require.Contains(t, text, `healthpb.HealthCheckResponse_SERVING`)
	require.Contains(t, text, `[-]linearizable_read failed:`)
	require.Contains(t, text, `prometheusCounterValue(baselineMetrics, "health_checkpoint_fallback", "check", check)`)
	require.Contains(t, text, `fallback metric must be initialized before the first fallback`)
}

func TestBackendPDDegradedNetworkRunsPorcupineLeaseHistories(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-degraded-network-lease-linearizability)`)
	require.Contains(t, script, `"Porcupine lease histories across cross-node PD network degradation"`)
	require.Contains(t, script, `TestClientV3LeaseGenerationHistoryIsLinearizable|TestClientV3LeaseLifecycleHistoryIsLinearizable`)
	require.Contains(t, script, `"$self --degrade-pd-all-cross-node"`)
	require.Contains(t, script, `run_degraded_network_lease_linearizability_test`)
	require.NotContains(t, script, "eval ")

	source, readErr := os.ReadFile("lease_linearizability_test.go")
	require.NoError(t, readErr)
	text := string(source)
	require.Contains(t, text, `os.Getenv("KUBEBRAIN_LINEARIZABILITY_FAULT_COMMAND")`)
	require.Contains(t, text, `require.Positive(t, ambiguousFailures.Load()`)
	require.Contains(t, text, `require.Less(t, ambiguousFailures.Load(), int64(len(history))`)
}

func TestBackendTiKVDegradedNetworkRunsPorcupineLeaseHistories(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-degraded-network-lease-linearizability)`)
	require.Contains(t, script, `"Porcupine lease histories across cross-node TiKV network degradation"`)
	require.Contains(t, script, `TestClientV3LeaseGenerationHistoryIsLinearizable|TestClientV3LeaseLifecycleHistoryIsLinearizable`)
	require.Contains(t, script, `"$self --degrade-tikv-all-cross-node"`)
	require.Contains(t, script, `run_degraded_network_lease_linearizability_test`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVDegradedNetworkRunsPorcupineLeaseExpiryHistory(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-degraded-network-lease-expiry-linearizability)`)
	require.Contains(t, script, `"Porcupine lease expiry history across cross-node TiKV network degradation"`)
	require.Contains(t, script, `TestClientV3LeaseNaturalExpiryHistoryIsLinearizable`)
	require.Contains(t, script, `"$self --degrade-tikv-all-cross-node"`)
	require.Contains(t, script, `run_degraded_network_lease_expiry_linearizability_test`)
	require.NotContains(t, script, "eval ")

	source, readErr := os.ReadFile("lease_linearizability_test.go")
	require.NoError(t, readErr)
	text := string(source)
	require.Contains(t, text, `externalFaultDone = startLinearizabilityFaultCommand`)
	require.Contains(t, text, `lease deadline did not overlap the active external fault`)
	require.Contains(t, text, `natural lease deletion committed after the external fault recovered`)
}

func TestBackendPDDegradedNetworkRunsPorcupineLeaseExpiryHistory(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `pd-cross-node-degraded-network-lease-expiry-linearizability)`)
	require.Contains(t, script, `"Porcupine lease expiry history across cross-node PD network degradation"`)
	require.Contains(t, script, `TestClientV3LeaseNaturalExpiryHistoryIsLinearizable`)
	require.Contains(t, script, `"$self --degrade-pd-all-cross-node"`)
	require.Contains(t, script, `run_degraded_network_lease_expiry_linearizability_test`)
	require.NotContains(t, script, "eval ")
}

func TestBackendTiKVDegradedNetworkRunsStreamingKeepAliveRecovery(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-degraded-network-streaming-keepalive)`)
	require.Contains(t, script, `KUBEBRAIN_TIKV_STREAMING_KEEPALIVE_FAULT_COMMAND="$command"`)
	require.Contains(t, script, `TestStreamingLeaseKeepAlivesRecoverAcrossTiKVDegradation`)
	require.Contains(t, script, `"$self --degrade-tikv-all-cross-node"`)
	require.Contains(t, script, `run_tikv_degraded_network_streaming_keepalive_test`)
	require.NotContains(t, script, "eval ")

	source, readErr := os.ReadFile("tikv_lease_keepalive_degraded_network_test.go")
	require.NoError(t, readErr)
	text := string(source)
	require.Contains(t, text, `time.After(15 * time.Second)`)
	require.Contains(t, text, `keepalive channel closed during TiKV degradation`)
	require.Contains(t, text, `receivePositiveKeepAlive(t, ctx, lease.ch, lease.id)`)
}

func TestBackendTiKVDegradedNetworkRunsRevokeStreamConvergence(t *testing.T) {
	data, err := os.ReadFile("../dev/backend-quorum-fault-smoke.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `tikv-cross-node-degraded-network-revoke-stream)`)
	require.Contains(t, script, `KUBEBRAIN_TIKV_REVOKE_STREAM_FAULT_COMMAND="$command"`)
	require.Contains(t, script, `TestLeaseRevokeClosesKeepAliveStreamsAcrossTiKVDegradation`)
	require.Contains(t, script, `"$self --degrade-tikv-all-cross-node"`)
	require.Contains(t, script, `run_tikv_degraded_network_revoke_stream_test`)
	require.NotContains(t, script, "eval ")

	source, readErr := os.ReadFile("tikv_lease_revoke_stream_degraded_network_test.go")
	require.NoError(t, readErr)
	text := string(source)
	require.Contains(t, text, `time.After(15 * time.Second)`)
	require.Contains(t, text, `collectLeaseRevokeWave(t, ctx, revokes, len(leases))`)
	require.Contains(t, text, `waitForKeepAliveChannelClose`)
	require.Contains(t, text, `require.Equal(t, int64(-1), ttl.TTL)`)
}

func TestReferenceEtcdProvenanceVerifierFailsClosed(t *testing.T) {
	tempDir := t.TempDir()
	binary := tempDir + "/etcd"
	require.NoError(t, os.WriteFile(binary, []byte("#!/usr/bin/env bash\nprintf 'etcd Version: 3.8.0-alpha.0\\nGit SHA: d947b2086\\n'\n"), 0o700))

	run := func(expected string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), compatScriptCommandTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "bash", "verify-reference-etcd-provenance.sh")
		cmd.Env = append(os.Environ(),
			"REFERENCE_ETCD_BIN="+binary,
			"REFERENCE_ETCD_EXPECTED_GIT_SHA="+expected,
		)
		return cmd.CombinedOutput()
	}

	output, err := run("d947b2086abcdef0123456789abcdef012345678")
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "reference etcd provenance verified: d947b2086")

	output, err = run("5cd9f4ee13801e18825d661e5005ae599460bc3a")
	require.Error(t, err)
	require.Contains(t, string(output), "reference etcd Git SHA mismatch")
	require.Contains(t, string(output), "binary=d947b2086")
	require.Contains(t, string(output), "expected=5cd9f4ee13801e18825d661e5005ae599460bc3a")
}

func TestReferenceEtcdProvenanceVerifierReadsGoBuildInfo(t *testing.T) {
	tempDir := t.TempDir()
	binary := tempDir + "/etcdctl"
	require.NoError(t, os.WriteFile(binary, []byte("#!/usr/bin/env bash\nprintf 'etcdctl version: 3.8.0-alpha.0\\nAPI version: 3.8\\n'\n"), 0o700))
	fakeGo := tempDir + "/go"
	require.NoError(t, os.WriteFile(fakeGo, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$2: go1.26.5\" $'\\tbuild\\tvcs.revision=5cd9f4ee13801e18825d661e5005ae599460bc3a'\n"), 0o700))

	run := func() ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), compatScriptCommandTimeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "bash", "verify-reference-etcd-provenance.sh")
		cmd.Env = append(os.Environ(),
			"PATH="+tempDir+":"+os.Getenv("PATH"),
			"REFERENCE_ETCD_BIN="+binary,
			"REFERENCE_ETCD_EXPECTED_GIT_SHA=5cd9f4ee13801e18825d661e5005ae599460bc3a",
		)
		return cmd.CombinedOutput()
	}
	output, err := run()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "reference etcd provenance verified: 5cd9f4ee13801e18825d661e5005ae599460bc3a")

	require.NoError(t, os.WriteFile(fakeGo, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$2: go1.26.5\" $'\\tbuild\\tvcs.revision=5cd9f4ee13801e18825d661e5005ae599460bc3a' $'\\tbuild\\tvcs.modified=true'\n"), 0o700))
	output, err = run()
	require.Error(t, err)
	require.Contains(t, string(output), "reference etcd binary was built from a modified worktree")
}

func TestEveryReferenceEtcdRunnerVerifiesProvenance(t *testing.T) {
	runners := []string{
		"run-alarm-restart-recovery.sh",
		"run-auth-differential.sh",
		"run-automatic-quota-differential.sh",
		"run-cold-header-recovery.sh",
		"run-differential.sh",
		"run-direct-moveleader-differential.sh",
		"run-jwt-differential.sh",
		"run-make-mirror-differential.sh",
		"run-rangestream-compaction-differential.sh",
		"run-rangestream-oversize-differential.sh",
		"run-replica-restart-revision.sh",
	}
	for _, runner := range runners {
		data, err := os.ReadFile(runner)
		require.NoError(t, err)
		require.Contains(t, string(data), `"$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"`, runner)
	}
}

func TestEveryEtcdctlRunnerVerifiesProvenance(t *testing.T) {
	runners := []string{
		"run-alarm-restart-recovery.sh",
		"run-auth-differential.sh",
		"run-automatic-quota-differential.sh",
		"run-cold-header-recovery.sh",
		"run-differential.sh",
		"run-direct-moveleader-differential.sh",
		"run-direct-replica-consistency.sh",
		"run-envoy-kubernetes-rollout.sh",
		"run-jwt-differential.sh",
		"run-make-mirror-differential.sh",
		"run-rangestream-compaction-differential.sh",
		"run-rangestream-oversize-differential.sh",
		"run-replica-restart-revision.sh",
	}
	for _, runner := range runners {
		data, err := os.ReadFile(runner)
		require.NoError(t, err)
		require.Contains(t, string(data), `REFERENCE_ETCD_BIN="$ETCDCTL_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"`, runner)
	}
}

func TestReferenceEtcdToolchainBuilderIsIsolatedAndVerified(t *testing.T) {
	data, err := os.ReadFile("build-reference-etcd-toolchain.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, `git -C "$REFERENCE_ETCD_SOURCE_DIR" status --porcelain --untracked-files=normal`)
	require.Contains(t, script, `reference etcd build directory must be empty`)
	require.Contains(t, script, `BINDIR="$relative_build_dir" GOWORK=off GO_BUILD_FLAGS=-mod=readonly ./scripts/build.sh`)
	require.Contains(t, script, `for binary_name in etcd etcdctl etcdutl`)
	require.Contains(t, script, `verify-reference-etcd-provenance.sh`)
	require.Contains(t, script, `REFERENCE_ETCD_BIN=%q`)
	require.Contains(t, script, `ETCDCTL_BIN=%q`)
	require.Contains(t, script, `ETCDUTL_BINARY=%q`)
	require.NotContains(t, script, `rm -rf`)
}

func TestCompatKubernetesRestartCommandsUseBoundedHelpers(t *testing.T) {
	for _, testFile := range []string{
		"admission_replica_restart_test.go",
		"auth_ha_test.go",
		"concurrency_recipes_test.go",
		"corrupt_alarm_restart_test.go",
		"lease_id_extremes_test.go",
		"linearizability_test.go",
		"ordering_replica_restart_test.go",
		"restart_persistence_test.go",
		"serializable_read_differential_test.go",
		"watch_quota_test.go",
	} {
		t.Run(testFile, func(t *testing.T) {
			data, err := os.ReadFile(testFile)
			require.NoError(t, err)
			text := string(data)
			require.NotContains(t, text, `exec.CommandContext(`)
			require.NotContains(t, text, ".CombinedOutput()")
			require.NotContains(t, text, ".Output()")
			require.Contains(t, text, "runCompat")
		})
	}
}

func TestCompatMakeMirrorCommandsUseBoundedHelpers(t *testing.T) {
	for _, testFile := range []string{
		"make_mirror_auth_differential_test.go",
		"make_mirror_differential_test.go",
		"make_mirror_revision_differential_test.go",
	} {
		t.Run(testFile, func(t *testing.T) {
			data, err := os.ReadFile(testFile)
			require.NoError(t, err)
			text := string(data)
			require.NotContains(t, text, `exec.CommandContext(`)
			require.NotContains(t, text, ".CombinedOutput()")
		})
	}

	revision, err := os.ReadFile("make_mirror_revision_differential_test.go")
	require.NoError(t, err)
	require.Contains(t, string(revision), "runCompatCommandContext(t, commandCtx, etcdctl,")

	mirror, err := os.ReadFile("make_mirror_differential_test.go")
	require.NoError(t, err)
	require.Contains(t, string(mirror), "startCompatCommand(t, commandName, args...)")
}

func TestCompatCommandHelpersUseWaitDelay(t *testing.T) {
	data, err := os.ReadFile("runner_command_test.go")
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, "const compatCommandWaitDelay = 5 * time.Second")
	require.GreaterOrEqual(t, strings.Count(text, "command.WaitDelay = compatCommandWaitDelay"), 2)
}

func TestRunnerPostflightsRemainReachableAfterTestFailure(t *testing.T) {
	for script := range readRunnerSafetyContracts(t) {
		t.Run(script, func(t *testing.T) {
			data, err := os.ReadFile(script)
			require.NoError(t, err)
			content := string(data)
			capture := strings.Index(content, ") || test_status=$?")
			require.NotEqual(t, -1, capture, "runner must capture the Go test exit status instead of exiting under set -e")
			postflight := strings.Index(content[capture:], "postflight")
			require.NotEqual(t, -1, postflight, "runner must execute a postflight after capturing test failure")
			propagate := strings.Index(content[capture+postflight:], `if [[ "$test_status" -ne 0 ]]`)
			require.NotEqual(t, -1, propagate, "runner must propagate the captured test failure after postflight")
		})
	}
}

type runnerSafetyContract struct {
	approvalVariables []string
	stateContract     string
}

func readRunnerSafetyContracts(t *testing.T) map[string]runnerSafetyContract {
	t.Helper()
	data, err := os.ReadFile("runner-safety-contracts.txt")
	require.NoError(t, err)
	contracts := make(map[string]runnerSafetyContract)
	for lineNumber, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 3, "runner safety manifest line %d", lineNumber+1)
		script, approvals, stateContract := fields[0], strings.Split(fields[1], ","), fields[2]
		require.Regexp(t, `^run-[a-z0-9-]+\.sh$`, script, "runner safety manifest line %d", lineNumber+1)
		require.NotContains(t, contracts, script, "duplicate runner safety manifest entry %s", script)
		require.Contains(t, map[string]struct{}{
			"baseline-prefix-control": {}, "baseline-visible": {}, "baseline-visible-tls": {},
			"disposable-empty": {}, "disposable-empty-owned-resources": {}, "recovery-prefix-control": {},
		}, stateContract, "unknown state contract for %s", script)
		contracts[script] = runnerSafetyContract{approvalVariables: approvals, stateContract: stateContract}
	}
	require.NotEmpty(t, contracts)
	return contracts
}

func TestEveryGoTestRunnerHasSafetyContract(t *testing.T) {
	contracts := readRunnerSafetyContracts(t)
	runners, err := filepath.Glob("run-*.sh")
	require.NoError(t, err)
	discovered := make(map[string]struct{})
	for _, runner := range runners {
		data, readErr := os.ReadFile(runner)
		require.NoError(t, readErr)
		if !strings.Contains(string(data), "go test") {
			continue
		}
		script := filepath.Base(runner)
		discovered[script] = struct{}{}
		contract, ok := contracts[script]
		require.True(t, ok, "%s runs Go tests but has no runner safety contract", script)
		content := string(data)
		for _, approval := range contract.approvalVariables {
			require.Regexp(t, `^ALLOW_(MUTATING|DESTRUCTIVE)_[A-Z0-9_]+$`, approval, script)
			require.Contains(t, content, approval+`="${`+approval+`:-false}"`, "%s must default %s to false", script, approval)
			require.GreaterOrEqual(t, strings.Count(content, approval), 3,
				"%s must validate and enforce %s in addition to declaring it", script, approval)
		}
		require.NotEmpty(t, contract.stateContract)
	}
	for script := range contracts {
		require.Contains(t, discovered, script, "stale runner safety contract for %s", script)
	}
}

func TestEverySharedClusterMutatingGoTestScriptHasSafetyContract(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	manifestPath := filepath.Join(repoRoot, "hack", "dev", "go-test-cluster-mutation-contracts.txt")
	data, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	contracts := make(map[string]runnerSafetyContract)
	for lineNumber, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 3, "cluster mutation manifest line %d", lineNumber+1)
		script, approval, stateContract := fields[0], fields[1], fields[2]
		require.Regexp(t, `^hack/[a-z0-9/-]+\.sh$`, script)
		require.Regexp(t, `^ALLOW_DESTRUCTIVE_[A-Z0-9_]+$`, approval)
		require.Contains(t, map[string]struct{}{
			"baseline-prefix-control": {}, "fault-cleanup": {}, "host-fault-cleanup": {},
		}, stateContract, "unknown state contract for %s", script)
		require.NotContains(t, contracts, script, "duplicate cluster mutation contract for %s", script)
		contracts[script] = runnerSafetyContract{
			approvalVariables: []string{approval},
			stateContract:     stateContract,
		}
	}
	require.NotEmpty(t, contracts)

	discovered := make(map[string]struct{})
	err = filepath.WalkDir(filepath.Join(repoRoot, "hack"), func(path string, entry os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)
		if entry.IsDir() || filepath.Ext(path) != ".sh" {
			return nil
		}
		relativePath, relativeErr := filepath.Rel(repoRoot, path)
		require.NoError(t, relativeErr)
		relativePath = filepath.ToSlash(relativePath)
		if strings.HasPrefix(relativePath, "hack/etcd-client-compat/") {
			return nil // Covered bidirectionally by runner-safety-contracts.txt.
		}
		scriptData, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		content := string(scriptData)
		if !strings.Contains(content, "go test") {
			return nil
		}
		// Direct Pod deletion, network fault injection, or an existing destructive
		// approval marks a Go-test wrapper as a shared-cluster mutation entrypoint.
		mutatesCluster := strings.Contains(content, " delete pod") ||
			strings.Contains(content, "iptables -w") ||
			strings.Contains(content, "tc qdisc") ||
			strings.Contains(content, "ALLOW_DESTRUCTIVE_")
		if !mutatesCluster {
			return nil
		}
		script := relativePath
		discovered[script] = struct{}{}
		contract, ok := contracts[script]
		require.True(t, ok, "%s mutates a shared cluster while running Go tests but has no safety contract", script)
		approval := contract.approvalVariables[0]
		require.Contains(t, content, approval+`="${`+approval+`:-false}"`, "%s must default %s to false", script, approval)
		require.GreaterOrEqual(t, strings.Count(content, approval), 3,
			"%s must validate and enforce %s in addition to declaring it", script, approval)
		return nil
	})
	require.NoError(t, err)
	for script := range contracts {
		require.Contains(t, discovered, script, "stale cluster mutation safety contract for %s", script)
	}
}

func TestSharedClusterMutatingGoTestScriptsFailClosedBeforeToolDiscovery(t *testing.T) {
	for script, approval := range map[string]string{
		"backend-quorum-fault-smoke.sh":   "ALLOW_DESTRUCTIVE_BACKEND_QUORUM_FAULT",
		"lease-renewal-failover-smoke.sh": "ALLOW_DESTRUCTIVE_LEASE_RENEWAL_FAILOVER",
	} {
		t.Run(script, func(t *testing.T) {
			path := filepath.Join("..", "dev", script)
			output, err := runCompatCommandContext(t, context.Background(), "bash", []string{path}, []string{"PATH=/nonexistent"})
			require.Error(t, err)
			require.Contains(t, string(output), approval+"=true")
			require.NotContains(t, string(output), "missing required command")

			output, err = runCompatCommandContext(t, context.Background(), "bash", []string{path}, []string{
				"PATH=/nonexistent",
				approval + "=invalid",
			})
			require.Error(t, err)
			require.Contains(t, string(output), approval+" must be true or false")
			require.NotContains(t, string(output), "missing required command")
		})
	}

	t.Run("native-pitr-host-network-fault", func(t *testing.T) {
		path := filepath.Join("..", "backup", "run-native-pitr-full-restore-integration.sh")
		output, err := runCompatCommandContext(t, context.Background(), "bash", []string{path}, []string{
			"PATH=/nonexistent",
			"KUBEBRAIN_NATIVE_PITR_TOPOLOGY_SIZE=3",
			"KUBEBRAIN_NATIVE_PITR_FAULT_INJECTION=target-pd-network-quorum-loss-resume",
		})
		require.Error(t, err)
		require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_HOST_NETWORK_FAULT=true")
		require.NotContains(t, string(output), "requires root")
	})
}

func TestIndirectClusterMutationCallersHaveSafetyContracts(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	data, err := os.ReadFile(filepath.Join(repoRoot, "hack", "dev", "indirect-cluster-mutation-contracts.txt"))
	require.NoError(t, err)
	contracts := 0
	for lineNumber, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 5, "indirect mutation manifest line %d", lineNumber+1)
		caller, runFlag, defaultValue, callee, approval := fields[0], fields[1], fields[2], fields[3], fields[4]
		require.Regexp(t, `^hack/[a-z0-9/-]+\.sh$`, caller)
		require.Regexp(t, `^RUN_[A-Z0-9_]+$`, runFlag)
		require.Equal(t, "true", defaultValue, "only default-enabled indirect mutations belong in this manifest")
		require.Regexp(t, `^hack/[a-z0-9/-]+\.sh$`, callee)
		require.Regexp(t, `^ALLOW_DESTRUCTIVE_[A-Z0-9_]+$`, approval)

		callerData, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(caller)))
		require.NoError(t, readErr)
		callerContent := string(callerData)
		require.Contains(t, callerContent, runFlag+`="${`+runFlag+`:-`+defaultValue+`}"`)
		require.Contains(t, callerContent, callee)

		calleeData, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(callee)))
		require.NoError(t, readErr)
		calleeContent := string(calleeData)
		require.Contains(t, calleeContent, approval+`="${`+approval+`:-false}"`)
		require.GreaterOrEqual(t, strings.Count(calleeContent, approval), 3)
		require.Contains(t, calleeContent, `KUBE_CONTEXT="${KUBE_CONTEXT:-}"`)
		require.NotContains(t, calleeContent, "\nkubectl ", "%s must not bypass its explicit-context kubectl array", callee)
		contracts++
	}
	require.Positive(t, contracts)
}

func TestDefaultHAIndirectMutationFailsClosedBeforeToolDiscovery(t *testing.T) {
	path := filepath.Join("..", "dev", "ha-smoke.sh")
	output, err := runCompatCommandContext(t, context.Background(), "bash", []string{path}, []string{"PATH=/nonexistent"})
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required for HA smoke")
	require.NotContains(t, string(output), "missing required command")

	output, err = runCompatCommandContext(t, context.Background(), "bash", []string{path}, []string{
		"PATH=/nonexistent",
		"KUBE_CONTEXT=kind-explicit",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_HA_SMOKE=true")
	require.NotContains(t, string(output), "missing required command")

	output, err = runCompatCommandContext(t, context.Background(), "bash", []string{path}, []string{
		"PATH=/nonexistent",
		"KUBE_CONTEXT=kind-explicit",
		"ALLOW_DESTRUCTIVE_HA_SMOKE=invalid",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_HA_SMOKE must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestDefaultOwnedKubernetesRunnersHaveSafetyContracts(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	data, err := os.ReadFile(filepath.Join(repoRoot, "hack", "dev", "owned-kubernetes-runner-contracts.txt"))
	require.NoError(t, err)
	contracts := 0
	for lineNumber, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 6, "owned Kubernetes manifest line %d", lineNumber+1)
		caller, runFlag, defaultValue, callee, resource, contextEnv := fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]
		require.Equal(t, "namespace", resource)
		require.Equal(t, "KUBE_CONTEXT", contextEnv)

		callerData, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(caller)))
		require.NoError(t, readErr)
		callerContent := string(callerData)
		require.Contains(t, callerContent, runFlag+`="${`+runFlag+`:-`+defaultValue+`}"`)
		require.Contains(t, callerContent, callee)

		calleeData, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(callee)))
		require.NoError(t, readErr)
		calleeContent := string(calleeData)
		require.Contains(t, calleeContent, `KUBE_CONTEXT="${KUBE_CONTEXT:-}"`)
		require.Contains(t, calleeContent, "namespace_created=false")
		require.Contains(t, calleeContent, `if [[ "$namespace_created" == true ]]`)
		require.Contains(t, calleeContent, "refusing to reuse existing TLS smoke namespace")
		require.NotContains(t, calleeContent, `create namespace "$NAMESPACE" --dry-run`)
		contracts++
	}
	require.Positive(t, contracts)
}

func TestDefaultTLSRunnerRejectsPreexistingNamespaceWithoutDeletingIt(t *testing.T) {
	fakeBin := t.TempDir()
	logPath := filepath.Join(fakeBin, "kubectl.log")
	fakeKubectl := filepath.Join(fakeBin, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_KUBECTL_LOG"
if [[ "$*" == "config get-contexts kind-owned -o name" ]]; then
  printf '%s\n' kind-owned
  exit 0
fi
if [[ "$*" == "--context kind-owned get namespace occupied -o json --ignore-not-found" ]]; then
  printf '%s\n' '{"metadata":{"name":"occupied"}}'
  exit 0
fi
exit 99
`), 0o755))
	for _, tool := range []string{"go", "openssl", "etcdctl"} {
		require.NoError(t, os.WriteFile(filepath.Join(fakeBin, tool), []byte("#!/usr/bin/env bash\nexit 0\n"), 0o755))
	}

	output, err := runCompatCommandContext(t, context.Background(), "bash", []string{filepath.Join("..", "dev", "tls-smoke.sh")}, []string{
		"PATH=" + fakeBin + ":" + os.Getenv("PATH"),
		"FAKE_KUBECTL_LOG=" + logPath,
		"KUBE_CONTEXT=kind-owned",
		"NAMESPACE=occupied",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing to reuse existing TLS smoke namespace: occupied")
	logData, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	require.NotContains(t, string(logData), "delete namespace")
	require.NotContains(t, string(logData), "create namespace")
}

func TestOwnedEndpointRunnersHaveSafetyContracts(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	data, err := os.ReadFile(filepath.Join(repoRoot, "hack", "dev", "owned-endpoint-runner-contracts.txt"))
	require.NoError(t, err)
	contracts := 0
	for lineNumber, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 6, "owned endpoint manifest line %d", lineNumber+1)
		caller, runFlag, defaultValue, callee, approval, resource := fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]
		require.Contains(t, []string{"true", "false"}, defaultValue)
		require.Equal(t, "etcd-prefix", resource)
		require.Regexp(t, `^ALLOW_MUTATING_[A-Z0-9_]+$`, approval)

		callerData, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(caller)))
		require.NoError(t, readErr)
		callerContent := string(callerData)
		require.Contains(t, callerContent, runFlag+`="${`+runFlag+`:-`+defaultValue+`}"`)
		require.Contains(t, callerContent, callee)
		require.Contains(t, callerContent, approval+"=true")
		require.Contains(t, callerContent, `ETCD_PREFIX="/registry-kubebrain-apiserver-`)

		calleeData, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(callee)))
		require.NoError(t, readErr)
		calleeContent := string(calleeData)
		require.Contains(t, calleeContent, approval+`="${`+approval+`:-false}"`)
		require.Contains(t, calleeContent, "prefix_owned=false")
		require.Contains(t, calleeContent, `if [[ "$prefix_owned" == true ]]`)
		require.Contains(t, calleeContent, `del "$ETCD_PREFIX" --prefix`)
		require.Contains(t, calleeContent, "baseline_lease_ids=\"$(list_lease_ids)\"")
		require.Contains(t, calleeContent, `lease revoke "$lease_id"`)
		require.Contains(t, calleeContent, "lease set differs from preflight after cleanup")
		require.Contains(t, calleeContent, "refusing non-empty apiserver")
		require.Contains(t, calleeContent, "process_owned=false")
		require.Contains(t, calleeContent, "refusing to replace active apiserver")
		contracts++
	}
	require.Equal(t, 5, contracts)

	rolloutData, err := os.ReadFile(filepath.Join(repoRoot, "hack", "dev", "apiserver-rollout-smoke.sh"))
	require.NoError(t, err)
	rolloutContent := string(rolloutData)
	require.Contains(t, rolloutContent, "hack/dev/apiserver-watch-soak.sh")
	require.Contains(t, rolloutContent, "ALLOW_MUTATING_APISERVER_WATCH_SOAK=true")
	require.Contains(t, rolloutContent, `ETCD_PREFIX="/registry-kubebrain-apiserver-rollout-watch-`)
}

func TestAPIServerSmokeRejectsNonEmptyPrefixWithoutDeletingIt(t *testing.T) {
	fakeBin := t.TempDir()
	logPath := filepath.Join(fakeBin, "etcdctl.log")
	fakeEtcdctl := filepath.Join(fakeBin, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_ETCDCTL_LOG"
if [[ "${1:-}" == "--version" ]]; then
  printf '%s\n' 'Git SHA: 5cd9f4ee13801e18825d661e5005ae599460bc3a'
  exit 0
fi
if [[ "$*" == *" get /registry-kubebrain-apiserver-occupied --prefix --limit=1 -w json"* ]]; then
  printf '%s\n' '{"header":{"revision":7},"count":1,"kvs":[{"key":"aw==","value":"dg=="}]}'
  exit 0
fi
exit 98
`), 0o755))
	for _, tool := range []string{"docker", "kubectl", "curl"} {
		require.NoError(t, os.WriteFile(filepath.Join(fakeBin, tool), []byte("#!/usr/bin/env bash\nexit 0\n"), 0o755))
	}

	output, err := runCompatCommandContext(t, context.Background(), "bash", []string{filepath.Join("..", "dev", "apiserver-smoke.sh")}, []string{
		"PATH=" + fakeBin + ":" + os.Getenv("PATH"),
		"FAKE_ETCDCTL_LOG=" + logPath,
		"ETCDCTL_BIN=" + fakeEtcdctl,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=5cd9f4ee13801e18825d661e5005ae599460bc3a",
		"ALLOW_MUTATING_APISERVER_SMOKE=true",
		"ETCD_PREFIX=/registry-kubebrain-apiserver-occupied",
		"WORK_DIR=" + filepath.Join(fakeBin, "work"),
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing non-empty apiserver smoke prefix")
	logData, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	require.NotContains(t, string(logData), " del ")

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	output, err = runCompatCommandContext(t, context.Background(), "bash", []string{filepath.Join("..", "dev", "apiserver-watch-soak.sh")}, []string{
		"PATH=" + fakeBin + ":" + os.Getenv("PATH"),
		"FAKE_ETCDCTL_LOG=" + logPath,
		"ETCDCTL_BIN=" + fakeEtcdctl,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=5cd9f4ee13801e18825d661e5005ae599460bc3a",
		"ALLOW_MUTATING_APISERVER_WATCH_SOAK=true",
		"ETCD_PREFIX=/registry-kubebrain-apiserver-occupied",
		"WORK_DIR=" + filepath.Join(fakeBin, "watch-work"),
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing non-empty apiserver watch-soak prefix")
	logData, readErr = os.ReadFile(logPath)
	require.NoError(t, readErr)
	require.NotContains(t, string(logData), " del ")
}

func TestCompatSuiteRunnerRejectsReferenceDifferentialOptIns(t *testing.T) {
	for _, envVar := range []string{
		"REFERENCE_ETCD_ENDPOINT",
		"REFERENCE_ETCD_GATEWAY_ENDPOINT",
		"REFERENCE_QUOTA_ETCD_ENDPOINT",
		"REFERENCE_JWT_ETCD_ENDPOINT",
		"ETCD_AUTH_DIFF_ENDPOINT",
		"KUBEBRAIN_AUTH_DIFF_ENDPOINT",
	} {
		t.Run(envVar, func(t *testing.T) {
			output, err := runCompatSuiteScript(t, []string{envVar + "=127.0.0.1:12379"})
			require.Error(t, err)
			require.Contains(t, string(output), "run.sh refuses reference/differential opt-in "+envVar)
			require.Contains(t, string(output), "run-differential.sh")
		})
	}
}

func TestCompatSuiteRunnerRequiresExplicitEndpoint(t *testing.T) {
	output, err := runCompatSuiteScript(t, []string{
		"KUBEBRAIN_ETCD_ENDPOINT=",
		"ENDPOINT=",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "run.sh requires KUBEBRAIN_ETCD_ENDPOINT")
	require.Contains(t, string(output), "refusing implicit 127.0.0.1:3379")
}

func TestCompatSuiteRunnerForwardsExplicitEndpoint(t *testing.T) {
	fakeBin := t.TempDir()
	fakeGo := fakeBin + "/go"
	require.NoError(t, os.WriteFile(fakeGo, []byte(
		"#!/bin/sh\nprintf 'endpoint=%s args=%s\\n' \"$KUBEBRAIN_ETCD_ENDPOINT\" \"$*\"\n",
	), 0o755))

	for _, tc := range []struct {
		name string
		env  []string
		want string
	}{
		{
			name: "preferred",
			env: []string{
				"KUBEBRAIN_ETCD_ENDPOINT=http://preferred:2379",
				"ENDPOINT=http://legacy:2379",
			},
			want: "endpoint=http://preferred:2379 args=test -count=1 -v ./...",
		},
		{
			name: "legacy",
			env: []string{
				"KUBEBRAIN_ETCD_ENDPOINT=",
				"ENDPOINT=http://legacy:2379",
			},
			want: "endpoint=http://legacy:2379 args=test -count=1 -v ./...",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := append([]string{"PATH=" + fakeBin + ":" + os.Getenv("PATH")}, tc.env...)
			output, err := runCompatSuiteScript(t, env)
			require.NoError(t, err, "output=%s", output)
			require.Contains(t, string(output), tc.want)
		})
	}
}
