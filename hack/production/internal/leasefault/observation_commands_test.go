package leasefault

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestCommandPlanObservation(t *testing.T) {
	p := commandPlan(t)
	calls := []string{}
	sentinel := errors.New("admission or retention failed")
	h := ObservationHooks{
		AdmitOriginal: func(context.Context) error { calls = append(calls, "original-admit"); return sentinel },
		AdmitStack:    func(_ context.Context, stage string) error { calls = append(calls, stage+"-admit"); return sentinel },
		RetainOriginal: func(context.Context, string, Binding, []byte, error) error {
			calls = append(calls, "original-retain")
			return sentinel
		},
		RetainStack: func(_ context.Context, stage string, _ WaitReceipt, err error) error {
			require.ErrorIs(t, err, sentinel)
			calls = append(calls, stage+"-retain")
			return sentinel
		},
	}
	o, err := p.Observation(h)
	require.NoError(t, err)
	require.Empty(t, calls)
	require.NoError(t, o.validate())
	require.Equal(t, p.Bindings.Initial(), o.Initial)
	require.Equal(t, p.BeforeDirectory, o.Before.Directory)
	require.Equal(t, p.AfterDirectory, o.After.Directory)
	require.Equal(t, 1, o.Before.Count)
	require.Equal(t, 0, o.After.Count)
	require.Equal(t, p.Bindings.Source, o.Before.Source)
	require.Equal(t, p.Bindings.SourceHash, o.After.SourceHash)
	ctx := context.Background()
	require.ErrorIs(t, o.Admit(ctx), sentinel)
	require.ErrorIs(t, o.Before.Admit(ctx), sentinel)
	require.ErrorIs(t, o.After.Admit(ctx), sentinel)
	require.ErrorIs(t, o.Before.Retain(ctx, WaitReceipt{}, sentinel), sentinel)
	require.ErrorIs(t, o.After.Retain(ctx, WaitReceipt{}, sentinel), sentinel)
	require.ErrorIs(t, o.Retain(ctx, "pending", o.Initial, nil, sentinel), sentinel)
	require.Equal(t, []string{"original-admit", "before-admit", "after-admit", "before-retain", "after-retain", "original-retain"}, calls)
	for i := 0; i < 4; i++ {
		bad := h
		switch i {
		case 0:
			bad.AdmitOriginal = nil
		case 1:
			bad.AdmitStack = nil
		case 2:
			bad.RetainOriginal = nil
		case 3:
			bad.RetainStack = nil
		}
		_, err := p.Observation(bad)
		require.Error(t, err)
	}
	p.Bindings.StackPorts[0] = 0
	_, err = p.Observation(h)
	require.Error(t, err)
}

func commandPlan(t *testing.T) ObservationCommandPlan {
	t.Helper()
	n := networkPlan()
	minimum := uint64(1)
	b := NativeExperimentBindings{
		Version: 1, Network: n, Protocol: ProtocolRecovery{Owner: n.Owner, NamespaceUID: n.NamespaceUID, StatefulSetUID: n.StatefulSetUID, ClusterID: ^uint64(0), AlarmMemberID: ^uint64(0) - 1, LeaseID: -3, Key: "/acceptance/fixture"},
		InitialTerm: 2, ObserverMemberID: 7, Source: strings.Repeat("a", 40), SourceHash: strings.Repeat("b", 64), Image: "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("c", 64), StackPorts: []int{18588, 18589, 18590, 18591},
		Metrics: []retirementmetrics.WorkerExpectation{{Binding: retirementmetrics.CaptureBinding{NamespaceUID: n.NamespaceUID, StatefulSetUID: n.StatefulSetUID, PodUID: n.PodUID, SpecSHA256: strings.Repeat("d", 64), Cluster: "test"}, Key: retirementmetrics.Key{Stage: "local", Outcome: "confirmed"}, MinimumCount: &minimum}},
	}
	dir := t.TempDir()
	log := func(name string) *os.File {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		return f
	}
	return ObservationCommandPlan{Bindings: b, OwnerDirectory: dir, ProbeExecutable: "/approved/lease-term-probe", StackExecutable: "/approved/protected-wait-worker.sh", Endpoint: "[::1]:2379", ServerName: "member.test", CA: "/approved/ca", Certificate: "/approved/cert", Key: "/approved/key", BeforeDirectory: filepath.Join(dir, "stack-worker.aaaaaaaa"), AfterDirectory: filepath.Join(dir, "stack-worker.bbbbbbbb"), Duration: 2 * time.Minute, Env: []string{"PATH=/approved/bin", "stack_owner=" + dir}, ProbeLog: log("probe.log"), BeforeLog: log("before.log"), AfterLog: log("after.log")}
}

func TestObservationCommands(t *testing.T) {
	p := commandPlan(t)
	c, err := p.Build()
	require.NoError(t, err)
	require.Equal(t, []string{"--endpoint", "[::1]:2379", "--cacert", "/approved/ca", "--cert", "/approved/cert", "--key", "/approved/key", "--tls-server-name", "member.test", "--lease-id", "-3", "--cluster-id", "18446744073709551615", "--member-id", "18446744073709551614", "--leased-key", "/acceptance/fixture", "--duration", "2m0s", "--wait-for-expiry"}, c.Probe.Args)
	require.Equal(t, []string{p.Bindings.Network.PodName, p.Bindings.Source, p.Bindings.SourceHash, "1", p.BeforeDirectory}, c.Before.Args)
	require.Equal(t, []string{p.Bindings.Network.PodName, p.Bindings.Source, p.Bindings.SourceHash, "0", p.AfterDirectory}, c.After.Args)
	require.Equal(t, append(append([]string{}, p.Env...), "stack_info_port=18588", "stack_anonymous_port=18589"), c.Before.Env)
	require.Equal(t, append(append([]string{}, p.Env...), "stack_info_port=18590", "stack_anonymous_port=18591"), c.After.Env)
	p.Env[0] = "PATH=/changed"
	c.Before.Env[0] = "PATH=/also-changed"
	require.Equal(t, "PATH=/approved/bin", c.After.Env[0])
	require.Equal(t, "PATH=/approved/bin", c.Probe.Env[0])
	// Building commands must neither launch nonexistent executables nor create receipts.
	_, err = os.Stat(p.BeforeDirectory)
	require.True(t, os.IsNotExist(err))
}

func TestObservationCommandsRejectInvalid(t *testing.T) {
	for name, change := range map[string]func(*ObservationCommandPlan){
		"bindings":        func(p *ObservationCommandPlan) { p.Bindings.Version = 2 },
		"relative":        func(p *ObservationCommandPlan) { p.ProbeExecutable = "probe" },
		"newline":         func(p *ObservationCommandPlan) { p.Key = "/key\nfile" },
		"receipt-name":    func(p *ObservationCommandPlan) { p.BeforeDirectory = filepath.Join(p.OwnerDirectory, "other") },
		"shared-receipt":  func(p *ObservationCommandPlan) { p.AfterDirectory = p.BeforeDirectory },
		"endpoint":        func(p *ObservationCommandPlan) { p.Endpoint = "host:0" },
		"server-name":     func(p *ObservationCommandPlan) { p.ServerName = "" },
		"duration":        func(p *ObservationCommandPlan) { p.Duration = 6 * time.Minute },
		"missing-owner":   func(p *ObservationCommandPlan) { p.Env = []string{} },
		"duplicate":       func(p *ObservationCommandPlan) { p.Env = append(p.Env, "PATH=/other") },
		"shell-hook":      func(p *ObservationCommandPlan) { p.Env = append(p.Env, "BASH_ENV=/hook") },
		"function-export": func(p *ObservationCommandPlan) { p.Env = append(p.Env, "BASH_FUNC_echo%%=() { false; }") },
		"port-override":   func(p *ObservationCommandPlan) { p.Env = append(p.Env, "stack_info_port=1") },
		"no-log":          func(p *ObservationCommandPlan) { p.ProbeLog = nil },
		"shared-log":      func(p *ObservationCommandPlan) { p.AfterLog = p.BeforeLog },
		"public-log":      func(p *ObservationCommandPlan) { require.NoError(t, p.BeforeLog.Chmod(0644)) },
	} {
		t.Run(name, func(t *testing.T) {
			p := commandPlan(t)
			change(&p)
			c, err := p.Build()
			require.Error(t, err)
			require.Empty(t, c.Probe.Executable)
		})
	}
}
