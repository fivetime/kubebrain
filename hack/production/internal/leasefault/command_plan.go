package leasefault

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	corev1 "k8s.io/api/core/v1"
	strictjson "sigs.k8s.io/json"
)

// NativeCommandPlan is the serialized input to command assembly. Its hash must
// be independently approved. Loading it authenticates neither CI nor live Pods,
// and never creates artifacts, acquires ownership or authorizes a fault.
// Durations are explicit Go duration strings; no implicit numeric time units.
type NativeCommandPlan struct {
	Version            int                      `json:"version"`
	Bindings           NativeExperimentBindings `json:"bindings"`
	OwnerDirectory     string                   `json:"owner_directory"`
	StatefulSetName    string                   `json:"statefulset_name"`
	ProbeExecutable    string                   `json:"probe_executable"`
	StackExecutable    string                   `json:"stack_executable"`
	MetricExecutable   string                   `json:"metric_executable"`
	JoinScript         string                   `json:"join_script"`
	Endpoint           string                   `json:"endpoint"`
	ServerName         string                   `json:"server_name"`
	ObserverEndpoint   string                   `json:"observer_endpoint"`
	ObserverServerName string                   `json:"observer_server_name"`
	CA                 string                   `json:"ca"`
	Certificate        string                   `json:"certificate"`
	Key                string                   `json:"key"`
	Kubeconfig         string                   `json:"kubeconfig"`
	KubeContext        string                   `json:"kube_context"`
	APIServer          string                   `json:"api_server"`
	BeforeDirectory    string                   `json:"before_directory"`
	AfterDirectory     string                   `json:"after_directory"`
	Duration           string                   `json:"duration"`
	RecoveryTimeout    string                   `json:"recovery_timeout"`
	CaptureSeconds     int                      `json:"capture_seconds"`
	TargetsSHA256      string                   `json:"targets_sha256"`
	Env                []string                 `json:"env"`
	Processes          CommandProcessInputs     `json:"processes"`
	Release            CommandRelease           `json:"release"`
	Targets            []CommandPlanTarget      `json:"targets"`
	Files              map[string]string        `json:"files"`
}

// File descriptors and callbacks cannot be deserialized from an execution plan.
type CommandPlanTarget struct {
	PodName       string `json:"pod_name"`
	PodUID        string `json:"pod_uid"`
	InfoPort      int    `json:"info_port"`
	AnonymousPort int    `json:"anonymous_port"`
}

func (p NativeCommandPlan) ObservationPlan() (ObservationCommandPlan, error) {
	duration, err := time.ParseDuration(p.Duration)
	if err != nil {
		return ObservationCommandPlan{}, errors.New("invalid command duration")
	}
	o := ObservationCommandPlan{Bindings: p.Bindings, OwnerDirectory: p.OwnerDirectory, ProbeExecutable: p.ProbeExecutable, StackExecutable: p.StackExecutable, Endpoint: p.Endpoint, ServerName: p.ServerName, CA: p.CA, Certificate: p.Certificate, Key: p.Key, BeforeDirectory: p.BeforeDirectory, AfterDirectory: p.AfterDirectory, Duration: duration, Env: append([]string{}, p.Env...)}
	return o, o.validateConfiguration()
}

func (p NativeCommandPlan) MetricTargets() []MetricCommandTarget {
	targets := make([]MetricCommandTarget, len(p.Targets))
	for i, t := range p.Targets {
		targets[i] = MetricCommandTarget{PodName: t.PodName, PodUID: t.PodUID, InfoPort: t.InfoPort, AnonymousPort: t.AnonymousPort}
	}
	return targets
}

func LoadNativeCommandPlan(path, digest string) (NativeCommandPlan, error) {
	var p NativeCommandPlan
	if !planinput.ValidSHA256(digest) {
		return p, errors.New("require independently approved command plan digest")
	}
	data, err := planinput.ReadFile(path, true, 4<<20)
	if err != nil {
		return p, err
	}
	if planinput.SHA256(data) != digest {
		return p, errors.New("command plan differs from admission")
	}
	strict, err := strictjson.UnmarshalStrict(data, &p, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 {
		return NativeCommandPlan{}, errors.New("invalid strict command plan JSON")
	}
	if err := p.CheckLocal(); err != nil {
		return NativeCommandPlan{}, err
	}
	return p, nil
}

// CheckLocal reads pinned files and creates then closes lazy transports. It
// starts no child processes and sends no API/RPC requests. Independent runtime
// admission must still cover transitive tools, environment, release provenance
// and fresh identities; a files map is not a closed dependency sandbox.
func (p NativeCommandPlan) CheckLocal() error {
	o, err := p.ObservationPlan()
	if err != nil {
		return err
	}
	recovery, err := time.ParseDuration(p.RecoveryTimeout)
	if err != nil || recovery <= 0 || recovery > 5*time.Minute || p.Version != 1 || !recoveryIdentity.MatchString(p.StatefulSetName) || p.CaptureSeconds < 1 || p.CaptureSeconds > 9 || !planinput.ValidSHA256(p.TargetsSHA256) || len(p.Files) == 0 || len(p.Files) > 128 {
		return errors.New("invalid command runtime settings")
	}
	if filepath.Dir(p.MetricExecutable) != filepath.Dir(p.StackExecutable) || filepath.Dir(p.JoinScript) != filepath.Dir(p.StackExecutable) {
		return errors.New("command scripts must share admitted tool directory")
	}
	for _, path := range []string{p.ProbeExecutable, p.StackExecutable, p.MetricExecutable, p.JoinScript, p.Processes.JQ, p.Processes.Predicate, p.Kubeconfig, p.CA, p.Certificate, p.Key} {
		if !planinput.ValidSHA256(p.Files[path]) {
			return errors.New("missing pinned command input")
		}
	}
	if err := p.VerifyFiles(context.Background()); err != nil {
		return err
	}
	if len(p.Targets) != len(p.Bindings.Metrics) || len(p.Processes.Metrics) != len(p.Targets) {
		return errors.New("command metric count mismatch")
	}
	ports := map[int]bool{}
	for _, port := range p.Bindings.StackPorts {
		ports[port] = true
	}
	names := map[string]string{p.Bindings.Network.PodName: p.Bindings.Network.PodUID}
	uids := map[string]string{p.Bindings.Network.PodUID: p.Bindings.Network.PodName}
	for i, target := range p.Targets {
		var pod corev1.Pod
		strict, decodeErr := strictjson.UnmarshalStrict(p.Processes.Metrics[i], &pod, strictjson.DisallowDuplicateFields)
		if decodeErr != nil || len(strict) != 0 || pod.Namespace != p.Bindings.Network.Namespace || pod.Name != target.PodName || string(pod.UID) != target.PodUID {
			return errors.New("metric snapshot differs from command target")
		}
		if !recoveryIdentity.MatchString(target.PodName) || target.PodUID != p.Bindings.Metrics[i].Binding.PodUID || (names[target.PodName] != "" && names[target.PodName] != target.PodUID) || (uids[target.PodUID] != "" && uids[target.PodUID] != target.PodName) {
			return errors.New("invalid command metric mapping")
		}
		names[target.PodName], uids[target.PodUID] = target.PodUID, target.PodName
		for _, port := range []int{target.InfoPort, target.AnonymousPort} {
			if port < 1024 || port > 65535 || ports[port] {
				return errors.New("command worker ports overlap or are invalid")
			}
			ports[port] = true
		}
	}
	if err := o.CheckProcessImages(p.Processes, p.Release); err != nil {
		return err
	}
	connections, err := o.OpenConnections(p.Kubeconfig, p.KubeContext, p.APIServer, p.ObserverEndpoint, p.ObserverServerName, p.Files)
	if err != nil {
		return err
	}
	r, err := connections.Bind(MeasuredNetworkFaultRuntime{})
	if err == nil {
		err = o.CheckCommandEndpoints(r, p.Processes)
	}
	return errors.Join(err, connections.Close())
}

// VerifyFiles rechecks the frozen plan's pinned bytes and executable/private
// modes without constructing clients. It is integrity checking, not CI or live
// process admission. Cancellation is checked before and after each bounded read;
// no retry, cached hash, deadline extension or inherited tool path is used.
func (p NativeCommandPlan) VerifyFiles(ctx context.Context) error {
	if ctx == nil || len(p.Files) == 0 || len(p.Files) > 128 {
		return errors.New("file verification requires context and bounded inputs")
	}
	paths := make([]string, 0, len(p.Files))
	for path := range p.Files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		digest := p.Files[path]
		if !planinput.ValidSHA256(digest) {
			return errors.New("invalid command input digest")
		}
		// Additional binaries include Bash and kubectl. Known data and script
		// inputs retain their tighter limit; all reads remain bounded.
		limit := int64(128 << 20)
		for _, dataPath := range []string{p.CA, p.Certificate, p.Key, p.Kubeconfig, p.StackExecutable, p.MetricExecutable, p.JoinScript, p.Processes.Predicate, filepath.Join(p.OwnerDirectory, "observer-pod.json"), filepath.Join(p.OwnerDirectory, "observer-targets.json")} {
			if path == dataPath {
				limit = 1 << 20
			}
		}
		private := path == p.Key || path == p.Kubeconfig || path == filepath.Join(p.OwnerDirectory, "observer-pod.json") || path == filepath.Join(p.OwnerDirectory, "observer-targets.json")
		data, err := planinput.ReadFile(path, private, limit)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if planinput.SHA256(data) != digest {
			return errors.New("command input differs from admission")
		}
	}
	for _, path := range []string{p.ProbeExecutable, p.StackExecutable, p.MetricExecutable, p.Processes.JQ} {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !planinput.ValidSHA256(p.Files[path]) {
			return errors.New("missing pinned executable input")
		}
		if err := processgroup.ValidateExecutable(path); err != nil {
			return err
		}
	}
	return ctx.Err()
}
