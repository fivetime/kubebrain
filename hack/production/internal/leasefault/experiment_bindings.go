package leasefault

import (
	"context"
	"errors"
	"regexp"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	strictjson "sigs.k8s.io/json"
)

// NativeExperimentBindings is the identity/expectation section of the future
// experiment command's plan, not an executable experiment or online admission.
// Source/image strings must still be authenticated against CI and live Pods.
// Commands, credentials, tool dependencies, retention and Join are separate
// required inputs; loading this file starts no processes and contacts no API.
type NativeExperimentBindings struct {
	Version          int              `json:"version"`
	Network          NetworkRecovery  `json:"network"`
	Protocol         ProtocolRecovery `json:"protocol"`
	InitialTerm      uint64           `json:"initial_term,string"`
	ObserverMemberID uint64           `json:"observer_member_id,string"`
	Source           string           `json:"source"`
	SourceHash       string           `json:"source_hash"`
	Image            string           `json:"image"`
	// Before info/anonymous, then after info/anonymous: four distinct ports.
	StackPorts []int                                 `json:"stack_ports"`
	Metrics    []retirementmetrics.WorkerExpectation `json:"metrics"`
}

var experimentSource = regexp.MustCompile(`^[a-f0-9]{40}$`)
var experimentImage = regexp.MustCompile(`^ghcr\.io/fivetime/kubebrain@sha256:[a-f0-9]{64}$`)

func LoadNativeExperimentBindings(path, approvedSHA256 string) (NativeExperimentBindings, error) {
	var p NativeExperimentBindings
	if !planinput.ValidSHA256(approvedSHA256) {
		return p, errors.New("require independently approved experiment bindings digest")
	}
	data, err := planinput.ReadFile(path, true, 1<<20)
	if err != nil {
		return p, err
	}
	if planinput.SHA256(data) != approvedSHA256 {
		return p, errors.New("experiment bindings differ from admission")
	}
	strict, err := strictjson.UnmarshalStrict(data, &p, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 {
		return NativeExperimentBindings{}, errors.New("invalid strict experiment bindings JSON")
	}
	if err := p.Validate(); err != nil {
		return NativeExperimentBindings{}, err
	}
	return p, nil
}

func (p NativeExperimentBindings) Validate() error {
	if p.Version != 1 || p.InitialTerm == 0 || p.ObserverMemberID == 0 || p.ObserverMemberID == p.Protocol.AlarmMemberID || !experimentSource.MatchString(p.Source) || !planinput.ValidSHA256(p.SourceHash) || !experimentImage.MatchString(p.Image) || len(p.StackPorts) != 4 || len(p.Metrics) == 0 || len(p.Metrics) > 16 {
		return errors.New("incomplete native experiment bindings")
	}
	if err := ValidateRecoveryPlans(p.Network, p.Protocol); err != nil {
		return err
	}
	ports := map[int]bool{}
	for _, port := range p.StackPorts {
		if port < 1024 || port > 65535 || ports[port] {
			return errors.New("stack sessions require four distinct unprivileged ports")
		}
		ports[port] = true
	}
	first := p.Metrics[0].Binding
	for _, e := range p.Metrics {
		b := e.Binding
		if b.NamespaceUID != p.Network.NamespaceUID || b.StatefulSetUID != p.Network.StatefulSetUID || !planinput.ValidSHA256(b.SpecSHA256) || b.SpecSHA256 != first.SpecSHA256 || b.Cluster != first.Cluster {
			return errors.New("metric bindings differ from experiment scope")
		}
	}
	// Constructor performs only static validation and freezes pointer data;
	// these callbacks are deliberately never executed by this local loader.
	_, err := retirementmetrics.NewWorkerGates(p.Metrics, func(context.Context) error { return errors.New("validation only") }, func(context.Context, int, retirementmetrics.WorkerMeasurement) error {
		return errors.New("validation only")
	})
	return err
}

func (p NativeExperimentBindings) Initial() Binding {
	return Binding{LeaseID: p.Protocol.LeaseID, ClusterID: p.Protocol.ClusterID, InitialMemberID: p.Protocol.AlarmMemberID, InitialTerm: p.InitialTerm}
}

func (p NativeExperimentBindings) Successor() SuccessorBinding {
	return SuccessorBinding{ClusterID: p.Protocol.ClusterID, OldLeaderID: p.Protocol.AlarmMemberID, OldTerm: p.InitialTerm, ObserverMemberID: p.ObserverMemberID}
}
