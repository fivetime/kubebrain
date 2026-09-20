package leasefault

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
)

// MetricCommandTarget must be independently mapped from the admitted Pod UID to
// its live name. Ports are unique across all workers and both stack sessions.
type MetricCommandTarget struct {
	PodName, PodUID         string
	InfoPort, AnonymousPort int
	Stderr                  *os.File
}

// MetricCommands constructs protected-metrics-worker argv in expectation order.
// Offset comes from the same expectation consumed by the metric evidence gate;
// no caller-supplied second offset can silently change the measured deadline.
// Building does not execute commands or authenticate tool/Pod provenance.
func (p ObservationCommandPlan) MetricCommands(executable string, targets []MetricCommandTarget) ([]metricsworker.Command, error) {
	if _, err := p.Build(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || executable == "/" || strings.ContainsAny(executable, "\x00\n\r\t") || len(targets) != len(p.Bindings.Metrics) {
		return nil, errors.New("invalid metric executable or target count")
	}
	ports := map[int]bool{}
	for _, port := range p.Bindings.StackPorts {
		ports[port] = true
	}
	var logs []os.FileInfo
	for _, log := range []*os.File{p.ProbeLog, p.BeforeLog, p.AfterLog} {
		st, err := log.Stat()
		if err != nil {
			return nil, err
		}
		logs = append(logs, st)
	}
	commands := make([]metricsworker.Command, 0, len(targets))
	// All workers run in the same namespace and admitted deployment. Repeated
	// samples of one Pod are valid, contradictory name/UID mappings are not.
	// Include the original network/probe target even when it has no metric.
	names := map[string]string{p.Bindings.Network.PodName: p.Bindings.Network.PodUID}
	uids := map[string]string{p.Bindings.Network.PodUID: p.Bindings.Network.PodName}
	for i, target := range targets {
		if !recoveryIdentity.MatchString(target.PodName) || target.PodUID != p.Bindings.Metrics[i].Binding.PodUID || target.Stderr == nil {
			return nil, errors.New("metric target differs from admitted expectation")
		}
		if uid, exists := names[target.PodName]; exists && uid != target.PodUID {
			return nil, errors.New("one metric Pod name cannot map to multiple UIDs")
		}
		if name, exists := uids[target.PodUID]; exists && name != target.PodName {
			return nil, errors.New("one metric Pod UID cannot map to multiple names")
		}
		names[target.PodName], uids[target.PodUID] = target.PodUID, target.PodName
		for _, port := range []int{target.InfoPort, target.AnonymousPort} {
			if port < 1024 || port > 65535 || ports[port] {
				return nil, errors.New("metric ports must be distinct from all observation ports")
			}
			ports[port] = true
		}
		st, err := target.Stderr.Stat()
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return nil, errors.New("metric stderr must be private")
		}
		for _, previous := range logs {
			if os.SameFile(previous, st) {
				return nil, errors.New("metric stderr must be independent")
			}
		}
		logs = append(logs, st)
		env := append([]string{}, p.Env...)
		env = append(env, "stack_info_port="+strconv.Itoa(target.InfoPort), "stack_anonymous_port="+strconv.Itoa(target.AnonymousPort))
		commands = append(commands, metricsworker.Command{Executable: executable, Args: []string{target.PodName, strconv.FormatInt(int64(p.Bindings.Metrics[i].Offset), 10)}, Env: env, Stderr: target.Stderr})
	}
	return commands, nil
}
