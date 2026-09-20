package leasefault

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
)

// ObservationCommandPlan builds argv, never a shell command string. It does not
// execute, admit tool hashes/live identities, create receipts, or acquire a claim.
// Env is the complete independently admitted environment; inherited shell startup
// hooks are forbidden. Stderr handles remain owned by the caller.
type ObservationCommandPlan struct {
	Bindings                                         NativeExperimentBindings
	OwnerDirectory, ProbeExecutable, StackExecutable string
	Endpoint, ServerName, CA, Certificate, Key       string
	BeforeDirectory, AfterDirectory                  string
	Duration                                         time.Duration
	Env                                              []string
	ProbeLog, BeforeLog, AfterLog                    *os.File
}

type ObservationCommands struct {
	Probe, Before, After metricsworker.Command
}

// ObservationHooks are mandatory online admission and durable retention, not
// defaults supplied by the command builder. AdmitStack must authenticate the
// corresponding command/receipt and source-bound live Pod for each stage.
type ObservationHooks struct {
	AdmitOriginal  func(context.Context) error
	AdmitStack     func(context.Context, string) error
	RetainOriginal func(context.Context, string, Binding, []byte, error) error
	RetainStack    func(context.Context, string, WaitReceipt, error) error
}

// Observation binds the built commands to the actual native lifecycle input.
// It performs local validation only; it does not invoke hooks or start children.
func (p ObservationCommandPlan) Observation(h ObservationHooks) (OriginalObservation, error) {
	if h.AdmitOriginal == nil || h.AdmitStack == nil || h.RetainOriginal == nil || h.RetainStack == nil {
		return OriginalObservation{}, errors.New("original observation requires admission and retention")
	}
	c, err := p.Build()
	if err != nil {
		return OriginalObservation{}, err
	}
	wait := func(stage, directory string, count int, command metricsworker.Command) WaitObservation {
		return WaitObservation{
			Command: command, Directory: directory, Source: p.Bindings.Source, SourceHash: p.Bindings.SourceHash, Count: count,
			Admit: func(ctx context.Context) error { return h.AdmitStack(ctx, stage) },
			Retain: func(ctx context.Context, receipt WaitReceipt, err error) error {
				return h.RetainStack(ctx, stage, receipt, err)
			},
		}
	}
	o := OriginalObservation{Probe: c.Probe, Initial: p.Bindings.Initial(), Before: wait("before", p.BeforeDirectory, 1, c.Before), After: wait("after", p.AfterDirectory, 0, c.After), Admit: h.AdmitOriginal, Retain: h.RetainOriginal}
	if err := o.validate(); err != nil {
		return OriginalObservation{}, err
	}
	return o, nil
}

var stackWorkerName = regexp.MustCompile(`^stack-worker\.[a-zA-Z0-9]{8}$`)
var commandEnvName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (p ObservationCommandPlan) Build() (ObservationCommands, error) {
	var out ObservationCommands
	if err := p.Bindings.Validate(); err != nil {
		return out, err
	}
	for _, path := range []string{p.OwnerDirectory, p.ProbeExecutable, p.StackExecutable, p.CA, p.Certificate, p.Key, p.BeforeDirectory, p.AfterDirectory} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.ContainsAny(path, "\x00\n\r\t") {
			return out, errors.New("invalid observation command path")
		}
	}
	for _, dir := range []string{p.BeforeDirectory, p.AfterDirectory} {
		if filepath.Dir(dir) != p.OwnerDirectory || !stackWorkerName.MatchString(filepath.Base(dir)) {
			return out, errors.New("stack receipt must be a direct owner child")
		}
	}
	host, port, err := net.SplitHostPort(p.Endpoint)
	n, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || host == "" || n < 1 || n > 65535 || p.ServerName == "" || strings.ContainsAny(p.Endpoint+p.ServerName, "\x00\n\r\t ") || p.Duration <= 0 || p.Duration > 5*time.Minute || p.BeforeDirectory == p.AfterDirectory {
		return out, errors.New("invalid observation endpoint, duration or receipt isolation")
	}
	seen := map[string]bool{}
	for _, entry := range p.Env {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || !commandEnvName.MatchString(name) || strings.ContainsRune(entry, 0) || seen[name] || name == "stack_info_port" || name == "stack_anonymous_port" || name == "BASH_ENV" || name == "ENV" || name == "SHELLOPTS" || name == "BASHOPTS" {
			return out, errors.New("ambiguous or conflicting observation environment")
		}
		seen[name] = true
	}
	ownerFound := false
	for _, entry := range p.Env {
		if entry == "stack_owner="+p.OwnerDirectory {
			ownerFound = true
		}
	}
	if !ownerFound {
		return out, errors.New("observation environment must bind owner directory")
	}
	var logFiles []os.FileInfo
	for _, log := range []*os.File{p.ProbeLog, p.BeforeLog, p.AfterLog} {
		if log == nil {
			return out, errors.New("observation requires private stderr files")
		}
		st, err := log.Stat()
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return out, errors.New("invalid observation stderr file")
		}
		for _, previous := range logFiles {
			if os.SameFile(previous, st) {
				return out, errors.New("observation stderr files must be independent")
			}
		}
		logFiles = append(logFiles, st)
	}
	b := p.Bindings
	out.Probe = metricsworker.Command{Executable: p.ProbeExecutable, Env: append([]string{}, p.Env...), Stderr: p.ProbeLog, Args: []string{
		"--endpoint", p.Endpoint, "--cacert", p.CA, "--cert", p.Certificate, "--key", p.Key, "--tls-server-name", p.ServerName,
		"--lease-id", strconv.FormatInt(b.Protocol.LeaseID, 10), "--cluster-id", strconv.FormatUint(b.Protocol.ClusterID, 10), "--member-id", strconv.FormatUint(b.Protocol.AlarmMemberID, 10), "--leased-key", b.Protocol.Key, "--duration", p.Duration.String(), "--wait-for-expiry",
	}}
	stack := func(dir string, count, portIndex int, log *os.File) metricsworker.Command {
		env := append([]string{}, p.Env...)
		env = append(env, "stack_info_port="+strconv.Itoa(b.StackPorts[portIndex]), "stack_anonymous_port="+strconv.Itoa(b.StackPorts[portIndex+1]))
		return metricsworker.Command{Executable: p.StackExecutable, Args: []string{b.Network.PodName, b.Source, b.SourceHash, strconv.Itoa(count), dir}, Env: env, Stderr: log}
	}
	out.Before = stack(p.BeforeDirectory, 1, 0, p.BeforeLog)
	out.After = stack(p.AfterDirectory, 0, 2, p.AfterLog)
	return out, nil
}
