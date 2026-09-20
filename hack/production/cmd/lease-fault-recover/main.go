// lease-fault-recover restores an already admitted dedicated-test attempt.
// It never acquires ownership, starts an experiment or reports fault acceptance.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"google.golang.org/grpc"
	"k8s.io/client-go/dynamic"
	strictjson "sigs.k8s.io/json"
)

const testKubeconfig = "/root/.kube/kubebrain-test-10.32.32.66.conf"
const testContext = "kubebrain-test-10.32.32.66"

type plan struct {
	Directory       string                      `json:"directory"`
	Network         leasefault.NetworkRecovery  `json:"network"`
	Protocol        leasefault.ProtocolRecovery `json:"protocol"`
	APIServer       string                      `json:"api_server"`
	Endpoint        string                      `json:"endpoint"`
	ServerName      string                      `json:"server_name"`
	CA              string                      `json:"ca"`
	Certificate     string                      `json:"certificate"`
	Key             string                      `json:"key"`
	ScriptDirectory string                      `json:"script_directory"`
	JoinScript      string                      `json:"join_script"`
	TargetsSHA256   string                      `json:"targets_sha256"`
	Files           map[string]string           `json:"files"`
	TimeoutSeconds  int                         `json:"timeout_seconds"`
}

var observerFiles = []string{"observe-local-network-restored.sh", "observe-local-fault-label.sh", "observe-local-policy-state.sh", "observe-local-backend-tcp.sh", "capture-local-cilium-endpoint.sh", "local-label-identity-transition.jq", "local-policy-observation.jq", "../../hack/production/same-pod-process.jq"}

func readFile(path string, private bool, limit int64) ([]byte, error) {
	return planinput.ReadFile(path, private, limit)
}
func digest(data []byte) string { return planinput.SHA256(data) }
func validDigest(s string) bool { return planinput.ValidSHA256(s) }

func loadPlan(path, approved string) (plan, error) {
	var p plan
	if !validDigest(approved) {
		return p, errors.New("require independently approved plan SHA256")
	}
	data, err := readFile(path, true, 1<<20)
	if err != nil {
		return p, err
	}
	if digest(data) != approved {
		return p, errors.New("plan differs from approved SHA256")
	}
	strict, err := strictjson.UnmarshalStrict(data, &p, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 {
		return p, errors.New("invalid strict recovery plan JSON")
	}
	if p.TimeoutSeconds < 1 || p.TimeoutSeconds > 300 || len(p.Files) == 0 || len(p.Files) > 64 || !validDigest(p.TargetsSHA256) {
		return p, errors.New("invalid recovery bounds")
	}
	for _, dir := range []string{p.Directory, p.ScriptDirectory} {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || dir == "/" {
			return p, errors.New("invalid recovery directory")
		}
	}
	st, err := os.Lstat(p.Directory)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return p, errors.New("owner directory must be private 0700")
	}
	// These observers are intentionally specific to the existing dedicated
	// cluster. Do not silently point them at a different Go API client/scope.
	n := p.Network
	if n.Namespace != "kubebrain-dbaas-test" || n.NamespaceUID != "6c57c242-912b-41bb-9020-f4fdb3225ef3" || n.StatefulSetUID != "7d760f53-5bb5-4429-a2f8-651b89665616" || p.Protocol.Owner != n.Owner || p.Protocol.NamespaceUID != n.NamespaceUID || p.Protocol.StatefulSetUID != n.StatefulSetUID {
		return p, errors.New("plan does not bind the dedicated observer scope")
	}
	if err := leasefault.ValidateRecoveryPlans(p.Network, p.Protocol); err != nil {
		return p, err
	}
	if !regexp.MustCompile(`^kubebrain-local-[012]$`).MatchString(n.PodName) || !regexp.MustCompile(`^kb-term-[a-z0-9]+$`).MatchString(n.PolicyName) || n.Nonce != strings.TrimPrefix(n.PolicyName, "kb-") {
		return p, errors.New("unsupported local observer target")
	}
	u, err := url.Parse(p.APIServer)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return p, errors.New("invalid admitted Kubernetes API URL")
	}
	host, port, err := net.SplitHostPort(p.Endpoint)
	if err != nil || host == "" || port == "" || p.ServerName == "" {
		return p, errors.New("require direct recovery endpoint and TLS identity")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return p, errors.New("invalid recovery endpoint port")
	}
	required := []string{testKubeconfig, p.CA, p.Certificate, p.Key, p.JoinScript, "/bin/bash"}
	for _, name := range observerFiles {
		required = append(required, filepath.Join(p.ScriptDirectory, name))
	}
	for _, name := range required {
		if !filepath.IsAbs(name) || !validDigest(p.Files[name]) {
			return p, errors.New("missing pinned recovery input")
		}
	}
	if p.Files[filepath.Join(p.Directory, "observer-pod.json")] != digest(n.PodBefore) || p.Files[filepath.Join(p.Directory, "observer-targets.json")] != p.TargetsSHA256 {
		return p, errors.New("observer snapshots must be pinned to independent admission")
	}
	return p, nil
}

func (p plan) verifyFiles(ctx context.Context) error {
	for path, wanted := range p.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !validDigest(wanted) {
			return errors.New("invalid file digest")
		}
		data, err := readFile(path, path == testKubeconfig || path == p.Key || path == filepath.Join(p.Directory, "observer-pod.json") || path == filepath.Join(p.Directory, "observer-targets.json"), 128<<20)
		if err != nil {
			return err
		}
		if digest(data) != wanted {
			return fmt.Errorf("admitted file changed: %s", path)
		}
	}
	return ctx.Err()
}

func clients(p plan) (dynamic.Interface, *grpc.ClientConn, error) {
	return leasefault.NewFaultConnections(leasefault.ConnectionPlan{
		Kubeconfig: testKubeconfig, Context: testContext, APIServer: p.APIServer,
		Endpoint: p.Endpoint, ServerName: p.ServerName, CA: p.CA,
		Certificate: p.Certificate, Key: p.Key, Files: p.Files,
	})
}

func retain(dir, stage string, output []byte, observed error) error {
	return leasefault.RetainRecoveryObserver(dir, stage, output, observed)
}

func execute(ctx context.Context, p plan, client dynamic.Interface, conn grpc.ClientConnInterface, checkFiles func(context.Context) error) error {
	var owner *leasefault.FaultOwner
	admit := func(ctx context.Context) error {
		if err := checkFiles(ctx); err != nil {
			return err
		}
		if owner == nil {
			var err error
			owner, err = leasefault.LoadFaultOwner(client, p.Directory, leasefault.FaultOwnerBinding{Owner: p.Network.Owner, Namespace: p.Network.Namespace, NamespaceUID: p.Network.NamespaceUID, StatefulSetName: "kubebrain-local", StatefulSetUID: p.Network.StatefulSetUID})
			if err != nil {
				return err
			}
		}
		return owner.Check(ctx)
	}
	env := []string{"PATH=/usr/local/bin:/usr/bin:/bin"}
	observer := leasefault.NetworkObserver{Directory: p.Directory, StatefulSetName: "kubebrain-local", ScriptDirectory: p.ScriptDirectory, TargetsSHA256: p.TargetsSHA256, Network: p.Network, Client: client, Env: env, Admit: admit, Retain: func(stage string, out []byte, err error) error { return retain(p.Directory, stage, out, err) }}
	join := func(ctx context.Context) error {
		// Join only through an independently audited and pinned owner-specific
		// script. It must verify process identities and handle escaped descendants.
		if err := checkFiles(ctx); err != nil {
			return err
		}
		cmd := exec.CommandContext(ctx, "/bin/bash", p.JoinScript, p.Directory)
		cmd.Env = env
		processgroup.Configure(cmd)
		cmd.WaitDelay = processgroup.DefaultWaitDelay
		out, err := processgroup.CombinedOutput(cmd, processgroup.DefaultOutputLimitBytes)
		return errors.Join(err, ctx.Err(), retain(p.Directory, "join", out, err))
	}
	return leasefault.RecoverFault(ctx, leasefault.FaultRecovery{Directory: p.Directory, StatefulSetName: "kubebrain-local", Network: p.Network, Protocol: p.Protocol, Client: client, Connection: conn, Own: admit, Join: join, NetworkRestored: observer.Restored, IdentityRestored: observer.Unlabelled})
}

func run(ctx context.Context, args []string, out io.Writer) error {
	return runWith(ctx, args, out, clients, func(ctx context.Context, p plan) error { return p.verifyFiles(ctx) })
}

func runWith(ctx context.Context, args []string, out io.Writer, connect func(plan) (dynamic.Interface, *grpc.ClientConn, error), verify func(context.Context, plan) error) error {
	f := flag.NewFlagSet("lease-fault-recover", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path := f.String("plan", "", "private independently admitted JSON plan")
	approved := f.String("approve-sha256", "", "independently approved plan SHA256; not proof of CI or runtime admission")
	executeFlag := f.Bool("execute", false, "execute post-join recovery; retain owner claim")
	seen := map[string]bool{}
	for i := 0; i < len(args); {
		name := args[i]
		if seen[name] {
			return errors.New("repeated option")
		}
		seen[name] = true
		switch name {
		case "--plan", "--approve-sha256":
			if i+1 >= len(args) {
				return errors.New("missing option value")
			}
			i += 2
		case "--execute":
			i++
		default:
			return errors.New("unknown option or unsupported option form")
		}
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	p, err := loadPlan(*path, *approved)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(p.TimeoutSeconds)*time.Second)
	defer cancel()
	check := func(ctx context.Context) error {
		data, err := readFile(*path, true, 1<<20)
		if err != nil {
			return err
		}
		if digest(data) != *approved {
			return errors.New("approved plan changed")
		}
		return verify(ctx, p)
	}
	if err := check(ctx); err != nil {
		return err
	}
	if !*executeFlag {
		_, err := fmt.Fprintln(out, "PLAN_FILES_VERIFIED_NO_CLUSTER_ACCESS_NO_ACCEPTANCE_PROOF")
		return err
	}
	client, conn, err := connect(p)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := execute(ctx, p, client, conn, check); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "RECOVERY_VERIFIED_OWNER_CLAIM_RETAINED_NOT_FAULT_ACCEPTANCE")
	return err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
