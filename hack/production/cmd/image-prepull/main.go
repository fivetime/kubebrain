package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/imageprepull"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Preparation only creates isolated holders. Verification reconstructs the
// prepared session from fresh evidence; neither mode changes business Pods.
func run(ctx context.Context, args []string, output io.Writer) (retErr error) {
	flags := flag.NewFlagSet("image-prepull", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	mode := flags.String("mode", "", "fetch-release, verify-release, check-admission, prepare, verify, or recover-cleanup")
	releasePlan := flags.String("release-plan", "", "private independently reviewed release download plan")
	releasePlanSHA := flags.String("release-plan-sha256", "", "independently approved release plan SHA256")
	image := flags.String("image", "", "approved immutable OCI index image")
	indexFile := flags.String("index-file", "", "absolute path to exact raw OCI index bytes")
	amd64 := flags.String("amd64-digest", "", "independently reviewed CI amd64 manifest digest")
	arm64 := flags.String("arm64-digest", "", "independently reviewed CI arm64 manifest digest")
	kubeconfig := flags.String("kubeconfig", "", "explicit absolute kubeconfig path")
	kubeContext := flags.String("context", "", "explicit kubeconfig context")
	namespace := flags.String("namespace", "", "expected recovery namespace")
	namespaceUID := flags.String("namespace-uid", "", "expected recovery Namespace UID")
	source := flags.String("statefulset", "", "expected source StatefulSet name")
	sourceUID := flags.String("statefulset-uid", "", "expected source StatefulSet UID")
	directory := flags.String("receipt-directory", "", "absolute private 0700 receipt directory")
	name := flags.String("receipt-name", "", "existing cleanup receipt name")
	requestTimeout := flags.Duration("request-timeout", 10*time.Second, "per-HTTP-attempt timeout, <=30s")
	cleanupTimeout := flags.Duration("cleanup-timeout", 30*time.Second, "total cleanup timeout, <=5m")
	responseBytes := flags.Int64("max-response-bytes", 16<<20, "maximum finite API response body, <=32MiB")
	prepareTimeout := flags.Duration("prepare-timeout", 10*time.Minute, "total planning/preparation timeout, <=1h")
	verifyTimeout := flags.Duration("verify-timeout", 30*time.Second, "total fresh verification timeout, <=1h")
	remainingHold := flags.Duration("minimum-remaining-hold", 30*time.Minute, "required live window remaining; runner must cover its entire rollout/rollback/cleanup budget")
	holdSeconds := flags.Int64("hold-seconds", 3600, "holder active deadline, 60..86400 seconds")
	ttlSeconds := flags.Int("ttl-seconds", 300, "finished Job TTL, 1..3600 seconds")
	clientService := flags.String("client-service", "kubebrain-client", "source client Service name")
	confirmCreate := flags.Bool("confirm-create-isolated-jobs", false, "required for prepare; never authorizes business workload mutation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch *mode {
	case "fetch-release":
		return fetchRelease(ctx, *releasePlan, *releasePlanSHA, output)
	case "verify-release":
		data, err := readRegularFile(*indexFile, 1<<20, false)
		if err != nil {
			return err
		}
		approved, err := imageprepull.ApprovedRuntimeDigests(*image, data, map[string]string{"linux/amd64": *amd64, "linux/arm64": *arm64})
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(struct {
			Image          string              `json:"image"`
			RuntimeDigests map[string][]string `json:"runtimeDigests"`
			Scope          string              `json:"scope"`
		}{*image, approved, "index/platform identity only; not CI authorization or rollout success"})
	case "recover-cleanup", "prepare", "verify", "check-admission":
		if !filepath.IsAbs(*directory) || *kubeContext == "" || *namespace == "" || *namespaceUID == "" || *source == "" || *sourceUID == "" {
			return errors.New("recovery requires an absolute receipt directory, explicit context and complete expected namespace/source identities")
		}
		if *cleanupTimeout <= 0 || *cleanupTimeout > 5*time.Minute {
			return errors.New("cleanup timeout must be positive and <=5m")
		}
		if *prepareTimeout <= 0 || *prepareTimeout > time.Hour || *verifyTimeout <= 0 || *verifyTimeout > time.Hour ||
			*remainingHold <= 0 || *remainingHold > 24*time.Hour || *holdSeconds < 60 || *holdSeconds > 86400 || *ttlSeconds < 1 || *ttlSeconds > 3600 {
			return errors.New("preparation, verification or holder timing controls are out of bounds")
		}
		if *mode == "prepare" && (!*confirmCreate || time.Duration(*holdSeconds)*time.Second <= *remainingHold) {
			return errors.New("prepare requires --confirm-create-isolated-jobs and a holder lifetime longer than the required remaining window")
		}
		var approved map[string][]string
		if *mode != "recover-cleanup" {
			indexBytes, err := readRegularFile(*indexFile, 1<<20, false)
			if err != nil {
				return err
			}
			approved, err = imageprepull.ApprovedRuntimeDigests(*image, indexBytes, map[string]string{"linux/amd64": *amd64, "linux/arm64": *arm64})
			if err != nil {
				return err
			}
		}
		expected := imageprepull.RecoveryScope{Namespace: *namespace, NamespaceUID: types.UID(*namespaceUID), SourceName: *source, SourceUID: types.UID(*sourceUID)}
		var journal *imageprepull.RecoveryJournal
		var err error
		if *mode == "prepare" || *mode == "check-admission" {
			journal, err = imageprepull.CreateRecoveryJournal(*directory, *name, expected)
		} else {
			journal, err = imageprepull.OpenRecoveryJournal(*directory, *name)
		}
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, journal.Close()) }()
		if journal.Scope() != expected {
			return errors.New("cleanup receipt differs from explicitly requested namespace/source scope")
		}
		data, err := readRegularFile(*kubeconfig, 1<<20, true)
		if err != nil {
			return err
		}
		loaded, err := clientcmd.Load(data)
		if err != nil {
			return errors.New("cannot decode explicit recovery kubeconfig")
		}
		for _, cluster := range loaded.Clusters {
			cluster.LocationOfOrigin = *kubeconfig
		}
		for _, auth := range loaded.AuthInfos {
			auth.LocationOfOrigin = *kubeconfig
		}
		if err := clientcmd.ResolveLocalPaths(loaded); err != nil {
			return err
		}
		config, err := clientcmd.NewNonInteractiveClientConfig(*loaded, *kubeContext, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
		if err != nil {
			return err
		}
		client, err := imageprepull.NewBoundedClient(config, *requestTimeout, *responseBytes)
		if err != nil {
			return err
		}
		executor := imageprepull.Executor{Client: client, Journal: journal, PrepareTimeout: *verifyTimeout, CleanupTimeout: *cleanupTimeout,
			PollInterval: 200 * time.Millisecond, MinRemainingHold: *remainingHold}
		if *mode == "prepare" || *mode == "check-admission" {
			executor.PrepareTimeout = *prepareTimeout
			phase, cancel := context.WithTimeout(ctx, *prepareTimeout)
			defer cancel()
			requests, err := executor.Plan(phase, *image, *clientService, *holdSeconds, int32(*ttlSeconds))
			if err != nil {
				return err
			}
			if *mode == "check-admission" {
				if err := executor.CheckAdmission(phase, requests); err != nil {
					return err
				}
				_, err = fmt.Fprintln(output, "PREPULL_ADMISSION_CONFIRMED dryRun=All containersExecuted=false")
				return err
			}
			session, err := executor.Prepare(phase, requests, approved)
			if err != nil {
				return err
			}
			// If publishing the success marker fails, compensate while the
			// journal is still open. A surrounding runner must also always clean
			// on exit, including crashes/signals that cannot reach this defer.
			defer func() {
				if retErr != nil {
					retErr = errors.Join(retErr, executor.Cleanup(context.WithoutCancel(ctx), session))
				}
			}()
			_, err = fmt.Fprintln(output, "PREPULL_READY")
			return err
		}
		if *mode == "verify" {
			if _, err := executor.RestoreVerified(ctx, *image, approved); err != nil {
				return err
			}
			_, err = fmt.Fprintln(output, "PREPULL_VERIFIED")
			return err
		}
		if err := executor.RecoverCleanup(ctx, journal); err != nil {
			return err
		}
		_, err = fmt.Fprintln(output, "PREPULL_CLEANUP_CONFIRMED")
		return err
	default:
		return errors.New("--mode must be verify-release, check-admission, prepare, verify or recover-cleanup")
	}
}

func readRegularFile(path string, limit int64, private bool) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("input file path must be absolute")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() > limit || (private && before.Mode().Perm() != 0600) {
		return nil, errors.New("input must be a bounded regular file; kubeconfig must have mode 0600")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		return nil, errors.New("input file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("input file exceeds its byte limit")
	}
	return data, nil
}
