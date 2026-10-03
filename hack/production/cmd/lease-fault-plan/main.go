// lease-fault-plan seals local command drafts, validates pinned inputs and
// optionally authenticates their GitHub release. It never admits a live
// deployment or executes a fault.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
)

func run(args []string, out io.Writer) error {
	return runWithRelease(context.Background(), args, out, leasefault.AuthenticateCommandRelease)
}

func runWithRelease(ctx context.Context, args []string, out io.Writer, authenticate func(context.Context, string, string, string, string) error) error {
	f := flag.NewFlagSet("lease-fault-plan", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path := f.String("bindings", "", "private identity/expectation JSON file")
	command := f.String("command-plan", "", "private full command-input JSON; local checks only")
	draft := f.String("seal-command-draft", "", "private NativeCommandPlan JSON with empty file digests; seal pins only")
	writePlan := f.String("write-command-plan", "", "new private output path for --seal-command-draft")
	approved := f.String("approve-sha256", "", "independently approved file SHA256")
	release := f.String("release-plan", "", "optional approved GitHub download plan; performs online release preflight")
	releaseDigest := f.String("release-approve-sha256", "", "independently approved release plan SHA256")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	sealing := *draft != "" || *writePlan != ""
	if sealing {
		if *draft == "" || *writePlan == "" || *path != "" || *command != "" || *approved != "" || *release != "" || *releaseDigest != "" {
			return errors.New("sealing requires only --seal-command-draft and --write-command-plan")
		}
		data, err := planinput.ReadFile(*draft, true, 4<<20)
		if err != nil {
			return err
		}
		sealed, err := leasefault.SealNativeCommandPlanDraft(ctx, data)
		if err != nil {
			return err
		}
		if err := writePrivatePlan(*writePlan, sealed); err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "COMMAND_PLAN_PINNED_NOT_INDEPENDENTLY_APPROVED sha256=%s\n", planinput.SHA256(sealed))
		return err
	}
	if (*path == "") == (*command == "") || *approved == "" {
		return errors.New("require exactly one of --bindings or --command-plan and independently approved --approve-sha256")
	}
	if (*release == "") != (*releaseDigest == "") || (*release != "" && *command == "") {
		return errors.New("release preflight requires --command-plan and both release options")
	}
	if *release != "" {
		if authenticate == nil {
			return errors.New("missing release authenticator")
		}
		if err := authenticate(ctx, *command, *approved, *release, *releaseDigest); err != nil {
			return err
		}
		_, err := fmt.Fprintln(out, "COMMAND_RELEASE_AUTHENTICATED_NOT_LIVE_OR_FAULT_ADMISSION")
		return err
	}
	if *command != "" {
		if _, err := leasefault.LoadNativeCommandPlan(*command, *approved); err != nil {
			return err
		}
		_, err := fmt.Fprintln(out, "LOCAL_COMMAND_INPUTS_VALID_NOT_EXPERIMENT_ADMISSION")
		return err
	}
	if _, err := leasefault.LoadNativeExperimentBindings(*path, *approved); err != nil {
		return err
	}
	_, err := fmt.Fprintln(out, "LOCAL_BINDINGS_VALID_NOT_EXPERIMENT_ADMISSION")
	return err
}

func writePrivatePlan(path string, data []byte) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(data) == 0 || len(data) > 4<<20 {
		return errors.New("invalid sealed plan destination")
	}
	parent := filepath.Dir(path)
	st, err := os.Lstat(parent)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return errors.New("sealed plan destination requires a private existing directory")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	readback, err := planinput.ReadFile(path, true, 4<<20)
	if err != nil || !bytes.Equal(readback, data) {
		return errors.Join(err, errors.New("sealed plan readback mismatch"))
	}
	dir, err := os.Open(parent)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runWithRelease(ctx, os.Args[1:], os.Stdout, leasefault.AuthenticateCommandRelease); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
