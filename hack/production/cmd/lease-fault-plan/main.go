// lease-fault-plan validates local inputs and optionally authenticates their
// GitHub release. It never admits a live deployment or executes a fault.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
)

func run(args []string, out io.Writer) error {
	return runWithRelease(context.Background(), args, out, leasefault.AuthenticateCommandRelease)
}

func runWithRelease(ctx context.Context, args []string, out io.Writer, authenticate func(context.Context, string, string, string, string) error) error {
	f := flag.NewFlagSet("lease-fault-plan", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path := f.String("bindings", "", "private identity/expectation JSON file")
	command := f.String("command-plan", "", "private full command-input JSON; local checks only")
	approved := f.String("approve-sha256", "", "independently approved file SHA256")
	release := f.String("release-plan", "", "optional approved GitHub download plan; performs online release preflight")
	releaseDigest := f.String("release-approve-sha256", "", "independently approved release plan SHA256")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || (*path == "") == (*command == "") || *approved == "" {
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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runWithRelease(ctx, os.Args[1:], os.Stdout, leasefault.AuthenticateCommandRelease); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
