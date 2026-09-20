// lease-fault-plan validates local bindings or serialized command inputs only.
// It never connects, starts children, admits a deployment or executes a fault.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
)

func run(args []string, out io.Writer) error {
	f := flag.NewFlagSet("lease-fault-plan", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path := f.String("bindings", "", "private identity/expectation JSON file")
	command := f.String("command-plan", "", "private full command-input JSON; local checks only")
	approved := f.String("approve-sha256", "", "independently approved file SHA256")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || (*path == "") == (*command == "") || *approved == "" {
		return errors.New("require exactly one of --bindings or --command-plan and independently approved --approve-sha256")
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
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
