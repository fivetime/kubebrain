package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/kubewharf/kubebrain/hack/backup/internal/etcdsnapshot"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("logical-etcd-snapshot", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "", "verified kubebrain.logical.v2 artifact")
	output := flags.String("output", "", "new etcd snapshot path")
	authDisabled := flags.Bool("acknowledge-auth-disabled", false, "acknowledge that the output snapshot has auth disabled")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *input == "" || *output == "" {
		return fmt.Errorf("--input and --output are required")
	}
	status, err := etcdsnapshot.Convert(*input, *output, etcdsnapshot.Options{AcknowledgeAuthDisabled: *authDisabled})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "converted %d records at revision %d to %s (auth disabled)\n", status.Records, status.Revision, *output)
	return err
}
