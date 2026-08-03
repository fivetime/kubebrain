package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/kubewharf/kubebrain/hack/backup/internal/etcdsnapshot"
)

func main() {
	input := flag.String("input", "", "verified kubebrain.logical.v2 artifact")
	output := flag.String("output", "", "new etcd snapshot path")
	authDisabled := flag.Bool("acknowledge-auth-disabled", false, "acknowledge that the output snapshot has auth disabled")
	flag.Parse()
	if *input == "" || *output == "" {
		log.Fatal("--input and --output are required")
	}
	status, err := etcdsnapshot.Convert(*input, *output, etcdsnapshot.Options{AcknowledgeAuthDisabled: *authDisabled})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("converted %d records at revision %d to %s (auth disabled)\n", status.Records, status.Revision, *output)
}
