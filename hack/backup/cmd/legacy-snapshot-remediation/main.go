package main

import (
	"context"
	"fmt"
	"os"

	"github.com/kubewharf/kubebrain/hack/backup/internal/legacyremediation"
)

func main() {
	config, err := legacyremediation.ConfigFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	_, code, err := legacyremediation.Run(ctx, config, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}
