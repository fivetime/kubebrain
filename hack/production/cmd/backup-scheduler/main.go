package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/backupscheduler"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var namespace, kubeconfig, contextName string
	var pollInterval time.Duration
	var once bool
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "policy and operation namespace")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path; empty uses in-cluster credentials")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.DurationVar(&pollInterval, "poll-interval", time.Minute, "policy reconciliation interval")
	flag.BoolVar(&once, "once", false, "reconcile once and exit")
	flag.Parse()
	if pollInterval < 10*time.Second {
		log.Fatal("poll-interval must be at least 10s")
	}

	config, err := clientConfig(kubeconfig, contextName)
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	scheduler := backupscheduler.New(client, namespace)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		submitted, err := scheduler.Reconcile(ctx)
		if err != nil {
			log.Printf("reconcile failed: %v", err)
		} else if submitted > 0 {
			log.Printf("reconciled %d due backup policies", submitted)
		}
		if once {
			if err != nil {
				os.Exit(1)
			}
			return
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func clientConfig(kubeconfig, contextName string) (*rest.Config, error) {
	if kubeconfig == "" {
		if config, err := rest.InClusterConfig(); err == nil {
			return config, nil
		}
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	loading.ExplicitPath = kubeconfig
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
}
