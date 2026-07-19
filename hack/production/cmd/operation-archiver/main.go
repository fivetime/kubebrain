package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationarchiver"
	"github.com/kubewharf/kubebrain/hack/production/internal/reconcilebudget"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var inventoryName, inventoryNamespace, inventoryKey string
	var executor, objectStoreID, bucket, prefix, retentionMode string
	var kubeconfig, contextName string
	var pollInterval, retentionDuration, reconcileTimeout, archiveTimeout time.Duration
	var maxBatch int
	var once bool
	flag.StringVar(&inventoryName, "namespace-inventory-configmap", "", "ConfigMap containing the namespace allowlist")
	flag.StringVar(&inventoryNamespace, "namespace-inventory-namespace", "kubebrain-operations", "namespace containing the inventory ConfigMap")
	flag.StringVar(&inventoryKey, "namespace-inventory-key", "namespaces.json", "ConfigMap data key containing a JSON namespace array")
	flag.StringVar(&executor, "archive-executor", "/usr/local/bin/kubebrain-logical-object", "Object Lock archive executor")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store account identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&prefix, "object-prefix", "operation-audit", "archive object key prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 8760*time.Hour, "retention measured from terminal completion time")
	flag.DurationVar(&pollInterval, "poll-interval", time.Minute, "reconciliation interval")
	flag.DurationVar(&reconcileTimeout, "reconcile-timeout", 15*time.Minute,
		"maximum duration of one reconciliation, including object archive uploads")
	flag.DurationVar(&archiveTimeout, "archive-timeout", 2*time.Minute,
		"maximum duration of one operation archive and finalizer release")
	flag.IntVar(&maxBatch, "max-batch", 32, "maximum terminal operations processed per reconciliation")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path; empty uses in-cluster credentials")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.BoolVar(&once, "once", false, "reconcile once and exit")
	flag.Parse()
	if inventoryName == "" || objectStoreID == "" || bucket == "" {
		log.Fatal("namespace-inventory-configmap, object-store-id, and bucket are required")
	}
	if pollInterval < 10*time.Second {
		log.Fatal("poll-interval must be at least 10s")
	}
	if reconcileTimeout <= 0 {
		log.Fatal("reconcile-timeout must be positive")
	}
	if archiveTimeout <= 0 || archiveTimeout > reconcileTimeout {
		log.Fatal("archive-timeout must be positive and no greater than reconcile-timeout")
	}
	config, err := clientConfig(kubeconfig, contextName)
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	processor, err := operationarchiver.NewArchiveProcessor(
		client, executor, objectStoreID, bucket, prefix, retentionMode, retentionDuration,
	)
	if err != nil {
		log.Fatal(err)
	}
	boundedProcessor, err := operationarchiver.WithTimeout(processor, archiveTimeout)
	if err != nil {
		log.Fatal(err)
	}
	controller, err := operationarchiver.New(
		client, boundedProcessor, inventoryNamespace, inventoryName, inventoryKey, maxBatch,
	)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for {
		processed, err := reconcilebudget.Run(ctx, reconcileTimeout, controller.Reconcile)
		if err != nil {
			log.Printf("reconcile failed after archiving %d operations: %v", processed, err)
		} else if processed > 0 {
			log.Printf("archived %d terminal operations", processed)
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

func init() {
	log.SetFlags(0)
}
