package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationarchiver"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationarchiveverifier"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/kubewharf/kubebrain/hack/production/internal/reconcilebudget"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var inClusterConfig = rest.InClusterConfig

func main() {
	var inventoryName, inventoryNamespace, inventoryKey string
	var executor, objectStoreID, bucket, prefix, retentionMode, kubeconfig, contextName string
	var retentionDuration, reconcileTimeout, verifyTimeout time.Duration
	var maxBatch int
	flag.StringVar(&inventoryName, "namespace-inventory-configmap", "", "ConfigMap containing the namespace allowlist")
	flag.StringVar(&inventoryNamespace, "namespace-inventory-namespace", "kubebrain-operations", "namespace containing the inventory ConfigMap")
	flag.StringVar(&inventoryKey, "namespace-inventory-key", "namespaces.json", "ConfigMap data key containing a JSON namespace array")
	flag.StringVar(&executor, "verify-executor", "/usr/local/bin/kubebrain-logical-object", "read-only Object Lock verifier")
	flag.StringVar(&objectStoreID, "object-store-id", "", "stable object store account identifier")
	flag.StringVar(&bucket, "bucket", "", "Object Lock bucket")
	flag.StringVar(&prefix, "object-prefix", "operation-audit", "archive object key prefix")
	flag.StringVar(&retentionMode, "retention-mode", "COMPLIANCE", "COMPLIANCE or GOVERNANCE")
	flag.DurationVar(&retentionDuration, "retention-duration", 8760*time.Hour, "retention measured from terminal completion time")
	flag.DurationVar(&reconcileTimeout, "reconcile-timeout", 15*time.Minute, "maximum duration of the verification batch")
	flag.DurationVar(&verifyTimeout, "verify-timeout", 2*time.Minute, "maximum duration of one exact-version verification")
	flag.IntVar(&maxBatch, "max-batch", 32, "maximum released terminal operations verified per run")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path; empty uses in-cluster credentials")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.Parse()
	if err := validateIAMSimulationExpiry(os.Getenv("IAM_SIMULATION_VALID_UNTIL_UNIX"), time.Now()); err != nil {
		log.Fatal(err)
	}
	if inventoryName == "" || objectStoreID == "" || bucket == "" || maxBatch <= 0 {
		log.Fatal("namespace-inventory-configmap, object-store-id, bucket, and positive max-batch are required")
	}
	var err error
	if inventoryKey, err = namespaceinventory.ValidateSource(inventoryNamespace, inventoryName, inventoryKey); err != nil {
		log.Fatal(err)
	}
	if reconcileTimeout <= 0 || verifyTimeout <= 0 || verifyTimeout > reconcileTimeout {
		log.Fatal("verification timeouts are invalid")
	}
	if err := processgroup.ValidateExecutable(executor); err != nil {
		log.Fatal(err)
	}
	config, err := clientConfig(kubeconfig, contextName)
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	processor, err := operationarchiveverifier.NewProcessor(executor, objectStoreID, bucket, prefix, retentionMode, retentionDuration)
	if err != nil {
		log.Fatal(err)
	}
	bounded, err := operationarchiver.WithTimeout(processor, verifyTimeout)
	if err != nil {
		log.Fatal(err)
	}
	controller, err := operationarchiveverifier.NewController(client, bounded, inventoryNamespace, inventoryName, inventoryKey, maxBatch)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	verified, err := reconcilebudget.Run(ctx, reconcileTimeout, controller.Reconcile)
	if err != nil {
		log.Printf("archive evidence verification failed after verifying %d operations: %v", verified, err)
		os.Exit(1)
	}
	log.Printf("verified %d released terminal operation archives", verified)
}

func validateIAMSimulationExpiry(raw string, now time.Time) error {
	validUntil, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || validUntil <= 0 {
		return fmt.Errorf("IAM_SIMULATION_VALID_UNTIL_UNIX must be a positive Unix timestamp")
	}
	if now.Unix() >= validUntil {
		return fmt.Errorf("IAM simulation evidence expired at Unix timestamp %d", validUntil)
	}
	return nil
}

func clientConfig(kubeconfig, contextName string) (*rest.Config, error) {
	if kubeconfig == "" {
		if config, err := inClusterConfig(); err == nil {
			return config, nil
		}
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	loading.ExplicitPath = kubeconfig
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, &clientcmd.ConfigOverrides{CurrentContext: contextName}).ClientConfig()
}

func init() { log.SetFlags(0) }
