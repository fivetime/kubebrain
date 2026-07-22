package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/backupscheduler"
	"github.com/kubewharf/kubebrain/hack/production/internal/reconcilebudget"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var inClusterConfig = rest.InClusterConfig

func main() {
	var namespace, namespacesText, inventoryName, inventoryNamespace, inventoryKey string
	var requester, kubeconfig, contextName string
	var pollInterval, reconcileTimeout time.Duration
	var maxPolicies int
	var once bool
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "policy and operation namespace")
	flag.StringVar(&namespacesText, "namespaces", "", "comma-separated allowlist of policy and operation namespaces; overrides --namespace")
	flag.StringVar(&inventoryName, "namespace-inventory-configmap", "", "ConfigMap read on every reconciliation for a dynamic namespace allowlist")
	flag.StringVar(&inventoryNamespace, "namespace-inventory-namespace", "kubebrain-operations", "namespace containing --namespace-inventory-configmap")
	flag.StringVar(&inventoryKey, "namespace-inventory-key", backupscheduler.DefaultInventoryKey, "ConfigMap data key containing a JSON namespace array")
	flag.StringVar(&requester, "requested-by", backupscheduler.DefaultRequester, "immutable requester identity written to generated operations")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path; empty uses in-cluster credentials")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.DurationVar(&pollInterval, "poll-interval", time.Minute, "policy reconciliation interval")
	flag.DurationVar(&reconcileTimeout, "reconcile-timeout", 2*time.Minute,
		"maximum duration of one policy reconciliation")
	flag.IntVar(&maxPolicies, "max-policies", backupscheduler.DefaultMaxPolicies,
		"maximum backup policies attempted per reconciliation")
	flag.BoolVar(&once, "once", false, "reconcile once and exit")
	flag.Parse()
	if pollInterval < 10*time.Second {
		log.Fatal("poll-interval must be at least 10s")
	}
	if reconcileTimeout <= 0 {
		log.Fatal("reconcile-timeout must be positive")
	}
	if maxPolicies <= 0 {
		log.Fatal("max-policies must be positive")
	}
	if requester == "" || len(requester) > 253 {
		log.Fatal("requested-by must contain 1 to 253 characters")
	}
	if inventoryName != "" && namespacesText != "" {
		log.Fatal("--namespace-inventory-configmap and --namespaces are mutually exclusive")
	}

	config, err := clientConfig(kubeconfig, contextName)
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	var scheduler *backupscheduler.Scheduler
	if inventoryName != "" {
		scheduler = backupscheduler.NewForInventory(
			client, inventoryNamespace, inventoryName, inventoryKey,
		)
	} else {
		namespaces, err := parseNamespaces(namespace, namespacesText)
		if err != nil {
			log.Fatal(err)
		}
		scheduler = backupscheduler.NewForNamespaces(client, namespaces)
	}
	scheduler.WithRequester(requester)
	if err := scheduler.SetMaxPolicies(maxPolicies); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		submitted, err := reconcilebudget.Run(ctx, reconcileTimeout, scheduler.Reconcile)
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

func parseNamespaces(single, multiple string) ([]string, error) {
	if multiple == "" {
		multiple = single
	}
	var namespaces []string
	for _, raw := range strings.Split(multiple, ",") {
		namespaces = append(namespaces, strings.TrimSpace(raw))
	}
	return backupscheduler.ValidateNamespaces(namespaces)
}

func clientConfig(kubeconfig, contextName string) (*rest.Config, error) {
	if kubeconfig == "" {
		if config, err := inClusterConfig(); err == nil {
			return config, nil
		}
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	loading.ExplicitPath = kubeconfig
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
}
