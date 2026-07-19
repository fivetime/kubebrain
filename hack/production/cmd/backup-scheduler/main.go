package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/backupscheduler"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var namespace, namespacesText, requester, kubeconfig, contextName string
	var pollInterval time.Duration
	var once bool
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "policy and operation namespace")
	flag.StringVar(&namespacesText, "namespaces", "", "comma-separated allowlist of policy and operation namespaces; overrides --namespace")
	flag.StringVar(&requester, "requested-by", backupscheduler.DefaultRequester, "immutable requester identity written to generated operations")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path; empty uses in-cluster credentials")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.DurationVar(&pollInterval, "poll-interval", time.Minute, "policy reconciliation interval")
	flag.BoolVar(&once, "once", false, "reconcile once and exit")
	flag.Parse()
	if pollInterval < 10*time.Second {
		log.Fatal("poll-interval must be at least 10s")
	}
	if requester == "" || len(requester) > 253 {
		log.Fatal("requested-by must contain 1 to 253 characters")
	}
	namespaces, err := parseNamespaces(namespace, namespacesText)
	if err != nil {
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
	scheduler := backupscheduler.NewForNamespaces(client, namespaces).WithRequester(requester)
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

func parseNamespaces(single, multiple string) ([]string, error) {
	if multiple == "" {
		multiple = single
	}
	seen := make(map[string]struct{})
	var namespaces []string
	for _, raw := range strings.Split(multiple, ",") {
		namespace := strings.TrimSpace(raw)
		if namespace == "" {
			return nil, errors.New("namespace allowlist contains an empty value")
		}
		if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
			return nil, errors.New("invalid namespace " + namespace + ": " + problems[0])
		}
		if _, duplicate := seen[namespace]; duplicate {
			return nil, errors.New("namespace allowlist contains duplicate " + namespace)
		}
		seen[namespace] = struct{}{}
		namespaces = append(namespaces, namespace)
	}
	return namespaces, nil
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
