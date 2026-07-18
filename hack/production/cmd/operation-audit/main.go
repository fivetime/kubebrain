package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditbuilder"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var namespace, name, output, kubeconfig, contextName string
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "operation namespace")
	flag.StringVar(&name, "name", "", "terminal operation name")
	flag.StringVar(&output, "output", "", "immutable audit artifact output")
	flag.StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "kubeconfig path")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.Parse()
	if name == "" || output == "" {
		log.Fatal("name and output are required")
	}

	loading := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	object, err := operationqueue.New(client, namespace).Get(ctx, name)
	if err != nil {
		log.Fatal(err)
	}
	artifact, err := operationauditbuilder.FromOperation(object)
	if err != nil {
		log.Fatal(err)
	}
	if err := operationaudit.WriteAtomic(output, artifact); err != nil {
		log.Fatal(err)
	}
}

func defaultKubeconfig() string {
	if value := os.Getenv("KUBECONFIG"); value != "" {
		return value
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".kube", "config")
	}
	return ""
}

func init() {
	log.SetFlags(0)
}
