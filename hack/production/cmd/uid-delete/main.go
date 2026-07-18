package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		apiVersion  string
		resource    string
		namespace   string
		name        string
		uid         string
		kubeconfig  string
		kubeContext string
		timeout     time.Duration
	)
	flag.StringVar(&apiVersion, "api-version", "", "resource API version, for example apps/v1")
	flag.StringVar(&resource, "resource", "", "resource plural, for example statefulsets")
	flag.StringVar(&namespace, "namespace", "", "resource namespace")
	flag.StringVar(&name, "name", "", "resource name")
	flag.StringVar(&uid, "uid", "", "required object UID precondition")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig; defaults to standard loading rules")
	flag.StringVar(&kubeContext, "context", "", "kubeconfig context override")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "API request timeout")
	flag.Parse()

	if apiVersion == "" || resource == "" || namespace == "" || name == "" || uid == "" {
		log.Fatal("--api-version, --resource, --namespace, --name, and --uid are required")
	}
	if timeout <= 0 {
		log.Fatal("--timeout must be positive")
	}
	groupVersion, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		log.Fatalf("parse API version: %v", err)
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeContext}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		log.Fatalf("load Kubernetes client config: %v", err)
	}
	client, err := dynamicClient(config)
	if err != nil {
		log.Fatalf("create Kubernetes client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err = deleteWithUID(ctx, client, groupVersion.String(), resource, namespace, name, uid)
	switch {
	case err == nil:
		fmt.Printf("delete accepted: %s %s/%s uid=%s\n", resource, namespace, name, uid)
	case apierrors.IsNotFound(err):
		fmt.Printf("already absent: %s %s/%s\n", resource, namespace, name)
	case apierrors.IsConflict(err):
		log.Fatalf("UID precondition failed for %s %s/%s: %v", resource, namespace, name, err)
	case errors.Is(err, context.DeadlineExceeded):
		log.Fatalf("delete request timed out for %s %s/%s: %v", resource, namespace, name, err)
	default:
		log.Fatalf("delete %s %s/%s: %v", resource, namespace, name, err)
	}
}

func dynamicClient(config *rest.Config) (dynamic.Interface, error) {
	return dynamic.NewForConfig(config)
}

func deleteWithUID(
	ctx context.Context,
	client dynamic.Interface,
	apiVersion, resource, namespace, name, uid string,
) error {
	groupVersion, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return fmt.Errorf("parse API version: %w", err)
	}
	propagation := metav1.DeletePropagationForeground
	expectedUID := types.UID(uid)
	return client.Resource(groupVersion.WithResource(resource)).Namespace(namespace).Delete(
		ctx,
		name,
		metav1.DeleteOptions{
			PropagationPolicy: &propagation,
			Preconditions:     &metav1.Preconditions{UID: &expectedUID},
		},
	)
}

func init() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
}
