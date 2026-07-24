package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var inClusterConfig = rest.InClusterConfig

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

	if timeout <= 0 {
		log.Fatal("--timeout must be positive")
	}
	groupVersion, err := validateDeleteRequest(apiVersion, resource, namespace, name, uid)
	if err != nil {
		log.Fatal(err)
	}

	config, err := clientConfig(kubeconfig, kubeContext)
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

func clientConfig(kubeconfig, kubeContext string) (*rest.Config, error) {
	if kubeconfig == "" {
		if config, err := inClusterConfig(); err == nil {
			return config, nil
		}
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeContext}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
}

func deleteWithUID(
	ctx context.Context,
	client dynamic.Interface,
	apiVersion, resource, namespace, name, uid string,
) error {
	groupVersion, err := validateDeleteRequest(apiVersion, resource, namespace, name, uid)
	if err != nil {
		return err
	}
	propagation := metav1.DeletePropagationForeground
	expectedUID := types.UID(uid)
	resourceClient := client.Resource(groupVersion.WithResource(resource))
	var target dynamic.ResourceInterface
	if namespace == "" {
		target = resourceClient
	} else {
		target = resourceClient.Namespace(namespace)
	}
	return target.Delete(
		ctx,
		name,
		metav1.DeleteOptions{
			PropagationPolicy: &propagation,
			Preconditions:     &metav1.Preconditions{UID: &expectedUID},
		},
	)
}

func validateDeleteRequest(
	apiVersion, resource, namespace, name, uid string,
) (schema.GroupVersion, error) {
	if apiVersion == "" || resource == "" || name == "" {
		return schema.GroupVersion{}, errors.New("--api-version, --resource, and --name are required")
	}
	if uid == "" {
		return schema.GroupVersion{}, errors.New("UID precondition is required")
	}
	groupVersion, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersion{}, fmt.Errorf("parse API version: %w", err)
	}
	if groupVersion.Group != "" {
		if problems := validation.IsDNS1123Subdomain(groupVersion.Group); len(problems) != 0 {
			return schema.GroupVersion{}, errors.New("invalid API group " + groupVersion.Group + ": " + problems[0])
		}
	}
	if problems := validation.IsDNS1123Label(groupVersion.Version); len(problems) != 0 {
		return schema.GroupVersion{}, errors.New("invalid API version " + groupVersion.Version + ": " + problems[0])
	}
	if problems := validation.IsDNS1123Label(resource); len(problems) != 0 {
		return schema.GroupVersion{}, errors.New("invalid resource " + resource + ": " + problems[0])
	}
	if namespace != "" {
		if err := namespaceinventory.ValidateOne(namespace); err != nil {
			return schema.GroupVersion{}, err
		}
	}
	if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
		return schema.GroupVersion{}, errors.New("invalid resource name " + name + ": " + problems[0])
	}
	return groupVersion, nil
}

func init() {
	log.SetFlags(0)
	log.SetOutput(os.Stderr)
}
