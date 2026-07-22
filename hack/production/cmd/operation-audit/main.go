package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditbuilder"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditrelease"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var inClusterConfig = rest.InClusterConfig

func main() {
	var action, namespace, name, output, receipt, kubeconfig, contextName string
	var objectStoreID, bucket, objectKey, retentionMode string
	var retainUntilUnix int64
	flag.StringVar(&action, "action", "capture", "capture or release")
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "operation namespace")
	flag.StringVar(&name, "name", "", "terminal operation name")
	flag.StringVar(&output, "output", "", "immutable audit artifact output")
	flag.StringVar(&receipt, "receipt", "", "verified Object Lock archive receipt")
	flag.StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "kubeconfig path")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.StringVar(&objectStoreID, "object-store-id", "", "expected archive receipt object store ID for release")
	flag.StringVar(&bucket, "bucket", "", "expected archive receipt bucket for release")
	flag.StringVar(&objectKey, "object-key", "", "expected archive receipt object key for release")
	flag.StringVar(&retentionMode, "retention-mode", "", "expected archive receipt retention mode for release")
	flag.Int64Var(&retainUntilUnix, "retain-until-unix", 0, "expected archive receipt retain-until Unix seconds for release")
	flag.Parse()
	if name == "" || output == "" || (action == "release" && receipt == "") {
		log.Fatal("name/output and release receipt are required")
	}
	if action == "release" && (objectStoreID == "" || bucket == "" || objectKey == "" ||
		retentionMode == "" || retainUntilUnix <= 0) {
		log.Fatal("release object-store-id/bucket/object-key/retention-mode/retain-until-unix are required")
	}

	config, err := clientConfig(kubeconfig, contextName)
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch action {
	case "capture":
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
	case "release":
		if _, err := operationauditrelease.ReleaseWithExpectedReceipt(
			ctx, client, namespace, name, output, receipt,
			operationauditrelease.ExpectedArchiveReceipt{
				ObjectStoreID: objectStoreID, Bucket: bucket, ObjectKey: objectKey,
				RetentionMode: retentionMode, RetainUntilUnix: retainUntilUnix,
			},
		); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("action must be capture or release")
	}
}

func defaultKubeconfig() string {
	return os.Getenv("KUBECONFIG")
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

func init() {
	log.SetFlags(0)
}
