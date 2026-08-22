package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationarchiveverifier"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditbuilder"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditrelease"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationretention"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var inClusterConfig = rest.InClusterConfig

func main() {
	var action, namespace, name, output, receipt, kubeconfig, contextName string
	var objectStoreID, bucket, objectKey, objectPrefix, retentionMode string
	var expectedUID, expectedResourceVersion, verifyExecutor string
	var retainUntilUnix int64
	var retentionDuration, deleteAfter, timeout time.Duration
	flag.StringVar(&action, "action", "capture", "capture, release, or reap")
	flag.StringVar(&namespace, "namespace", "kubebrain-operations", "operation namespace")
	flag.StringVar(&name, "name", "", "terminal operation name")
	flag.StringVar(&output, "output", "", "immutable audit artifact output")
	flag.StringVar(&receipt, "receipt", "", "verified Object Lock archive receipt")
	flag.StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "kubeconfig path")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.StringVar(&objectStoreID, "object-store-id", "", "expected archive receipt object store ID for release")
	flag.StringVar(&bucket, "bucket", "", "expected archive receipt bucket for release")
	flag.StringVar(&objectKey, "object-key", "", "expected archive receipt object key for release")
	flag.StringVar(&objectPrefix, "object-prefix", "operation-audit", "archive object key prefix for reap")
	flag.StringVar(&retentionMode, "retention-mode", "", "expected archive receipt retention mode for release")
	flag.Int64Var(&retainUntilUnix, "retain-until-unix", 0, "expected archive receipt retain-until Unix seconds for release")
	flag.StringVar(&expectedUID, "expected-uid", "", "exact operation UID required for reap")
	flag.StringVar(&expectedResourceVersion, "expected-resource-version", "", "exact operation resourceVersion required for reap")
	flag.StringVar(&verifyExecutor, "verify-executor", "/usr/local/bin/kubebrain-logical-object", "read-only Object Lock verifier for reap")
	flag.DurationVar(&retentionDuration, "retention-duration", 8760*time.Hour, "archive retention from terminal completion for reap")
	flag.DurationVar(&deleteAfter, "delete-after", 720*time.Hour, "minimum terminal operation age for reap")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "overall Kubernetes and Object Lock operation deadline")
	flag.Parse()
	if name == "" || timeout <= 0 || ((action == "capture" || action == "release") && output == "") ||
		(action == "release" && receipt == "") {
		log.Fatal("name, positive timeout, capture/release output, and release receipt are required")
	}
	if err := namespaceinventory.ValidateOne(namespace); err != nil {
		log.Fatal("--namespace: ", err)
	}
	if action == "release" && (objectStoreID == "" || bucket == "" || objectKey == "" ||
		retentionMode == "" || retainUntilUnix <= 0) {
		log.Fatal("release object-store-id/bucket/object-key/retention-mode/retain-until-unix are required")
	}
	if action == "reap" {
		if objectStoreID == "" || bucket == "" || expectedUID == "" || expectedResourceVersion == "" ||
			(retentionMode != "COMPLIANCE" && retentionMode != "GOVERNANCE") || retentionDuration <= 0 ||
			deleteAfter <= 0 || deleteAfter >= retentionDuration {
			log.Fatal("reap object-store scope, exact UID/resourceVersion, retention mode, and 0 < delete-after < retention-duration are required")
		}
		if err := processgroup.ValidateExecutable(verifyExecutor); err != nil {
			log.Fatal(err)
		}
	}

	config, err := clientConfig(kubeconfig, contextName)
	if err != nil {
		log.Fatal(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
	case "reap":
		verifier, err := operationarchiveverifier.NewProcessor(
			verifyExecutor, objectStoreID, bucket, objectPrefix, retentionMode, retentionDuration,
		)
		if err != nil {
			log.Fatal(err)
		}
		if err := operationretention.ReapOne(
			ctx, client, verifier, namespace, name, types.UID(expectedUID), expectedResourceVersion,
			deleteAfter, time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("action must be capture, release, or reap")
	}
}

func defaultKubeconfig() string {
	return ""
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
