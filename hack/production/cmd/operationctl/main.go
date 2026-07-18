package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var action, namespace, name, operationID, instance, operationType, parametersSHA string
	var owner, receiptSHA, message, kubeconfig, contextName string
	var maxAttempts, attempt int64
	var lease time.Duration
	flag.StringVar(&action, "action", "", "submit, claim, heartbeat, retry, succeed, fail, or get")
	flag.StringVar(&namespace, "namespace", "kubebrain-system", "operation namespace")
	flag.StringVar(&name, "name", "", "operation resource name")
	flag.StringVar(&operationID, "operation-id", "", "stable external operation ID")
	flag.StringVar(&instance, "instance", "", "instance ID")
	flag.StringVar(&operationType, "type", "", "operation type or claim filter")
	flag.StringVar(&parametersSHA, "parameters-sha256", "", "immutable parameters digest")
	flag.Int64Var(&maxAttempts, "max-attempts", 3, "maximum worker claims")
	flag.StringVar(&owner, "owner", "", "worker identity")
	flag.Int64Var(&attempt, "attempt", 0, "worker fencing attempt")
	flag.DurationVar(&lease, "lease", 2*time.Minute, "worker claim lease")
	flag.StringVar(&receiptSHA, "receipt-sha256", "", "successful operation receipt digest")
	flag.StringVar(&message, "message", "", "terminal status message")
	flag.StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "kubeconfig path")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.Parse()

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
	queue := operationqueue.New(client, namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var output any
	switch action {
	case "submit":
		output, err = queue.Submit(ctx, name, operationqueue.Spec{
			OperationID: operationID, Instance: instance, Type: operationType,
			ParametersSHA256: parametersSHA, MaxAttempts: maxAttempts,
		})
	case "claim":
		output, err = queue.Claim(ctx, owner, operationType, lease)
	case "heartbeat":
		output, err = queue.Heartbeat(ctx, name, owner, attempt, lease)
	case "retry":
		output, err = queue.Requeue(ctx, name, owner, attempt, message)
	case "succeed":
		output, err = queue.Finish(ctx, name, owner, attempt, true, receiptSHA, message)
	case "fail":
		output, err = queue.Finish(ctx, name, owner, attempt, false, "", message)
	case "get":
		output, err = queue.Get(ctx, name)
	default:
		log.Fatal("action must be submit, claim, heartbeat, retry, succeed, fail, or get")
	}
	if err != nil {
		if errors.Is(err, operationqueue.ErrNoOperation) {
			os.Exit(3)
		}
		log.Fatal(err)
	}
	if object, ok := output.(*unstructured.Unstructured); ok {
		output = object.Object
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(output); err != nil {
		log.Fatal(fmt.Errorf("encode result: %w", err))
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
