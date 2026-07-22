package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/namespaceinventory"
	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const maxBrokerParametersBytes = 4 << 20
const maxBrokerTokenBytes = 16 << 10
const maxBrokerCABytes = 1 << 20

func main() {
	var action, namespace, name, operationID, tenant, requestedBy, instance, operationType, parametersSHA string
	var parametersSecret, parametersKey string
	var parametersEndpoint, parametersTokenFile, parametersCAFile string
	var inventoryName, inventoryNamespace, inventoryKey string
	var owner, receiptSHA, message, approvalID, approvedBy, kubeconfig, contextName string
	var maxAttempts, attempt int64
	var lease time.Duration
	flag.StringVar(
		&action, "action", "",
		"submit, claim, parameters, heartbeat, retry, succeed, fail, get, or approve",
	)
	flag.StringVar(&namespace, "namespace", "kubebrain-system", "operation namespace")
	flag.StringVar(&inventoryName, "namespace-inventory-configmap",
		os.Getenv("OPERATION_NAMESPACE_INVENTORY_CONFIGMAP"),
		"ConfigMap used for a cross-namespace claim")
	flag.StringVar(&inventoryNamespace, "namespace-inventory-namespace",
		envOrDefault("OPERATION_NAMESPACE_INVENTORY_NAMESPACE", "kubebrain-operations"),
		"namespace containing --namespace-inventory-configmap")
	flag.StringVar(&inventoryKey, "namespace-inventory-key",
		envOrDefault("OPERATION_NAMESPACE_INVENTORY_KEY", namespaceinventory.DefaultKey),
		"ConfigMap data key containing a JSON namespace array")
	flag.StringVar(&name, "name", "", "operation resource name")
	flag.StringVar(&operationID, "operation-id", "", "stable external operation ID")
	flag.StringVar(&tenant, "tenant", "", "tenant ID recorded in the immutable operation spec")
	flag.StringVar(&requestedBy, "requested-by", "", "requester identity recorded in the immutable operation spec")
	flag.StringVar(&instance, "instance", "", "instance ID")
	flag.StringVar(&operationType, "type", "", "operation type or claim filter")
	flag.StringVar(&parametersSHA, "parameters-sha256", "", "immutable parameters digest")
	flag.StringVar(&parametersSecret, "parameters-secret", "", "immutable parameters Secret name")
	flag.StringVar(&parametersKey, "parameters-key", "", "immutable parameters Secret key")
	flag.StringVar(&parametersEndpoint, "parameters-endpoint",
		os.Getenv("OPERATION_PARAMETERS_ENDPOINT"), "HTTPS operation parameter broker endpoint")
	flag.StringVar(&parametersTokenFile, "parameters-token-file",
		envOrDefault("OPERATION_PARAMETERS_TOKEN_FILE", "/var/run/secrets/kubebrain-parameter/token"),
		"projected service account token used by the parameter broker")
	flag.StringVar(&parametersCAFile, "parameters-ca-file",
		envOrDefault("OPERATION_PARAMETERS_CA_FILE", "/var/run/secrets/kubebrain-parameter-ca/ca.crt"),
		"parameter broker CA bundle")
	flag.Int64Var(&maxAttempts, "max-attempts", 3, "maximum worker claims")
	flag.StringVar(&owner, "owner", "", "worker identity")
	flag.Int64Var(&attempt, "attempt", 0, "worker fencing attempt")
	flag.DurationVar(&lease, "lease", 2*time.Minute, "worker claim lease")
	flag.StringVar(&receiptSHA, "receipt-sha256", "", "successful operation receipt digest")
	flag.StringVar(&message, "message", "", "terminal status message")
	flag.StringVar(&approvalID, "approval-id", "", "external approval decision ID")
	flag.StringVar(&approvedBy, "approved-by", operationaudit.ApproverUsername, "approver Kubernetes username")
	flag.StringVar(&kubeconfig, "kubeconfig", defaultKubeconfig(), "kubeconfig path")
	flag.StringVar(&contextName, "context", "", "kubeconfig context")
	flag.Parse()

	config, err := clientConfig(kubeconfig, contextName)
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
			OperationID: operationID, Tenant: tenant, RequestedBy: requestedBy,
			Instance: instance, Type: operationType,
			ParametersSHA256: parametersSHA, ParametersSecret: parametersSecret,
			ParametersKey: parametersKey, MaxAttempts: maxAttempts,
		})
	case "claim":
		if inventoryName == "" {
			output, err = queue.Claim(ctx, owner, operationType, lease)
		} else {
			var namespaces []string
			namespaces, err = namespaceinventory.Load(
				ctx, client, inventoryNamespace, inventoryName, inventoryKey,
			)
			if err == nil {
				output, err = operationqueue.ClaimAcrossNamespaces(
					ctx, client, namespaces, owner, operationType, lease,
				)
			}
		}
	case "parameters":
		var data []byte
		if parametersEndpoint != "" {
			data, err = brokerParameters(
				ctx, parametersEndpoint, parametersTokenFile, parametersCAFile,
				namespace, name, owner, attempt,
			)
		} else {
			data, err = queue.Parameters(ctx, name)
		}
		if err == nil {
			if _, writeErr := os.Stdout.Write(data); writeErr != nil {
				log.Fatal(writeErr)
			}
			return
		}
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
	case "approve":
		output, err = queue.Approve(ctx, name, approvedBy, approvalID)
	default:
		log.Fatal("action must be submit, claim, parameters, heartbeat, retry, succeed, fail, get, or approve")
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
	return os.Getenv("KUBECONFIG")
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

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func brokerParameters(
	ctx context.Context, endpoint, tokenFile, caFile, namespace, name, owner string, attempt int64,
) ([]byte, error) {
	if name == "" || owner == "" || attempt <= 0 {
		return nil, errors.New("broker parameters require name, owner, and positive attempt")
	}
	base, err := url.Parse(endpoint)
	if err != nil || base.Scheme != "https" || base.Host == "" ||
		base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("parameters endpoint must be an HTTPS origin")
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + "/v1/parameters"
	query := base.Query()
	query.Set("namespace", namespace)
	query.Set("name", name)
	query.Set("owner", owner)
	query.Set("attempt", strconv.FormatInt(attempt, 10))
	base.RawQuery = query.Encode()
	token, err := readBrokerToken(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read parameter broker token: %w", err)
	}
	ca, err := readBrokerCA(caFile)
	if err != nil {
		return nil, fmt.Errorf("read parameter broker CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("parameter broker CA contains no certificates")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, RootCAs: roots,
		}},
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("parameter broker request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("parameter broker returned HTTP %d", response.StatusCode)
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		return nil, errors.New("parameter broker returned non-JSON response")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBrokerParametersBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBrokerParametersBytes {
		return nil, fmt.Errorf("parameter broker response exceeds %d bytes", maxBrokerParametersBytes)
	}
	return body, nil
}

func readBrokerCA(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxBrokerCABytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxBrokerCABytes {
		return nil, fmt.Errorf("CA exceeds %d bytes", maxBrokerCABytes)
	}
	return data, nil
}

func readBrokerToken(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxBrokerTokenBytes+2))
	if err != nil {
		return "", err
	}
	if len(data) > maxBrokerTokenBytes+1 {
		return "", fmt.Errorf("token exceeds %d bytes", maxBrokerTokenBytes)
	}
	token := strings.TrimSpace(string(data))
	if token == "" || len(token) > maxBrokerTokenBytes {
		return "", errors.New("token is empty or too large")
	}
	return token, nil
}
