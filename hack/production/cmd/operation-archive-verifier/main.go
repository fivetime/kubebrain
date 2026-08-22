package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	iamValidUntil, err := parseIAMSimulationExpiry(os.Getenv("IAM_SIMULATION_VALID_UNTIL_UNIX"), time.Now())
	if err != nil {
		log.Fatal(err)
	}
	if err := validateCredentialSecretDataDigest(os.Getenv("CREDENTIAL_SECRET_DATA_SHA256"), os.Getenv); err != nil {
		log.Fatal(err)
	}
	if inventoryName == "" || objectStoreID == "" || bucket == "" || maxBatch <= 0 {
		log.Fatal("namespace-inventory-configmap, object-store-id, bucket, and positive max-batch are required")
	}
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
	effectiveReconcileTimeout, err := evidenceBoundReconcileTimeout(reconcileTimeout, iamValidUntil, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	verified, err := reconcilebudget.Run(ctx, effectiveReconcileTimeout, controller.Reconcile)
	if err != nil {
		log.Printf("archive evidence verification failed after verifying %d operations: %v", verified, err)
		os.Exit(1)
	}
	log.Printf("verified %d released terminal operation archives", verified)
}

func validateCredentialSecretDataDigest(expected string, getenv func(string) string) error {
	decoded, err := hex.DecodeString(expected)
	if err != nil || len(decoded) != sha256.Size || expected != fmt.Sprintf("%x", decoded) {
		return fmt.Errorf("CREDENTIAL_SECRET_DATA_SHA256 must be 64 lowercase hexadecimal characters")
	}
	fields := map[string]string{
		"access-key-id":     "AWS_ACCESS_KEY_ID",
		"bucket":            "S3_BUCKET",
		"endpoint":          "S3_ENDPOINT",
		"force-path-style":  "S3_FORCE_PATH_STYLE",
		"object-store-id":   "OBJECT_STORE_ID",
		"region":            "AWS_REGION",
		"secret-access-key": "AWS_SECRET_ACCESS_KEY",
	}
	data := make(map[string]string, len(fields))
	for key, env := range fields {
		value := getenv(env)
		if value == "" {
			return fmt.Errorf("credential environment %s is empty", env)
		}
		data[key] = base64.StdEncoding.EncodeToString([]byte(value))
	}
	canonical, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("canonicalize credential Secret data: %w", err)
	}
	actual := sha256.Sum256(canonical)
	if subtle.ConstantTimeCompare(decoded, actual[:]) != 1 {
		return fmt.Errorf("resolved credential Secret data does not match its approved SHA-256 binding")
	}
	return nil
}

func parseIAMSimulationExpiry(raw string, now time.Time) (time.Time, error) {
	validUntil, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || validUntil <= 0 {
		return time.Time{}, fmt.Errorf("IAM_SIMULATION_VALID_UNTIL_UNIX must be a positive Unix timestamp")
	}
	if now.Unix() >= validUntil {
		return time.Time{}, fmt.Errorf("IAM simulation evidence expired at Unix timestamp %d", validUntil)
	}
	if validUntil > now.Add(24*time.Hour+5*time.Minute).Unix() {
		return time.Time{}, fmt.Errorf("IAM simulation evidence expiry exceeds the 24-hour window plus clock-skew allowance")
	}
	return time.Unix(validUntil, 0), nil
}

func evidenceBoundReconcileTimeout(configured time.Duration, validUntil, now time.Time) (time.Duration, error) {
	if configured <= 0 {
		return 0, fmt.Errorf("reconcile timeout must be positive")
	}
	remaining := validUntil.Sub(now)
	if remaining <= 0 {
		return 0, fmt.Errorf("IAM simulation evidence expired before reconciliation")
	}
	if remaining < configured {
		return remaining, nil
	}
	return configured, nil
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
