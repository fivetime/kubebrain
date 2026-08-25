// Command jwt-rotation-publisher reconciles one audited JWT rollout phase.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/jwtrotationpublisher"
)

const maxKubectlOutput = 2 << 20

type boundedBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	remaining := maxKubectlOutput - b.Len()
	if remaining <= 0 {
		b.overflow = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.Buffer.Write(p[:remaining])
		b.overflow = true
		return len(p), nil
	}
	return b.Buffer.Write(p)
}

type kubectlClient struct {
	path, context, kubeconfig string
}

func (k kubectlClient) args(namespace string, rest ...string) []string {
	args := []string{}
	if k.context != "" {
		args = append(args, "--context", k.context)
	}
	if k.kubeconfig != "" {
		args = append(args, "--kubeconfig", k.kubeconfig)
	}
	args = append(args, "-n", namespace)
	return append(args, rest...)
}

func (k kubectlClient) command(ctx context.Context, input []byte, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, k.path, args...)
	command.Stdin = bytes.NewReader(input)
	var stdout, stderr boundedBuffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if stdout.overflow || stderr.overflow {
		return nil, errors.New("kubectl response exceeds 2 MiB")
	}
	if err != nil {
		return nil, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func (k kubectlClient) Get(ctx context.Context, namespace, resource, name string) ([]byte, bool, error) {
	data, err := k.command(ctx, nil, k.args(namespace, "get", resource, name, "-o", "json")...)
	if err != nil && strings.Contains(err.Error(), "NotFound") {
		return nil, false, nil
	}
	return data, err == nil, err
}

func (k kubectlClient) Create(ctx context.Context, namespace string, object []byte) error {
	_, err := k.command(ctx, object, k.args(namespace, "create", "-f", "-")...)
	return err
}

func (k kubectlClient) Patch(ctx context.Context, namespace, resource, name string, patch []byte) error {
	_, err := k.command(ctx, nil, k.args(namespace, "patch", resource, name, "--type=json", "-p", string(patch))...)
	return err
}

func main() {
	var o jwtrotationpublisher.Options
	var kubectlPath, kubeContext, kubeconfig string
	flag.StringVar(&o.Phase, "phase", "", "phase-a, phase-b, or phase-c")
	flag.StringVar(&o.OperationID, "operation-id", "", "stable rotation operation ID")
	flag.StringVar(&o.Instance, "instance", "", "instance identity")
	flag.StringVar(&o.Namespace, "namespace", "", "KubeBrain namespace")
	flag.StringVar(&o.StatefulSet, "statefulset", "kubebrain", "KubeBrain StatefulSet")
	flag.StringVar(&o.Secret, "key-secret", "", "immutable dual-key Secret")
	flag.StringVar(&o.OldKeyField, "old-key-field", "old-key", "old key Secret field")
	flag.StringVar(&o.NewKeyField, "new-key-field", "new-key", "new key Secret field")
	flag.StringVar(&o.OldKeySource, "old-key-source", "", "approved 0600 old key source")
	flag.StringVar(&o.NewKeySource, "new-key-source", "", "approved 0600 new key source")
	flag.StringVar(&o.OldKeySHA256, "old-key-sha256", "", "approved old key SHA-256")
	flag.StringVar(&o.NewKeySHA256, "new-key-sha256", "", "approved new key SHA-256")
	flag.StringVar(&o.KeyVolume, "key-volume", "jwt-keys", "existing Secret volume name")
	flag.StringVar(&o.KeyMountDir, "key-mount-dir", "/etc/kubebrain-jwt", "existing Secret mount directory")
	flag.StringVar(&o.SignMethod, "sign-method", "", "approved JWT signing method")
	flag.Int64Var(&o.ExpectedReplicas, "expected-replicas", 3, "expected StatefulSet replicas")
	flag.Int64Var(&o.JWTTokenTTL, "jwt-ttl-seconds", 0, "approved JWT TTL")
	flag.StringVar(&o.PreviousReceipt, "previous-receipt", "", "previous phase publish receipt")
	flag.StringVar(&o.ReceiptOutput, "receipt-output", "", "new no-clobber publish receipt")
	flag.DurationVar(&o.PollInterval, "poll-interval", 2*time.Second, "rollout poll interval")
	flag.DurationVar(&o.RolloutTimeout, "rollout-timeout", 10*time.Minute, "rollout timeout")
	flag.StringVar(&kubectlPath, "kubectl", "kubectl", "kubectl executable")
	flag.StringVar(&kubeContext, "kube-context", "", "explicit Kubernetes context")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "explicit kubeconfig")
	flag.Parse()

	path, err := exec.LookPath(kubectlPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "JWT rotation publisher: kubectl is required")
		os.Exit(2)
	}
	receipt, err := jwtrotationpublisher.Run(context.Background(), kubectlClient{path: path, context: kubeContext, kubeconfig: kubeconfig}, o, time.Now)
	if err != nil {
		fmt.Fprintln(os.Stderr, "JWT rotation publisher:", err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(receipt); err != nil {
		fmt.Fprintln(os.Stderr, "JWT rotation publisher: encode receipt:", err)
		os.Exit(1)
	}
}
