// pod-log-capture preserves one non-reconnecting log stream across a fault.
// Kubernetes' log API has no UID precondition: identity is bracketed around
// opening the stream, not claimed to be atomically fenced at the kubelet.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
)

type scope struct {
	Namespace, NamespaceUID, Pod, PodUID, OwnerUID, Container string
}

type identity struct {
	NamespaceUID string `json:"namespace_uid"`
	PodUID       string `json:"pod_uid"`
	OwnerUID     string `json:"statefulset_uid"`
	Node         string `json:"node"`
	ContainerID  string `json:"container_id"`
	ImageID      string `json:"image_id"`
	RestartCount int32  `json:"restart_count"`
}

type receipt struct {
	StartedAt      time.Time `json:"started_at"`
	EndedAt        time.Time `json:"ended_at"`
	Source         identity  `json:"source"`
	OpeningChecked bool      `json:"identity_checked_around_open"`
	EndSource      *identity `json:"end_source,omitempty"`
	EndCheckError  string    `json:"end_identity_error,omitempty"`
	Reason         string    `json:"termination"`
	Bytes          int64     `json:"bytes"`
	SHA256         string    `json:"sha256,omitempty"`
	Error          string    `json:"error,omitempty"`
	// Neither EOF nor an intentional stop proves a complete log history.
	CompleteHistory bool `json:"complete_history"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("pod-log-capture", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	var s scope
	f.StringVar(&s.Namespace, "namespace", "", "expected namespace")
	f.StringVar(&s.NamespaceUID, "namespace-uid", "", "expected namespace UID")
	f.StringVar(&s.Pod, "pod", "", "exact pod name")
	f.StringVar(&s.PodUID, "pod-uid", "", "expected pod UID")
	f.StringVar(&s.OwnerUID, "statefulset-uid", "", "expected controller StatefulSet UID")
	f.StringVar(&s.Container, "container", "kubebrain", "exact container name")
	kubeconfig := f.String("kubeconfig", "", "explicit absolute kubeconfig path")
	kubeContext := f.String("context", "", "explicit kubeconfig context")
	directory := f.String("directory", "", "new evidence directory under a private non-symlink parent")
	duration := f.Duration("duration", 15*time.Minute, "maximum capture duration, up to 30m")
	maxBytes := f.Int64("max-bytes", 64<<20, "maximum stored log bytes, up to 1GiB")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || !filepath.IsAbs(*kubeconfig) || *kubeContext == "" ||
		*duration <= 0 || *duration > 30*time.Minute || *maxBytes <= 0 || *maxBytes > 1<<30 {
		return errors.New("explicit config/context and bounded duration/bytes are required")
	}
	if err := validateScope(s); err != nil {
		return err
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: *kubeContext},
	).ClientConfig()
	if err != nil {
		return fmt.Errorf("load client configuration: %w", err)
	}
	client, err := coreclient.NewForConfig(config)
	if err != nil {
		return err
	}
	if err := createDirectory(*directory); err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	return capture(bounded, client, s, *directory, *maxBytes)
}

func validateScope(s scope) error {
	if len(validation.IsDNS1123Label(s.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(s.Pod)) != 0 || len(validation.IsDNS1123Label(s.Container)) != 0 {
		return errors.New("invalid namespace, pod or container")
	}
	for _, uid := range []string{s.NamespaceUID, s.PodUID, s.OwnerUID} {
		if uid == "" || len(validation.IsDNS1123Subdomain(uid)) != 0 {
			return errors.New("explicit valid namespace, pod and StatefulSet UIDs are required")
		}
	}
	return nil
}

func createDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return errors.New("evidence directory must be a clean absolute new path")
	}
	parent := filepath.Dir(path)
	real, err := filepath.EvalSymlinks(parent)
	if err != nil || real != parent {
		return errors.New("evidence parent must exist without symlink components")
	}
	info, err := os.Stat(parent)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("evidence parent must be a private directory")
	}
	return os.Mkdir(path, 0700) // Never overwrite an existing attempt.
}

func writeJSON(path string, value any) (err error) {
	// Publish only a complete, synced receipt. Link is atomic and fails if the
	// destination already exists; Rename would silently replace another receipt.
	f, err := os.CreateTemp(filepath.Dir(path), ".receipt-*")
	if err != nil {
		return err
	}
	defer f.Close()
	defer func() { err = errors.Join(err, os.Remove(f.Name())) }()
	if err = json.NewEncoder(f).Encode(value); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Link(f.Name(), path)
}

func inspect(ctx context.Context, client coreclient.CoreV1Interface, s scope) (identity, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ns, err := client.Namespaces().Get(ctx, s.Namespace, metav1.GetOptions{})
	if err != nil {
		return identity{}, err
	}
	if string(ns.UID) != s.NamespaceUID || ns.DeletionTimestamp != nil {
		return identity{}, errors.New("namespace identity changed or terminating")
	}
	pod, err := client.Pods(s.Namespace).Get(ctx, s.Pod, metav1.GetOptions{})
	if err != nil {
		return identity{}, err
	}
	if string(pod.UID) != s.PodUID || pod.DeletionTimestamp != nil {
		return identity{}, errors.New("pod identity changed or terminating")
	}
	owned := false
	for _, owner := range pod.OwnerReferences {
		if owner.APIVersion == "apps/v1" && owner.Kind == "StatefulSet" && string(owner.UID) == s.OwnerUID && owner.Controller != nil && *owner.Controller {
			owned = true
		}
	}
	if !owned {
		return identity{}, errors.New("pod controller identity mismatch")
	}
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name == s.Container && container.State.Running != nil && container.ContainerID != "" && container.ImageID != "" && pod.Spec.NodeName != "" {
			return identity{s.NamespaceUID, s.PodUID, s.OwnerUID, pod.Spec.NodeName, container.ContainerID, container.ImageID, container.RestartCount}, nil
		}
	}
	return identity{}, errors.New("expected running container has no runtime identity")
}

func capture(ctx context.Context, client coreclient.CoreV1Interface, s scope, directory string, maxBytes int64) (retErr error) {
	r := receipt{StartedAt: time.Now().UTC(), Reason: "setup_error"}
	defer func() {
		r.EndedAt = time.Now().UTC()
		if retErr != nil {
			r.Error = retErr.Error()
		}
		retErr = errors.Join(retErr, writeJSON(filepath.Join(directory, "result.json"), r))
	}()
	before, err := inspect(ctx, client, s)
	if err != nil {
		return err
	}
	r.Source = before
	if err := writeJSON(filepath.Join(directory, "before.json"), before); err != nil {
		return err
	}
	lookback := int64(120)
	stream, err := client.Pods(s.Namespace).GetLogs(s.Pod, &corev1.PodLogOptions{
		Container: s.Container, Follow: true, Timestamps: true, SinceSeconds: &lookback,
	}).Stream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	after, err := inspect(ctx, client, s)
	if err != nil {
		return err
	}
	if before != after {
		return errors.New("container identity changed while opening log stream")
	}
	r.OpeningChecked = true
	f, err := os.OpenFile(filepath.Join(directory, "container.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	// Fault injectors must also check that this capture process is still live.
	if err := writeJSON(filepath.Join(directory, "ready.json"), struct {
		At     time.Time `json:"at"`
		Source identity  `json:"source"`
	}{time.Now().UTC(), before}); err != nil {
		return errors.Join(err, f.Close())
	}
	hash := sha256.New()
	r.Bytes, err = io.Copy(io.MultiWriter(f, hash), io.LimitReader(stream, maxBytes))
	retErr = errors.Join(f.Sync(), f.Close())
	r.SHA256 = hex.EncodeToString(hash.Sum(nil))
	switch {
	case r.Bytes >= maxBytes:
		r.Reason = "byte_limit"
		retErr = errors.Join(retErr, errors.New("log byte limit reached; capture incomplete"))
	case ctx.Err() != nil:
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			r.Reason = "deadline"
			retErr = errors.Join(retErr, ctx.Err())
		} else {
			r.Reason = "cancelled" // Explicit operator stop; not a completeness claim.
			if err != nil && !errors.Is(err, context.Canceled) {
				retErr = errors.Join(retErr, err)
			}
		}
	case err != nil:
		r.Reason = "read_error"
		retErr = errors.Join(retErr, err)
	default:
		r.Reason = "eof"
	}
	// End identity is diagnostic: deletion/replacement after admission is expected.
	end, endErr := inspect(context.Background(), client, s)
	if endErr != nil {
		r.EndCheckError = endErr.Error()
	} else {
		r.EndSource = &end
	}
	return retErr
}
