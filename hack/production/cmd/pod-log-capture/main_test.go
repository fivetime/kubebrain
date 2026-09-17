package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

var testScope = scope{"test", "namespace-uid", "brain-0", "pod-uid", "owner-uid", "kubebrain"}

func fakeSource(t *testing.T, drift string, mode string) (coreclient.CoreV1Interface, *atomic.Int32) {
	t.Helper()
	var opened atomic.Int32
	var podGets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected mutation: %s %s", r.Method, r.URL)
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		changed := opened.Load() > 0 || strings.HasPrefix(drift, "before-")
		switch r.URL.Path {
		case "/api/v1/namespaces/test":
			uid := testScope.NamespaceUID
			if changed && strings.Contains(drift, "namespace") {
				uid = "replacement-namespace"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test", UID: types.UID(uid)}})
		case "/api/v1/namespaces/test/pods/brain-0":
			reads := podGets.Add(1)
			controller := true
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "brain-0", UID: types.UID(testScope.PodUID), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", UID: types.UID(testScope.OwnerUID), Controller: &controller}}},
				Spec:       corev1.PodSpec{NodeName: "worker1"},
				Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "kubebrain", ContainerID: "cri-o://original", ImageID: "image@sha256:original", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
			}
			if changed {
				switch strings.TrimPrefix(drift, "before-") {
				case "pod":
					pod.UID = "replacement-pod"
				case "owner":
					pod.OwnerReferences[0].UID = "replacement-owner"
				case "container":
					pod.Status.ContainerStatuses[0].ContainerID = "cri-o://replacement"
				case "restart":
					pod.Status.ContainerStatuses[0].RestartCount++
				case "image":
					pod.Status.ContainerStatuses[0].ImageID = "image@sha256:replacement"
				case "terminating":
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				case "missing-runtime":
					pod.Status.ContainerStatuses[0].ContainerID = ""
				}
			}
			if drift == "end-pod" && reads >= 3 {
				pod.UID = "replacement-pod"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pod)
		case "/api/v1/namespaces/test/pods/brain-0/log":
			opened.Add(1)
			q := r.URL.Query()
			if q.Get("follow") != "true" || q.Get("timestamps") != "true" || q.Get("container") != "kubebrain" || q.Get("sinceSeconds") != "120" || q.Get("insecureSkipTLSVerifyBackend") == "true" {
				t.Errorf("unexpected log options: %s", r.URL.RawQuery)
			}
			w.Header().Set("Content-Type", "text/plain")
			if mode == "truncated" {
				w.Header().Set("Content-Length", "1000")
			}
			_, _ = fmt.Fprint(w, "2026-09-17T01:00:00Z log line\n")
			w.(http.Flusher).Flush()
			if mode == "follow" {
				<-r.Context().Done()
			}
		default:
			t.Errorf("unexpected resource: %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := coreclient.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	return client, &opened
}

func loadReceipt(t *testing.T, directory string) receipt {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, "result.json"))
	require.NoError(t, err)
	var r receipt
	require.NoError(t, json.Unmarshal(data, &r))
	return r
}

func TestCaptureStableEOFHasPrivateHashedEvidence(t *testing.T) {
	client, calls := fakeSource(t, "", "eof")
	dir := t.TempDir()
	require.NoError(t, capture(t.Context(), client, testScope, dir, 4096))
	r := loadReceipt(t, dir)
	require.Equal(t, "eof", r.Reason)
	require.True(t, r.OpeningChecked)
	require.False(t, r.CompleteHistory)
	require.NotNil(t, r.EndSource)
	require.Equal(t, r.Source, *r.EndSource)
	require.Equal(t, int32(1), calls.Load())
	data, err := os.ReadFile(filepath.Join(dir, "container.log"))
	require.NoError(t, err)
	h := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(h[:]), r.SHA256)
	require.Equal(t, int64(len(data)), r.Bytes)
	for _, name := range []string{"before.json", "ready.json", "container.log", "result.json"} {
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
}

func TestCaptureRejectsIdentityDriftWithoutReadyMarker(t *testing.T) {
	for _, drift := range []string{"before-namespace", "before-pod", "before-owner", "before-terminating", "before-missing-runtime", "namespace", "pod", "owner", "container", "restart", "image", "terminating"} {
		t.Run(drift, func(t *testing.T) {
			client, calls := fakeSource(t, drift, "eof")
			dir := t.TempDir()
			require.Error(t, capture(t.Context(), client, testScope, dir, 4096))
			require.NoFileExists(t, filepath.Join(dir, "ready.json"))
			require.NoFileExists(t, filepath.Join(dir, "container.log"))
			r := loadReceipt(t, dir)
			require.Equal(t, "setup_error", r.Reason)
			require.False(t, r.OpeningChecked)
			if strings.HasPrefix(drift, "before-") {
				require.Zero(t, calls.Load())
			} else {
				require.Equal(t, int32(1), calls.Load())
			}
		})
	}
}

func TestCaptureLimitsAndTransportFailurePreservePartialLogs(t *testing.T) {
	for _, tc := range []struct {
		name, mode, reason string
		limit              int64
	}{
		{"byte limit", "eof", "byte_limit", 5},
		{"transport truncation", "truncated", "read_error", 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, calls := fakeSource(t, "", tc.mode)
			dir := t.TempDir()
			require.Error(t, capture(t.Context(), client, testScope, dir, tc.limit))
			r := loadReceipt(t, dir)
			require.Equal(t, tc.reason, r.Reason)
			require.Positive(t, r.Bytes)
			require.LessOrEqual(t, r.Bytes, tc.limit)
			require.NotEmpty(t, r.Error)
			require.False(t, r.CompleteHistory)
			require.Equal(t, int32(1), calls.Load(), "must never reconnect by pod name")
		})
	}
}

func TestCaptureEndReplacementDoesNotRelabelOriginalStream(t *testing.T) {
	client, calls := fakeSource(t, "end-pod", "eof")
	dir := t.TempDir()
	require.NoError(t, capture(t.Context(), client, testScope, dir, 4096))
	r := loadReceipt(t, dir)
	require.True(t, r.OpeningChecked)
	require.Equal(t, testScope.PodUID, r.Source.PodUID)
	require.Nil(t, r.EndSource)
	require.Contains(t, r.EndCheckError, "pod identity changed")
	require.False(t, r.CompleteHistory)
	require.Equal(t, int32(1), calls.Load())
}

func TestCaptureCancellationAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			client, _ := fakeSource(t, "", "follow")
			dir := t.TempDir()
			ctx, cancel := context.WithCancel(t.Context())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
			}
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- capture(ctx, client, testScope, dir, 4096) }()
			require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(dir, "ready.json")); return err == nil }, 2*time.Second, time.Millisecond)
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				if deadline {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Equal(t, "deadline", loadReceipt(t, dir).Reason)
				} else {
					require.NoError(t, err)
					require.Equal(t, "cancelled", loadReceipt(t, dir).Reason)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("capture did not stop")
			}
		})
	}
}

func TestEvidenceDirectoryAndReceiptRefuseOverwrite(t *testing.T) {
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0700))
	dir := filepath.Join(parent, "new")
	require.NoError(t, createDirectory(dir))
	require.Error(t, createDirectory(dir))
	require.NoError(t, writeJSON(filepath.Join(dir, "receipt"), "original"))
	require.Error(t, writeJSON(filepath.Join(dir, "receipt"), "replacement"))
	data, err := os.ReadFile(filepath.Join(dir, "receipt"))
	require.NoError(t, err)
	require.Equal(t, "\"original\"\n", string(data))
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1, "temporary receipt files must be removed even on collision")
	link := filepath.Join(parent, "link")
	require.NoError(t, os.Symlink(dir, link))
	require.Error(t, createDirectory(filepath.Join(link, "child")))
	require.NoError(t, os.Chmod(parent, 0755))
	require.Error(t, createDirectory(filepath.Join(parent, "public")))
}

func TestInvalidScopeRejected(t *testing.T) {
	require.NoError(t, validateScope(testScope))
	for _, mutate := range []func(*scope){
		func(s *scope) { s.PodUID = "" },
		func(s *scope) { s.NamespaceUID = "" },
		func(s *scope) { s.OwnerUID = "" },
		func(s *scope) { s.Pod = "../other" },
		func(s *scope) { s.Container = "" },
	} {
		s := testScope
		mutate(&s)
		require.Error(t, validateScope(s))
	}
}

func TestInvalidCLIStopsBeforeLoadingConfigOrCreatingEvidence(t *testing.T) {
	base := []string{"--kubeconfig=/missing/config", "--context=test", "--namespace=test", "--namespace-uid=namespace-uid", "--pod=brain-0", "--pod-uid=pod-uid", "--statefulset-uid=owner-uid"}
	for _, extra := range [][]string{{"--duration=0s"}, {"--duration=31m"}, {"--max-bytes=0"}, {"--max-bytes=1073741825"}, {"--context="}, {"--kubeconfig=relative"}, {"unexpected"}} {
		err := run(t.Context(), append(append([]string{}, base...), extra...))
		require.ErrorContains(t, err, "explicit config/context and bounded duration/bytes")
	}
}
