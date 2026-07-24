package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/testcommand"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestDeleteOptionsUIDPreconditionShape(t *testing.T) {
	var options metav1.DeleteOptions
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodDelete, r.Method)
		require.Equal(t, "/apis/apps/v1/namespaces/instance-a/statefulsets/kubebrain", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&options))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Success","code":200}`))
	}))
	defer server.Close()

	config := testRESTConfig(server.URL)
	client, err := dynamicClient(config)
	require.NoError(t, err)
	require.NoError(t, deleteWithUID(
		context.Background(),
		client,
		"apps/v1",
		"statefulsets",
		"instance-a",
		"kubebrain",
		"uid-123",
	))
	require.NotNil(t, options.Preconditions)
	require.NotNil(t, options.Preconditions.UID)
	require.Equal(t, "uid-123", string(*options.Preconditions.UID))
	require.NotNil(t, options.PropagationPolicy)
	require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
}

func TestMainRejectsInvalidNamespaceBeforeKubeconfig(t *testing.T) {
	output, err := testcommand.GoRun(t, ".",
		"--api-version", "v1",
		"--resource", "pods",
		"--namespace", "tenant.a",
		"--name", "kubebrain",
		"--uid", "uid-1",
	)
	require.Error(t, err)
	require.Contains(t, string(output), "invalid namespace tenant.a")
}

func TestDeleteWithUIDRejectsUnsafeRequestBeforeAPI(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	defer server.Close()

	client, err := dynamicClient(testRESTConfig(server.URL))
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		apiVersion string
		resource   string
		namespace  string
		objectName string
		uid        string
		message    string
	}{
		{
			name: "resource", apiVersion: "v1", resource: "pods/status",
			namespace: "tenant-a", objectName: "pod-a", uid: "uid-1",
			message: "invalid resource pods/status",
		},
		{
			name: "namespace", apiVersion: "v1", resource: "pods",
			namespace: "tenant.a", objectName: "pod-a", uid: "uid-1",
			message: "invalid namespace tenant.a",
		},
		{
			name: "name", apiVersion: "v1", resource: "pods",
			namespace: "tenant-a", objectName: "pod/a", uid: "uid-1",
			message: "invalid resource name pod/a",
		},
		{
			name: "uid", apiVersion: "v1", resource: "pods",
			namespace: "tenant-a", objectName: "pod-a",
			message: "UID precondition is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called = false
			err := deleteWithUID(
				context.Background(), client,
				tc.apiVersion, tc.resource, tc.namespace, tc.objectName, tc.uid,
			)
			require.ErrorContains(t, err, tc.message)
			require.False(t, called)
		})
	}
}

func TestDeleteWithUIDReturnsPreconditionConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","message":"UID precondition failed","code":409}`))
	}))
	defer server.Close()

	client, err := dynamicClient(testRESTConfig(server.URL))
	require.NoError(t, err)
	err = deleteWithUID(context.Background(), client, "v1", "persistentvolumeclaims", "storage-a", "data-0", "old-uid")
	require.Error(t, err)
	require.True(t, apierrors.IsConflict(err), err)
}

func TestDeleteWithUIDSupportsClusterScopedResources(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodDelete, r.Method)
		require.Equal(t, "/api/v1/namespaces/instance-a", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Success","code":200}`))
	}))
	defer server.Close()

	client, err := dynamicClient(testRESTConfig(server.URL))
	require.NoError(t, err)
	require.NoError(t, deleteWithUID(
		context.Background(), client, "v1", "namespaces", "", "instance-a", "uid-ns",
	))
}

func TestClientConfigPrefersInClusterWhenKubeconfigIsEmpty(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, "https://ambient.example.invalid"))
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) {
		return &rest.Config{Host: "https://kubernetes.default.svc"}, nil
	}

	config, err := clientConfig("", "")
	require.NoError(t, err)
	require.Equal(t, "https://kubernetes.default.svc", config.Host)
}

func TestClientConfigSkipsInClusterWhenExplicitKubeconfigIsProvided(t *testing.T) {
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	called := false
	inClusterConfig = func() (*rest.Config, error) {
		called = true
		return nil, errors.New("in-cluster should not be used")
	}

	kubeconfig := writeKubeconfig(t, "https://api.example.invalid")

	config, err := clientConfig(kubeconfig, "")
	require.NoError(t, err)
	require.False(t, called)
	require.Equal(t, "https://api.example.invalid", config.Host)
}

func TestClientConfigFallsBackToStandardLocalKubeconfigOutsideCluster(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, "https://local.example.invalid"))
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) {
		return nil, errors.New("not running in a Kubernetes pod")
	}

	config, err := clientConfig("", "")
	require.NoError(t, err)
	require.Equal(t, "https://local.example.invalid", config.Host)
}

func writeKubeconfig(t *testing.T, server string) string {
	t.Helper()
	kubeconfig := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: `+server+`
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user:
    token: token
`), 0o600))
	return kubeconfig
}

func testRESTConfig(server string) *rest.Config {
	return &rest.Config{
		Host: server,
		ContentConfig: rest.ContentConfig{
			ContentType: "application/json",
		},
	}
}
