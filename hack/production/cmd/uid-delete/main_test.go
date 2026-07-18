package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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

func testRESTConfig(server string) *rest.Config {
	return &rest.Config{
		Host: server,
		ContentConfig: rest.ContentConfig{
			ContentType: "application/json",
		},
	}
}
