package leasefault

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func TestDynamicClientSingleAttempt(t *testing.T) {
	for _, code := range []int{307, 308, 429, 503} {
		for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
			t.Run(fmt.Sprintf("%d/%s", code, method), func(t *testing.T) {
				var calls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Method != method {
						t.Errorf("method = %s, want %s", r.Method, method)
					}
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Retry-After", "0")
					w.Header().Set("Location", r.URL.Path)
					w.WriteHeader(code)
					fmt.Fprintf(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","message":"uncertain result","code":%d}`, code)
				}))
				defer srv.Close()
				client, err := NewDynamicClient(&rest.Config{Host: srv.URL})
				require.NoError(t, err)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				r := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace("test")
				obj := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "owner", "namespace": "test"}}}
				switch method {
				case "GET":
					_, err = r.Get(ctx, "owner", metav1.GetOptions{})
				case "POST":
					_, err = r.Create(ctx, obj, metav1.CreateOptions{})
				case "PUT":
					_, err = r.Update(ctx, obj, metav1.UpdateOptions{})
				case "PATCH":
					_, err = r.Patch(ctx, "owner", types.JSONPatchType, []byte(`[]`), metav1.PatchOptions{})
				case "DELETE":
					err = r.Delete(ctx, "owner", metav1.DeleteOptions{})
				}
				require.Error(t, err)
				var status apierrors.APIStatus
				require.ErrorAs(t, err, &status)
				require.Equal(t, int32(code), status.Status().Code)
				require.Equal(t, int32(1), calls.Load())
			})
		}
	}
}

// Control: the same Retry-After response really causes the standard dynamic
// client to repeat a mutation, so the test is not relying on an inert header.
func TestStandardDynamicClientRetriesMutation(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","code":429}`)
			return
		}
		fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"owner"}}`)
	}))
	defer srv.Close()
	client, err := dynamic.NewForConfig(&rest.Config{Host: srv.URL})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace("test").Create(ctx,
		&unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "owner"}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(2), calls.Load())
}
