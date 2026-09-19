package operationqueue

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	clientgotesting "k8s.io/client-go/testing"
)

func TestReleaseInstanceLeaseHTTPPreconditions(t *testing.T) {
	for _, raced := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "takeover"}[raced], func(t *testing.T) {
			name := instanceLeaseName("instance-a")
			var calls atomic.Int32
			deletes := make(chan metav1.DeleteOptions, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path != "/apis/coordination.k8s.io/v1/namespaces/test/leases/"+name {
					http.Error(w, "unexpected path", 500)
					return
				}
				switch r.Method {
				case http.MethodGet:
					_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": map[string]any{"namespace": "test", "name": name, "uid": "lease-uid", "resourceVersion": "18446744073709551615"}, "spec": map[string]any{"holderIdentity": "worker-a"}})
				case http.MethodDelete:
					var options metav1.DeleteOptions
					if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
					deletes <- options
					// A new holder updates this same object between the GET and
					// DELETE; only the version precondition can fence the delete.
					currentVersion := "18446744073709551615"
					if raced {
						currentVersion = "18446744073709551616"
					}
					if options.Preconditions != nil && options.Preconditions.ResourceVersion != nil && *options.Preconditions.ResourceVersion != currentVersion {
						w.WriteHeader(http.StatusConflict)
						_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonConflict, Code: 409, Message: "resourceVersion precondition failed"})
						return
					}
					_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess, Code: 200})
				default:
					http.Error(w, "unexpected method", 500)
				}
			}))
			defer server.Close()
			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = New(client, "test").releaseInstanceLease(ctx, "instance-a", "worker-a")
			if raced {
				require.True(t, apierrors.IsConflict(err), "%v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int32(2), calls.Load())
			select {
			case options := <-deletes:
				require.NotNil(t, options.Preconditions)
				require.NotNil(t, options.Preconditions.UID)
				require.NotNil(t, options.Preconditions.ResourceVersion)
				require.Equal(t, "lease-uid", string(*options.Preconditions.UID))
				require.Equal(t, "18446744073709551615", *options.Preconditions.ResourceVersion)
			default:
				t.Fatal("DELETE was not sent")
			}
		})
	}
}

func TestReleaseInstanceLeaseVersionFencing(t *testing.T) {
	for _, mode := range []string{"unchanged", "takeover", "renewal", "foreign-holder", "missing-uid", "missing-version"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			client := fakeQueueClient()
			queue := New(client, "test")
			require.NoError(t, queue.acquireInstanceLease(ctx, "instance-a", "worker-a", time.Minute))
			name := instanceLeaseName("instance-a")
			live, err := queue.leases.Get(ctx, name, metav1.GetOptions{})
			require.NoError(t, err)
			live.SetResourceVersion("18446744073709551615")
			if mode == "missing-uid" {
				live.SetUID("")
			}
			if mode == "missing-version" {
				live.SetResourceVersion("")
			}
			if mode == "foreign-holder" {
				require.NoError(t, unstructured.SetNestedField(live.Object, "worker-b", "spec", "holderIdentity"))
			}
			require.NoError(t, client.Tracker().Update(LeaseResource, live, "test"))
			deletes := 0
			client.PrependReactor("delete", LeaseResource.Resource, func(action clientgotesting.Action) (bool, runtime.Object, error) {
				deletes++
				pre := action.(clientgotesting.DeleteAction).GetDeleteOptions().Preconditions
				require.NotNil(t, pre)
				require.NotNil(t, pre.UID)
				require.NotNil(t, pre.ResourceVersion)
				require.Equal(t, live.GetUID(), *pre.UID)
				require.Equal(t, "18446744073709551615", *pre.ResourceVersion)
				// Emulate an API-server update between GET and DELETE. UID stays
				// the same; the server must reject stale resourceVersion deletion.
				if mode == "takeover" || mode == "renewal" {
					next := live.DeepCopy()
					next.SetResourceVersion("18446744073709551616")
					if mode == "takeover" {
						require.NoError(t, unstructured.SetNestedField(next.Object, "worker-b", "spec", "holderIdentity"))
					}
					require.NoError(t, client.Tracker().Update(LeaseResource, next, "test"))
					if *pre.ResourceVersion != next.GetResourceVersion() {
						return true, nil, apierrors.NewConflict(schema.GroupResource{Group: LeaseResource.Group, Resource: LeaseResource.Resource}, name, errors.New("resourceVersion precondition failed"))
					}
				}
				return false, nil, nil
			})
			err = queue.releaseInstanceLease(ctx, "instance-a", "worker-a")
			if mode == "unchanged" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			switch mode {
			case "takeover", "renewal":
				require.True(t, apierrors.IsConflict(err))
				require.Equal(t, 1, deletes, "release must not retry a stale delete")
				current, getErr := queue.leases.Get(ctx, name, metav1.GetOptions{})
				require.NoError(t, getErr)
				require.Equal(t, "18446744073709551616", current.GetResourceVersion())
				holder, _, _ := unstructured.NestedString(current.Object, "spec", "holderIdentity")
				if mode == "takeover" {
					require.Equal(t, "worker-b", holder)
				} else {
					require.Equal(t, "worker-a", holder)
				}
			case "foreign-holder":
				require.ErrorIs(t, err, ErrFenced)
				require.Zero(t, deletes)
			case "missing-uid", "missing-version":
				require.Zero(t, deletes)
			case "unchanged":
				require.Equal(t, 1, deletes)
				_, getErr := queue.leases.Get(ctx, name, metav1.GetOptions{})
				require.True(t, apierrors.IsNotFound(getErr))
			}
		})
	}
}
