package leasefault

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// Real independent client-go HTTP clients; the server implements only the small
// ConfigMap CAS model needed by this test, not Kubernetes admission or storage.
func TestFaultOwnerConcurrentHTTPClaims(t *testing.T) {
	for _, lostResponse := range []bool{false, true} {
		t.Run(map[bool]string{false: "one-winner", true: "committed-response-lost"}[lostResponse], func(t *testing.T) {
			// The CAS outcome is asserted independently of startup and fsync
			// duration; this deadline only prevents a hung fixture.
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			base := FaultOwnerBinding{Owner: "attempt-a", Namespace: "test", NamespaceUID: "ns-uid", StatefulSetName: "brain", StatefulSetUID: "sts-uid"}
			var mu sync.Mutex
			var live *unstructured.Unstructured
			var posts, deletes atomic.Int32
			arrived := make(chan struct{}, 2)
			proceed := make(chan struct{})
			status := func(w http.ResponseWriter, code int, reason metav1.StatusReason) {
				w.WriteHeader(code)
				state := metav1.StatusFailure
				if code == 200 {
					state = metav1.StatusSuccess
				}
				_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: state, Reason: reason, Code: int32(code)})
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/test" {
					_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "test", "uid": "ns-uid", "resourceVersion": "1"}})
					return
				}
				if r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/test/statefulsets/brain" {
					_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]any{"name": "brain", "namespace": "test", "uid": "sts-uid", "resourceVersion": "1"}})
					return
				}
				collection := "/api/v1/namespaces/test/configmaps"
				if r.Method == http.MethodPost && r.URL.Path == collection {
					posts.Add(1)
					data, err := io.ReadAll(io.LimitReader(r.Body, 8193))
					if err != nil || len(data) > 8192 {
						status(w, 400, metav1.StatusReasonBadRequest)
						return
					}
					var proposed unstructured.Unstructured
					if proposed.UnmarshalJSON(data) != nil || proposed.GetName() != faultOwnerName || proposed.GetNamespace() != "test" || proposed.GetUID() != "" || proposed.GetResourceVersion() != "" || proposed.Object["immutable"] != true {
						status(w, 400, metav1.StatusReasonBadRequest)
						return
					}
					// Both requests reach the server before either is allowed to
					// commit, making the contention deterministic rather than luck.
					arrived <- struct{}{}
					select {
					case <-proceed:
					case <-r.Context().Done():
						return
					}
					mu.Lock()
					defer mu.Unlock()
					if live != nil {
						status(w, 409, metav1.StatusReasonAlreadyExists)
						return
					}
					proposed.SetUID("actual-created-uid")
					proposed.SetResourceVersion("18446744073709551615")
					live = proposed.DeepCopy()
					if lostResponse {
						status(w, 500, metav1.StatusReasonInternalError)
						return
					}
					w.WriteHeader(201)
					_ = json.NewEncoder(w).Encode(live.Object)
					return
				}
				if r.URL.Path != collection+"/"+faultOwnerName {
					status(w, 404, metav1.StatusReasonNotFound)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if live == nil {
					status(w, 404, metav1.StatusReasonNotFound)
					return
				}
				switch r.Method {
				case http.MethodGet:
					_ = json.NewEncoder(w).Encode(live.Object)
				case http.MethodDelete:
					deletes.Add(1)
					var options metav1.DeleteOptions
					if json.NewDecoder(r.Body).Decode(&options) != nil || options.Preconditions == nil || options.Preconditions.UID == nil || options.Preconditions.ResourceVersion == nil || *options.Preconditions.UID != live.GetUID() || *options.Preconditions.ResourceVersion != live.GetResourceVersion() {
						status(w, 409, metav1.StatusReasonConflict)
						return
					}
					live = nil
					status(w, 200, "")
				default:
					status(w, 405, metav1.StatusReasonMethodNotAllowed)
				}
			}))
			defer server.Close()
			type attempt struct {
				owner   *FaultOwner
				err     error
				client  dynamic.Interface
				dir     string
				binding FaultOwnerBinding
			}
			results := make(chan attempt, 2)
			for _, name := range []string{"attempt-a", "attempt-b"} {
				client, err := NewDynamicClient(&rest.Config{Host: server.URL})
				require.NoError(t, err)
				dir := t.TempDir()
				require.NoError(t, os.Chmod(dir, 0700))
				binding := base
				binding.Owner = name
				go func() {
					owner, err := AcquireFaultOwner(ctx, client, dir, binding)
					results <- attempt{owner, err, client, dir, binding}
				}()
			}
			for i := 0; i < 2; i++ {
				select {
				case <-arrived:
				case <-ctx.Done():
					t.Fatal("two CREATE requests did not reach contention barrier")
				}
			}
			close(proceed)
			var winner *FaultOwner
			busy, uncertain := 0, 0
			for i := 0; i < 2; i++ {
				var got attempt
				select {
				case got = <-results:
				case <-ctx.Done():
					stacks := make([]byte, 64<<10)
					n := goruntime.Stack(stacks, true)
					t.Fatalf("claim did not finish (CREATE=%d DELETE=%d):\n%s", posts.Load(), deletes.Load(), stacks[:n])
				}
				if got.err == nil {
					require.Nil(t, winner)
					winner = got.owner
					require.NoError(t, winner.Check(ctx))
				} else {
					require.Nil(t, got.owner)
					if apierrors.IsAlreadyExists(got.err) {
						busy++
					} else {
						require.True(t, apierrors.IsInternalError(got.err), "%v", got.err)
						uncertain++
					}
					_, err := LoadFaultOwner(got.client, got.dir, got.binding)
					require.ErrorIs(t, err, os.ErrNotExist, "failed CREATE must not invent a receipt")
				}
			}
			require.Equal(t, int32(2), posts.Load(), "no hidden CREATE retry")
			require.Equal(t, 1, busy)
			require.Zero(t, deletes.Load(), "acquisition must not delete a competing claim")
			if lostResponse {
				require.Nil(t, winner)
				require.Equal(t, 1, uncertain)
				mu.Lock()
				require.NotNil(t, live)
				mu.Unlock()
				return
			}
			require.NotNil(t, winner)
			require.Zero(t, uncertain)
			proofCalled := false
			require.NoError(t, winner.Release(ctx, func(context.Context) error { proofCalled = true; return nil }))
			require.True(t, proofCalled)
			require.Equal(t, int32(1), deletes.Load())
			require.Error(t, winner.Check(ctx))
		})
	}
}
