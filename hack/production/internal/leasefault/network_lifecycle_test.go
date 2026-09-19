package leasefault

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// Exercise real dynamic-client HTTP serialization across reservation, activation
// and removal. The fixture enforces patch/delete preconditions but is NOT a real
// API server, Cilium dataplane, admission implementation or complete coordinator.
func TestNetworkLifecycleHTTP(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprintf("ambiguous-activation-%t", ambiguous), func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := networkPlan()
			var mu sync.Mutex
			var current *unstructured.Unstructured
			var requests []string
			var serverErr error
			base := "/apis/cilium.io/v2/namespaces/" + plan.Namespace + "/ciliumnetworkpolicies"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				requests = append(requests, r.Method)
				w.Header().Set("Content-Type", "application/json")
				fail := func(err error) {
					serverErr = err
					http.Error(w, err.Error(), http.StatusBadRequest)
				}
				status := func(code int, reason string) {
					w.WriteHeader(code)
					_ = json.NewEncoder(w).Encode(map[string]interface{}{"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": reason, "code": code})
				}
				wantPath := base + "/" + plan.PolicyName
				if r.Method == "POST" {
					wantPath = base
				}
				if r.URL.Path != wantPath || r.URL.Query().Has("dryRun") {
					fail(fmt.Errorf("unexpected URL %s", r.URL))
					return
				}
				switch r.Method {
				case "POST":
					if _, err := LoadNetworkRecovery(dir, plan); err != nil {
						fail(err)
						return
					}
					current = &unstructured.Unstructured{}
					if err := json.NewDecoder(r.Body).Decode(current); err != nil {
						fail(err)
						return
					}
					current.SetUID("server-created-policy")
					current.SetResourceVersion("18446744073709551615")
					w.WriteHeader(http.StatusCreated)
				case "GET":
					if current == nil {
						status(404, "NotFound")
						return
					}
				case "PATCH":
					if r.Header.Get("Content-Type") != "application/json-patch+json" {
						fail(fmt.Errorf("unexpected patch content type"))
						return
					}
					data, err := io.ReadAll(r.Body)
					if err != nil {
						fail(err)
						return
					}
					patch, err := jsonpatch.DecodePatch(data)
					if err != nil {
						fail(err)
						return
					}
					before, err := current.MarshalJSON()
					if err != nil {
						fail(err)
						return
					}
					after, err := patch.Apply(before)
					if err != nil {
						fail(err)
						return
					}
					if err := current.UnmarshalJSON(after); err != nil {
						fail(err)
						return
					}
					current.SetResourceVersion("18446744073709551616")
					if ambiguous {
						// The mutation took effect, but the client cannot infer that
						// from its error. Never retry activation or skip recovery.
						status(500, "InternalError")
						return
					}
				case "DELETE":
					var opts metav1.DeleteOptions
					if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
						fail(err)
						return
					}
					pre := opts.Preconditions
					if current == nil || pre == nil || pre.UID == nil || pre.ResourceVersion == nil || *pre.UID != current.GetUID() || *pre.ResourceVersion != current.GetResourceVersion() {
						fail(fmt.Errorf("missing or stale delete preconditions"))
						return
					}
					current = nil
					_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Success","code":200}`))
					return
				default:
					fail(fmt.Errorf("unexpected method %s", r.Method))
					return
				}
				_ = json.NewEncoder(w).Encode(current)
			}))
			defer server.Close()
			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			admit := func(context.Context) error { return nil } // fixture only
			require.NoError(t, ReserveNetwork(ctx, client, dir, plan, admit))
			err = ActivateNetwork(ctx, client, dir, plan, time.Now(), admit)
			if ambiguous {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			mu.Lock()
			label, _, labelErr := unstructured.NestedString(current.Object, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner")
			mu.Unlock()
			require.NoError(t, labelErr)
			require.Equal(t, plan.Nonce, label)
			cancel()
			require.ErrorIs(t, RemoveNetworkPolicy(ctx, client, dir, plan, admit), context.Canceled)
			recovery, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			require.NoError(t, RemoveNetworkPolicy(recovery, client, dir, plan, admit))
			// Repeat reconciliation only after independently verifying absence;
			// it must not send another DELETE or discard the original receipt.
			require.NoError(t, RemoveNetworkPolicy(recovery, client, dir, plan, admit))
			_, err = LoadNetworkReservation(dir, plan)
			require.NoError(t, err)
			mu.Lock()
			defer mu.Unlock()
			require.NoError(t, serverErr)
			require.Nil(t, current)
			require.Equal(t, []string{"POST", "GET", "PATCH", "GET", "DELETE", "GET", "GET"}, requests)
		})
	}
}
