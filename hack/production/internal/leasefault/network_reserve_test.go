package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func TestReserveNetworkHTTP(t *testing.T) {
	for _, mode := range []string{"success", "conflict", "invalid-response", "save-failure", "admission-failure"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			plan := networkPlan()
			var calls atomic.Int32
			errs := make(chan error, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var obj unstructured.Unstructured
				err := json.NewDecoder(r.Body).Decode(&obj)
				if err == nil {
					_, err = LoadNetworkRecovery(dir, plan)
				}
				label, _, _ := unstructured.NestedString(obj.Object, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner")
				if r.Method != "POST" || r.URL.Path != "/apis/cilium.io/v2/namespaces/test-ns/ciliumnetworkpolicies" || r.URL.Query().Has("dryRun") || label != "reserved" {
					err = errors.New("unexpected reservation request")
				}
				if mode == "save-failure" {
					err = errors.Join(err, os.WriteFile(filepath.Join(dir, networkReceiptFile), []byte("partial"), 0600))
				}
				select {
				case errs <- err:
				default:
				}
				w.Header().Set("Content-Type", "application/json")
				if mode == "conflict" {
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"AlreadyExists","code":409}`))
					return
				}
				obj.SetUID("actual-created-uid")
				obj.SetResourceVersion("18446744073709551615")
				if mode == "invalid-response" {
					obj.SetUID("")
				}
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(&obj)
			}))
			defer server.Close()
			client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			admit := func(context.Context) error {
				if mode == "admission-failure" {
					return errors.New("ownership lost")
				}
				return nil
			}
			err = ReserveNetwork(ctx, client, dir, plan, admit)
			if mode == "success" {
				require.NoError(t, err)
				raw, err := LoadNetworkReservation(dir, plan)
				require.NoError(t, err)
				require.Contains(t, string(raw), `"uid":"actual-created-uid"`)
				require.Contains(t, string(raw), `"resourceVersion":"18446744073709551615"`)
			} else {
				require.Error(t, err)
			}
			if mode == "admission-failure" {
				require.Zero(t, calls.Load())
				_, err := os.Stat(filepath.Join(dir, networkRecoveryFile))
				require.True(t, os.IsNotExist(err))
				return
			}
			require.Equal(t, int32(1), calls.Load())
			require.NoError(t, <-errs)
			// Even ambiguous failure must not cause another create/adoption attempt.
			require.Error(t, ReserveNetwork(ctx, client, dir, plan, admit))
			require.Equal(t, int32(1), calls.Load())
			if mode == "save-failure" {
				data, err := os.ReadFile(filepath.Join(dir, networkReceiptFile))
				require.NoError(t, err)
				require.Equal(t, "partial", string(data))
			}
		})
	}
}
