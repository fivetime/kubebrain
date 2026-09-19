package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Exercise the repository plan through the actual dynamic-client HTTP encoder.
// The server only simulates API responses, not Kubernetes precondition enforcement
// or Cilium withdrawal. No real kubeconfig or cluster is involved.
func TestFaultPolicyPlanDeleteTransport(t *testing.T) {
	const observations = `{
  "binding":{"namespace":"test-ns","namespaceUID":"ns-uid","name":"owned-policy","nonce":"active","reservedNonce":"reserved"},
  "namespace":{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"test-ns","uid":"ns-uid"}},
  "approved":{"apiVersion":"cilium.io/v2","kind":"CiliumNetworkPolicy","metadata":{"namespace":"test-ns","name":"owned-policy"},"spec":{"endpointSelector":{"matchLabels":{"kubebrain.io/fault-owner":"active"}}}},
  "expected":{"apiVersion":"cilium.io/v2","kind":"CiliumNetworkPolicy","metadata":{"namespace":"test-ns","name":"owned-policy","uid":"original-uid","resourceVersion":"9007199254740993"},"spec":{"endpointSelector":{"matchLabels":{"kubebrain.io/fault-owner":"reserved"}}}},
  "current":{"apiVersion":"cilium.io/v2","kind":"CiliumNetworkPolicy","metadata":{"namespace":"test-ns","name":"owned-policy","uid":"original-uid","resourceVersion":"18446744073709551615"},"spec":{"endpointSelector":{"matchLabels":{"kubebrain.io/fault-owner":"active"}}}}
}`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "jq", "-e", "-f", "../../fault-policy-delete-plan.jq")
	cmd.Stdin = strings.NewReader(observations)
	data, err := cmd.Output()
	require.NoError(t, err)
	var plan struct {
		APIVersion, Resource, Namespace, Name, UID, ResourceVersion string
	}
	require.NoError(t, json.Unmarshal(data, &plan))
	for _, code := range []int{http.StatusOK, http.StatusConflict, http.StatusNotFound} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			type request struct {
				method, path string
				options      metav1.DeleteOptions
				err          error
			}
			requests := make(chan request, 4)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				req := request{method: r.Method, path: r.URL.Path}
				req.err = json.NewDecoder(r.Body).Decode(&req.options)
				select {
				case requests <- req:
				default:
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: int32(code), Status: metav1.StatusSuccess}
				if code != http.StatusOK {
					status.Status = metav1.StatusFailure
					if code == http.StatusConflict {
						status.Reason = metav1.StatusReasonConflict
					} else {
						status.Reason = metav1.StatusReasonNotFound
					}
				}
				_ = json.NewEncoder(w).Encode(status)
			}))
			defer server.Close()
			client, err := dynamicClient(testRESTConfig(server.URL))
			require.NoError(t, err)
			err = deleteWithPreconditions(ctx, client, plan.APIVersion, plan.Resource, plan.Namespace, plan.Name, plan.UID, plan.ResourceVersion)
			switch code {
			case http.StatusOK:
				require.NoError(t, err)
			case http.StatusConflict:
				require.True(t, apierrors.IsConflict(err), "%v", err)
			case http.StatusNotFound:
				require.True(t, apierrors.IsNotFound(err), "%v", err)
			}
			require.EqualValues(t, 1, calls.Load(), "no unconditional retry after conflict")
			req := <-requests
			require.NoError(t, req.err)
			require.Equal(t, http.MethodDelete, req.method)
			require.Equal(t, "/apis/cilium.io/v2/namespaces/test-ns/ciliumnetworkpolicies/owned-policy", req.path)
			require.NotNil(t, req.options.Preconditions)
			require.NotNil(t, req.options.Preconditions.UID)
			require.NotNil(t, req.options.Preconditions.ResourceVersion)
			require.Equal(t, "original-uid", string(*req.options.Preconditions.UID))
			require.Equal(t, "18446744073709551615", *req.options.Preconditions.ResourceVersion)
		})
	}
}
