package parameterbroker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const testAudience = "kubebrain-operation-parameters"

func TestHandlerReturnsOnlyCurrentTypeBoundWorkerParameters(t *testing.T) {
	dynamicClient, claim, parameters := claimedOperation(t)
	handler, err := NewHandler(
		tokenClient("system:serviceaccount:test:kubebrain-post-restore-audit-executor",
			[]string{testAudience}, true),
		dynamicClient, "test", testAudience, time.Second,
	)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/v1/parameters?namespace=test&name=%s&owner=%s&attempt=%d",
			claim.Name, claim.Owner, claim.Attempt), nil)
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	requireNoStoreHeaders(t, response)
	require.Equal(t, parameters, response.Body.Bytes())
}

func TestHandlerRejectsAmbiguousRequiredQueryParameters(t *testing.T) {
	dynamicClient, claim, _ := claimedOperation(t)
	handler, err := NewHandler(
		tokenClient("system:serviceaccount:test:kubebrain-post-restore-audit-executor",
			[]string{testAudience}, true),
		dynamicClient, "test", testAudience, time.Second,
	)
	require.NoError(t, err)

	base := fmt.Sprintf("/v1/parameters?namespace=test&name=%s&owner=%s&attempt=%d",
		claim.Name, claim.Owner, claim.Attempt)
	for _, duplicate := range []string{
		"namespace=test",
		"name=" + claim.Name,
		"owner=" + claim.Owner,
		fmt.Sprintf("attempt=%d", claim.Attempt),
	} {
		t.Run(duplicate, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, base+"&"+duplicate, nil)
			request.Header.Set("Authorization", "Bearer valid")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusBadRequest, response.Code)
			requireNoStoreHeaders(t, response)
			require.NotContains(t, response.Body.String(), "audit")
		})
	}
}

func TestHandlerRejectsNonJSONObjectParameters(t *testing.T) {
	for _, tc := range []struct {
		name       string
		parameters []byte
	}{
		{name: "not json", parameters: []byte("not-json")},
		{name: "trailing json", parameters: []byte("{}{}")},
		{name: "null", parameters: []byte("null")},
		{name: "array", parameters: []byte("[]")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dynamicClient, claim, _ := claimedOperationWithParameters(t, tc.parameters)
			handler, err := NewHandler(
				tokenClient("system:serviceaccount:test:kubebrain-post-restore-audit-executor",
					[]string{testAudience}, true),
				dynamicClient, "test", testAudience, time.Second,
			)
			require.NoError(t, err)

			request := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("/v1/parameters?namespace=test&name=%s&owner=%s&attempt=%d",
					claim.Name, claim.Owner, claim.Attempt), nil)
			request.Header.Set("Authorization", "Bearer valid")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusForbidden, response.Code)
			requireNoStoreHeaders(t, response)
			require.NotContains(t, response.Body.String(), string(tc.parameters))
		})
	}
}

func TestHandlerRejectsMalformedQueryBeforeAuthenticationAndOperationAPI(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{name: "namespace", query: "namespace=ops.ns&name=audit-1&owner=audit-worker&attempt=1"},
		{name: "name", query: "namespace=test&name=audit/1&owner=audit-worker&attempt=1"},
		{name: "owner", query: "namespace=test&name=audit-1&owner=audit%0Aworker&attempt=1"},
		{name: "attempt", query: "namespace=test&name=audit-1&owner=audit-worker&attempt=0"},
		{name: "unknown", query: "namespace=test&name=audit-1&owner=audit-worker&attempt=1&debug=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dynamicClient, _, _ := claimedOperation(t)
			tokens := tokenClient(
				"system:serviceaccount:test:kubebrain-post-restore-audit-executor",
				[]string{testAudience}, true,
			)
			handler, err := NewHandler(tokens, dynamicClient, "test", testAudience, time.Second)
			require.NoError(t, err)
			dynamicClient.ClearActions()
			tokens.ClearActions()

			request := httptest.NewRequest(http.MethodGet, "/v1/parameters?"+tc.query, nil)
			request.Header.Set("Authorization", "Bearer valid")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusBadRequest, response.Code)
			require.Empty(t, tokens.Actions())
			require.Empty(t, dynamicClient.Actions())
			requireNoStoreHeaders(t, response)
		})
	}
}

func TestHandlerRejectsUnexpectedBodyBeforeAuthenticationAndOperationAPI(t *testing.T) {
	dynamicClient, claim, _ := claimedOperation(t)
	tokens := tokenClient(
		"system:serviceaccount:test:kubebrain-post-restore-audit-executor",
		[]string{testAudience}, true,
	)
	handler, err := NewHandler(tokens, dynamicClient, "test", testAudience, time.Second)
	require.NoError(t, err)
	dynamicClient.ClearActions()
	tokens.ClearActions()

	request := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/v1/parameters?namespace=test&name=%s&owner=%s&attempt=%d",
			claim.Name, claim.Owner, claim.Attempt),
		strings.NewReader(`{"debug":true}`),
	)
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Empty(t, tokens.Actions())
	require.Empty(t, dynamicClient.Actions())
	requireNoStoreHeaders(t, response)
}

func TestHandlerFailsClosedForIdentityTypeAudienceAndFencing(t *testing.T) {
	tests := []struct {
		name          string
		username      string
		audiences     []string
		authenticated bool
		authorization string
		owner         string
		attemptDelta  int64
		status        int
	}{
		{
			name: "wrong executor type", username: "system:serviceaccount:test:kubebrain-backup-executor",
			audiences: []string{testAudience}, authenticated: true,
			authorization: "Bearer valid", status: http.StatusForbidden,
		},
		{
			name: "unlisted service account", username: "system:serviceaccount:test:default",
			audiences: []string{testAudience}, authenticated: true,
			authorization: "Bearer valid", status: http.StatusUnauthorized,
		},
		{
			name: "wrong audience", username: "system:serviceaccount:test:kubebrain-post-restore-audit-executor",
			audiences: []string{"kubernetes"}, authenticated: true,
			authorization: "Bearer valid", status: http.StatusUnauthorized,
		},
		{
			name: "unauthenticated", username: "",
			audiences: []string{testAudience}, authenticated: false,
			authorization: "Bearer invalid", status: http.StatusUnauthorized,
		},
		{
			name: "missing bearer", username: "",
			audiences: []string{testAudience}, authenticated: false,
			status: http.StatusUnauthorized,
		},
		{
			name: "wrong owner", username: "system:serviceaccount:test:kubebrain-post-restore-audit-executor",
			audiences: []string{testAudience}, authenticated: true,
			authorization: "Bearer valid", owner: "other", status: http.StatusForbidden,
		},
		{
			name: "wrong attempt", username: "system:serviceaccount:test:kubebrain-post-restore-audit-executor",
			audiences: []string{testAudience}, authenticated: true,
			authorization: "Bearer valid", attemptDelta: 1, status: http.StatusForbidden,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dynamicClient, claim, _ := claimedOperation(t)
			handler, err := NewHandler(
				tokenClient(tc.username, tc.audiences, tc.authenticated),
				dynamicClient, "test", testAudience, time.Second,
			)
			require.NoError(t, err)
			owner := claim.Owner
			if tc.owner != "" {
				owner = tc.owner
			}
			request := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("/v1/parameters?namespace=test&name=%s&owner=%s&attempt=%d",
					claim.Name, owner, claim.Attempt+tc.attemptDelta), nil)
			if tc.authorization != "" {
				request.Header.Set("Authorization", tc.authorization)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, tc.status, response.Code)
			requireNoStoreHeaders(t, response)
			require.NotContains(t, response.Body.String(), "audit")
		})
	}
}

func TestHandlerSetsNoStoreHeadersOnHealthAndNotFound(t *testing.T) {
	handler, err := NewHandler(
		tokenClient("", nil, false),
		fake.NewSimpleDynamicClient(runtime.NewScheme()), "test", testAudience, time.Second,
	)
	require.NoError(t, err)
	for _, tc := range []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodGet, path: "/healthz", status: http.StatusNoContent},
		{method: http.MethodPost, path: "/v1/parameters", status: http.StatusNotFound},
		{method: http.MethodGet, path: "/missing", status: http.StatusNotFound},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			require.Equal(t, tc.status, response.Code)
			requireNoStoreHeaders(t, response)
		})
	}
}

func TestBearerTokenParsingIsStrictAndCaseInsensitive(t *testing.T) {
	token, err := bearerToken("bearer projected.jwt")
	require.NoError(t, err)
	require.Equal(t, "projected.jwt", token)

	for _, tc := range []struct {
		name   string
		header string
		want   string
	}{
		{name: "missing", header: "", want: "bearer token is required"},
		{name: "wrong scheme", header: "Basic projected.jwt", want: "bearer token is required"},
		{name: "empty token", header: "Bearer ", want: "invalid bearer token"},
		{name: "leading token space", header: "Bearer  projected.jwt", want: "invalid bearer token"},
		{name: "trailing token space", header: "Bearer projected.jwt ", want: "invalid bearer token"},
		{name: "embedded token space", header: "Bearer projected jwt", want: "invalid bearer token"},
		{name: "tab separator", header: "Bearer\tprojected.jwt", want: "bearer token is required"},
		{name: "oversized", header: "Bearer " + strings.Repeat("x", maxTokenBytes+1), want: "invalid bearer token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bearerToken(tc.header)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestReadyChecksOperationSecretAndTokenReviewAPIs(t *testing.T) {
	dynamicClient := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			operationqueue.Resource:       "KubeBrainOperationList",
			operationqueue.SecretResource: "SecretList",
		},
	)
	tokens := tokenClient("", nil, false)
	handler, err := NewHandler(tokens, dynamicClient, "test", testAudience, time.Second)
	require.NoError(t, err)
	require.NoError(t, handler.Ready(context.Background()))
	require.Len(t, dynamicClient.Actions(), 2)
	require.Equal(t, "kubebrainoperations", dynamicClient.Actions()[0].GetResource().Resource)
	require.Equal(t, "secrets", dynamicClient.Actions()[1].GetResource().Resource)
	require.Len(t, tokens.Actions(), 1)
	require.Equal(t, "tokenreviews", tokens.Actions()[0].GetResource().Resource)
}

func TestReadyFailsClosedWhenAnyCriticalAPIIsUnavailable(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		tokens   bool
		err      error
		want     string
	}{
		{name: "operation API", resource: "kubebrainoperations", err: errors.New("dependency unavailable"), want: "probe operation API: dependency unavailable"},
		{name: "Secret API", resource: "secrets", err: errors.New("dependency unavailable"), want: "probe Secret API: dependency unavailable"},
		{
			name: "operation CRD route", resource: "kubebrainoperations",
			err: apierrors.NewNotFound(schema.GroupResource{
				Group: operationqueue.Resource.Group, Resource: operationqueue.Resource.Resource,
			}, ""),
			want: `probe operation API: kubebrainoperations.dbaas.kubebrain.io "" not found`,
		},
		{name: "TokenReview API", tokens: true, want: "probe TokenReview API: dependency unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dynamicClient := fake.NewSimpleDynamicClientWithCustomListKinds(
				runtime.NewScheme(), map[schema.GroupVersionResource]string{
					operationqueue.Resource:       "KubeBrainOperationList",
					operationqueue.SecretResource: "SecretList",
				},
			)
			tokens := tokenClient("", nil, false)
			if tc.resource != "" {
				dynamicClient.PrependReactor("get", tc.resource, func(
					k8stesting.Action,
				) (bool, runtime.Object, error) {
					return true, nil, tc.err
				})
			}
			if tc.tokens {
				tokens.PrependReactor("create", "tokenreviews", func(
					k8stesting.Action,
				) (bool, runtime.Object, error) {
					return true, nil, errors.New("dependency unavailable")
				})
			}
			handler, err := NewHandler(tokens, dynamicClient, "test", testAudience, time.Second)
			require.NoError(t, err)
			require.ErrorContains(t, handler.Ready(context.Background()), tc.want)
		})
	}
}

func TestNewHandlerRequiresPositiveDependencyTimeout(t *testing.T) {
	_, err := NewHandler(
		kubernetesfake.NewSimpleClientset(), fake.NewSimpleDynamicClient(runtime.NewScheme()),
		"test", testAudience, 0,
	)
	require.ErrorContains(t, err, "timeout must be positive")
}

func TestNewHandlerRejectsInvalidNamespaceBeforeAPI(t *testing.T) {
	tokens := kubernetesfake.NewSimpleClientset()
	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme())
	handler, err := NewHandler(tokens, dynamicClient, "ops.ns", testAudience, time.Second)
	require.Nil(t, handler)
	require.ErrorContains(t, err, "invalid parameter broker namespace ops.ns")
	require.Empty(t, dynamicClient.Actions())
	require.Empty(t, tokens.Actions())
}

func claimedOperation(t *testing.T) (*fake.FakeDynamicClient, *operationqueue.Claim, []byte) {
	t.Helper()
	return claimedOperationWithParameters(t, []byte("{\"audit\":\"bound\"}\n"))
}

func claimedOperationWithParameters(
	t *testing.T,
	parameters []byte,
) (*fake.FakeDynamicClient, *operationqueue.Claim, []byte) {
	t.Helper()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			operationqueue.Resource:       "KubeBrainOperationList",
			operationqueue.LeaseResource:  "LeaseList",
			operationqueue.SecretResource: "SecretList",
		},
	)
	queue := operationqueue.New(client, "test")
	digest := fmt.Sprintf("%x", sha256.Sum256(parameters))
	_, err := client.Resource(operationqueue.SecretResource).Namespace("test").Create(
		context.Background(), &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata":  map[string]any{"name": "audit-parameters"},
			"immutable": true,
			"data": map[string]any{
				"parameters.json": base64.StdEncoding.EncodeToString(parameters),
			},
		}}, metav1.CreateOptions{},
	)
	require.NoError(t, err)
	_, err = queue.Submit(context.Background(), "audit-1", operationqueue.Spec{
		OperationID: "audit-1", Instance: "instance-a", Type: "PostRestoreAudit",
		ParametersSHA256: digest, ParametersSecret: "audit-parameters",
		ParametersKey: "parameters.json", MaxAttempts: 3,
	})
	require.NoError(t, err)
	claim, err := queue.Claim(context.Background(), "audit-worker", "PostRestoreAudit", time.Hour)
	require.NoError(t, err)
	return client, claim, parameters
}

func tokenClient(
	username string, audiences []string, authenticated bool,
) *kubernetesfake.Clientset {
	client := kubernetesfake.NewSimpleClientset()
	client.PrependReactor("create", "tokenreviews", func(
		action k8stesting.Action,
	) (bool, runtime.Object, error) {
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: authenticated,
			Audiences:     audiences,
			User:          authenticationv1.UserInfo{Username: username},
		}}, nil
	})
	return client
}

func requireNoStoreHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
}
