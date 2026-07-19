package parameterbroker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
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
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, parameters, response.Body.Bytes())
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
			require.NotContains(t, response.Body.String(), "audit")
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
	}{
		{name: "operation API", resource: "kubebrainoperations"},
		{name: "Secret API", resource: "secrets"},
		{name: "TokenReview API", tokens: true},
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
					return true, nil, errors.New("dependency unavailable")
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
			require.ErrorContains(t, handler.Ready(context.Background()), "dependency unavailable")
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

func claimedOperation(t *testing.T) (*fake.FakeDynamicClient, *operationqueue.Claim, []byte) {
	t.Helper()
	client := fake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(), map[schema.GroupVersionResource]string{
			operationqueue.Resource:       "KubeBrainOperationList",
			operationqueue.LeaseResource:  "LeaseList",
			operationqueue.SecretResource: "SecretList",
		},
	)
	queue := operationqueue.New(client, "test")
	parameters := []byte("{\"audit\":\"bound\"}\n")
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
