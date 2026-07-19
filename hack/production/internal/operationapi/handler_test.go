package operationapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type staticAuthenticator struct {
	principal Principal
	err       error
}

func (a staticAuthenticator) Authenticate(context.Context, string) (Principal, error) {
	return a.principal, a.err
}

type memoryOperationStore struct {
	objects map[string]*unstructured.Unstructured
	spec    operationqueue.Spec
}

func (s *memoryOperationStore) Submit(
	_ context.Context,
	name string,
	spec operationqueue.Spec,
) (*unstructured.Unstructured, error) {
	s.spec = spec
	if existing := s.objects[name]; existing != nil {
		return existing.DeepCopy(), nil
	}
	object := operationObject(name, spec)
	s.objects[name] = object
	return object.DeepCopy(), nil
}

func (s *memoryOperationStore) Get(_ context.Context, name string) (*unstructured.Unstructured, error) {
	if object := s.objects[name]; object != nil {
		return object.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "dbaas.kubebrain.io", Resource: "operations"}, name)
}

func operationObject(name string, spec operationqueue.Spec) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbaas.kubebrain.io/v1alpha1",
		"kind":       "KubeBrainOperation",
		"metadata": map[string]any{
			"name": name, "uid": "uid-1", "resourceVersion": "1",
		},
		"spec": map[string]any{
			"operationID": spec.OperationID, "tenant": spec.Tenant,
			"requestedBy": spec.RequestedBy, "instance": spec.Instance,
			"type": spec.Type, "parametersSHA256": spec.ParametersSHA256,
			"maxAttempts": spec.MaxAttempts,
		},
	}}
}

func authorizedPrincipal() Principal {
	return Principal{
		Subject: "user-123", Tenant: "tenant-a",
		Instances: map[string]struct{}{"instance-a": {}},
	}
}

func TestHandlerSubmitsImmutableTenantIdentityAndReturnsSanitizedObject(t *testing.T) {
	store := &memoryOperationStore{objects: make(map[string]*unstructured.Unstructured)}
	handler, err := NewHandler(staticAuthenticator{principal: authorizedPrincipal()}, store)
	require.NoError(t, err)
	body := `{
		"name":"backup-1","operation_id":"backup-1","tenant":"tenant-a",
		"instance":"instance-a","type":"Backup",
		"parameters_sha256":"` + strings.Repeat("a", 64) + `","max_attempts":3
	}`
	request := httptest.NewRequest(http.MethodPost, "/v1/operations", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusAccepted, response.Code)
	require.Equal(t, "tenant-a", store.spec.Tenant)
	require.Equal(t, "user-123", store.spec.RequestedBy)
	require.Equal(t, "instance-a", store.spec.Instance)
	var result operationResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Equal(t, "user-123", result.RequestedBy)
	require.NotContains(t, response.Body.String(), "parameters_secret")
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
}

func TestHandlerPreventsCrossTenantAndCrossInstanceEnumeration(t *testing.T) {
	store := &memoryOperationStore{objects: map[string]*unstructured.Unstructured{
		"other": operationObject("other", operationqueue.Spec{
			OperationID: "other", Tenant: "tenant-b", RequestedBy: "other-user",
			Instance: "instance-b", Type: "Backup", ParametersSHA256: strings.Repeat("b", 64), MaxAttempts: 3,
		}),
	}}
	handler, err := NewHandler(staticAuthenticator{principal: authorizedPrincipal()}, store)
	require.NoError(t, err)
	for _, test := range []struct {
		name   string
		method string
		target string
		body   string
	}{
		{name: "get other tenant", method: http.MethodGet, target: "/v1/operations/other"},
		{name: "submit other tenant", method: http.MethodPost, target: "/v1/operations", body: `{
			"name":"other","operation_id":"other","tenant":"tenant-b","instance":"instance-b",
			"type":"Backup","parameters_sha256":"` + strings.Repeat("b", 64) + `","max_attempts":3
		}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.target, strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer token")
			if test.method == http.MethodPost {
				request.Header.Set("Content-Type", "application/json")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusNotFound, response.Code)
		})
	}
}

func TestHandlerFailsClosedOnAuthenticationAndMalformedInput(t *testing.T) {
	store := &memoryOperationStore{objects: make(map[string]*unstructured.Unstructured)}
	denied, err := NewHandler(staticAuthenticator{err: errors.New("invalid token")}, store)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/v1/operations/missing", nil)
	response := httptest.NewRecorder()
	denied.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Contains(t, response.Header().Get("WWW-Authenticate"), "Bearer")

	allowed, err := NewHandler(staticAuthenticator{principal: authorizedPrincipal()}, store)
	require.NoError(t, err)
	request = httptest.NewRequest(http.MethodPost, "/v1/operations", strings.NewReader(`{"unknown":true}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	allowed.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)

	request = httptest.NewRequest(http.MethodPost, "/v1/operations", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "text/plain")
	response = httptest.NewRecorder()
	allowed.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnsupportedMediaType, response.Code)
}

func TestHandlerRejectsUnboundParameterSecret(t *testing.T) {
	store := &memoryOperationStore{objects: make(map[string]*unstructured.Unstructured)}
	handler, err := NewHandler(staticAuthenticator{principal: authorizedPrincipal()}, store)
	require.NoError(t, err)
	for _, fields := range []string{
		`"parameters_secret":"params-tenant-b-backup","parameters_key":"parameters.json",`,
		`"parameters_secret":"params-tenant-a-backup","parameters_key":"token",`,
		`"parameters_secret":"params-tenant-a-backup",`,
	} {
		body := `{
			"name":"backup-1","operation_id":"backup-1","tenant":"tenant-a",
			"instance":"instance-a","type":"Backup",` + fields + `
			"parameters_sha256":"` + strings.Repeat("a", 64) + `","max_attempts":3
		}`
		request := httptest.NewRequest(http.MethodPost, "/v1/operations", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.Equal(t, http.StatusBadRequest, response.Code)
	}
	require.Empty(t, store.objects)
}

func TestHandlerHealthDoesNotRequireIdentity(t *testing.T) {
	handler, err := NewHandler(staticAuthenticator{err: errors.New("must not be called")}, &memoryOperationStore{})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
}

func TestHTTPSAPIAuthenticatesOIDCTokenAndSubmitsOperation(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	fixture := &oidcFixture{keys: map[string]*rsa.PrivateKey{"key": key}}
	var oidcServer *httptest.Server
	oidcServer = httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fixture.serve(oidcServer.URL).ServeHTTP(response, request)
	}))
	defer oidcServer.Close()
	authenticator, err := NewOIDCAuthenticator(context.Background(), OIDCConfig{
		Issuer: oidcServer.URL, Audience: "kubebrain-operation-api",
		HTTPClient: oidcServer.Client(), CacheTTL: time.Minute,
	})
	require.NoError(t, err)
	store := &memoryOperationStore{objects: make(map[string]*unstructured.Unstructured)}
	handler, err := NewHandler(authenticator, store)
	require.NoError(t, err)
	apiServer := httptest.NewTLSServer(handler)
	defer apiServer.Close()

	body := `{
		"name":"backup-1","operation_id":"backup-1","tenant":"tenant-a",
		"instance":"instance-a","type":"Backup",
		"parameters_sha256":"` + strings.Repeat("a", 64) + `","max_attempts":3
	}`
	request, err := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/operations", strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+signOIDCToken(
		t, key, "key", oidcServer.URL, "kubebrain-operation-api", "tenant-a", []string{"instance-a"},
	))
	response, err := apiServer.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	require.Equal(t, "tenant-a", store.spec.Tenant)
	require.Equal(t, "user-123", store.spec.RequestedBy)
}
