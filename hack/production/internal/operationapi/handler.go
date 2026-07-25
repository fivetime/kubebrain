package operationapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const requestBodyLimit = 64 << 10

type OperationStore interface {
	Submit(context.Context, string, operationqueue.Spec) (*unstructured.Unstructured, error)
	Get(context.Context, string) (*unstructured.Unstructured, error)
}

type Handler struct {
	authenticator  Authenticator
	store          OperationStore
	mux            *http.ServeMux
	requestTimeout time.Duration
}

type submitRequest struct {
	Name             string `json:"name"`
	OperationID      string `json:"operation_id"`
	Tenant           string `json:"tenant"`
	Instance         string `json:"instance"`
	Type             string `json:"type"`
	ParametersSHA256 string `json:"parameters_sha256"`
	ParametersSecret string `json:"parameters_secret,omitempty"`
	ParametersKey    string `json:"parameters_key,omitempty"`
	MaxAttempts      int64  `json:"max_attempts"`
}

type operationResponse struct {
	Name             string `json:"name"`
	UID              string `json:"uid,omitempty"`
	ResourceVersion  string `json:"resource_version,omitempty"`
	OperationID      string `json:"operation_id"`
	Tenant           string `json:"tenant"`
	RequestedBy      string `json:"requested_by"`
	Instance         string `json:"instance"`
	Type             string `json:"type"`
	ParametersSHA256 string `json:"parameters_sha256"`
	MaxAttempts      int64  `json:"max_attempts"`
	Phase            string `json:"phase,omitempty"`
	Attempt          int64  `json:"attempt,omitempty"`
	Message          string `json:"message,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func NewHandler(
	authenticator Authenticator, store OperationStore, requestTimeout time.Duration,
) (*Handler, error) {
	if authenticator == nil || store == nil {
		return nil, errors.New("operation API authenticator and store are required")
	}
	if requestTimeout <= 0 {
		return nil, errors.New("operation API request timeout must be positive")
	}
	handler := &Handler{
		authenticator: authenticator, store: store, mux: http.NewServeMux(),
		requestTimeout: requestTimeout,
	}
	handler.mux.HandleFunc("GET /healthz", handler.health)
	handler.mux.HandleFunc("POST /v1/operations", handler.submit)
	handler.mux.HandleFunc("GET /v1/operations/{name}", handler.get)
	return handler, nil
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if request.URL.Path != "/healthz" {
		ctx, cancel := context.WithTimeout(request.Context(), h.requestTimeout)
		defer cancel()
		request = request.WithContext(ctx)
	}
	h.mux.ServeHTTP(response, request)
}

func (h *Handler) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	const probeName = "kubebrain-readiness-probe-do-not-create"
	_, err := h.store.Get(ctx, probeName)
	if err == nil || isExpectedProbeNotFound(err, probeName) {
		return nil
	}
	return err
}

func isExpectedProbeNotFound(err error, probeName string) bool {
	if !apierrors.IsNotFound(err) {
		return false
	}
	statusError, ok := err.(apierrors.APIStatus)
	if !ok {
		return false
	}
	details := statusError.Status().Details
	return details != nil && details.Name == probeName
}

func (h *Handler) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) principal(response http.ResponseWriter, request *http.Request) (Principal, bool) {
	principal, err := h.authenticator.Authenticate(request.Context(), request.Header.Get("Authorization"))
	if err != nil {
		if dependencyContextError(err) {
			writeJSON(response, http.StatusServiceUnavailable, errorResponse{Error: "authentication dependency unavailable"})
			return Principal{}, false
		}
		response.Header().Set("WWW-Authenticate", `Bearer realm="kubebrain-operation-api"`)
		writeJSON(response, http.StatusUnauthorized, errorResponse{Error: "unauthorized"})
		return Principal{}, false
	}
	return principal, true
}

func (h *Handler) submit(response http.ResponseWriter, request *http.Request) {
	if !validateNoQuery(response, request) {
		return
	}
	input, ok := decodeSubmitRequest(response, request)
	if !ok {
		return
	}
	if !validateRequestOperationName(response, input.Name) {
		return
	}
	principal, ok := h.principal(response, request)
	if !ok {
		return
	}
	if input.Tenant != principal.Tenant || !principal.Allows(input.Instance) {
		writeJSON(response, http.StatusNotFound, errorResponse{Error: "operation target not found"})
		return
	}
	if !authorizedParametersSecret(input, principal.Tenant) {
		writeJSON(response, http.StatusBadRequest, errorResponse{Error: "operation parameter reference is invalid"})
		return
	}
	object, err := h.store.Submit(request.Context(), input.Name, operationqueue.Spec{
		OperationID: input.OperationID, Tenant: input.Tenant, RequestedBy: principal.Subject,
		Instance: input.Instance, Type: input.Type, ParametersSHA256: input.ParametersSHA256,
		ParametersSecret: input.ParametersSecret, ParametersKey: input.ParametersKey,
		MaxAttempts: input.MaxAttempts,
	})
	if err != nil {
		switch {
		case dependencyContextError(err):
			writeJSON(response, http.StatusServiceUnavailable, errorResponse{Error: "operation dependency unavailable"})
		case apierrors.IsConflict(err), strings.Contains(err.Error(), "different immutable spec"):
			writeJSON(response, http.StatusConflict, errorResponse{Error: "operation conflicts with an existing request"})
		case errors.Is(err, operationqueue.ErrInvalidSpec), apierrors.IsInvalid(err),
			strings.Contains(err.Error(), "invalid operation"),
			strings.Contains(err.Error(), "operation spec is incomplete"),
			strings.Contains(err.Error(), "specified together"):
			writeJSON(response, http.StatusBadRequest, errorResponse{Error: "operation request is invalid"})
		default:
			writeJSON(response, http.StatusInternalServerError, errorResponse{Error: "operation submission failed"})
		}
		return
	}
	writeJSON(response, http.StatusAccepted, summarizeOperation(object))
}

func decodeSubmitRequest(response http.ResponseWriter, request *http.Request) (submitRequest, bool) {
	contentType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		writeJSON(response, http.StatusUnsupportedMediaType, errorResponse{Error: "content type must be application/json"})
		return submitRequest{}, false
	}
	body := http.MaxBytesReader(response, request.Body, requestBodyLimit)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var input submitRequest
	if err := decoder.Decode(&input); err != nil {
		writeJSON(response, http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return submitRequest{}, false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		writeJSON(response, http.StatusBadRequest, errorResponse{Error: "request body must contain one JSON object"})
		return submitRequest{}, false
	}
	return input, true
}

func authorizedParametersSecret(input submitRequest, tenant string) bool {
	if input.ParametersSecret == "" && input.ParametersKey == "" {
		return true
	}
	return input.ParametersSecret != "" &&
		input.ParametersKey == "parameters.json" &&
		strings.HasPrefix(input.ParametersSecret, authorizedParameterSecretPrefix(tenant)) &&
		operationqueue.ValidParameterSecretName(input.ParametersSecret)
}

func authorizedParameterSecretPrefix(tenant string) string {
	return "params-l" + strconv.Itoa(len(tenant)) + "-" + tenant + "-"
}

func (h *Handler) get(response http.ResponseWriter, request *http.Request) {
	if !validateNoQuery(response, request) {
		return
	}
	name := request.PathValue("name")
	if !validateRequestOperationName(response, name) {
		return
	}
	principal, ok := h.principal(response, request)
	if !ok {
		return
	}
	object, err := h.store.Get(request.Context(), name)
	if err != nil {
		if dependencyContextError(err) {
			writeJSON(response, http.StatusServiceUnavailable, errorResponse{Error: "operation dependency unavailable"})
		} else if apierrors.IsNotFound(err) {
			writeJSON(response, http.StatusNotFound, errorResponse{Error: "operation not found"})
		} else {
			writeJSON(response, http.StatusInternalServerError, errorResponse{Error: "operation lookup failed"})
		}
		return
	}
	tenant, _, _ := unstructured.NestedString(object.Object, "spec", "tenant")
	instance, _, _ := unstructured.NestedString(object.Object, "spec", "instance")
	if tenant != principal.Tenant || !principal.Allows(instance) {
		writeJSON(response, http.StatusNotFound, errorResponse{Error: "operation not found"})
		return
	}
	writeJSON(response, http.StatusOK, summarizeOperation(object))
}

func validateNoQuery(response http.ResponseWriter, request *http.Request) bool {
	if request.URL.RawQuery != "" {
		writeJSON(response, http.StatusBadRequest, errorResponse{Error: "query parameters are not supported"})
		return false
	}
	return true
}

func validateRequestOperationName(response http.ResponseWriter, name string) bool {
	if err := operationqueue.ValidateOperationName(name); err != nil {
		writeJSON(response, http.StatusBadRequest, errorResponse{Error: "operation name is invalid"})
		return false
	}
	return true
}

func dependencyContextError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrOIDCUnavailable) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsForbidden(err) ||
		apierrors.IsUnauthorized(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsInternalError(err)
}

func summarizeOperation(object *unstructured.Unstructured) operationResponse {
	text := func(fields ...string) string {
		value, _, _ := unstructured.NestedString(object.Object, fields...)
		return value
	}
	number := func(fields ...string) int64 {
		value, _, _ := unstructured.NestedInt64(object.Object, fields...)
		return value
	}
	return operationResponse{
		Name: object.GetName(), UID: string(object.GetUID()), ResourceVersion: object.GetResourceVersion(),
		OperationID: text("spec", "operationID"), Tenant: text("spec", "tenant"),
		RequestedBy: text("spec", "requestedBy"), Instance: text("spec", "instance"),
		Type: text("spec", "type"), ParametersSHA256: text("spec", "parametersSHA256"),
		MaxAttempts: number("spec", "maxAttempts"), Phase: text("status", "phase"),
		Attempt: number("status", "attempt"), Message: text("status", "message"),
	}
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
