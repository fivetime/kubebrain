package parameterbroker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationqueue"
	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const maxTokenBytes = 16 << 10

var serviceAccountTypes = map[string]string{
	"kubebrain-backup-executor":               "Backup",
	"kubebrain-backup-deletion-executor":      "BackupDeletion",
	"kubebrain-restore-cutover-executor":      "RestoreCutover",
	"kubebrain-post-restore-audit-executor":   "PostRestoreAudit",
	"kubebrain-certificate-rotation-executor": "CertificateRotation",
	"kubebrain-destroy-executor":              "Destroy",
}

type Handler struct {
	tokens            kubernetes.Interface
	dynamic           dynamic.Interface
	identityNamespace string
	audience          string
	requestTimeout    time.Duration
}

type parameterRequestIdentity struct {
	namespace string
	name      string
	owner     string
	attempt   int64
}

func NewHandler(
	tokens kubernetes.Interface, dynamicClient dynamic.Interface, namespace, audience string,
	requestTimeout time.Duration,
) (*Handler, error) {
	if tokens == nil || dynamicClient == nil {
		return nil, errors.New("kubernetes clients are required")
	}
	if namespace == "" || audience == "" {
		return nil, errors.New("namespace and audience are required")
	}
	if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
		return nil, errors.New("invalid parameter broker namespace " + namespace + ": " + problems[0])
	}
	if requestTimeout <= 0 {
		return nil, errors.New("request timeout must be positive")
	}
	return &Handler{
		tokens: tokens, dynamic: dynamicClient, identityNamespace: namespace, audience: audience,
		requestTimeout: requestTimeout,
	}, nil
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if request.Method == http.MethodGet && request.URL.Path == "/healthz" {
		response.WriteHeader(http.StatusNoContent)
		return
	}
	if request.Method != http.MethodGet || request.URL.Path != "/v1/parameters" {
		http.Error(response, "not found", http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), h.requestTimeout)
	defer cancel()
	request = request.WithContext(ctx)
	token, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		http.Error(response, "unauthorized", http.StatusUnauthorized)
		return
	}
	identity, err := parameterIdentityFromQuery(request.URL.Query())
	if err != nil {
		http.Error(response, "namespace, name, owner, and positive attempt are required", http.StatusBadRequest)
		return
	}
	operationType, err := h.authenticate(request, token)
	if err != nil {
		http.Error(response, "unauthorized", http.StatusUnauthorized)
		return
	}
	parameters, err := operationqueue.New(h.dynamic, identity.namespace).
		ParametersForWorker(request.Context(), identity.name, operationType, identity.owner, identity.attempt)
	if err != nil {
		http.Error(response, "parameters unavailable", http.StatusForbidden)
		return
	}
	if err := validateParametersJSON(parameters); err != nil {
		http.Error(response, "parameters unavailable", http.StatusForbidden)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write(parameters)
}

func validateParametersJSON(parameters []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(parameters))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil || document == nil {
		return errors.New("operation parameters must be a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("operation parameters must contain one JSON object")
	}
	return nil
}

func parameterIdentityFromQuery(query url.Values) (parameterRequestIdentity, error) {
	if len(query) != 4 {
		return parameterRequestIdentity{}, errors.New("parameter request identity is invalid")
	}
	name, nameOK := requiredQueryValue(query, "name")
	namespace, namespaceOK := requiredQueryValue(query, "namespace")
	owner, ownerOK := requiredQueryValue(query, "owner")
	rawAttempt, attemptOK := requiredQueryValue(query, "attempt")
	attempt, err := strconv.ParseInt(rawAttempt, 10, 64)
	if len(validation.IsDNS1123Label(namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(name)) != 0 ||
		!nameOK || !namespaceOK || !ownerOK || !attemptOK ||
		name == "" || err != nil {
		return parameterRequestIdentity{}, errors.New("parameter request identity is invalid")
	}
	if err := operationqueue.ValidateWorkerIdentity(owner, attempt); err != nil {
		return parameterRequestIdentity{}, errors.New("parameter request identity is invalid")
	}
	return parameterRequestIdentity{
		namespace: namespace,
		name:      name,
		owner:     owner,
		attempt:   attempt,
	}, nil
}

func requiredQueryValue(query url.Values, name string) (string, bool) {
	values, found := query[name]
	if !found || len(values) != 1 {
		return "", false
	}
	return values[0], true
}

// Ready verifies every Kubernetes API path required to serve a parameter
// request. Missing probe objects are expected; transport, discovery, and RBAC
// failures make the broker unready.
func (h *Handler) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, h.requestTimeout)
	defer cancel()
	const probeName = "kubebrain-readiness-probe-do-not-create"
	if _, err := h.dynamic.Resource(operationqueue.Resource).Namespace(h.identityNamespace).
		Get(ctx, probeName, metav1.GetOptions{}); err != nil && !isExpectedProbeNotFound(err, probeName) {
		return fmt.Errorf("probe operation API: %w", err)
	}
	if _, err := h.dynamic.Resource(operationqueue.SecretResource).Namespace(h.identityNamespace).
		Get(ctx, probeName, metav1.GetOptions{}); err != nil && !isExpectedProbeNotFound(err, probeName) {
		return fmt.Errorf("probe Secret API: %w", err)
	}
	if _, err := h.tokens.AuthenticationV1().TokenReviews().Create(
		ctx,
		&authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{
			Token: "kubebrain-readiness-invalid-token", Audiences: []string{h.audience},
		}},
		metav1.CreateOptions{},
	); err != nil {
		return fmt.Errorf("probe TokenReview API: %w", err)
	}
	return nil
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

func (h *Handler) authenticate(request *http.Request, token string) (string, error) {
	review, err := h.tokens.AuthenticationV1().TokenReviews().Create(
		request.Context(),
		&authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{
			Token: token, Audiences: []string{h.audience},
		}},
		metav1.CreateOptions{},
	)
	if err != nil || !review.Status.Authenticated || review.Status.Error != "" {
		return "", errors.New("token review failed")
	}
	if !contains(review.Status.Audiences, h.audience) {
		return "", errors.New("token audience was not authenticated")
	}
	prefix := fmt.Sprintf("system:serviceaccount:%s:", h.identityNamespace)
	serviceAccount := strings.TrimPrefix(review.Status.User.Username, prefix)
	if serviceAccount == review.Status.User.Username {
		return "", errors.New("caller is not an executor service account")
	}
	operationType, ok := serviceAccountTypes[serviceAccount]
	if !ok {
		return "", errors.New("caller service account is not allowed")
	}
	return operationType, nil
}

func bearerToken(header string) (string, error) {
	if strings.TrimSpace(header) != header {
		return "", errors.New("invalid bearer token")
	}
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", errors.New("bearer token is required")
	}
	if token == "" || len(token) > maxTokenBytes ||
		strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("invalid bearer token")
	}
	return token, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
