package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMutateUsesDedicatedTLSAPIAndChainsPromotion(t *testing.T) {
	operation := operationBytes(t)
	operationSum := sha256Bytes(operation)
	promotion := []byte(`{"format":"kubebrain.jwt-kms-lifecycle-envelope.v1","payload":"e30=","signature":"c2ln"}`)
	promotionSum := sha256Bytes(promotion)
	for _, action := range []string{"promote", "revoke"} {
		t.Run(action, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				require.Equal(t, "/v1/jwt-key-versions:"+action, request.URL.Path)
				require.Equal(t, "Bearer lifecycle-token", request.Header.Get("Authorization"))
				var input lifecycleRequest
				require.NoError(t, json.NewDecoder(request.Body).Decode(&input))
				require.Equal(t, hex.EncodeToString(operationSum), input.OperationReceiptSHA256)
				if action == "promote" {
					require.Empty(t, input.PreviousReceiptSHA256)
				} else {
					require.Equal(t, hex.EncodeToString(promotionSum), input.PreviousReceiptSHA256)
				}
				response.Header().Set("Content-Type", "application/json; charset=utf-8")
				require.NoError(t, json.NewEncoder(response).Encode(lifecycleResponse{Format: "kubebrain.jwt-kms-lifecycle-response.v1", Receipt: promotion}))
			}))
			defer server.Close()
			dir := t.TempDir()
			operationPath, tokenPath, caPath := filepath.Join(dir, "operation.json"), filepath.Join(dir, "token"), filepath.Join(dir, "ca.pem")
			require.NoError(t, os.WriteFile(operationPath, operation, 0o600))
			require.NoError(t, os.WriteFile(tokenPath, []byte("lifecycle-token\n"), 0o600))
			require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o644))
			previousPath := ""
			if action == "revoke" {
				previousPath = filepath.Join(dir, "promotion.json")
				require.NoError(t, os.WriteFile(previousPath, promotion, 0o600))
			}
			receipt, err := mutate(t.Context(), action, server.URL, tokenPath, caPath, operationPath, previousPath)
			require.NoError(t, err)
			require.JSONEq(t, string(promotion), string(receipt))
		})
	}
}

func TestMutateRejectsMissingPredecessorAndProviderDrift(t *testing.T) {
	dir := t.TempDir()
	operationPath := filepath.Join(dir, "operation.json")
	require.NoError(t, os.WriteFile(operationPath, operationBytes(t), 0o600))
	_, err := mutate(t.Context(), "revoke", "https://provider.example", "missing", "missing", operationPath, "")
	require.ErrorContains(t, err, "requires the promotion")
	for _, endpoint := range []string{"http://provider.example", "https://user@provider.example", "https://provider.example/path"} {
		_, err := mutate(t.Context(), "promote", endpoint, "missing", "missing", operationPath, "")
		require.ErrorContains(t, err, "HTTPS origin")
	}
}

func TestLifecycleWriteIsPrivateAndNoClobber(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "receipt")
	require.NoError(t, writeNoClobber(path, []byte("receipt")))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Error(t, writeNoClobber(path, []byte("changed")))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "receipt", string(data))
}

func operationBytes(t *testing.T) []byte {
	t.Helper()
	sha := strings.Repeat("a", 64)
	receipt := operationReceipt{Attempt: 1, CompletedAtUnix: time.Now().Add(-time.Minute).Unix(), Format: "kubebrain.jwt-key-rotation.operation.receipt.v3", Instance: "instance-a", NewKeyVersionID: "kms/v/42", OldKeyVersionID: "kms/v/41", OperationID: "jwt-key-rotate-0123456789abcdefabcd", OperationUID: "uid", ParametersSHA256: sha, PhaseAGateSHA: sha, PhaseAPublishSHA: sha, PhaseBGateSHA: sha, PhaseBPublishSHA: sha, PhaseCGateSHA: sha, PhaseCPublishSHA: sha, RequestID: "change-1"}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	return data
}
func sha256Bytes(data []byte) []byte { sum := sha256.Sum256(data); return sum[:] }
