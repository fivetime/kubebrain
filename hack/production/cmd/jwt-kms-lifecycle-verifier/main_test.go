package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifyEnforcesPromotionThenRevocationChain(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	operation := operationBytes(t, now.Add(-time.Minute))
	operationSum := sha256.Sum256(operation)
	operationSHA := hex.EncodeToString(operationSum[:])
	promotePayload := lifecycleReceipt{Format: "kubebrain.jwt-kms-lifecycle.v1", Action: "promote", RequestID: "change-1", OperationID: "jwt-key-rotate-0123456789abcdefabcd", Instance: "instance-a", OldVersionID: "kms/v/41", NewVersionID: "kms/v/42", OperationReceiptSHA256: operationSHA, PreviousReceiptSHA256: "", State: "new-primary", ObservedAtUnix: now.Unix() - 30, ExpiresAtUnix: now.Unix() + 300}
	promote := signedLifecycle(t, privateKey, promotePayload)
	require.NoError(t, verify("promote", promote, publicKey, operation, nil, now))
	promoteSum := sha256.Sum256(promote)
	revokePayload := promotePayload
	revokePayload.Action = "revoke"
	revokePayload.State = "old-revoked"
	revokePayload.PreviousReceiptSHA256 = hex.EncodeToString(promoteSum[:])
	revokePayload.ObservedAtUnix = now.Unix() - 10
	revoke := signedLifecycle(t, privateKey, revokePayload)
	require.NoError(t, verify("revoke", revoke, publicKey, operation, promote, now))
	require.ErrorContains(t, verify("revoke", revoke, publicKey, operation, nil, now), "requires")
	wrongPrevious := append([]byte(nil), promote...)
	wrongPrevious[len(wrongPrevious)-2] ^= 1
	require.ErrorContains(t, verify("revoke", revoke, publicKey, operation, wrongPrevious, now), "previous promotion")

	early := revokePayload
	early.ObservedAtUnix = promotePayload.ObservedAtUnix
	require.ErrorContains(t, verify("revoke", signedLifecycle(t, privateKey, early), publicKey, operation, promote, now), "after promotion")
}

func TestVerifyRejectsWrongOperationStateAndSignature(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	operation := operationBytes(t, now.Add(-time.Minute))
	sum := sha256.Sum256(operation)
	payload := lifecycleReceipt{Format: "kubebrain.jwt-kms-lifecycle.v1", Action: "promote", RequestID: "change-1", OperationID: "jwt-key-rotate-0123456789abcdefabcd", Instance: "instance-a", OldVersionID: "kms/v/41", NewVersionID: "kms/v/42", OperationReceiptSHA256: hex.EncodeToString(sum[:]), State: "old-revoked", ObservedAtUnix: now.Unix() - 10, ExpiresAtUnix: now.Unix() + 100}
	require.ErrorContains(t, verify("promote", signedLifecycle(t, privateKey, payload), publicKey, operation, nil, now), "binding")
	otherPublic, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.ErrorContains(t, verify("promote", signedLifecycle(t, privateKey, payload), otherPublic, operation, nil, now), "signature")
	operation[0] = ' '
	require.ErrorContains(t, verify("promote", signedLifecycle(t, privateKey, payload), publicKey, operation, nil, now), "canonical v3")
}

func operationBytes(t *testing.T, completed time.Time) []byte {
	t.Helper()
	sha := strings.Repeat("a", 64)
	receipt := operationReceipt{Attempt: 1, CompletedAtUnix: completed.Unix(), Format: "kubebrain.jwt-key-rotation.operation.receipt.v3", Instance: "instance-a", NewKeyVersionID: "kms/v/42", OldKeyVersionID: "kms/v/41", OperationID: "jwt-key-rotate-0123456789abcdefabcd", OperationUID: "operation-uid", ParametersSHA256: sha, PhaseAGateSHA: sha, PhaseAPublishSHA: sha, PhaseBGateSHA: sha, PhaseBPublishSHA: sha, PhaseCGateSHA: sha, PhaseCPublishSHA: sha, RequestID: "change-1"}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	return data
}
func signedLifecycle(t *testing.T, key ed25519.PrivateKey, receipt lifecycleReceipt) []byte {
	t.Helper()
	payload, err := json.Marshal(receipt)
	require.NoError(t, err)
	outer := envelope{Format: "kubebrain.jwt-kms-lifecycle-envelope.v1", Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))}
	data, err := json.Marshal(outer)
	require.NoError(t, err)
	return data
}
