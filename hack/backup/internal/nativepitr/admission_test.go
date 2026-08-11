package nativepitr

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestoreAdmissionReceiptBindsPlanAndToken(t *testing.T) {
	plan := validReceiptPlan(t)
	receipt, token, err := BuildRestoreAdmissionReceipt(plan, digest, "restore-1", 123, false)
	require.NoError(t, err)
	want, err := token.SHA256()
	require.NoError(t, err)
	require.Equal(t, want, receipt.TokenSHA256)
	var encoded bytes.Buffer
	require.NoError(t, json.NewEncoder(&encoded).Encode(receipt))
	_, err = DecodeRestoreAdmissionReceipt(&encoded)
	require.NoError(t, err)
}
