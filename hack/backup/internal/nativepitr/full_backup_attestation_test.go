package nativepitr

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullBackupAttestationRequiresExplicitCanonicalPlaintextInvocation(t *testing.T) {
	args := []string{"backup", "txn", "--storage=s3://bucket/full/a", "--backupts=120", "--crypter.method=plaintext"}
	receipt, err := BuildFullBackupAttestation(digest, digest, "s3://bucket/full/a", digest, 120, args, 2_000_000_000)
	require.NoError(t, err)
	require.Equal(t, "plaintext", receipt.CipherMethod)

	var encoded bytes.Buffer
	require.NoError(t, json.NewEncoder(&encoded).Encode(receipt))
	_, err = DecodeFullBackupAttestation(&encoded)
	require.NoError(t, err)

	for _, mutate := range []func(*FullBackupAttestation){
		func(r *FullBackupAttestation) {
			r.CanonicalArgs[4] = "--crypter.method=aes256-ctr"
			r.CanonicalArgsSHA256 = digestStringSlice(r.CanonicalArgs)
		},
		func(r *FullBackupAttestation) {
			r.CanonicalArgs = append(r.CanonicalArgs, "--crypter.key=secret")
			r.CanonicalArgsSHA256 = digestStringSlice(r.CanonicalArgs)
		},
		func(r *FullBackupAttestation) {
			r.CanonicalArgs[2] = "--storage=s3://user:secret@bucket/full/a"
			r.CanonicalArgsSHA256 = digestStringSlice(r.CanonicalArgs)
		},
		func(r *FullBackupAttestation) { r.CanonicalArgsSHA256 = digest },
		func(r *FullBackupAttestation) { r.ExitSuccessful = false },
	} {
		bad := receipt
		bad.CanonicalArgs = append([]string(nil), receipt.CanonicalArgs...)
		mutate(&bad)
		require.Error(t, bad.Validate())
	}
}
