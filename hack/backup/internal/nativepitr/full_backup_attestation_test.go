package nativepitr

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

const pinnedBRVersionText = "Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\nGit Branch: refs/tags/v7.5.1\n"

func TestFullBackupAttestationRequiresExplicitCanonicalPlaintextInvocation(t *testing.T) {
	args := []string{"backup", "txn", "--storage=s3://bucket/full/a", "--backupts=120", "--crypter.method=plaintext"}
	receipt, err := BuildFullBackupAttestation(pinnedBRVersionText, digest, digest, "s3://bucket/full/a", 120, args, 2_000_000_000)
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
		func(r *FullBackupAttestation) { r.BRVersion = "Release Version: v8.5.3\n" },
	} {
		bad := receipt
		bad.CanonicalArgs = append([]string(nil), receipt.CanonicalArgs...)
		mutate(&bad)
		require.Error(t, bad.Validate())
	}
}
