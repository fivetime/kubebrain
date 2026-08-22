package main

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateIAMSimulationExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	require.NoError(t, validateIAMSimulationExpiry("1700000001", now))
	for _, raw := range []string{"", "pending", "0", "1699999999", "1700000000"} {
		t.Run(raw, func(t *testing.T) {
			require.Error(t, validateIAMSimulationExpiry(raw, now))
		})
	}
}

func TestValidateCredentialSecretDataDigest(t *testing.T) {
	env := map[string]string{
		"AWS_ACCESS_KEY_ID": "access", "S3_BUCKET": "audit-bucket", "S3_ENDPOINT": "https://s3.example",
		"S3_FORCE_PATH_STYLE": "false", "OBJECT_STORE_ID": "store-a", "AWS_REGION": "us-east-1", "AWS_SECRET_ACCESS_KEY": "secret",
	}
	lookup := func(key string) string { return env[key] }
	expected := sha256.Sum256([]byte(`{"access-key-id":"YWNjZXNz","bucket":"YXVkaXQtYnVja2V0","endpoint":"aHR0cHM6Ly9zMy5leGFtcGxl","force-path-style":"ZmFsc2U=","object-store-id":"c3RvcmUtYQ==","region":"dXMtZWFzdC0x","secret-access-key":"c2VjcmV0"}`))
	require.NoError(t, validateCredentialSecretDataDigest(fmt.Sprintf("%x", expected), lookup))

	for _, tc := range []struct {
		name, digest, missing string
	}{
		{name: "missing digest"},
		{name: "uppercase digest", digest: fmt.Sprintf("%X", expected)},
		{name: "wrong digest", digest: fmt.Sprintf("%064x", 1)},
		{name: "missing credential", digest: fmt.Sprintf("%x", expected), missing: "AWS_ACCESS_KEY_ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := func(key string) string {
				if key == tc.missing {
					return ""
				}
				return lookup(key)
			}
			require.Error(t, validateCredentialSecretDataDigest(tc.digest, candidate))
		})
	}
}
