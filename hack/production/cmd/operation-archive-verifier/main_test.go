package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/reconcilebudget"
	"github.com/stretchr/testify/require"
)

func TestParseIAMSimulationExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	validUntil, err := parseIAMSimulationExpiry("1700000001", now)
	require.NoError(t, err)
	require.Equal(t, time.Unix(1_700_000_001, 0), validUntil)
	validUntil, err = parseIAMSimulationExpiry("1700086700", now)
	require.NoError(t, err)
	require.Equal(t, time.Unix(1_700_086_700, 0), validUntil)
	for _, raw := range []string{"", "pending", "0", "1699999999", "1700000000", "1700086701"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parseIAMSimulationExpiry(raw, now)
			require.Error(t, err)
		})
	}
}

func TestValidateRetentionConfiguration(t *testing.T) {
	require.NoError(t, validateRetentionConfiguration(365*24*time.Hour, 30*24*time.Hour, 32))
	require.NoError(t, validateRetentionConfiguration(365*24*time.Hour, 0, 32))
	for _, tc := range []struct {
		retention, deleteAfter time.Duration
		batch                  int
	}{
		{0, 0, 1},
		{time.Hour, -time.Second, 1},
		{time.Hour, time.Hour, 1},
		{time.Hour, 2 * time.Hour, 1},
		{time.Hour, 0, 0},
	} {
		require.Error(t, validateRetentionConfiguration(tc.retention, tc.deleteAfter, tc.batch))
	}
}

func TestEvidenceBoundReconcileTimeout(t *testing.T) {
	now := time.Unix(1_700_000_000, 500_000_000)
	for _, tc := range []struct {
		name       string
		configured time.Duration
		validUntil time.Time
		want       time.Duration
		wantErr    bool
	}{
		{name: "configured budget first", configured: 15 * time.Minute, validUntil: now.Add(time.Hour), want: 15 * time.Minute},
		{name: "evidence expiry first", configured: 15 * time.Minute, validUntil: now.Add(45 * time.Second), want: 45 * time.Second},
		{name: "equal", configured: time.Minute, validUntil: now.Add(time.Minute), want: time.Minute},
		{name: "expired", configured: time.Minute, validUntil: now, wantErr: true},
		{name: "invalid configured", configured: 0, validUntil: now.Add(time.Minute), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := evidenceBoundReconcileTimeout(tc.configured, tc.validUntil, now)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestEvidenceBoundReconcileTimeoutCancelsRunAtExpiry(t *testing.T) {
	now := time.Now()
	timeout, err := evidenceBoundReconcileTimeout(time.Minute, now.Add(20*time.Millisecond), now)
	require.NoError(t, err)
	require.Less(t, timeout, time.Minute)
	_, err = reconcilebudget.Run(context.Background(), timeout, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
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
