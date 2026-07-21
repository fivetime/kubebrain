package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
)

func TestRequireGrantedTTL(t *testing.T) {
	tests := []struct {
		name      string
		lease     *record.Lease
		wantError string
	}{
		{name: "current lease", lease: &record.Lease{ID: 1, TTL: 30, GrantedTTL: 60}},
		{name: "legacy lease", lease: &record.Lease{ID: 1, TTL: 30}, wantError: "lacks a valid granted_ttl"},
		{name: "no leases"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "backup.jsonl")
			writer, err := backupfile.NewAtomicWriter(path, "/registry", 42)
			require.NoError(t, err)
			if tc.lease != nil {
				require.NoError(t, writer.AddLease(*tc.lease))
			}
			_, err = writer.Commit()
			require.NoError(t, err)

			err = requireGrantedTTL(path)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
		})
	}
}

func TestValidateCompletion(t *testing.T) {
	now := time.Unix(2000, 0)
	status := backupfile.Status{Prefix: "/registry", Records: 10, CreatedAtUnix: 1900}

	t.Setenv("EXPECTED_PREFIX", "/registry")
	t.Setenv("MIN_RECORDS", "10")
	t.Setenv("MAX_AGE_SECONDS", "100")
	require.NoError(t, validateCompletion(status, now))
}

func TestValidateCompletionRejectsInvalidArtifactContract(t *testing.T) {
	now := time.Unix(2000, 0)
	base := backupfile.Status{Prefix: "/registry", Records: 10, CreatedAtUnix: 1900}
	tests := []struct {
		name      string
		status    backupfile.Status
		env       map[string]string
		wantError string
	}{
		{name: "wrong prefix", status: base, env: map[string]string{"EXPECTED_PREFIX": "/other"}, wantError: "expected prefix"},
		{name: "too few records", status: base, env: map[string]string{"MIN_RECORDS": "11"}, wantError: "at least 11"},
		{name: "missing timestamp", status: backupfile.Status{Prefix: "/registry", Records: 10}, env: map[string]string{"MAX_AGE_SECONDS": "100"}, wantError: "does not contain"},
		{name: "stale", status: base, env: map[string]string{"MAX_AGE_SECONDS": "99"}, wantError: "maximum is 99"},
		{name: "future", status: backupfile.Status{CreatedAtUnix: 2301}, env: map[string]string{"MAX_AGE_SECONDS": "100"}, wantError: "in the future"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{"EXPECTED_PREFIX", "MIN_RECORDS", "MAX_AGE_SECONDS"} {
				t.Setenv(key, "")
			}
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			err := validateCompletion(tc.status, now)
			require.ErrorContains(t, err, tc.wantError)
		})
	}
}

func TestValidateCompletionRejectsInvalidConfiguration(t *testing.T) {
	status := backupfile.Status{CreatedAtUnix: time.Now().Unix()}
	for key, value := range map[string]string{
		"MIN_RECORDS":     "-1",
		"MAX_AGE_SECONDS": "0",
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("MIN_RECORDS", "")
			t.Setenv("MAX_AGE_SECONDS", "")
			require.NoError(t, os.Setenv(key, value))
			t.Cleanup(func() { _ = os.Unsetenv(key) })
			require.Error(t, validateCompletion(status, time.Now()))
		})
	}
}
