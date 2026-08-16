package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/stretchr/testify/require"
)

func TestBuildBRArgsUsesExplicitWholeClusterTxnImport(t *testing.T) {
	args := buildBRArgs([]string{"pd-a:2379", "pd-b:2379"}, "/verified/full", "/tls/ca", "/tls/cert", "/tls/key", nativepitr.EncryptionIdentity{Method: nativepitr.CipherMethodPlaintext}, "")
	require.Equal(t, []string{"restore", "txn", "--pd", "pd-a:2379,pd-b:2379", "--storage", "local:///verified/full", "--send-credentials-to-tikv=false", "--check-requirements=true", "--checksum=false", "--log-file", "/dev/stderr", "--crypter.method=plaintext", "--ca", "/tls/ca", "--cert", "/tls/cert", "--key", "/tls/key"}, args)
	for _, forbidden := range []string{"point", "--start", "--end", "--restored-ts", "--full-backup-storage"} {
		require.NotContains(t, args, forbidden)
	}
}

func TestRunWithAdmissionMonitorCancelsImportOnFenceLoss(t *testing.T) {
	var checks atomic.Int64
	runStarted := make(chan struct{})
	err := runWithAdmissionMonitor(context.Background(), time.Millisecond, func(context.Context) error {
		checks.Add(1)
		return errors.New("gate missing")
	}, func(ctx context.Context) error {
		close(runStarted)
		<-ctx.Done()
		return ctx.Err()
	})
	require.ErrorContains(t, err, "restore admission lost during BR import")
	require.GreaterOrEqual(t, checks.Load(), int64(1))
	select {
	case <-runStarted:
	default:
		t.Fatal("import did not start")
	}
}

func TestRunWithAdmissionMonitorStopsAfterSuccessfulImport(t *testing.T) {
	var checks atomic.Int64
	checked := make(chan struct{}, 1)
	err := runWithAdmissionMonitor(context.Background(), time.Millisecond, func(context.Context) error {
		checks.Add(1)
		select {
		case checked <- struct{}{}:
		default:
		}
		return nil
	}, func(context.Context) error {
		<-checked
		return nil
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, checks.Load(), int64(1))
}

func TestBuildBRArgsUsesPlanBoundAES256KeyFile(t *testing.T) {
	identity := nativepitr.EncryptionIdentity{Method: nativepitr.CipherMethodAES256CTR, KeyID: "kms/prod/backup/versions/7"}
	args := buildBRArgs([]string{"pd:2379"}, "/verified/full", "", "", "", identity, "/run/secrets/backup/key")
	require.Contains(t, args, "--crypter.method=aes256-ctr")
	require.Contains(t, args, "--crypter.key-file=/run/secrets/backup/key")
	require.NotContains(t, args, identity.KeyID, "the non-secret version ID is evidence, not a BR key argument")
}
func TestValidateOptionsFailsClosed(t *testing.T) {
	good := options{plan: "p", full: "f", artifacts: "a", inventory: "i", artifactRoot: "/mirror", sourceExclusive: "s", target: "t", targetProvisioning: "tp", targetQualification: "tq", writerExclusion: "we", admission: "d", pdAddrs: "pd:2379", brBinary: "br", admissionCheckInterval: 100 * time.Millisecond, timeout: time.Second}
	require.NoError(t, validateOptions(good))
	bad := good
	bad.artifactRoot = "relative"
	require.ErrorContains(t, validateOptions(bad), "absolute")
	bad = good
	bad.cert = "cert"
	require.ErrorContains(t, validateOptions(bad), "together")
	bad = good
	bad.admissionCheckInterval = bad.timeout
	require.ErrorContains(t, validateOptions(bad), "shorter than timeout")
}
func TestPinnedBRAndAddressParsing(t *testing.T) {
	require.True(t, pinnedBR("Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\nGit Branch: x"))
	require.False(t, pinnedBR("Release Version: v7.5.1\nGit Commit Hash: other\n"))
	got, err := parseAddrs("pd-b:2379,pd-a:2379")
	require.NoError(t, err)
	require.Equal(t, []string{"pd-a:2379", "pd-b:2379"}, got)
	_, err = parseAddrs("pd:2379,pd:2379")
	require.Error(t, err)
}

func TestReadSmallRejectsOversizedReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxReceiptBytes+1), 0o600))
	_, err := readSmall(path)
	require.ErrorContains(t, err, "exceeds")
}

func TestFileDigestHashesExactContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "br")
	require.NoError(t, os.WriteFile(path, []byte("pinned-br"), 0o700))
	got, err := fileDigest(path)
	require.NoError(t, err)
	require.Equal(t, digest([]byte("pinned-br")), got)
}
