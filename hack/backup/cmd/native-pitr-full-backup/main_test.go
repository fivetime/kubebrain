package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/nativepitr"
	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	err     error
	version string
	hook    func()
	name    string
	args    []string
}

func (r *fakeRunner) Output(_ context.Context, _ *os.File, _ string, _ ...string) ([]byte, error) {
	version := r.version
	if version == "" {
		version = pinnedBRVersion
	}
	return []byte(version), nil
}

func (r *fakeRunner) Run(_ context.Context, _ *os.File, name string, args []string, _, _ io.Writer) error {
	r.name, r.args = name, append([]string(nil), args...)
	if r.hook != nil {
		r.hook()
	}
	return r.err
}

const pinnedBRVersion = "Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\nGit Branch: refs/tags/v7.5.1\n"

func TestOSRunnerExecutesHashedOpenInodeAfterPathReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "br")
	trueBytes, err := os.ReadFile("/bin/true")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, trueBytes, 0o700))
	opened, resolved, _, err := openExecutable(path)
	require.NoError(t, err)
	defer opened.Close()
	falseBytes, err := os.ReadFile("/bin/false")
	require.NoError(t, err)
	replacement := filepath.Join(dir, "replacement")
	require.NoError(t, os.WriteFile(replacement, falseBytes, 0o700))
	require.NoError(t, os.Rename(replacement, path))
	require.NoError(t, (osRunner{}).Run(context.Background(), opened, resolved, nil, io.Discard, io.Discard))
}

func TestExecuteRunsCanonicalPlaintextBackupAndAttestsSuccess(t *testing.T) {
	dir := t.TempDir()
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("pinned-br-binary"), 0o700))
	output := filepath.Join(dir, "attestation.json")
	runner := &fakeRunner{}
	o := options{brBinary: br, pdAddrs: "pd-b:2379,pd-a:2379", storage: "s3://bucket/immutable/full-a", backupTS: 120, output: output, timeout: time.Minute}
	require.NoError(t, execute(context.Background(), o, runner, &bytes.Buffer{}, func() time.Time { return time.Unix(2_000_000_000, 0) }))
	require.Equal(t, br, runner.name)
	require.Equal(t, []string{
		"backup", "txn", "--pd=pd-a:2379,pd-b:2379", "--storage=s3://bucket/immutable/full-a",
		"--backupts=120", "--checksum=false", "--crypter.method=plaintext", "--log-file=/dev/stderr",
	}, runner.args)
	f, err := os.Open(output)
	require.NoError(t, err)
	defer f.Close()
	receipt, err := nativepitr.DecodeFullBackupAttestation(f)
	require.NoError(t, err)
	require.Equal(t, uint64(120), receipt.BackupTS)
	require.Equal(t, int64(2_000_000_000), receipt.CompletedAtUnix)
	require.True(t, receipt.ExitSuccessful)
	require.Equal(t, pinnedBRVersion, receipt.BRVersion)
}

func TestExecuteRunsAES256BackupWithoutAttestingKeyMaterial(t *testing.T) {
	dir := t.TempDir()
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("pinned-br-binary"), 0o700))
	keyFile := filepath.Join(dir, "encryption.key")
	keyHex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	require.NoError(t, os.WriteFile(keyFile, []byte(keyHex+"\n"), 0o400))
	output := filepath.Join(dir, "attestation.json")
	runner := &fakeRunner{}
	o := options{
		brBinary: br, pdAddrs: "pd:2379", storage: "s3://bucket/immutable/full-a",
		backupTS: 120, output: output, timeout: time.Minute,
		cipherMethod:    nativepitr.CipherMethodAES256CTR,
		encryptionKeyID: "kms/prod/backup-key/versions/7", encryptionKeyFile: keyFile,
	}
	require.NoError(t, execute(context.Background(), o, runner, &bytes.Buffer{}, func() time.Time { return time.Unix(2_000_000_000, 0) }))
	require.Contains(t, runner.args, "--crypter.method=aes256-ctr")
	var runtimeKeyFile string
	for _, arg := range runner.args {
		if strings.HasPrefix(arg, "--crypter.key-file=") {
			runtimeKeyFile = strings.TrimPrefix(arg, "--crypter.key-file=")
		}
	}
	require.NotEmpty(t, runtimeKeyFile)
	require.NotEqual(t, keyFile, runtimeKeyFile, "BR must consume a private key snapshot, not the mutable source path")
	require.NoFileExists(t, runtimeKeyFile, "the private key snapshot must be removed after execution")
	body, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NotContains(t, string(body), keyFile)
	require.NotContains(t, string(body), keyHex)
	receipt, err := nativepitr.DecodeFullBackupAttestation(bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, nativepitr.CipherMethodAES256CTR, receipt.CipherMethod)
	require.Equal(t, "kms/prod/backup-key/versions/7", receipt.EncryptionKeyID)
}

func TestExecuteRejectsChangedAES256KeyAfterBackup(t *testing.T) {
	dir := t.TempDir()
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("br"), 0o700))
	keyFile := filepath.Join(dir, "encryption.key")
	require.NoError(t, os.WriteFile(keyFile, []byte(strings.Repeat("a", 64)), 0o400))
	output := filepath.Join(dir, "attestation.json")
	runner := &fakeRunner{hook: func() {
		require.NoError(t, os.Chmod(keyFile, 0o600))
		require.NoError(t, os.WriteFile(keyFile, []byte(strings.Repeat("b", 64)), 0o400))
	}}
	o := options{brBinary: br, pdAddrs: "pd:2379", storage: "s3://bucket/full", backupTS: 120, output: output, timeout: time.Minute,
		cipherMethod: nativepitr.CipherMethodAES256CTR, encryptionKeyID: "key-version-7", encryptionKeyFile: keyFile}
	require.ErrorContains(t, execute(context.Background(), o, runner, io.Discard, time.Now), "encryption key changed")
	require.NoFileExists(t, output)
}

func TestExecuteNeverAttestsFailedBackup(t *testing.T) {
	for _, tt := range []struct {
		name   string
		runner func(string) *fakeRunner
		want   string
	}{
		{"child failure", func(string) *fakeRunner { return &fakeRunner{err: errors.New("exit 1")} }, "backup failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			br := filepath.Join(dir, "br")
			require.NoError(t, os.WriteFile(br, []byte("original"), 0o700))
			output := filepath.Join(dir, "attestation.json")
			o := options{brBinary: br, pdAddrs: "pd:2379", storage: "s3://bucket/immutable/full-a", backupTS: 120, output: output, timeout: time.Minute}
			err := execute(context.Background(), o, tt.runner(br), &bytes.Buffer{}, time.Now)
			require.ErrorContains(t, err, tt.want)
			_, statErr := os.Stat(output)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}

func TestExecuteRejectsUnsafeInputsBeforeBR(t *testing.T) {
	dir := t.TempDir()
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("br"), 0o700))
	base := options{brBinary: br, pdAddrs: "pd:2379", storage: "s3://bucket/immutable/full-a", backupTS: 120, output: filepath.Join(dir, "attestation.json"), timeout: time.Minute}
	for _, edit := range []func(*options){
		func(o *options) { o.pdAddrs = "pd:2379,pd:2379" },
		func(o *options) { o.storage = "s3://user:secret@bucket/full" },
		func(o *options) { o.ca = "/ca.pem" },
		func(o *options) { o.output = "relative.json" },
	} {
		o := base
		edit(&o)
		runner := &fakeRunner{}
		require.Error(t, execute(context.Background(), o, runner, &bytes.Buffer{}, time.Now))
		require.Empty(t, runner.args)
	}
}

func TestExecuteRejectsUnpinnedBRBeforeBackup(t *testing.T) {
	dir := t.TempDir()
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("br"), 0o700))
	o := options{brBinary: br, pdAddrs: "pd:2379", storage: "s3://bucket/immutable/full-a", backupTS: 120, output: filepath.Join(dir, "attestation.json"), timeout: time.Minute}
	runner := &fakeRunner{version: "Release Version: v8.5.3\nGit Commit Hash: other\n"}
	require.ErrorContains(t, execute(context.Background(), o, runner, &bytes.Buffer{}, time.Now), "exact v7.5.1")
	require.Empty(t, runner.args)
}

func TestExecuteDoesNotOverwriteAttestationPublishedDuringBackup(t *testing.T) {
	dir := t.TempDir()
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("br"), 0o700))
	output := filepath.Join(dir, "attestation.json")
	runner := &fakeRunner{hook: func() { require.NoError(t, os.WriteFile(output, []byte("winner"), 0o600)) }}
	o := options{brBinary: br, pdAddrs: "pd:2379", storage: "s3://bucket/immutable/full-a", backupTS: 120, output: output, timeout: time.Minute}
	require.ErrorContains(t, execute(context.Background(), o, runner, &bytes.Buffer{}, time.Now), "appeared during backup")
	body, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, []byte("winner"), body)
}
