package main

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunPublishesTokenWithoutPassingCredentialsOutsideIssuer(t *testing.T) {
	dir := t.TempDir()
	credentials := filepath.Join(dir, "credentials")
	require.NoError(t, os.Mkdir(credentials, 0o700))
	writeReadOnly(t, filepath.Join(credentials, "username"), "root")
	writeReadOnly(t, filepath.Join(credentials, "password"), "password with spaces")
	output := filepath.Join(dir, "token.jwt")
	called := 0
	err := run(context.Background(), options{endpoint: "https://member-0:2379", credentialRoot: credentials, usernameFile: filepath.Join(credentials, "username"), passwordFile: filepath.Join(credentials, "password"), output: output, timeout: time.Second}, func(_ context.Context, _ options, username, password string, _ *tls.Config) (string, error) {
		called++
		require.Equal(t, "root", username)
		require.Equal(t, "password with spaces", password)
		return "header.payload.signature", nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, called)
	require.Equal(t, "header.payload.signature", string(mustRead(t, output)))
	info, err := os.Lstat(output)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	err = run(context.Background(), options{endpoint: "https://member-0:2379", credentialRoot: credentials, usernameFile: filepath.Join(credentials, "username"), passwordFile: filepath.Join(credentials, "password"), output: output, timeout: time.Second}, func(context.Context, options, string, string, *tls.Config) (string, error) {
		called++
		return "replacement", nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, called)
	require.Equal(t, "header.payload.signature", string(mustRead(t, output)))
}

func TestReadCredentialFileAllowsKubernetesAtomicWriterSymlinkInsideRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "credentials")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "..2026_01"), 0o700))
	writeReadOnly(t, filepath.Join(root, "..2026_01", "password"), "secret")
	require.NoError(t, os.Symlink("..2026_01", filepath.Join(root, "..data")))
	require.NoError(t, os.Symlink("..data/password", filepath.Join(root, "password")))
	data, err := readCredentialFile(root, filepath.Join(root, "password"), maxCredentialBytes)
	require.NoError(t, err)
	require.Equal(t, "secret", string(data))
}

func TestReadCredentialFileRejectsEscapeAndWritableTarget(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "credentials")
	require.NoError(t, os.Mkdir(root, 0o700))
	outside := filepath.Join(dir, "outside")
	writeReadOnly(t, outside, "secret")
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "escaped")))
	_, err := readCredentialFile(root, filepath.Join(root, "escaped"), maxCredentialBytes)
	require.ErrorContains(t, err, "below credential root")
	writable := filepath.Join(root, "writable")
	require.NoError(t, os.WriteFile(writable, []byte("secret"), 0o600))
	_, err = readCredentialFile(root, writable, maxCredentialBytes)
	require.ErrorContains(t, err, "read-only")
}

func TestRunDoesNotPublishInvalidOrFailedToken(t *testing.T) {
	dir := t.TempDir()
	credentials := filepath.Join(dir, "credentials")
	require.NoError(t, os.Mkdir(credentials, 0o700))
	writeReadOnly(t, filepath.Join(credentials, "username"), "root")
	writeReadOnly(t, filepath.Join(credentials, "password"), "secret")
	base := options{endpoint: "https://member-0:2379", credentialRoot: credentials, usernameFile: filepath.Join(credentials, "username"), passwordFile: filepath.Join(credentials, "password"), timeout: time.Second}
	for _, token := range []string{"", "has whitespace", strings.Repeat("x", maxTokenBytes+1)} {
		output := filepath.Join(dir, "token-"+strings.ReplaceAll(token, " ", "-")[:min(8, len(strings.ReplaceAll(token, " ", "-")))])
		base.output = output
		err := run(context.Background(), base, func(context.Context, options, string, string, *tls.Config) (string, error) { return token, nil })
		require.Error(t, err)
		require.NoFileExists(t, output)
	}
}

func TestRunRedactsIssuerErrorAndPreservesCompetingOutput(t *testing.T) {
	dir := t.TempDir()
	credentials := filepath.Join(dir, "credentials")
	require.NoError(t, os.Mkdir(credentials, 0o700))
	writeReadOnly(t, filepath.Join(credentials, "username"), "root")
	writeReadOnly(t, filepath.Join(credentials, "password"), "sensitive-password")
	output := filepath.Join(dir, "token.jwt")
	o := options{endpoint: "https://member-0:2379", credentialRoot: credentials, usernameFile: filepath.Join(credentials, "username"), passwordFile: filepath.Join(credentials, "password"), output: output, timeout: time.Second}
	err := run(context.Background(), o, func(context.Context, options, string, string, *tls.Config) (string, error) {
		return "", errors.New("sensitive-password server-token")
	})
	require.EqualError(t, err, "etcd Authenticate failed")
	require.NotContains(t, err.Error(), "sensitive-password")
	require.NotContains(t, err.Error(), "server-token")
	require.NoFileExists(t, output)

	require.NoError(t, os.WriteFile(output, []byte("winner.token"), 0o600))
	err = publishNoClobber(output, []byte("loser.token"))
	require.EqualError(t, err, "publish token without overwrite")
	require.Equal(t, "winner.token", string(mustRead(t, output)))
}

func writeReadOnly(t *testing.T, path, value string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(value), 0o400))
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
