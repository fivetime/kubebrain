package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadSecretFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(path, []byte("jwt"), 0o600))
	value, err := readSecretFile(path, maxTokenBytes)
	require.NoError(t, err)
	require.Equal(t, "jwt", string(value))

	require.NoError(t, os.Chmod(path, 0o640))
	_, err = readSecretFile(path, maxTokenBytes)
	require.ErrorContains(t, err, "inaccessible to group/other")
}

func TestReadSecretFileRejectsSymlinkEmptyAndOversize(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	require.NoError(t, os.WriteFile(target, []byte("jwt"), 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "link")))
	_, err := readSecretFile(filepath.Join(dir, "link"), maxTokenBytes)
	require.ErrorContains(t, err, "non-symlink")

	empty := filepath.Join(dir, "empty")
	require.NoError(t, os.WriteFile(empty, nil, 0o600))
	_, err = readSecretFile(empty, maxTokenBytes)
	require.ErrorContains(t, err, "1..1048576")

	large := filepath.Join(dir, "large")
	require.NoError(t, os.WriteFile(large, []byte(strings.Repeat("x", maxTokenBytes+1)), 0o600))
	_, err = readSecretFile(large, maxTokenBytes)
	require.ErrorContains(t, err, "1..1048576")
}

func TestReadPublicFileAllowsReadableCertificateButRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(path, []byte("pem"), 0o644))
	value, err := readPublicFile(path, maxTokenBytes)
	require.NoError(t, err)
	require.Equal(t, "pem", string(value))
	require.NoError(t, os.Symlink(path, filepath.Join(dir, "ca-link")))
	_, err = readPublicFile(filepath.Join(dir, "ca-link"), maxTokenBytes)
	require.ErrorContains(t, err, "non-symlink")
}

func TestRunRejectsInputsBeforeDial(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(token, []byte("bad token"), 0o600))
	require.ErrorContains(t, run(t.Context(), options{endpoint: "https://a:2379,https://b:2379", tokenFile: token}), "one non-empty endpoint")
	require.ErrorContains(t, run(t.Context(), options{endpoint: "https://a:2379", tokenFile: token, probeKey: "/probe", timeout: time.Second}), "token must be non-empty")
}
