package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("projected-token\n"), 0o600))
	token, err := readToken(path)
	require.NoError(t, err)
	require.Equal(t, "projected-token", token)

	for _, contents := range [][]byte{
		[]byte("bad\ntoken"),
		[]byte(" projected-token"),
		[]byte("projected-token "),
		[]byte("projected token"),
		[]byte("projected\ttoken"),
	} {
		require.NoError(t, os.WriteFile(path, contents, 0o600))
		_, err = readToken(path)
		require.ErrorContains(t, err, "malformed")
	}

	require.NoError(t, os.WriteFile(path, make([]byte, maxPrometheusBearerTokenBytes+1), 0o600))
	_, err = readToken(path)
	require.ErrorContains(t, err, "exceeds")
}

func TestPrometheusClientRejectsInvalidCA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))
	_, err := prometheusClient(path, "prometheus.internal")
	require.ErrorContains(t, err, "contains no certificates")

	require.NoError(t, os.WriteFile(path, make([]byte, maxPrometheusCABytes+1), 0o600))
	_, err = prometheusClient(path, "prometheus.internal")
	require.ErrorContains(t, err, "exceeds")

	client, err := prometheusClient("", "")
	require.NoError(t, err)
	require.NotNil(t, client.Transport)
}
