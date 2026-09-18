package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScopeCommand(t *testing.T) {
	var output bytes.Buffer
	require.NoError(t, run([]string{"--storage-cluster-id=42", "--keyspace=", "--election-prefix=/endpoint-pair"}, &output))
	var result map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, "retirement-v1:16b5b4e34c199a3080ea2e0c871ef4588b2d605a208cad2dd961ed14e7511306", result["scope"])
	require.Equal(t, false, result["inputs_verified"])
	output.Reset()
	require.NoError(t, run([]string{"--storage-cluster-id=18446744073709551615", "--keyspace=tenant", "--election-prefix=/prefix"}, &output))
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Equal(t, "18446744073709551615", result["storage_cluster_id"], "never round uint64 identity through a JSON floating-point number")
}

func TestScopeCommandRejectsIncompleteOrInvalidInputs(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--storage-cluster-id=42", "--election-prefix=/p"},
		{"--storage-cluster-id=0", "--keyspace=t", "--election-prefix=/p"},
		{"--storage-cluster-id=-1", "--keyspace=t", "--election-prefix=/p"},
		{"--storage-cluster-id=18446744073709551616", "--keyspace=t", "--election-prefix=/p"},
		{"--storage-cluster-id=42", "--keyspace=t", "--election-prefix="},
		{"--storage-cluster-id=42", "--keyspace=t", "--election-prefix=/p", "extra"},
		{"--storage-cluster-id=42", "--keyspace=t", "--election-prefix=/p", "--unknown=x"},
	} {
		var output bytes.Buffer
		require.Error(t, run(args, &output))
		require.Empty(t, output.String())
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestScopeCommandPropagatesOutputFailure(t *testing.T) {
	require.Error(t, run([]string{"--storage-cluster-id=42", "--keyspace=", "--election-prefix=/p"}, failedWriter{}))
}
