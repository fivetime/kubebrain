package compat

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGRPCTarget(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		endpoint string
		want     string
	}{
		{name: "bare", endpoint: "127.0.0.1:2379", want: "127.0.0.1:2379"},
		{name: "http", endpoint: "http://127.0.0.1:2379", want: "127.0.0.1:2379"},
		{name: "https", endpoint: "https://etcd.example.test:2379", want: "etcd.example.test:2379"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, grpcTarget(testCase.endpoint))
		})
	}
}
