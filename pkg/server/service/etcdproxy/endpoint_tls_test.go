package etcdproxy

import (
	"crypto/tls"
	"errors"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type endpointCredentialView struct {
	leader.LeaderElection
	value credentials.TransportCredentials
	err   error
}

func (v endpointCredentialView) ProxyCredentialsForEndpoint(string) (credentials.TransportCredentials, error) {
	return v.value, v.err
}

func TestEndpointCredentialsNeverFallBack(t *testing.T) {
	for _, mode := range []string{"valid", "provider error", "nil", "plaintext", "empty authority", "fallback"} {
		t.Run(mode, func(t *testing.T) {
			var value credentials.TransportCredentials = credentials.NewTLS(&tls.Config{ServerName: "peer.invalid"})
			var failure error
			switch mode {
			case "provider error":
				failure = errors.New("unavailable")
			case "nil":
				value = nil
			case "plaintext":
				value = insecure.NewCredentials()
			case "empty authority":
				value = credentials.NewTLS(&tls.Config{})
			}
			e := &etcdProxy{election: endpointCredentialView{value: value, err: failure}, allowInsecure: mode == "fallback"}
			actual, err := e.credentialsForEndpoint("https://peer.invalid")
			if mode == "valid" {
				require.NoError(t, err)
				require.Same(t, value, actual)
			} else {
				require.Error(t, err)
				require.Nil(t, actual)
			}
		})
	}
	value, err := (&etcdProxy{}).credentialsForEndpoint("legacy")
	require.NoError(t, err)
	require.Nil(t, value)
}

type endpointTLSView struct {
	leader.LeaderElection
	selectTLS func(string) (*tls.Config, error)
}

func (v endpointTLSView) ProxyTLSForEndpoint(endpoint string) (*tls.Config, error) {
	return v.selectTLS(endpoint)
}

func TestEndpointBoundTLSNeverFallsBack(t *testing.T) {
	for _, mode := range []string{"valid", "provider error", "nil TLS", "skip verify", "plaintext fallback"} {
		t.Run(mode, func(t *testing.T) {
			bound := &tls.Config{MinVersion: tls.VersionTLS12}
			e := &etcdProxy{tlsConfig: &tls.Config{}, election: endpointTLSView{selectTLS: func(endpoint string) (*tls.Config, error) {
				require.Equal(t, "https://exact-peer:3380", endpoint)
				switch mode {
				case "provider error":
					return nil, errors.New("unknown endpoint")
				case "nil TLS":
					return nil, nil
				case "skip verify":
					bound.InsecureSkipVerify = true
				}
				return bound, nil
			}}}
			e.allowInsecure = mode == "plaintext fallback"
			configs, err := e.dialTLSConfigsForEndpoint("https://exact-peer:3380")
			if mode == "valid" {
				require.NoError(t, err)
				require.Len(t, configs, 1)
				require.Same(t, bound, configs[0])
			} else {
				require.Error(t, err)
				require.Empty(t, configs)
			}
		})
	}
}

func TestOrdinaryProxyTLSSelectionUnchanged(t *testing.T) {
	base := &tls.Config{}
	e := &etcdProxy{tlsConfig: base, allowInsecure: true}
	configs, err := e.dialTLSConfigsForEndpoint("legacy-peer")
	require.NoError(t, err)
	require.Len(t, configs, 2)
	require.Same(t, base, configs[0])
	require.Nil(t, configs[1])
}
