package endpoint

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/stretchr/testify/require"
)

func TestPeerRetirementEndpointPreparesOwnedDynamicPolicy(t *testing.T) {
	ca := newRotationCA(t)
	dir := t.TempDir()
	cert, key := writeRotationCertificate(t, dir, "local", ca, 91, "peer.test")
	caFile := filepath.Join(dir, "ca.pem")
	writeRotationCA(t, caFile, ca)
	config := &Config{PeerSecurityConfig: &SecurityConfig{CertFile: cert, KeyFile: key, CA: caFile, ClientAuth: true, ServerName: "peer.test"},
		ExperimentalPeerRetirement: &PeerRetirementOptions{Scope: "scope", HolderPins: map[string][]string{"local": {"pin"}},
			EndpointHolders: map[string]string{"https://second.test:3380": "second", "https://first.test:3380": "first"},
			ReadBudget:      time.Second, OperationBudget: time.Second, SendBudget: time.Second, Concurrency: 2, RequestsPerSecond: 10}}
	prepared, bootstrap, err := config.preparePeerRetirement(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"https://first.test:3380", "https://second.test:3380"}, prepared.PeerURLs)
	require.NotNil(t, prepared.ControlCredentialSource)
	require.Same(t, prepared.ControlCredentialSource, prepared.ProxyCredentialSource)
	require.Empty(t, bootstrap.ServerName, "bootstrap is only for startup validation")
	require.False(t, bootstrap.InsecureSkipVerify)
	require.Nil(t, bootstrap.VerifyConnection)
	config.ExperimentalPeerRetirement.HolderPins["local"][0] = "mutated"
	config.ExperimentalPeerRetirement.EndpointHolders["https://first.test:3380"] = "other"
	require.Equal(t, "pin", prepared.HolderPins["local"][0])
	require.Equal(t, "first", prepared.SuccessorHolders["https://first.test:3380"])
	material, err := prepared.ProxyCredentialSource.LoadClientCredentialMaterial(context.Background())
	require.NoError(t, err)
	require.Equal(t, "peer.test", material.ServerName, "runtime must retain operator name policy")
	writeRotationCertificate(t, dir, "rotated", ca, 92, "peer.test")
	rotated, err := prepared.ControlCredentialSource.LoadClientCredentialMaterial(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(92), rotated.Certificate.Leaf.SerialNumber.Int64())
	require.Equal(t, int64(91), bootstrap.Certificates[0].Leaf.SerialNumber.Int64(), "startup snapshot is not the runtime source")
}

type retirementStartupBackend struct {
	backend.Backend
	closed bool
}

func (b *retirementStartupBackend) Close() error { b.closed = true; return nil }

func TestPeerRetirementEndpointRejectsUnsafeModeAndCleansUp(t *testing.T) {
	for _, mode := range []string{"plaintext", "mixed", "missing CA", "missing targets", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			config := &Config{PeerSecurityConfig: &SecurityConfig{}, ExperimentalPeerRetirement: &PeerRetirementOptions{
				EndpointHolders: map[string]string{"https://peer.test:3380": "peer"},
			}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "mixed":
				config.PeerSecurityConfig = &SecurityConfig{CertFile: "missing", KeyFile: "missing", AllowInsecure: true}
			case "missing CA":
				config.PeerSecurityConfig = &SecurityConfig{CertFile: "missing", KeyFile: "missing", ClientAuth: true}
			case "missing targets":
				config.ExperimentalPeerRetirement.EndpointHolders = nil
			case "canceled":
				cancel()
			}
			b := &retirementStartupBackend{}
			endpoint := &Endpoint{config: config, backend: b}
			err := endpoint.Run(ctx)
			require.Error(t, err)
			require.Nil(t, endpoint.server)
			require.True(t, b.closed)
		})
	}
}
