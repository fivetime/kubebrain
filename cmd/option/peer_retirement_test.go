package option

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/endpoint"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestPeerRetirementFlagLoadsPolicyAndFailsClosed(t *testing.T) {
	o := NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	require.NoError(t, o.loadPeerRetirementConfig())
	require.Nil(t, o.epsConf.ExperimentalPeerRetirement)
	file := filepath.Join(t.TempDir(), "policy.json")
	policy := `{"scope":"retirement-v1:test","holder_pins":{"local:2380":["` + strings.Repeat("01", 32) + `"],"remote:2380":["` + strings.Repeat("02", 32) + `"]},"endpoint_holders":{"https://remote:2380":"remote:2380"},"read_budget":"1s","operation_budget":"1s","send_budget":"250ms","concurrency":2,"requests_per_second":4}`
	require.NoError(t, os.WriteFile(file, []byte(policy), 0600))
	require.NoError(t, fs.Parse([]string{"--experimental-peer-retirement-config=" + file, "--advertise-host=local", "--peer-port=2380"}))
	require.Error(t, o.loadPeerRetirementConfig(), "plaintext must not enable this protocol")
	o.epsConf.PeerSecurityConfig = &endpoint.SecurityConfig{CertFile: "cert", KeyFile: "key", CA: "ca", ClientAuth: true}
	require.NoError(t, o.loadPeerRetirementConfig())
	require.Equal(t, "remote:2380", o.epsConf.ExperimentalPeerRetirement.EndpointHolders["https://remote:2380"])
	o.epsConf.PeerSecurityConfig.AllowInsecure = true
	require.Error(t, o.loadPeerRetirementConfig())
	require.Nil(t, o.epsConf.ExperimentalPeerRetirement)
	o.epsConf.PeerSecurityConfig.AllowInsecure = false
	o.advertiseHost = "unconfigured"
	require.Error(t, o.loadPeerRetirementConfig())
	o.advertiseHost = "local"
	require.NoError(t, o.loadPeerRetirementConfig())
	require.NoError(t, os.WriteFile(file, []byte(`{}`), 0600))
	require.Error(t, o.Validate())
	require.Nil(t, o.epsConf.ExperimentalPeerRetirement)
	// Run must fail before touching storage even if validation was bypassed or
	// the projected file changed after a previous successful load.
	o.storageConfig = nil
	require.Error(t, o.Run(context.Background()))
	o.peerRetirementConfigFile = ""
	require.NoError(t, o.loadPeerRetirementConfig())
	require.Nil(t, o.epsConf.ExperimentalPeerRetirement)
}
