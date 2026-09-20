package leasefault

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCommandConnectionsBindActualProbeConfiguration(t *testing.T) {
	for _, mode := range []string{"valid", "same-ip", "dns-original", "dns-observer", "missing-observer-tls", "changed-probe-key", "conflicting-runtime"} {
		t.Run(mode, func(t *testing.T) {
			base, _ := connectionFixture(t)
			p := ObservationCommandPlan{Endpoint: base.Endpoint, ServerName: base.ServerName, CA: base.CA, Certificate: base.Certificate, Key: base.Key}
			observer, name := "127.0.0.2:2379", base.ServerName
			switch mode {
			case "same-ip":
				observer = "127.0.0.1:3379"
			case "dns-original":
				p.Endpoint = "service.example:2379"
			case "dns-observer":
				observer = "observer.example:2379"
			case "missing-observer-tls":
				name = ""
			case "changed-probe-key":
				p.Key += ".wrong"
			}
			connections, err := p.OpenConnections(base.Kubeconfig, base.Context, base.APIServer, observer, name, base.Files)
			if mode != "valid" && mode != "conflicting-runtime" {
				require.Error(t, err)
				require.Nil(t, connections)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, connections.Close()) })
			require.Equal(t, "passthrough:///"+p.Endpoint, connections.Original.Target())
			require.Equal(t, "passthrough:///"+observer, connections.Successor.Target())
			r := MeasuredNetworkFaultRuntime{}
			if mode == "conflicting-runtime" {
				r.Network.Lifecycle.Preparation.Connection = connections.Successor
			}
			bound, err := connections.Bind(r)
			if mode == "conflicting-runtime" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Same(t, connections.Original, bound.Network.Lifecycle.Preparation.Connection)
			require.Same(t, connections.Successor, bound.Network.SuccessorConnection)
			require.Same(t, connections.Client, bound.Network.Lifecycle.Preparation.Client)
			require.Nil(t, bound.Network.Lifecycle.RecoveryConnection)
			_, err = connections.Bind(bound)
			require.Error(t, err)
		})
	}
}
