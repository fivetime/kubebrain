package option

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitialClusterValidationRequiresLocalIdentity(t *testing.T) {
	o := NewOptions()
	o.storageConfig.pdAddrs = []string{"127.0.0.1:2379"}
	o.advertiseHost = "10.0.0.1"
	o.initialCluster = "kb-1=http://10.0.0.1:2380,kb-2=http://10.0.0.2:2380"
	require.NoError(t, o.Validate())

	o.initialCluster = "kb-2=http://10.0.0.2:2380"
	err := o.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not contain this replica identity")
}

func TestInitialClusterValidationAcceptsLocalIdentityInRepeatedMember(t *testing.T) {
	o := NewOptions()
	o.storageConfig.pdAddrs = []string{"127.0.0.1:2379"}
	o.advertiseHost = "10.0.0.1"
	o.initialCluster = "kb-1=http://10.0.0.2:2380,kb-1=http://10.0.0.1:2380"
	require.NoError(t, o.Validate())
}

func TestInitialClusterValidationUsesConfiguredPeerPort(t *testing.T) {
	o := NewOptions()
	o.storageConfig.pdAddrs = []string{"127.0.0.1:2379"}
	o.advertiseHost = "10.0.0.1"
	o.epsConf.PeerPort = 3380
	o.initialCluster = "kb-1=http://10.0.0.1:3380"
	require.NoError(t, o.Validate())

	o.initialCluster = "kb-1=http://10.0.0.1:2380"
	require.Error(t, o.Validate())
}
