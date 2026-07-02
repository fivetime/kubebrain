package etcdutil

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeoutFromEnv(t *testing.T) {
	old := os.Getenv("TIMEOUT")
	defer os.Setenv("TIMEOUT", old)

	require.NoError(t, os.Unsetenv("TIMEOUT"))
	timeout, err := TimeoutFromEnv()
	require.NoError(t, err)
	require.Equal(t, 10*time.Minute, timeout)

	require.NoError(t, os.Setenv("TIMEOUT", "30s"))
	timeout, err = TimeoutFromEnv()
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, timeout)

	require.NoError(t, os.Setenv("TIMEOUT", "0s"))
	_, err = TimeoutFromEnv()
	require.Error(t, err)

	require.NoError(t, os.Setenv("TIMEOUT", "bad"))
	_, err = TimeoutFromEnv()
	require.Error(t, err)
}

func TestTLSConfigFromEnvRequiresCertAndKeyTogether(t *testing.T) {
	oldCACert := os.Getenv("ETCDCTL_CACERT")
	oldCert := os.Getenv("ETCDCTL_CERT")
	oldKey := os.Getenv("ETCDCTL_KEY")
	defer func() {
		_ = os.Setenv("ETCDCTL_CACERT", oldCACert)
		_ = os.Setenv("ETCDCTL_CERT", oldCert)
		_ = os.Setenv("ETCDCTL_KEY", oldKey)
	}()

	require.NoError(t, os.Unsetenv("CACERT"))
	require.NoError(t, os.Unsetenv("CERT"))
	require.NoError(t, os.Unsetenv("KEY"))
	require.NoError(t, os.Unsetenv("ETCD_CACERT"))
	require.NoError(t, os.Unsetenv("ETCD_CERT"))
	require.NoError(t, os.Unsetenv("ETCD_KEY"))
	require.NoError(t, os.Unsetenv("ETCDCTL_CACERT"))
	require.NoError(t, os.Unsetenv("ETCDCTL_CERT"))
	require.NoError(t, os.Unsetenv("ETCDCTL_KEY"))

	cfg, err := TLSConfigFromEnv()
	require.NoError(t, err)
	require.Nil(t, cfg)

	require.NoError(t, os.Setenv("ETCDCTL_CERT", "/tmp/client.crt"))
	_, err = TLSConfigFromEnv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires both")
}
