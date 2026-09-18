package option

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeaderDurationAdmission(t *testing.T) {
	o := NewOptions()
	o.storageConfig.pdAddrs = []string{"127.0.0.1:2379"}
	require.NoError(t, o.Validate())
	o.epsConf.LeaseDuration = 1500 * time.Millisecond
	o.epsConf.RenewDeadline = 1200 * time.Millisecond
	o.epsConf.RetryPeriod = 100 * time.Millisecond
	require.ErrorContains(t, o.Validate(), "stored lease")
	o.epsConf.LeaseDuration = 4 * time.Second
	o.epsConf.RetryPeriod = time.Second
	require.ErrorContains(t, o.Validate(), "JitterFactor")
	o.epsConf.LeaseDuration = 30 * time.Second
	o.epsConf.RenewDeadline = 25 * time.Second
	o.epsConf.RetryPeriod = 500 * time.Millisecond
	require.NoError(t, o.Validate(), "existing test cluster configuration must remain valid")
}
