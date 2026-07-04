// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeaderConfigDefaultsAndValidate(t *testing.T) {
	// Zero config takes the historical 8/5/1s defaults and is valid.
	d := Config{}.withDefaults()
	require.Equal(t, defaultLeaseDuration, d.LeaseDuration)
	require.Equal(t, defaultRenewDeadline, d.RenewDeadline)
	require.Equal(t, defaultRetryPeriod, d.RetryPeriod)
	require.NoError(t, Config{}.Validate())

	// A valid tighter set (faster failover) passes.
	require.NoError(t, Config{LeaseDuration: 4 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 500 * time.Millisecond}.Validate())

	// Partial config: only the set fields override; the rest default. Here a
	// too-large RenewDeadline (>= default LeaseDuration 8s) must be rejected.
	require.Error(t, Config{RenewDeadline: 9 * time.Second}.Validate())

	// Ordering violations are rejected: renew >= lease, and retry >= renew.
	require.Error(t, Config{LeaseDuration: 5 * time.Second, RenewDeadline: 5 * time.Second, RetryPeriod: time.Second}.Validate())
	require.Error(t, Config{LeaseDuration: 5 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: 3 * time.Second}.Validate())

	// The #39 write-fence self-fencing bound (leaderElection.renewDeadline, used by
	// EpochAndLeadingFresh) tracks the configured RenewDeadline.
	cfg := Config{LeaseDuration: 6 * time.Second, RenewDeadline: 3 * time.Second, RetryPeriod: time.Second}.withDefaults()
	le := &leaderElection{leaseDuration: cfg.LeaseDuration, renewDeadline: cfg.RenewDeadline, retryPeriod: cfg.RetryPeriod}
	require.Equal(t, 3*time.Second, le.renewDeadline, "leadership-validity bound must track RenewDeadline")
	require.Equal(t, 6*time.Second, le.leaseDuration)
}
