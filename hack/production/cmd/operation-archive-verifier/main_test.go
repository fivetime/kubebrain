package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateIAMSimulationExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	require.NoError(t, validateIAMSimulationExpiry("1700000001", now))
	for _, raw := range []string{"", "pending", "0", "1699999999", "1700000000"} {
		t.Run(raw, func(t *testing.T) {
			require.Error(t, validateIAMSimulationExpiry(raw, now))
		})
	}
}
