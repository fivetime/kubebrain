package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAddrsCanonicalizesAndRejectsAmbiguity(t *testing.T) {
	got, err := parseAddrs("pd-b:2379, pd-a:2379")
	require.NoError(t, err)
	require.Equal(t, []string{"pd-a:2379", "pd-b:2379"}, got)
	for _, raw := range []string{"", "pd:2379,pd:2379", "http://pd:2379"} {
		_, err := parseAddrs(raw)
		require.Error(t, err)
	}
}
