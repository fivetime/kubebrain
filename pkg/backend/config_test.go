package backend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigCompleteClampsWatchProgressIntervalLikeEtcd(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{name: "unset uses kubebrain default", interval: 0, want: time.Second},
		{name: "negative uses kubebrain default", interval: -time.Second, want: time.Second},
		{name: "positive below etcd minimum", interval: time.Millisecond, want: 100 * time.Millisecond},
		{name: "etcd minimum", interval: 100 * time.Millisecond, want: 100 * time.Millisecond},
		{name: "above minimum", interval: 250 * time.Millisecond, want: 250 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{WatchProgressNotifyInterval: tt.interval}
			config.complete()
			require.Equal(t, tt.want, config.WatchProgressNotifyInterval)
		})
	}
}
