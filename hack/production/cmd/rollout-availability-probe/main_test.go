package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	valid := config{endpoint: "http://etcd:2379", prefix: "/probe/", iterations: 1, interval: time.Millisecond, commandTimeout: time.Second, dialTimeout: time.Second, maxLatency: time.Second, leaseTTL: 15}
	require.NoError(t, valid.validate())

	for name, mutate := range map[string]func(*config){
		"endpoint":    func(cfg *config) { cfg.endpoint = "" },
		"prefix":      func(cfg *config) { cfg.prefix = "" },
		"iterations":  func(cfg *config) { cfg.iterations = 0 },
		"interval":    func(cfg *config) { cfg.interval = 0 },
		"command":     func(cfg *config) { cfg.commandTimeout = 0 },
		"dial":        func(cfg *config) { cfg.dialTimeout = 0 },
		"latency":     func(cfg *config) { cfg.maxLatency = 0 },
		"latency cap": func(cfg *config) { cfg.maxLatency = 2 * cfg.commandTimeout },
		"lease TTL":   func(cfg *config) { cfg.leaseTTL = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			require.Error(t, candidate.validate())
		})
	}
}
