package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadPDLeaderFallsBackAndValidatesIdentity(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/pd/api/v1/leader", request.URL.Path)
		_, _ = w.Write([]byte(`{"name":"pd-1","member_id":42}`))
	}))
	defer good.Close()

	leader, err := readPDLeader(context.Background(), []string{bad.URL, good.URL}, time.Second)
	require.NoError(t, err)
	require.Equal(t, pdLeader{Name: "pd-1", MemberID: 42}, leader)
}

func TestVerifyPDStoresRejectsStaleHeartbeat(t *testing.T) {
	heartbeat := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/pd/api/v1/stores", request.URL.Path)
		_, _ = w.Write([]byte(`{"count":1,"stores":[{"store":{"id":1,"address":"tikv-0:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}}]}`))
	}))
	defer server.Close()

	err := verifyPDStores(context.Background(), []string{server.URL}, time.Second, 20*time.Second, 1)
	require.ErrorContains(t, err, "TiKV store unhealthy")
}

func TestVerifyPDStoresAcceptsExactHealthySet(t *testing.T) {
	heartbeat := time.Now().UTC().Format(time.RFC3339Nano)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"count":1,"stores":[{"store":{"id":1,"address":"tikv-0:20160","state_name":"Up"},"status":{"last_heartbeat_ts":"` + heartbeat + `"}}]}`))
	}))
	defer server.Close()
	require.NoError(t, verifyPDStores(context.Background(), []string{server.URL}, time.Second, 20*time.Second, 1))
}

func TestConfigValidation(t *testing.T) {
	valid := config{endpoint: "http://etcd:2379", prefix: "/probe/", iterations: 1, interval: time.Millisecond, commandTimeout: time.Second, dialTimeout: time.Second, maxLatency: time.Second, leaseTTL: 15, pdEndpoints: []string{"http://pd:2379"}, expectedStores: 3, maxHeartbeatAge: 20 * time.Second}
	require.NoError(t, valid.validate())

	for name, mutate := range map[string]func(*config){
		"endpoint":     func(cfg *config) { cfg.endpoint = "" },
		"prefix":       func(cfg *config) { cfg.prefix = "" },
		"iterations":   func(cfg *config) { cfg.iterations = 0 },
		"interval":     func(cfg *config) { cfg.interval = 0 },
		"command":      func(cfg *config) { cfg.commandTimeout = 0 },
		"dial":         func(cfg *config) { cfg.dialTimeout = 0 },
		"latency":      func(cfg *config) { cfg.maxLatency = 0 },
		"latency cap":  func(cfg *config) { cfg.maxLatency = 2 * cfg.commandTimeout },
		"lease TTL":    func(cfg *config) { cfg.leaseTTL = 0 },
		"PD endpoints": func(cfg *config) { cfg.pdEndpoints = nil },
		"PD scheme":    func(cfg *config) { cfg.pdEndpoints = []string{"pd:2379"} },
		"stores":       func(cfg *config) { cfg.expectedStores = 0 },
		"heartbeat":    func(cfg *config) { cfg.maxHeartbeatAge = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			require.Error(t, candidate.validate())
		})
	}
}
