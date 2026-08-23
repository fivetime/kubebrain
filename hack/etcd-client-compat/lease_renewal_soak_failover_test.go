package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	leaseRenewalSoakMaxClients         = 256
	leaseRenewalSoakMaxLeasesPerClient = 4096
	leaseRenewalSoakMaxLeases          = 100000
	leaseRenewalSoakMaxDuration        = 7 * 24 * time.Hour
)

type leaseRenewalSoakConfig struct {
	clientCount     int
	leasesPerClient int
	failoverCycles  int
	leaseTTL        int64
	duration        time.Duration
	auditInterval   time.Duration
	auditMaxOutage  time.Duration
	auditSample     int
}

type leaseRenewalLiveLease struct {
	id        clientv3.LeaseID
	key       string
	responses atomic.Int64
}

type leaseRenewalAuditClient interface {
	Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	TimeToLive(context.Context, clientv3.LeaseID, ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error)
}

type leaseRenewalAuditStats struct {
	completedSamples int64
	auditedLeases    int64
	transientErrors  int64
	maxSuccessGap    time.Duration
	firstRevision    int64
	lastRevision     int64
}

func parseLeaseRenewalSoakConfig(lookup func(string) (string, bool)) (leaseRenewalSoakConfig, error) {
	config := leaseRenewalSoakConfig{
		clientCount: 8, leasesPerClient: 8, failoverCycles: 3, leaseTTL: 30,
	}
	integerOptions := []struct {
		name string
		min  int
		max  int
		set  func(int)
	}{
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_CLIENTS", 1, leaseRenewalSoakMaxClients, func(value int) { config.clientCount = value }},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_LEASES_PER_CLIENT", 1, leaseRenewalSoakMaxLeasesPerClient, func(value int) { config.leasesPerClient = value }},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_CYCLES", 1, 100, func(value int) { config.failoverCycles = value }},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL", 5, 86400, func(value int) { config.leaseTTL = int64(value) }},
	}
	for _, option := range integerOptions {
		raw, configured := lookup(option.name)
		if !configured {
			continue
		}
		value, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || value < option.min || value > option.max {
			return leaseRenewalSoakConfig{}, fmt.Errorf("%s must be an integer in [%d,%d], got %q",
				option.name, option.min, option.max, raw)
		}
		option.set(value)
	}
	if config.clientCount > leaseRenewalSoakMaxLeases/config.leasesPerClient {
		return leaseRenewalSoakConfig{}, fmt.Errorf("lease renewal soak requests %d leases; maximum is %d",
			config.clientCount*config.leasesPerClient, leaseRenewalSoakMaxLeases)
	}
	if raw, configured := lookup("KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION"); configured {
		duration, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil || duration < time.Duration(config.failoverCycles)*time.Second || duration > leaseRenewalSoakMaxDuration {
			return leaseRenewalSoakConfig{}, fmt.Errorf(
				"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION must be between %s and %s, got %q",
				time.Duration(config.failoverCycles)*time.Second, leaseRenewalSoakMaxDuration, raw)
		}
		config.duration = duration
	}
	if raw, configured := lookup("KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL"); configured {
		interval, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil || interval < 100*time.Millisecond || interval > time.Minute {
			return leaseRenewalSoakConfig{}, fmt.Errorf("KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL must be between 100ms and 1m, got %q", raw)
		}
		config.auditInterval = interval
	} else if config.duration > 0 {
		config.auditInterval = 5 * time.Second
	}
	if raw, configured := lookup("KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE"); configured {
		outage, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil || outage < time.Second || outage > 5*time.Minute {
			return leaseRenewalSoakConfig{}, fmt.Errorf("KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE must be between 1s and 5m, got %q", raw)
		}
		config.auditMaxOutage = outage
	} else if config.duration > 0 {
		config.auditMaxOutage = 45 * time.Second
	}
	if raw, configured := lookup("KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE"); configured {
		sample, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || sample < 1 || sample > 4096 {
			return leaseRenewalSoakConfig{}, fmt.Errorf("KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE must be an integer in [1,4096], got %q", raw)
		}
		config.auditSample = sample
	} else if config.duration > 0 {
		config.auditSample = min(64, config.clientCount*config.leasesPerClient)
	}
	if config.duration == 0 && (config.auditInterval != 0 || config.auditMaxOutage != 0 || config.auditSample != 0) {
		return leaseRenewalSoakConfig{}, errors.New("lease renewal soak audit options require KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION")
	}
	if config.duration > 0 && (config.auditInterval >= config.auditMaxOutage || config.auditSample > config.clientCount*config.leasesPerClient) {
		return leaseRenewalSoakConfig{}, errors.New("lease renewal soak requires audit interval < max outage and sample <= total leases")
	}
	if config.duration > 0 {
		total := config.clientCount * config.leasesPerClient
		minimumAuditDuration := config.auditInterval * time.Duration((total+config.auditSample-1)/config.auditSample)
		if config.duration < minimumAuditDuration {
			return leaseRenewalSoakConfig{}, fmt.Errorf("lease renewal soak duration must be at least %s to audit every lease once", minimumAuditDuration)
		}
	}
	return config, nil
}

// TestLeaseRenewalSoakAcrossRepeatedLeaderFailover keeps multiple clientv3
// keepalive loops live across repeated leader replacement. The command must
// discover and delete the current leader on every invocation.
func TestLeaseRenewalSoakAcrossRepeatedLeaderFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	config, err := parseLeaseRenewalSoakConfig(os.LookupEnv)
	require.NoError(t, err)
	t.Logf("lease renewal soak config: clients=%d leases/client=%d total=%d failovers=%d ttl=%ds duration=%s audit=%s/%s sample=%d",
		config.clientCount, config.leasesPerClient, config.clientCount*config.leasesPerClient,
		config.failoverCycles, config.leaseTTL, config.duration, config.auditInterval, config.auditMaxOutage, config.auditSample)

	testTimeout := 4 * time.Minute
	if configuredTimeout := config.duration + time.Duration(config.failoverCycles)*time.Minute + time.Minute; configuredTimeout > testTimeout {
		testTimeout = configuredTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	keepAliveCtx, stopKeepAlives := context.WithCancel(ctx)
	defer stopKeepAlives()

	clients := make([]*clientv3.Client, config.clientCount)
	for i := range clients {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{compatEndpoint(t)},
			DialTimeout: 3 * time.Second,
		})
		require.NoError(t, err)
		clients[i] = cli
	}
	defer func() {
		stopKeepAlives()
		for _, cli := range clients {
			require.NoError(t, cli.Close())
		}
	}()

	prefix := fmt.Sprintf("/dbaas-lease-renewal-soak/%d/", time.Now().UnixNano())
	leaseCount := config.clientCount * config.leasesPerClient
	leases := make([]*leaseRenewalLiveLease, 0, leaseCount)
	errs := make(chan error, leaseCount)
	var readers sync.WaitGroup

	for clientIndex, cli := range clients {
		for leaseIndex := range config.leasesPerClient {
			grant, err := cli.Grant(ctx, config.leaseTTL)
			require.NoError(t, err)
			key := fmt.Sprintf("%s%02d-%02d", prefix, clientIndex, leaseIndex)
			_, err = cli.Put(ctx, key, "live", clientv3.WithLease(grant.ID))
			require.NoError(t, err)
			live := &leaseRenewalLiveLease{id: grant.ID, key: key}
			leases = append(leases, live)

			responses, err := cli.KeepAlive(keepAliveCtx, grant.ID)
			require.NoError(t, err)
			readers.Add(1)
			go func() {
				defer readers.Done()
				for response := range responses {
					if response == nil || response.ID != live.id || response.TTL <= 0 {
						select {
						case errs <- fmt.Errorf("lease %d returned invalid keepalive response: %#v", live.id, response):
						default:
						}
						return
					}
					live.responses.Add(1)
				}
				if keepAliveCtx.Err() == nil {
					select {
					case errs <- fmt.Errorf("lease %d keepalive channel closed while soak was active", live.id):
					default:
					}
				}
			}()
		}
	}

	counts := make([]*atomic.Int64, len(leases))
	for i := range leases {
		counts[i] = &leases[i].responses
	}
	require.NoError(t, waitForLeaseResponses(ctx, errs, counts, nil, 20*time.Second),
		"every lease must receive an initial keepalive response")
	auditCtx, stopAudits := context.WithCancel(ctx)
	var auditReaders sync.WaitGroup
	var auditStats <-chan leaseRenewalAuditStats
	if config.auditInterval > 0 {
		stats := make(chan leaseRenewalAuditStats, 1)
		auditStats = stats
		auditReaders.Add(1)
		go func() {
			defer auditReaders.Done()
			stats <- runLeaseRenewalAuditor(auditCtx, clients[0], leases, config, errs)
		}()
	}

	soakStarted := time.Now()
	for cycle := 1; cycle <= config.failoverCycles; cycle++ {
		if config.duration > 0 {
			cycleDeadline := soakStarted.Add(config.duration * time.Duration(cycle) / time.Duration(config.failoverCycles))
			require.NoErrorf(t, waitForLeaseSoakDeadline(ctx, errs, cycleDeadline),
				"keepalive stream failed before failover cycle %d", cycle)
		}
		output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
		require.NoErrorf(t, err, "failover cycle %d command: %s", cycle, strings.TrimSpace(string(output)))
		t.Logf("failover cycle %d: %s", cycle, strings.TrimSpace(string(output)))
		output, err = waitForKubeBrainRollout(t, ctx, namespace)
		require.NoErrorf(t, err, "failover cycle %d recovery: %s", cycle, strings.TrimSpace(string(output)))

		baseline := make([]int64, len(leases))
		for i := range leases {
			baseline[i] = leases[i].responses.Load()
		}
		require.NoErrorf(t, waitForLeaseResponses(ctx, errs, counts, baseline, 30*time.Second),
			"all leases must receive a fresh keepalive response after failover cycle %d", cycle)

		got, err := clients[0].Get(ctx, prefix, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, got.Kvs, len(leases))
		expectedLeases := make(map[string]clientv3.LeaseID, len(leases))
		for _, live := range leases {
			expectedLeases[live.key] = live.id
		}
		for _, kv := range got.Kvs {
			require.Equal(t, expectedLeases[string(kv.Key)], clientv3.LeaseID(kv.Lease))
		}
	}

	stopKeepAlives()
	stopAudits()
	auditReaders.Wait()
	if auditStats != nil {
		stats := <-auditStats
		require.Positive(t, stats.completedSamples, "duration soak must complete at least one continuous audit sample")
		require.GreaterOrEqual(t, stats.auditedLeases, int64(len(leases)), "duration soak must audit every lease at least once")
		require.Positive(t, stats.firstRevision)
		require.GreaterOrEqual(t, stats.lastRevision, stats.firstRevision)
		t.Logf("lease renewal continuous audit: samples=%d leases=%d transient_errors=%d max_success_gap=%s revisions=%d..%d",
			stats.completedSamples, stats.auditedLeases, stats.transientErrors, stats.maxSuccessGap.Round(time.Millisecond), stats.firstRevision, stats.lastRevision)
	}
	readers.Wait()
	select {
	case keepAliveErr := <-errs:
		require.NoError(t, keepAliveErr)
	default:
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	for i := range leases {
		_, err := clients[i%len(clients)].Revoke(cleanupCtx, leases[i].id)
		require.NoError(t, err)
	}
	for i := range leases {
		response, err := clients[i%len(clients)].TimeToLive(cleanupCtx, leases[i].id)
		require.NoError(t, err)
		require.Equal(t, int64(-1), response.TTL, "lease %d remained live after soak cleanup", leases[i].id)
	}
	listed, err := clients[0].Leases(cleanupCtx)
	require.NoError(t, err)
	owned := make(map[clientv3.LeaseID]struct{}, len(leases))
	for _, lease := range leases {
		owned[lease.id] = struct{}{}
	}
	for _, lease := range listed.Leases {
		_, belongsToSoak := owned[lease.ID]
		require.False(t, belongsToSoak, "lease %d remained listed after soak cleanup", lease.ID)
	}
	got, err := clients[0].Get(cleanupCtx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, got.Kvs)
}

func TestParseLeaseRenewalSoakConfig(t *testing.T) {
	lookup := func(values map[string]string) func(string) (string, bool) {
		return func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		}
	}

	defaults, err := parseLeaseRenewalSoakConfig(lookup(nil))
	require.NoError(t, err)
	require.Equal(t, leaseRenewalSoakConfig{
		clientCount: 8, leasesPerClient: 8, failoverCycles: 3, leaseTTL: 30,
	}, defaults)

	configured, err := parseLeaseRenewalSoakConfig(lookup(map[string]string{
		"KUBEBRAIN_LEASE_RENEWAL_SOAK_CLIENTS":           " 16 ",
		"KUBEBRAIN_LEASE_RENEWAL_SOAK_LEASES_PER_CLIENT": "32",
		"KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_CYCLES":   "6",
		"KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL":               "90",
		"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION":          "2h",
	}))
	require.NoError(t, err)
	require.Equal(t, leaseRenewalSoakConfig{
		clientCount: 16, leasesPerClient: 32, failoverCycles: 6, leaseTTL: 90, duration: 2 * time.Hour,
		auditInterval: 5 * time.Second, auditMaxOutage: 45 * time.Second, auditSample: 64,
	}, configured)

	invalid := []map[string]string{
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_CLIENTS": "0"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_LEASES_PER_CLIENT": "4097"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_CYCLES": "101"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL": "4"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "not-a-duration"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "2s"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "168h1s"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL": "1s"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "3s", "KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL": "99ms"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "3s", "KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE": "301s"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "3s", "KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL": "2s", "KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE": "2s"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "3s", "KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE": "4097"},
		{"KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION": "3s", "KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE": "65"},
		{
			"KUBEBRAIN_LEASE_RENEWAL_SOAK_CLIENTS":           "256",
			"KUBEBRAIN_LEASE_RENEWAL_SOAK_LEASES_PER_CLIENT": "4096",
		},
	}
	for _, values := range invalid {
		_, err := parseLeaseRenewalSoakConfig(lookup(values))
		require.Error(t, err, values)
	}
}

type leaseRenewalAuditError struct {
	semantic bool
	err      error
}

func (e *leaseRenewalAuditError) Error() string { return e.err.Error() }
func (e *leaseRenewalAuditError) Unwrap() error { return e.err }

func runLeaseRenewalAuditor(
	ctx context.Context, cli leaseRenewalAuditClient, leases []*leaseRenewalLiveLease, config leaseRenewalSoakConfig, errs chan<- error,
) leaseRenewalAuditStats {
	ticker := time.NewTicker(config.auditInterval)
	defer ticker.Stop()
	lastSuccess, offset, lastRevision := time.Now(), 0, int64(0)
	var stats leaseRenewalAuditStats
	for {
		select {
		case <-ctx.Done():
			return stats
		case <-ticker.C:
		}
		auditCtx, cancel := context.WithTimeout(ctx, min(config.auditMaxOutage/2, 30*time.Second))
		revision, err := auditLeaseRenewalSample(auditCtx, cli, leases, offset, config.auditSample, lastRevision)
		cancel()
		if err == nil {
			now := time.Now()
			if gap := now.Sub(lastSuccess); gap > stats.maxSuccessGap {
				stats.maxSuccessGap = gap
			}
			lastSuccess, lastRevision = now, revision
			stats.completedSamples++
			stats.auditedLeases += int64(config.auditSample)
			if stats.firstRevision == 0 {
				stats.firstRevision = revision
			}
			stats.lastRevision = revision
			offset = (offset + config.auditSample) % len(leases)
			continue
		}
		if ctx.Err() != nil {
			return stats
		}
		var auditErr *leaseRenewalAuditError
		if (errors.As(err, &auditErr) && auditErr.semantic) || time.Since(lastSuccess) >= config.auditMaxOutage {
			select {
			case errs <- fmt.Errorf("continuous lease audit failed after %s without a complete sample: %w", time.Since(lastSuccess).Round(time.Millisecond), err):
			default:
			}
			return stats
		}
		stats.transientErrors++
	}
}

func auditLeaseRenewalSample(
	ctx context.Context, cli leaseRenewalAuditClient, leases []*leaseRenewalLiveLease, offset, sample int, lastRevision int64,
) (int64, error) {
	revision := lastRevision
	for index := 0; index < sample; index++ {
		live := leases[(offset+index)%len(leases)]
		response, err := cli.Get(ctx, live.key)
		if err != nil {
			return revision, &leaseRenewalAuditError{err: fmt.Errorf("get lease %d key: %w", live.id, err)}
		}
		if response.Header == nil || response.Header.Revision < revision {
			return revision, &leaseRenewalAuditError{semantic: true, err: fmt.Errorf("get lease %d returned non-monotonic revision", live.id)}
		}
		revision = response.Header.Revision
		if len(response.Kvs) != 1 || string(response.Kvs[0].Key) != live.key || string(response.Kvs[0].Value) != "live" || clientv3.LeaseID(response.Kvs[0].Lease) != live.id {
			return revision, &leaseRenewalAuditError{semantic: true, err: fmt.Errorf("lease %d key binding changed", live.id)}
		}
		ttl, err := cli.TimeToLive(ctx, live.id, clientv3.WithAttachedKeys())
		if err != nil {
			return revision, &leaseRenewalAuditError{err: fmt.Errorf("time-to-live lease %d: %w", live.id, err)}
		}
		if ttl.ResponseHeader == nil || ttl.ResponseHeader.Revision < revision {
			return revision, &leaseRenewalAuditError{semantic: true, err: fmt.Errorf("time-to-live lease %d returned non-monotonic revision", live.id)}
		}
		revision = ttl.ResponseHeader.Revision
		if ttl.ID != live.id || ttl.TTL <= 0 || len(ttl.Keys) != 1 || string(ttl.Keys[0]) != live.key {
			return revision, &leaseRenewalAuditError{semantic: true, err: fmt.Errorf("lease %d TTL or attached keys changed", live.id)}
		}
	}
	return revision, nil
}

type fakeLeaseRenewalAuditClient struct {
	getResponse *clientv3.GetResponse
	getErr      error
	ttlResponse *clientv3.LeaseTimeToLiveResponse
	ttlErr      error
}

func (f fakeLeaseRenewalAuditClient) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return f.getResponse, f.getErr
}

func (f fakeLeaseRenewalAuditClient) TimeToLive(context.Context, clientv3.LeaseID, ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	return f.ttlResponse, f.ttlErr
}

func TestAuditLeaseRenewalSampleChecksBindingTTLAndRevision(t *testing.T) {
	lease := &leaseRenewalLiveLease{id: 7, key: "/lease/key"}
	valid := fakeLeaseRenewalAuditClient{
		getResponse: &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 11}, Kvs: []*mvccpb.KeyValue{{Key: []byte(lease.key), Value: []byte("live"), Lease: int64(lease.id)}}},
		ttlResponse: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: &etcdserverpb.ResponseHeader{Revision: 12}, ID: lease.id, TTL: 20, Keys: [][]byte{[]byte(lease.key)}},
	}
	revision, err := auditLeaseRenewalSample(context.Background(), valid, []*leaseRenewalLiveLease{lease}, 0, 1, 10)
	require.NoError(t, err)
	require.Equal(t, int64(12), revision)

	for _, tc := range []struct {
		name     string
		client   fakeLeaseRenewalAuditClient
		want     string
		semantic bool
	}{
		{name: "transport", client: fakeLeaseRenewalAuditClient{getErr: context.DeadlineExceeded}, want: "deadline exceeded"},
		{name: "revision", client: fakeLeaseRenewalAuditClient{getResponse: &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 9}}}, want: "non-monotonic", semantic: true},
		{name: "binding", client: fakeLeaseRenewalAuditClient{getResponse: &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 11}}}, want: "binding changed", semantic: true},
		{name: "ttl", client: fakeLeaseRenewalAuditClient{getResponse: valid.getResponse, ttlResponse: &clientv3.LeaseTimeToLiveResponse{ResponseHeader: &etcdserverpb.ResponseHeader{Revision: 12}, ID: lease.id, TTL: -1}}, want: "TTL or attached keys", semantic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := auditLeaseRenewalSample(context.Background(), tc.client, []*leaseRenewalLiveLease{lease}, 0, 1, 10)
			require.ErrorContains(t, err, tc.want)
			var auditErr *leaseRenewalAuditError
			require.ErrorAs(t, err, &auditErr)
			require.Equal(t, tc.semantic, auditErr.semantic)
		})
	}
}

func waitForLeaseSoakDeadline(ctx context.Context, errs <-chan error, deadline time.Time) error {
	wait := time.Until(deadline)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func TestWaitForLeaseSoakDeadline(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		err := waitForLeaseSoakDeadline(context.Background(), make(chan error), time.Now().Add(time.Millisecond))
		require.NoError(t, err)
	})
	t.Run("stream error", func(t *testing.T) {
		want := fmt.Errorf("keepalive stream closed")
		errs := make(chan error, 1)
		errs <- want
		require.ErrorIs(t, waitForLeaseSoakDeadline(context.Background(), errs, time.Now().Add(time.Hour)), want)
	})
	t.Run("context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, waitForLeaseSoakDeadline(ctx, make(chan error), time.Now().Add(time.Hour)), context.Canceled)
	})
}

func waitForLeaseResponses(
	ctx context.Context,
	errs <-chan error,
	counts []*atomic.Int64,
	baseline []int64,
	timeout time.Duration,
) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err := <-errs:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for fresh responses from all %d leases", len(counts))
		case <-ticker.C:
			allFresh := true
			for i, count := range counts {
				want := int64(0)
				if baseline != nil {
					want = baseline[i]
				}
				if count.Load() <= want {
					allFresh = false
					break
				}
			}
			if allFresh {
				return nil
			}
		}
	}
}
