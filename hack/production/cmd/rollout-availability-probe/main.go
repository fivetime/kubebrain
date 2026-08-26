package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type config struct {
	endpoint         string
	directEndpoints  []string
	prefix           string
	iterations       int
	interval         time.Duration
	commandTimeout   time.Duration
	dialTimeout      time.Duration
	maxLatency       time.Duration
	maxDirectLatency time.Duration
	leaseTTL         int64
	pdEndpoints      []string
	expectedStores   int
	maxHeartbeatAge  time.Duration
	maxTSOLatency    time.Duration
	maxRegionLatency time.Duration
	rangeInterval    time.Duration
	snapshotDelay    time.Duration
	streamTimeout    time.Duration
	streamBackoff    time.Duration
	streamMaxBackoff time.Duration
	snapshotDir      string
	caFile           string
	certFile         string
	keyFile          string
	tlsServerName    string
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.endpoint, "endpoint", "", "etcd endpoint")
	var directEndpoints string
	flag.StringVar(&directEndpoints, "direct-endpoints", "", "comma-separated stable direct KubeBrain Pod endpoints")
	flag.StringVar(&cfg.prefix, "prefix", "/kubebrain-rollout-availability/", "exclusive probe key prefix")
	flag.IntVar(&cfg.iterations, "iterations", 0, "number of write/watch probes")
	flag.DurationVar(&cfg.interval, "interval", 100*time.Millisecond, "interval between probes")
	flag.DurationVar(&cfg.commandTimeout, "command-timeout", 10*time.Second, "per-operation timeout")
	flag.DurationVar(&cfg.dialTimeout, "dial-timeout", time.Second, "client dial timeout")
	flag.DurationVar(&cfg.maxLatency, "max-operation-latency", 5*time.Second, "maximum Put-to-Watch latency")
	flag.DurationVar(&cfg.maxDirectLatency, "max-direct-stream-latency", 30*time.Second, "maximum recovery latency for the one direct endpoint being rolled")
	flag.Int64Var(&cfg.leaseTTL, "lease-ttl", 15, "lease TTL in seconds")
	flag.IntVar(&cfg.expectedStores, "expected-up-stores", 3, "exact number of Up TiKV stores")
	flag.DurationVar(&cfg.maxHeartbeatAge, "max-store-heartbeat-age", 20*time.Second, "maximum TiKV store heartbeat age")
	flag.DurationVar(&cfg.maxTSOLatency, "max-pd-tso-latency", time.Second, "maximum PD TSO request latency")
	flag.DurationVar(&cfg.maxRegionLatency, "max-tikv-region-latency", time.Second, "maximum TiKV Region point-read latency")
	flag.DurationVar(&cfg.rangeInterval, "range-stream-interval", time.Second, "minimum interval between complete public RangeStream probes")
	flag.DurationVar(&cfg.snapshotDelay, "snapshot-start-delay", 25*time.Second, "delay before the single complete public Snapshot probe")
	flag.DurationVar(&cfg.streamTimeout, "stream-attempt-timeout", 2*time.Minute, "timeout for one RangeStream or Snapshot attempt")
	flag.DurationVar(&cfg.streamBackoff, "stream-retry-backoff", 100*time.Millisecond, "initial retry backoff after a retryable stream failure")
	flag.DurationVar(&cfg.streamMaxBackoff, "stream-max-retry-backoff", 2*time.Second, "maximum retry backoff after consecutive stream failures")
	flag.StringVar(&cfg.snapshotDir, "snapshot-artifact-dir", "", "writable directory for transient Snapshot artifact validation")
	flag.StringVar(&cfg.caFile, "cacert", "", "trusted CA file for KubeBrain HTTPS endpoints")
	flag.StringVar(&cfg.certFile, "cert", "", "client certificate file for KubeBrain HTTPS endpoints")
	flag.StringVar(&cfg.keyFile, "key", "", "client private key file for KubeBrain HTTPS endpoints")
	flag.StringVar(&cfg.tlsServerName, "tls-server-name", "", "TLS server name for KubeBrain HTTPS endpoints")
	var pdEndpoints string
	flag.StringVar(&pdEndpoints, "pd-endpoints", "", "comma-separated PD HTTP endpoints")
	flag.Parse()
	if directEndpoints != "" {
		cfg.directEndpoints = strings.Split(directEndpoints, ",")
	}
	if pdEndpoints != "" {
		cfg.pdEndpoints = strings.Split(pdEndpoints, ",")
	}
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "PROBE_FAIL", err)
		os.Exit(1)
	}
}

func (cfg config) validate() error {
	if cfg.endpoint == "" || cfg.prefix == "" || cfg.iterations <= 0 || cfg.leaseTTL <= 0 || cfg.expectedStores <= 0 {
		return fmt.Errorf("endpoint, prefix, and positive iterations are required")
	}
	if cfg.interval <= 0 || cfg.commandTimeout <= 0 || cfg.dialTimeout <= 0 || cfg.maxLatency <= 0 || cfg.maxLatency > cfg.commandTimeout || cfg.maxDirectLatency < cfg.maxLatency || cfg.maxHeartbeatAge <= 0 || cfg.maxTSOLatency <= 0 || cfg.maxTSOLatency > cfg.maxLatency || cfg.maxRegionLatency <= 0 || cfg.maxRegionLatency > cfg.maxLatency ||
		cfg.rangeInterval <= 0 || cfg.snapshotDelay <= 0 || cfg.streamTimeout <= 0 || cfg.streamBackoff <= 0 || cfg.streamMaxBackoff < cfg.streamBackoff {
		return fmt.Errorf("interval and timeouts must be positive")
	}
	if cfg.snapshotDir != "" {
		info, err := os.Stat(cfg.snapshotDir)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("snapshot artifact directory must be an existing directory: %q", cfg.snapshotDir)
		}
		canary, err := os.CreateTemp(cfg.snapshotDir, ".kubebrain-rollout-write-check-*")
		if err != nil {
			return fmt.Errorf("snapshot artifact directory must be writable: %q: %w", cfg.snapshotDir, err)
		}
		canaryPath := canary.Name()
		if closeErr := canary.Close(); closeErr != nil {
			_ = os.Remove(canaryPath)
			return fmt.Errorf("close snapshot artifact write check: %w", closeErr)
		}
		if removeErr := os.Remove(canaryPath); removeErr != nil {
			return fmt.Errorf("remove snapshot artifact write check: %w", removeErr)
		}
	}
	if len(cfg.pdEndpoints) == 0 {
		return fmt.Errorf("PD endpoints are required")
	}
	if len(cfg.directEndpoints) < 3 {
		return fmt.Errorf("at least three direct KubeBrain endpoints are required")
	}
	tlsEnabled := cfg.caFile != "" || cfg.certFile != "" || cfg.keyFile != "" || cfg.tlsServerName != ""
	if tlsEnabled && (cfg.caFile == "" || cfg.certFile == "" || cfg.keyFile == "" || cfg.tlsServerName == "") {
		return fmt.Errorf("TLS requires cacert, cert, key, and tls-server-name")
	}
	if strings.HasPrefix(cfg.endpoint, "https://") != tlsEnabled || (!strings.HasPrefix(cfg.endpoint, "http://") && !strings.HasPrefix(cfg.endpoint, "https://")) {
		return fmt.Errorf("KubeBrain endpoint scheme and TLS identity must match: %q", cfg.endpoint)
	}
	seenDirectEndpoints := make(map[string]struct{}, len(cfg.directEndpoints))
	for _, endpoint := range cfg.directEndpoints {
		if (!strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://")) || strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://") == "" {
			return fmt.Errorf("direct KubeBrain endpoint must use http or https: %q", endpoint)
		}
		if _, duplicate := seenDirectEndpoints[endpoint]; duplicate {
			return fmt.Errorf("direct KubeBrain endpoints must be unique: %q", endpoint)
		}
		if strings.HasPrefix(endpoint, "https://") != tlsEnabled {
			return fmt.Errorf("direct KubeBrain endpoint scheme and TLS identity must match: %q", endpoint)
		}
		seenDirectEndpoints[endpoint] = struct{}{}
	}
	for _, endpoint := range cfg.pdEndpoints {
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			return fmt.Errorf("PD endpoint must use http or https: %q", endpoint)
		}
	}
	return nil
}

func (cfg config) clientTLSConfig() (*tls.Config, error) {
	if cfg.caFile == "" {
		return nil, nil
	}
	return transport.TLSInfo{
		TrustedCAFile: cfg.caFile,
		CertFile:      cfg.certFile,
		KeyFile:       cfg.keyFile,
		ServerName:    cfg.tlsServerName,
	}.ClientConfig()
}

func (cfg config) kubeBrainClientConfig(endpoint string, tlsConfig *tls.Config) clientv3.Config {
	clientConfig := clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: cfg.dialTimeout,
		TLS:         tlsConfig,
	}
	if tlsConfig != nil {
		// grpc-go 1.79+ derives both :authority and certificate verification
		// from the dial target unless WithAuthority is explicit. Preserve the
		// audited server name when Service and stable Pod DNS differ from the SAN.
		clientConfig.DialOptions = append(clientConfig.DialOptions, grpc.WithAuthority(cfg.tlsServerName))
	}
	return clientConfig
}

type tikvRegionReader interface {
	Read(context.Context) error
}

type snapshotRegionReader struct {
	client *txnkv.Client
	key    []byte
}

func (r snapshotRegionReader) Read(ctx context.Context) error {
	_, err := r.client.GetSnapshot(math.MaxUint64).Get(ctx, r.key)
	if tikverr.IsErrNotFound(err) {
		return nil
	}
	return err
}

func sampleTiKVRegion(ctx context.Context, reader tikvRegionReader, maxLatency time.Duration) (time.Duration, error) {
	started := time.Now()
	sampleCtx, cancel := context.WithTimeout(ctx, maxLatency)
	err := reader.Read(sampleCtx)
	cancel()
	latency := time.Since(started)
	if err != nil {
		return latency, fmt.Errorf("TiKV Region read failed after %s: %w", latency, err)
	}
	if latency > maxLatency {
		return latency, fmt.Errorf("TiKV Region read latency %s exceeds %s", latency, maxLatency)
	}
	return latency, nil
}

type pdTimestampClient interface {
	GetTS(context.Context) (int64, int64, error)
}

func samplePDTimestamp(ctx context.Context, client pdTimestampClient, maxLatency time.Duration) (time.Duration, error) {
	started := time.Now()
	sampleCtx, cancel := context.WithTimeout(ctx, maxLatency)
	physical, logical, err := client.GetTS(sampleCtx)
	cancel()
	latency := time.Since(started)
	if err != nil {
		return latency, fmt.Errorf("PD TSO request failed after %s: %w", latency, err)
	}
	if physical <= 0 || physical > math.MaxInt64>>18 || logical < 0 || logical >= 1<<18 {
		return latency, fmt.Errorf("PD TSO returned invalid timestamp physical=%d logical=%d", physical, logical)
	}
	if latency > maxLatency {
		return latency, fmt.Errorf("PD TSO latency %s exceeds %s", latency, maxLatency)
	}
	return latency, nil
}

type pdStoresResponse struct {
	Count  int `json:"count"`
	Stores []struct {
		Store struct {
			ID        uint64 `json:"id"`
			Address   string `json:"address"`
			StateName string `json:"state_name"`
		} `json:"store"`
		Status struct {
			LastHeartbeat string `json:"last_heartbeat_ts"`
		} `json:"status"`
	} `json:"stores"`
}

func verifyPDStores(ctx context.Context, endpoints []string, timeout, maxAge time.Duration, expected int) error {
	client := &http.Client{Timeout: timeout}
	var lastErr error
	for _, endpoint := range endpoints {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/pd/api/v1/stores", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			lastErr = err
			continue
		}
		var stores pdStoresResponse
		decodeErr := json.NewDecoder(response.Body).Decode(&stores)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil {
			lastErr = fmt.Errorf("endpoint %s returned status=%d decode=%v", endpoint, response.StatusCode, decodeErr)
			continue
		}
		if stores.Count != expected || len(stores.Stores) != expected {
			return fmt.Errorf("expected %d stores, PD reports count=%d entries=%d", expected, stores.Count, len(stores.Stores))
		}
		now := time.Now()
		storeIDs := make(map[uint64]struct{}, len(stores.Stores))
		storeAddresses := make(map[string]struct{}, len(stores.Stores))
		for _, store := range stores.Stores {
			heartbeat, parseErr := time.Parse(time.RFC3339Nano, store.Status.LastHeartbeat)
			age := now.Sub(heartbeat)
			host, portText, addressErr := net.SplitHostPort(store.Store.Address)
			port, portErr := strconv.Atoi(portText)
			_, duplicateID := storeIDs[store.Store.ID]
			_, duplicateAddress := storeAddresses[store.Store.Address]
			if store.Store.ID == 0 || host == "" || addressErr != nil || portErr != nil || port < 1 || port > 65535 || duplicateID || duplicateAddress || store.Store.StateName != "Up" || parseErr != nil || age < 0 || age > maxAge {
				return fmt.Errorf("TiKV store unhealthy: id=%d address=%q state=%q heartbeat=%q age=%s parse=%v", store.Store.ID, store.Store.Address, store.Store.StateName, store.Status.LastHeartbeat, age, parseErr)
			}
			storeIDs[store.Store.ID] = struct{}{}
			storeAddresses[store.Store.Address] = struct{}{}
		}
		return nil
	}
	return fmt.Errorf("read PD stores: %w", lastErr)
}

type pdLeader struct {
	Name     string `json:"name"`
	MemberID uint64 `json:"member_id"`
}

type directStreamProbe struct {
	endpoint  string
	client    *clientv3.Client
	watch     clientv3.WatchChan
	stopWatch context.CancelFunc
	keepAlive *keepAliveMonitor
}

// Match clientv3's retryConnWait between failed keepalive stream reconnects.
// The first replacement remains immediate; only a replacement that itself
// closes is paced by this interval within the original recovery deadline.
const directKeepAliveRestartWait = 500 * time.Millisecond

type directWatchResult struct {
	endpoint string
	response clientv3.WatchResponse
	ok       bool
	received time.Time
}

type directWatchObservation struct {
	endpoint string
	latency  time.Duration
}

func receiveDirectWatchResponses(ctx context.Context, probes []*directStreamProbe, timeout time.Duration) ([]directWatchResult, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	results := make(chan directWatchResult, len(probes))
	for _, probe := range probes {
		go func(probe *directStreamProbe) {
			select {
			case response, ok := <-probe.watch:
				results <- directWatchResult{endpoint: probe.endpoint, response: response, ok: ok, received: time.Now()}
			case <-waitCtx.Done():
			}
		}(probe)
	}

	received := make([]directWatchResult, 0, len(probes))
	for len(received) < len(probes) {
		select {
		case result := <-results:
			if !result.ok {
				return nil, fmt.Errorf("direct watch closed endpoint=%s", result.endpoint)
			}
			received = append(received, result)
		case <-waitCtx.Done():
			return nil, fmt.Errorf("direct watch recovery exceeded %s: %w", timeout, waitCtx.Err())
		}
	}
	return received, nil
}

func validateDirectWatchLatency(observations []directWatchObservation, fastLimit, recoveryLimit time.Duration) error {
	requiredFast := len(observations) - 1
	fast := 0
	for _, observation := range observations {
		if observation.latency < 0 || observation.latency > recoveryLimit {
			return fmt.Errorf("direct endpoint %s latency %s exceeds bounded recovery latency %s", observation.endpoint, observation.latency, recoveryLimit)
		}
		if observation.latency <= fastLimit {
			fast++
		}
	}
	if fast < requiredFast {
		return fmt.Errorf("fewer than %d direct endpoints met %s latency: got %d", requiredFast, fastLimit, fast)
	}
	return nil
}

func (probe *directStreamProbe) close() {
	if probe.stopWatch != nil {
		probe.stopWatch()
	}
	if probe.keepAlive != nil {
		probe.keepAlive.stop()
	}
	if probe.client != nil {
		_ = probe.client.Close()
	}
}

func readPDLeader(ctx context.Context, endpoints []string, timeout time.Duration) (pdLeader, error) {
	client := &http.Client{Timeout: timeout}
	var lastErr error
	for _, endpoint := range endpoints {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/pd/api/v1/leader", nil)
		if err != nil {
			return pdLeader{}, err
		}
		response, err := client.Do(request)
		if err != nil {
			lastErr = err
			continue
		}
		var leader pdLeader
		decodeErr := json.NewDecoder(response.Body).Decode(&leader)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil || leader.Name == "" || leader.MemberID == 0 {
			lastErr = fmt.Errorf("endpoint %s returned status=%d leader=%+v decode=%v", endpoint, response.StatusCode, leader, decodeErr)
			continue
		}
		return leader, nil
	}
	return pdLeader{}, fmt.Errorf("read PD leader: %w", lastErr)
}

func run(ctx context.Context, cfg config) (retErr error) {
	tlsConfig, err := cfg.clientTLSConfig()
	if err != nil {
		return fmt.Errorf("load KubeBrain TLS identity: %w", err)
	}
	initialPDLeader, err := readPDLeader(ctx, cfg.pdEndpoints, cfg.dialTimeout)
	if err != nil {
		return fmt.Errorf("backend preflight: %w", err)
	}
	if err := verifyPDStores(ctx, cfg.pdEndpoints, cfg.dialTimeout, cfg.maxHeartbeatAge, cfg.expectedStores); err != nil {
		return fmt.Errorf("backend preflight: %w", err)
	}
	pdClientCtx, stopPDClient := context.WithCancel(ctx)
	defer stopPDClient()
	pdClient, err := pd.NewClientWithContext(pdClientCtx, cfg.pdEndpoints, pd.SecurityOption{})
	if err != nil {
		return fmt.Errorf("backend preflight: create PD client: %w", err)
	}
	defer pdClient.Close()
	maxObservedTSOLatency, err := samplePDTimestamp(ctx, pdClient, cfg.maxTSOLatency)
	if err != nil {
		return fmt.Errorf("backend preflight: %w", err)
	}
	tikvClient, err := txnkv.NewClientWithContext(ctx, cfg.pdEndpoints)
	if err != nil {
		return fmt.Errorf("backend preflight: create TiKV client: %w", err)
	}
	defer tikvClient.Close()
	regionReader := snapshotRegionReader{client: tikvClient, key: []byte(cfg.prefix + "tikv-region-probe")}
	maxObservedRegionLatency, err := sampleTiKVRegion(ctx, regionReader, cfg.maxRegionLatency)
	if err != nil {
		return fmt.Errorf("backend preflight: %w", err)
	}
	client, err := clientv3.New(cfg.kubeBrainClientConfig(cfg.endpoint, tlsConfig))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close()

	opCtx, cancel := context.WithTimeout(ctx, cfg.commandTimeout)
	cleaned, err := client.Delete(opCtx, cfg.prefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return fmt.Errorf("clean prefix: %w", err)
	}
	clusterID, lastRevision, err := validateDeleteResponse(cleaned, 0, 1)
	if err != nil {
		return fmt.Errorf("clean prefix: %w", err)
	}

	leaseCtx, stopLease := context.WithCancel(ctx)
	defer stopLease()
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	lease, err := client.Grant(opCtx, cfg.leaseTTL)
	cancel()
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}
	leaseID, leaseRevision, err := validateGrantResponse(lease, clusterID, lastRevision, cfg.leaseTTL)
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}
	lastRevision = leaseRevision
	historyLeaseIDs := make([]clientv3.LeaseID, 0, 2)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), max(5*time.Second, cfg.commandTimeout))
		defer cleanupCancel()
		for index := len(historyLeaseIDs) - 1; index >= 0; index-- {
			historyLeaseID := historyLeaseIDs[index]
			revokedHistory, cleanupErr := client.Revoke(cleanupCtx, historyLeaseID)
			if cleanupErr == nil {
				if revokedHistory == nil {
					cleanupErr = fmt.Errorf("empty response")
				} else {
					_, lastRevision, cleanupErr = validateResponseHeader(revokedHistory.Header, clusterID, lastRevision)
				}
			}
			if cleanupErr != nil && retErr == nil {
				retErr = fmt.Errorf("cleanup revoke Snapshot history lease %d: %w", historyLeaseID, cleanupErr)
			}
		}
		revoked, cleanupErr := client.Revoke(cleanupCtx, leaseID)
		if cleanupErr == nil {
			if revoked == nil {
				cleanupErr = fmt.Errorf("empty response")
			} else {
				_, _, cleanupErr = validateResponseHeader(revoked.Header, clusterID, lastRevision)
			}
		}
		if cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("cleanup revoke lease: %w", cleanupErr)
		}
		deleted, cleanupErr := client.Delete(cleanupCtx, cfg.prefix, clientv3.WithPrefix())
		if cleanupErr == nil {
			_, lastRevision, cleanupErr = validateDeleteResponse(deleted, clusterID, lastRevision)
		}
		if cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("cleanup prefix: %w", cleanupErr)
		}
		remaining, cleanupErr := client.Get(cleanupCtx, cfg.prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
		if cleanupErr == nil {
			cleanupErr = validateAbsentRange(remaining, clusterID, lastRevision)
		}
		if cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("verify prefix cleanup: %w", cleanupErr)
		}
	}()
	keepAlive, err := client.KeepAlive(leaseCtx, leaseID)
	if err != nil {
		return fmt.Errorf("start lease keepalive: %w", err)
	}
	lastKeepAliveRevision := leaseRevision
	select {
	case response, ok := <-keepAlive:
		if !ok {
			return fmt.Errorf("initial lease keepalive closed")
		}
		keepAliveRevision, validateErr := validateKeepAliveResponse(response, clusterID, leaseRevision, leaseID, lease.TTL)
		if validateErr != nil {
			return fmt.Errorf("initial lease keepalive: %w", validateErr)
		}
		lastKeepAliveRevision = keepAliveRevision
		lastRevision = max(lastRevision, keepAliveRevision)
	case <-time.After(cfg.commandTimeout):
		return fmt.Errorf("initial lease keepalive timed out")
	}
	publicKeepAlive := startKeepAliveMonitor(leaseCtx, stopLease, keepAlive, keepAliveMonitorConfig{
		label:           "public",
		clusterID:       clusterID,
		leaseID:         leaseID,
		grantedTTL:      lease.TTL,
		initialRevision: lastKeepAliveRevision,
	})
	defer publicKeepAlive.stop()

	leaseKey := cfg.prefix + "lease"
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	attached, err := client.Put(opCtx, leaseKey, "alive", clientv3.WithLease(leaseID))
	cancel()
	if err != nil {
		return fmt.Errorf("attach lease key: %w", err)
	}
	lastRevision, err = validatePutResponse(attached, clusterID, lastRevision)
	if err != nil {
		return fmt.Errorf("attach lease key: %w", err)
	}
	for index, historyLeaseTTL := range [...]int64{streamProbeHistoryLeaseTTL, streamProbeSecondHistoryLeaseTTL} {
		opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
		historyLease, historyLeaseErr := client.Grant(opCtx, historyLeaseTTL)
		cancel()
		if historyLeaseErr != nil {
			return fmt.Errorf("grant Snapshot history lease %d: %w", index+1, historyLeaseErr)
		}
		historyLeaseID, grantRevision, historyLeaseErr := validateGrantResponse(
			historyLease, clusterID, lastRevision, historyLeaseTTL,
		)
		if historyLeaseErr != nil {
			return fmt.Errorf("grant Snapshot history lease %d: %w", index+1, historyLeaseErr)
		}
		historyLeaseIDs = append(historyLeaseIDs, historyLeaseID)
		lastRevision = grantRevision
	}
	if historyLeaseIDs[0] == historyLeaseIDs[1] {
		return fmt.Errorf("Snapshot history leases returned duplicate IDs")
	}

	streamExpected := newStreamProbeExpectations(cfg.prefix)
	for index := range streamExpected {
		expected := &streamExpected[index]
		opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
		seeded, seedErr := client.Put(opCtx, expected.key, expected.value)
		cancel()
		if seedErr != nil {
			return fmt.Errorf("seed RangeStream key %q: %w", expected.key, seedErr)
		}
		lastRevision, seedErr = validatePutResponse(seeded, clusterID, lastRevision)
		if seedErr != nil {
			return fmt.Errorf("seed RangeStream key %q: %w", expected.key, seedErr)
		}
		expected.revision = lastRevision
	}
	historySeed := &streamExpected[0]
	historyCreateRevision := historySeed.revision
	historySeed.events = append(historySeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: historySeed.value, hash: historySeed.hash, revision: historyCreateRevision,
		createRevision: historyCreateRevision, version: 1,
	})
	updatedValue := historySeed.value + "-updated"
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	updated, updateErr := client.Put(opCtx, historySeed.key, updatedValue)
	cancel()
	if updateErr != nil {
		return fmt.Errorf("update Snapshot history seed %q: %w", historySeed.key, updateErr)
	}
	lastRevision, updateErr = validatePutResponse(updated, clusterID, lastRevision)
	if updateErr != nil {
		return fmt.Errorf("update Snapshot history seed %q: %w", historySeed.key, updateErr)
	}
	historySeed.events = append(historySeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: updatedValue, hash: sha256.Sum256([]byte(updatedValue)), revision: lastRevision,
		createRevision: historyCreateRevision, version: 2,
	})
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	deleted, deleteErr := client.Delete(opCtx, historySeed.key)
	cancel()
	if deleteErr != nil {
		return fmt.Errorf("delete Snapshot history seed %q: %w", historySeed.key, deleteErr)
	}
	_, lastRevision, deleteErr = validateDeleteResponse(deleted, clusterID, lastRevision)
	if deleteErr != nil {
		return fmt.Errorf("delete Snapshot history seed %q: %w", historySeed.key, deleteErr)
	}
	if deleted.Deleted != 1 {
		return fmt.Errorf("delete Snapshot history seed %q returned deleted=%d, want 1", historySeed.key, deleted.Deleted)
	}
	historySeed.events = append(historySeed.events, streamProbeEventExpectation{eventType: mvccpb.DELETE, revision: lastRevision})
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	recreated, recreateErr := client.Put(opCtx, historySeed.key, historySeed.value)
	cancel()
	if recreateErr != nil {
		return fmt.Errorf("recreate Snapshot history seed %q: %w", historySeed.key, recreateErr)
	}
	lastRevision, recreateErr = validatePutResponse(recreated, clusterID, lastRevision)
	if recreateErr != nil {
		return fmt.Errorf("recreate Snapshot history seed %q: %w", historySeed.key, recreateErr)
	}
	historySeed.revision = lastRevision
	historySeed.events = append(historySeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: historySeed.value, hash: historySeed.hash, revision: lastRevision,
		createRevision: lastRevision, version: 1,
	})
	leaseHistorySeed := &streamExpected[1]
	leaseHistoryCreateRevision := leaseHistorySeed.revision
	leaseHistorySeed.events = append(leaseHistorySeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: leaseHistorySeed.value, hash: leaseHistorySeed.hash, revision: leaseHistoryCreateRevision,
		createRevision: leaseHistoryCreateRevision, version: 1,
	})
	leasedHistoryValue := leaseHistorySeed.value + "-lease-a"
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	leasedHistory, leaseHistoryErr := client.Put(opCtx, leaseHistorySeed.key, leasedHistoryValue, clientv3.WithLease(historyLeaseIDs[0]))
	cancel()
	if leaseHistoryErr != nil {
		return fmt.Errorf("attach lease to Snapshot history seed %q: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	lastRevision, leaseHistoryErr = validatePutResponse(leasedHistory, clusterID, lastRevision)
	if leaseHistoryErr != nil {
		return fmt.Errorf("attach lease to Snapshot history seed %q: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	leaseHistorySeed.events = append(leaseHistorySeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: leasedHistoryValue, hash: sha256.Sum256([]byte(leasedHistoryValue)), revision: lastRevision,
		createRevision: leaseHistoryCreateRevision, version: 2, lease: int64(historyLeaseIDs[0]),
		leaseGrantedTTL: streamProbeHistoryLeaseTTL,
	})
	reassignedLeaseHistoryValue := leaseHistorySeed.value + "-lease-b"
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	reassignedLeaseHistory, leaseHistoryErr := client.Put(
		opCtx, leaseHistorySeed.key, reassignedLeaseHistoryValue, clientv3.WithLease(historyLeaseIDs[1]),
	)
	cancel()
	if leaseHistoryErr != nil {
		return fmt.Errorf("reassign Snapshot history seed %q to second lease: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	lastRevision, leaseHistoryErr = validatePutResponse(reassignedLeaseHistory, clusterID, lastRevision)
	if leaseHistoryErr != nil {
		return fmt.Errorf("reassign Snapshot history seed %q to second lease: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	leaseHistorySeed.events = append(leaseHistorySeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: reassignedLeaseHistoryValue, hash: sha256.Sum256([]byte(reassignedLeaseHistoryValue)), revision: lastRevision,
		createRevision: leaseHistoryCreateRevision, version: 3, lease: int64(historyLeaseIDs[1]),
		leaseGrantedTTL: streamProbeSecondHistoryLeaseTTL,
	})
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	deletedLeaseHistory, leaseHistoryErr := client.Delete(opCtx, leaseHistorySeed.key)
	cancel()
	if leaseHistoryErr != nil {
		return fmt.Errorf("delete leased Snapshot history seed %q: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	_, lastRevision, leaseHistoryErr = validateDeleteResponse(deletedLeaseHistory, clusterID, lastRevision)
	if leaseHistoryErr != nil {
		return fmt.Errorf("delete leased Snapshot history seed %q: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	if deletedLeaseHistory.Deleted != 1 {
		return fmt.Errorf("delete leased Snapshot history seed %q returned deleted=%d, want 1", leaseHistorySeed.key, deletedLeaseHistory.Deleted)
	}
	leaseHistorySeed.events = append(leaseHistorySeed.events, streamProbeEventExpectation{eventType: mvccpb.DELETE, revision: lastRevision})
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	recreatedLeaseHistory, leaseHistoryErr := client.Put(opCtx, leaseHistorySeed.key, leaseHistorySeed.value)
	cancel()
	if leaseHistoryErr != nil {
		return fmt.Errorf("recreate unleased Snapshot history seed %q: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	lastRevision, leaseHistoryErr = validatePutResponse(recreatedLeaseHistory, clusterID, lastRevision)
	if leaseHistoryErr != nil {
		return fmt.Errorf("recreate unleased Snapshot history seed %q: %w", leaseHistorySeed.key, leaseHistoryErr)
	}
	leaseHistorySeed.revision = lastRevision
	leaseHistorySeed.events = append(leaseHistorySeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: leaseHistorySeed.value, hash: leaseHistorySeed.hash, revision: lastRevision,
		createRevision: lastRevision, version: 1,
	})

	txnSeeds, txnErr := streamProbeTxnSeeds(streamExpected)
	if txnErr != nil {
		return txnErr
	}
	txnValues := make([]string, len(txnSeeds))
	for index, seed := range txnSeeds {
		seed.events = append(seed.events, streamProbeEventExpectation{
			eventType: mvccpb.PUT, value: seed.value, hash: seed.hash, revision: seed.revision,
			createRevision: seed.revision, version: 1,
		})
		txnValues[index] = fmt.Sprintf("%s-txn-%d", seed.value, index)
	}
	txnOps := []clientv3.Op{
		clientv3.OpPut(txnSeeds[0].key, txnValues[0]),
		clientv3.OpGet(txnSeeds[0].key),
		clientv3.OpDelete(txnSeeds[1].key),
		clientv3.OpPut(txnSeeds[2].key, txnValues[2]),
	}
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	txnResponse, txnErr := client.Txn(opCtx).Then(txnOps...).Commit()
	cancel()
	if txnErr != nil {
		return fmt.Errorf("update Snapshot subrevision seeds: %w", txnErr)
	}
	if txnResponse == nil || !txnResponse.Succeeded || len(txnResponse.Responses) != len(txnOps) {
		return fmt.Errorf("update Snapshot subrevision seeds returned invalid response: response=%+v", txnResponse)
	}
	_, txnRevision, txnErr := validateResponseHeader(txnResponse.Header, clusterID, lastRevision)
	if txnErr != nil {
		return fmt.Errorf("update Snapshot subrevision seeds returned invalid header: %w", txnErr)
	}
	if txnRevision <= lastRevision {
		return fmt.Errorf("update Snapshot subrevision seeds did not advance revision: got=%d previous=%d", txnRevision, lastRevision)
	}
	lastRevision = txnRevision
	validateTxnHeader := func(header *etcdserverpb.ResponseHeader, operation string) error {
		if headerErr := validateTxnOperationHeader(header, txnResponse.Header); headerErr != nil {
			return fmt.Errorf("%s returned invalid nested header: %w", operation, headerErr)
		}
		return nil
	}
	firstPut := txnResponse.Responses[0].GetResponsePut()
	if firstPut == nil || firstPut.PrevKv != nil {
		return fmt.Errorf("update Snapshot subrevision seed %q returned invalid first Put response", txnSeeds[0].key)
	}
	if txnErr = validateTxnHeader(firstPut.Header, "Snapshot Txn first Put"); txnErr != nil {
		return txnErr
	}
	stagedRange := txnResponse.Responses[1].GetResponseRange()
	if stagedRange == nil {
		return fmt.Errorf("read staged Snapshot subrevision seed %q returned an empty Range response", txnSeeds[0].key)
	}
	if txnErr = validateTxnHeader(stagedRange.Header, "Snapshot Txn staged Range"); txnErr != nil {
		return txnErr
	}
	if stagedRange.More || stagedRange.Count != 1 || len(stagedRange.Kvs) != 1 {
		return fmt.Errorf("read staged Snapshot subrevision seed %q returned invalid cardinality: count=%d values=%d more=%t",
			txnSeeds[0].key, stagedRange.Count, len(stagedRange.Kvs), stagedRange.More)
	}
	stagedKV := stagedRange.Kvs[0]
	if stagedKV == nil || string(stagedKV.Key) != txnSeeds[0].key || string(stagedKV.Value) != txnValues[0] ||
		stagedKV.CreateRevision != txnSeeds[0].events[0].createRevision || stagedKV.ModRevision != txnRevision ||
		stagedKV.Version != 2 || stagedKV.Lease != 0 {
		return fmt.Errorf("read staged Snapshot subrevision seed %q returned invalid data", txnSeeds[0].key)
	}
	deletedTxn := txnResponse.Responses[2].GetResponseDeleteRange()
	if deletedTxn == nil || deletedTxn.Deleted != 1 || len(deletedTxn.PrevKvs) != 0 {
		return fmt.Errorf("delete Snapshot subrevision seed %q returned invalid response", txnSeeds[1].key)
	}
	if txnErr = validateTxnHeader(deletedTxn.Header, "Snapshot Txn Delete"); txnErr != nil {
		return txnErr
	}
	lastPut := txnResponse.Responses[3].GetResponsePut()
	if lastPut == nil || lastPut.PrevKv != nil {
		return fmt.Errorf("update Snapshot subrevision seed %q returned invalid last Put response", txnSeeds[2].key)
	}
	if txnErr = validateTxnHeader(lastPut.Header, "Snapshot Txn last Put"); txnErr != nil {
		return txnErr
	}
	for _, index := range []int{0, 2} {
		seed := txnSeeds[index]
		seed.value = txnValues[index]
		seed.hash = sha256.Sum256([]byte(seed.value))
		seed.revision = txnRevision
		seed.events = append(seed.events, streamProbeEventExpectation{
			eventType: mvccpb.PUT, value: seed.value, hash: seed.hash, revision: txnRevision,
			subRevision: int64(index), totalChanges: int64(len(txnSeeds)),
			createRevision: seed.events[0].createRevision, version: 2,
		})
	}
	deletedSeed := txnSeeds[1]
	deletedSeed.events = append(deletedSeed.events, streamProbeEventExpectation{
		eventType: mvccpb.DELETE, revision: txnRevision, subRevision: 1, totalChanges: int64(len(txnSeeds)),
	})
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	recreatedTxnSeed, recreateTxnErr := client.Put(opCtx, deletedSeed.key, deletedSeed.value)
	cancel()
	if recreateTxnErr != nil {
		return fmt.Errorf("recreate deleted Snapshot subrevision seed %q: %w", deletedSeed.key, recreateTxnErr)
	}
	lastRevision, recreateTxnErr = validatePutResponse(recreatedTxnSeed, clusterID, lastRevision)
	if recreateTxnErr != nil {
		return fmt.Errorf("recreate deleted Snapshot subrevision seed %q: %w", deletedSeed.key, recreateTxnErr)
	}
	deletedSeed.revision = lastRevision
	deletedSeed.events = append(deletedSeed.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: deletedSeed.value, hash: deletedSeed.hash, revision: lastRevision,
		createRevision: lastRevision, version: 1,
	})

	nestedSeeds, nestedTxnErr := streamProbeNestedTxnSeeds(streamExpected)
	if nestedTxnErr != nil {
		return nestedTxnErr
	}
	for _, seed := range []*streamProbeExpectation{nestedSeeds.outerPut, nestedSeeds.deleted, nestedSeeds.innerPut} {
		seed.events = append(seed.events, streamProbeEventExpectation{
			eventType: mvccpb.PUT, value: seed.value, hash: seed.hash, revision: seed.revision,
			createRevision: seed.revision, version: 1,
		})
	}
	outerNestedValue := nestedSeeds.outerPut.value + "-nested-outer"
	innerNestedValue := nestedSeeds.innerPut.value + "-nested-inner"
	nestedOp := clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value(nestedSeeds.compare.key), "=", nestedSeeds.compare.value+"-missing")},
		[]clientv3.Op{clientv3.OpPut(nestedSeeds.deleted.key, nestedSeeds.deleted.value+"-unexpected")},
		[]clientv3.Op{
			clientv3.OpGet(nestedSeeds.outerPut.key),
			clientv3.OpDelete(nestedSeeds.deleted.key),
			clientv3.OpPut(nestedSeeds.innerPut.key, innerNestedValue),
		},
	)
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	nestedTxnResponse, nestedTxnErr := client.Txn(opCtx).Then(
		clientv3.OpPut(nestedSeeds.outerPut.key, outerNestedValue), nestedOp,
	).Commit()
	cancel()
	if nestedTxnErr != nil {
		return fmt.Errorf("update nested Snapshot transaction seeds: %w", nestedTxnErr)
	}
	nestedTxnRevision, nestedTxnErr := validateNestedFailureTxnResponse(
		nestedTxnResponse, clusterID, lastRevision, nestedSeeds, outerNestedValue,
	)
	if nestedTxnErr != nil {
		return nestedTxnErr
	}
	lastRevision = nestedTxnRevision
	nestedSeeds.outerPut.value = outerNestedValue
	nestedSeeds.outerPut.hash = sha256.Sum256([]byte(outerNestedValue))
	nestedSeeds.outerPut.revision = nestedTxnRevision
	nestedSeeds.outerPut.events = append(nestedSeeds.outerPut.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: outerNestedValue, hash: nestedSeeds.outerPut.hash, revision: nestedTxnRevision,
		subRevision: 0, totalChanges: 3, createRevision: nestedSeeds.outerPut.events[0].createRevision, version: 2,
	})
	nestedSeeds.deleted.events = append(nestedSeeds.deleted.events, streamProbeEventExpectation{
		eventType: mvccpb.DELETE, revision: nestedTxnRevision, subRevision: 1, totalChanges: 3,
	})
	nestedSeeds.innerPut.value = innerNestedValue
	nestedSeeds.innerPut.hash = sha256.Sum256([]byte(innerNestedValue))
	nestedSeeds.innerPut.revision = nestedTxnRevision
	nestedSeeds.innerPut.events = append(nestedSeeds.innerPut.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: innerNestedValue, hash: nestedSeeds.innerPut.hash, revision: nestedTxnRevision,
		subRevision: 2, totalChanges: 3, createRevision: nestedSeeds.innerPut.events[0].createRevision, version: 2,
	})
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	recreatedNestedSeed, nestedTxnErr := client.Put(opCtx, nestedSeeds.deleted.key, nestedSeeds.deleted.value)
	cancel()
	if nestedTxnErr != nil {
		return fmt.Errorf("recreate nested Snapshot transaction seed %q: %w", nestedSeeds.deleted.key, nestedTxnErr)
	}
	lastRevision, nestedTxnErr = validatePutResponse(recreatedNestedSeed, clusterID, lastRevision)
	if nestedTxnErr != nil {
		return fmt.Errorf("recreate nested Snapshot transaction seed %q: %w", nestedSeeds.deleted.key, nestedTxnErr)
	}
	nestedSeeds.deleted.revision = lastRevision
	nestedSeeds.deleted.events = append(nestedSeeds.deleted.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: nestedSeeds.deleted.value, hash: nestedSeeds.deleted.hash, revision: lastRevision,
		createRevision: lastRevision, version: 1,
	})

	multilevelSeeds, multilevelTxnErr := streamProbeMultilevelTxnSeeds(streamExpected)
	if multilevelTxnErr != nil {
		return multilevelTxnErr
	}
	for _, seed := range []*streamProbeExpectation{
		multilevelSeeds.outerPut, multilevelSeeds.middlePut, multilevelSeeds.deleted, multilevelSeeds.innerPut,
	} {
		seed.events = append(seed.events, streamProbeEventExpectation{
			eventType: mvccpb.PUT, value: seed.value, hash: seed.hash, revision: seed.revision,
			createRevision: seed.revision, version: 1,
		})
	}
	outerMultilevelValue := multilevelSeeds.outerPut.value + "-multilevel-outer"
	middleMultilevelValue := multilevelSeeds.middlePut.value + "-multilevel-middle"
	innerMultilevelValue := multilevelSeeds.innerPut.value + "-multilevel-inner"
	levelTwoOp := clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value(multilevelSeeds.innerCompare.key), "=", multilevelSeeds.innerCompare.value)},
		[]clientv3.Op{
			clientv3.OpGet(multilevelSeeds.middlePut.key),
			clientv3.OpDelete(multilevelSeeds.deleted.key),
			clientv3.OpPut(multilevelSeeds.innerPut.key, innerMultilevelValue),
		},
		[]clientv3.Op{clientv3.OpPut(multilevelSeeds.deleted.key, multilevelSeeds.deleted.value+"-unexpected")},
	)
	levelOneOp := clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Value(multilevelSeeds.outerCompare.key), "=", multilevelSeeds.outerCompare.value)},
		[]clientv3.Op{
			clientv3.OpGet(multilevelSeeds.outerPut.key),
			clientv3.OpPut(multilevelSeeds.middlePut.key, middleMultilevelValue),
			levelTwoOp,
		},
		[]clientv3.Op{clientv3.OpPut(multilevelSeeds.middlePut.key, multilevelSeeds.middlePut.value+"-unexpected")},
	)
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	multilevelTxnResponse, multilevelTxnErr := client.Txn(opCtx).Then(
		clientv3.OpPut(multilevelSeeds.outerPut.key, outerMultilevelValue), levelOneOp,
	).Commit()
	cancel()
	if multilevelTxnErr != nil {
		return fmt.Errorf("update multilevel nested Snapshot transaction seeds: %w", multilevelTxnErr)
	}
	multilevelTxnRevision, multilevelTxnErr := validateMultilevelSuccessTxnResponse(
		multilevelTxnResponse, clusterID, lastRevision, multilevelSeeds, outerMultilevelValue, middleMultilevelValue,
	)
	if multilevelTxnErr != nil {
		return multilevelTxnErr
	}
	lastRevision = multilevelTxnRevision
	for _, update := range []struct {
		seed        *streamProbeExpectation
		value       string
		subRevision int64
	}{
		{seed: multilevelSeeds.outerPut, value: outerMultilevelValue, subRevision: 0},
		{seed: multilevelSeeds.middlePut, value: middleMultilevelValue, subRevision: 1},
		{seed: multilevelSeeds.innerPut, value: innerMultilevelValue, subRevision: 3},
	} {
		update.seed.value = update.value
		update.seed.hash = sha256.Sum256([]byte(update.value))
		update.seed.revision = multilevelTxnRevision
		update.seed.events = append(update.seed.events, streamProbeEventExpectation{
			eventType: mvccpb.PUT, value: update.value, hash: update.seed.hash, revision: multilevelTxnRevision,
			subRevision: update.subRevision, totalChanges: 4,
			createRevision: update.seed.events[0].createRevision, version: 2,
		})
	}
	multilevelSeeds.deleted.events = append(multilevelSeeds.deleted.events, streamProbeEventExpectation{
		eventType: mvccpb.DELETE, revision: multilevelTxnRevision, subRevision: 2, totalChanges: 4,
	})
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	recreatedMultilevelSeed, multilevelTxnErr := client.Put(opCtx, multilevelSeeds.deleted.key, multilevelSeeds.deleted.value)
	cancel()
	if multilevelTxnErr != nil {
		return fmt.Errorf("recreate multilevel nested Snapshot transaction seed %q: %w", multilevelSeeds.deleted.key, multilevelTxnErr)
	}
	lastRevision, multilevelTxnErr = validatePutResponse(recreatedMultilevelSeed, clusterID, lastRevision)
	if multilevelTxnErr != nil {
		return fmt.Errorf("recreate multilevel nested Snapshot transaction seed %q: %w", multilevelSeeds.deleted.key, multilevelTxnErr)
	}
	multilevelSeeds.deleted.revision = lastRevision
	multilevelSeeds.deleted.events = append(multilevelSeeds.deleted.events, streamProbeEventExpectation{
		eventType: mvccpb.PUT, value: multilevelSeeds.deleted.value, hash: multilevelSeeds.deleted.hash, revision: lastRevision,
		createRevision: lastRevision, version: 1,
	})

	watchKey := cfg.prefix + "watch"
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	watch := client.Watch(watchCtx, watchKey, clientv3.WithCreatedNotify())
	select {
	case response, ok := <-watch:
		if !ok {
			return fmt.Errorf("watch creation failed: stream closed")
		}
		lastRevision, err = validateCreatedWatch(response, clusterID, lastRevision)
		if err != nil {
			return fmt.Errorf("watch creation failed: %w", err)
		}
	case <-time.After(cfg.commandTimeout):
		return fmt.Errorf("watch creation timed out")
	}

	directProbes := make([]*directStreamProbe, 0, len(cfg.directEndpoints))
	defer func() {
		for _, probe := range directProbes {
			probe.close()
		}
	}()
	for _, endpoint := range cfg.directEndpoints {
		directClient, directErr := clientv3.New(cfg.kubeBrainClientConfig(endpoint, tlsConfig))
		if directErr != nil {
			return fmt.Errorf("create direct client %s: %w", endpoint, directErr)
		}
		probe := &directStreamProbe{endpoint: endpoint, client: directClient}
		directProbes = append(directProbes, probe)

		directWatchCtx, stopDirectWatch := context.WithCancel(ctx)
		probe.stopWatch = stopDirectWatch
		probe.watch = directClient.Watch(directWatchCtx, watchKey, clientv3.WithCreatedNotify())
		select {
		case response, ok := <-probe.watch:
			if !ok {
				return fmt.Errorf("direct watch creation failed endpoint=%s: stream closed", endpoint)
			}
			if _, validateErr := validateCreatedWatch(response, clusterID, lastRevision); validateErr != nil {
				return fmt.Errorf("direct watch creation failed endpoint=%s: %w", endpoint, validateErr)
			}
		case <-time.After(cfg.commandTimeout):
			return fmt.Errorf("direct watch creation timed out endpoint=%s", endpoint)
		}

		directLeaseCtx, stopDirectLease := context.WithCancel(ctx)
		directKeepAlive, directErr := directClient.KeepAlive(directLeaseCtx, leaseID)
		if directErr != nil {
			stopDirectLease()
			return fmt.Errorf("start direct lease keepalive endpoint=%s: %w", endpoint, directErr)
		}
		directKeepAliveRevision := leaseRevision
		select {
		case response, ok := <-directKeepAlive:
			if !ok {
				stopDirectLease()
				return fmt.Errorf("initial direct lease keepalive closed endpoint=%s", endpoint)
			}
			directKeepAliveRevision, directErr = validateKeepAliveResponse(response, clusterID, directKeepAliveRevision, leaseID, lease.TTL)
			if directErr != nil {
				stopDirectLease()
				return fmt.Errorf("initial direct lease keepalive endpoint=%s: %w", endpoint, directErr)
			}
		case <-time.After(cfg.commandTimeout):
			stopDirectLease()
			return fmt.Errorf("initial direct lease keepalive timed out endpoint=%s", endpoint)
		}
		probe.keepAlive = startKeepAliveMonitor(directLeaseCtx, stopDirectLease, directKeepAlive, keepAliveMonitorConfig{
			label:           "direct endpoint=" + endpoint,
			clusterID:       clusterID,
			leaseID:         leaseID,
			grantedTTL:      lease.TTL,
			initialRevision: directKeepAliveRevision,
			recoveryTimeout: cfg.maxDirectLatency,
			retryWait:       directKeepAliveRestartWait,
			restart: func(restartCtx context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
				return directClient.KeepAlive(restartCtx, leaseID)
			},
		})
	}

	authFixture := newSnapshotAuthFixture(cfg.prefix)
	defer func() {
		var cleanupErr error
		lastRevision, cleanupErr = authFixture.cleanup(client, clusterID, lastRevision, cfg.commandTimeout)
		if cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("cleanup Snapshot auth fixture: %w", cleanupErr)
		}
	}()
	lastRevision, err = authFixture.install(ctx, client, clusterID, lastRevision, cfg.commandTimeout)
	if err != nil {
		return fmt.Errorf("install Snapshot auth fixture: %w", err)
	}

	streamProbe := startStreamProbeGroup(ctx, client, cfg.prefix, streamExpected, clusterID,
		streamWorkerConfig{interval: cfg.rangeInterval, attemptTimeout: cfg.streamTimeout, retryBackoff: cfg.streamBackoff, maxBackoff: cfg.streamMaxBackoff},
		streamWorkerConfig{initialDelay: cfg.snapshotDelay, attemptTimeout: cfg.streamTimeout, retryBackoff: cfg.streamBackoff, maxBackoff: cfg.streamMaxBackoff, successLimit: 1, artifactDir: cfg.snapshotDir,
			restoredTLS:  restoredSnapshotTLSConfig{caFile: cfg.caFile, certFile: cfg.certFile, keyFile: cfg.keyFile, serverName: cfg.tlsServerName},
			restoredAuth: &authFixture.expected, restoredMembers: 3},
	)
	streamProbeStopped := false
	defer func() {
		if !streamProbeStopped {
			_, _ = streamProbe.stop()
		}
	}()

	fmt.Println("PROBE_STARTED")
	var maxLatency time.Duration
	var maxDirectLatency time.Duration
	lastPDCheck := time.Now()
	for i := 1; i <= cfg.iterations; i++ {
		if streamErr := streamProbe.err(); streamErr != nil {
			return fmt.Errorf("iteration=%d: %w", i, streamErr)
		}
		if keepAliveErr := publicKeepAlive.err(); keepAliveErr != nil {
			return fmt.Errorf("iteration=%d: %w", i, keepAliveErr)
		}
		for _, probe := range directProbes {
			if keepAliveErr := probe.keepAlive.err(); keepAliveErr != nil {
				return fmt.Errorf("iteration=%d: %w", i, keepAliveErr)
			}
		}
		tsoLatency, tsoErr := samplePDTimestamp(ctx, pdClient, cfg.maxTSOLatency)
		if tsoErr != nil {
			return fmt.Errorf("iteration=%d backend instability: %w", i, tsoErr)
		}
		if tsoLatency > maxObservedTSOLatency {
			maxObservedTSOLatency = tsoLatency
		}
		regionLatency, regionErr := sampleTiKVRegion(ctx, regionReader, cfg.maxRegionLatency)
		if regionErr != nil {
			return fmt.Errorf("iteration=%d backend instability: %w", i, regionErr)
		}
		if regionLatency > maxObservedRegionLatency {
			maxObservedRegionLatency = regionLatency
		}
		if time.Since(lastPDCheck) >= 500*time.Millisecond {
			currentPDLeader, pdErr := readPDLeader(ctx, cfg.pdEndpoints, cfg.dialTimeout)
			if pdErr != nil {
				return fmt.Errorf("iteration=%d backend instability: %w", i, pdErr)
			}
			if currentPDLeader != initialPDLeader {
				return fmt.Errorf("iteration=%d backend instability: PD leader changed from %+v to %+v", i, initialPDLeader, currentPDLeader)
			}
			if pdErr := verifyPDStores(ctx, cfg.pdEndpoints, cfg.dialTimeout, cfg.maxHeartbeatAge, cfg.expectedStores); pdErr != nil {
				return fmt.Errorf("iteration=%d backend instability: %w", i, pdErr)
			}
			lastPDCheck = time.Now()
		}
		started := time.Now()
		value := strconv.Itoa(i)
		deadline := started.Add(cfg.commandTimeout)
		var putRevision int64
		for {
			opCtx, cancel = context.WithDeadline(ctx, deadline)
			putResponse, putErr := client.Put(opCtx, watchKey, value)
			cancel()
			if putErr == nil {
				putRevision, putErr = validatePutResponse(putResponse, clusterID, lastRevision)
				if putErr == nil {
					break
				}
			}
			opCtx, cancel = context.WithDeadline(ctx, deadline)
			observed, getErr := client.Get(opCtx, watchKey)
			cancel()
			if getErr == nil {
				putRevision, getErr = validateObservedPut(observed, clusterID, lastRevision, watchKey, value)
				if getErr == nil {
					break
				}
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("iteration=%d put unresolved before deadline: put=%v get=%v", i, putErr, getErr)
			}
			time.Sleep(50 * time.Millisecond)
		}

		select {
		case response, ok := <-watch:
			if !ok {
				return fmt.Errorf("iteration=%d watch closed", i)
			}
			watchRevision, validateErr := validatePutWatch(response, clusterID, putRevision, watchKey, value)
			if validateErr != nil {
				return fmt.Errorf("iteration=%d unexpected watch response: %w", i, validateErr)
			}
			lastRevision = max(lastRevision, watchRevision)
		case <-time.After(cfg.commandTimeout):
			return fmt.Errorf("iteration=%d watch timed out", i)
		}
		latency := time.Since(started)
		if latency > maxLatency {
			maxLatency = latency
		}
		if latency > cfg.maxLatency {
			return fmt.Errorf("iteration=%d Put-to-Watch latency %s exceeds %s", i, latency, cfg.maxLatency)
		}

		directRemaining := time.Until(started.Add(cfg.maxDirectLatency))
		if directRemaining <= 0 {
			return fmt.Errorf("iteration=%d direct watch recovery exceeded %s", i, cfg.maxDirectLatency)
		}
		directResults, directErr := receiveDirectWatchResponses(ctx, directProbes, directRemaining)
		if directErr != nil {
			return fmt.Errorf("iteration=%d: %w", i, directErr)
		}
		directObservations := make([]directWatchObservation, 0, len(directResults))
		for _, result := range directResults {
			if _, validateErr := validatePutWatch(result.response, clusterID, putRevision, watchKey, value); validateErr != nil {
				return fmt.Errorf("iteration=%d unexpected direct watch response endpoint=%s: %w", i, result.endpoint, validateErr)
			}
			directLatency := result.received.Sub(started)
			if directLatency > maxDirectLatency {
				maxDirectLatency = directLatency
			}
			directObservations = append(directObservations, directWatchObservation{endpoint: result.endpoint, latency: directLatency})
		}
		if directErr := validateDirectWatchLatency(directObservations, cfg.maxLatency, cfg.maxDirectLatency); directErr != nil {
			return fmt.Errorf("iteration=%d: %w", i, directErr)
		}

		if keepAliveErr := publicKeepAlive.err(); keepAliveErr != nil {
			return fmt.Errorf("iteration=%d: %w", i, keepAliveErr)
		}
		for _, probe := range directProbes {
			if keepAliveErr := probe.keepAlive.err(); keepAliveErr != nil {
				return fmt.Errorf("iteration=%d: %w", i, keepAliveErr)
			}
		}
		time.Sleep(cfg.interval)
	}
	if err := streamProbe.waitForMinimum(ctx, cfg.streamTimeout); err != nil {
		return err
	}
	streamResult, err := streamProbe.stop()
	streamProbeStopped = true
	if err != nil {
		return err
	}
	if err := publicKeepAlive.waitForResponses(ctx, 1, cfg.commandTimeout); err != nil {
		return fmt.Errorf("final: %w", err)
	}
	for _, probe := range directProbes {
		if err := probe.keepAlive.waitForResponses(ctx, 1, cfg.maxDirectLatency); err != nil {
			return fmt.Errorf("final: %w", err)
		}
	}
	keepAliveFinalMarker := time.Now()
	if err := publicKeepAlive.waitForFreshResponse(ctx, keepAliveFinalMarker, cfg.commandTimeout); err != nil {
		return fmt.Errorf("final: %w", err)
	}
	for _, probe := range directProbes {
		if err := probe.keepAlive.waitForFreshResponse(ctx, keepAliveFinalMarker, cfg.maxDirectLatency); err != nil {
			return fmt.Errorf("final: %w", err)
		}
	}
	lastRevision = max(lastRevision, publicKeepAlive.snapshot().lastRevision)
	for _, probe := range directProbes {
		lastRevision = max(lastRevision, probe.keepAlive.snapshot().lastRevision)
	}

	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	ttl, err := client.TimeToLive(opCtx, leaseID, clientv3.WithAttachedKeys())
	cancel()
	if err != nil {
		return fmt.Errorf("final lease verification failed: %w", err)
	}
	if _, err := validateTimeToLiveResponse(ttl, clusterID, lastRevision, leaseID, lease.TTL, leaseKey); err != nil {
		return fmt.Errorf("final lease verification failed: %w", err)
	}
	directKeepAliveRestarts := 0
	directKeepAliveResponses := 0
	var maxDirectKeepAliveRecovery time.Duration
	for _, probe := range directProbes {
		snapshot := probe.keepAlive.snapshot()
		directKeepAliveRestarts += snapshot.restarts
		directKeepAliveResponses += snapshot.responses
		if snapshot.maxRecovery > maxDirectKeepAliveRecovery {
			maxDirectKeepAliveRecovery = snapshot.maxRecovery
		}
	}
	publicKeepAliveResponses := publicKeepAlive.snapshot().responses
	fmt.Printf("PROBE_SUMMARY ok=%d fail=0 total=%d watch=%d direct_watch=%dx%d lease=alive lease_responses=%d direct_lease=alive direct_lease_responses=%d direct_lease_restarts=%d max_direct_lease_recovery_ms=%d direct_endpoints=%d range_stream=%d snapshot=%d stream_retries=%d stream_partial_retries=%d max_latency_ms=%d max_direct_latency_ms=%d max_tso_latency_ms=%d max_region_latency_ms=%d\n", cfg.iterations, cfg.iterations, cfg.iterations, cfg.iterations, len(directProbes), publicKeepAliveResponses, directKeepAliveResponses, directKeepAliveRestarts, maxDirectKeepAliveRecovery.Milliseconds(), len(directProbes), streamResult.rangeOK, streamResult.snapshotOK, streamResult.retries, streamResult.partialRetries, maxLatency.Milliseconds(), maxDirectLatency.Milliseconds(), maxObservedTSOLatency.Milliseconds(), maxObservedRegionLatency.Milliseconds())
	return nil
}
