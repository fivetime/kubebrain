package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type config struct {
	endpoint       string
	prefix         string
	iterations     int
	interval       time.Duration
	commandTimeout time.Duration
	dialTimeout    time.Duration
	maxLatency     time.Duration
	leaseTTL       int64
	pdEndpoints    []string
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.endpoint, "endpoint", "", "etcd endpoint")
	flag.StringVar(&cfg.prefix, "prefix", "/kubebrain-rollout-availability/", "exclusive probe key prefix")
	flag.IntVar(&cfg.iterations, "iterations", 0, "number of write/watch probes")
	flag.DurationVar(&cfg.interval, "interval", 100*time.Millisecond, "interval between probes")
	flag.DurationVar(&cfg.commandTimeout, "command-timeout", time.Second, "per-operation timeout")
	flag.DurationVar(&cfg.dialTimeout, "dial-timeout", time.Second, "client dial timeout")
	flag.DurationVar(&cfg.maxLatency, "max-operation-latency", 5*time.Second, "maximum Put-to-Watch latency")
	flag.Int64Var(&cfg.leaseTTL, "lease-ttl", 15, "lease TTL in seconds")
	var pdEndpoints string
	flag.StringVar(&pdEndpoints, "pd-endpoints", "", "comma-separated PD HTTP endpoints")
	flag.Parse()
	if pdEndpoints != "" {
		cfg.pdEndpoints = strings.Split(pdEndpoints, ",")
	}
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintln(os.Stderr, "PROBE_FAIL", err)
		os.Exit(1)
	}
}

func (cfg config) validate() error {
	if cfg.endpoint == "" || cfg.prefix == "" || cfg.iterations <= 0 || cfg.leaseTTL <= 0 {
		return fmt.Errorf("endpoint, prefix, and positive iterations are required")
	}
	if cfg.interval <= 0 || cfg.commandTimeout <= 0 || cfg.dialTimeout <= 0 || cfg.maxLatency <= 0 || cfg.maxLatency > cfg.commandTimeout {
		return fmt.Errorf("interval and timeouts must be positive")
	}
	if len(cfg.pdEndpoints) == 0 {
		return fmt.Errorf("PD endpoints are required")
	}
	for _, endpoint := range cfg.pdEndpoints {
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			return fmt.Errorf("PD endpoint must use http or https: %q", endpoint)
		}
	}
	return nil
}

type pdLeader struct {
	Name     string `json:"name"`
	MemberID uint64 `json:"member_id"`
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
	initialPDLeader, err := readPDLeader(ctx, cfg.pdEndpoints, cfg.dialTimeout)
	if err != nil {
		return fmt.Errorf("backend preflight: %w", err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.endpoint},
		DialTimeout: cfg.dialTimeout,
	})
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close()

	opCtx, cancel := context.WithTimeout(ctx, cfg.commandTimeout)
	_, err = client.Delete(opCtx, cfg.prefix, clientv3.WithPrefix())
	cancel()
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
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, cleanupErr := client.Revoke(cleanupCtx, lease.ID); cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("cleanup revoke lease: %w", cleanupErr)
		}
		if _, cleanupErr := client.Delete(cleanupCtx, cfg.prefix, clientv3.WithPrefix()); cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("cleanup prefix: %w", cleanupErr)
		}
		remaining, cleanupErr := client.Get(cleanupCtx, cfg.prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
		if cleanupErr != nil && retErr == nil {
			retErr = fmt.Errorf("verify prefix cleanup: %w", cleanupErr)
		} else if cleanupErr == nil && len(remaining.Kvs) != 0 && retErr == nil {
			retErr = fmt.Errorf("verify prefix cleanup: %d key remains", len(remaining.Kvs))
		}
	}()
	keepAlive, err := client.KeepAlive(leaseCtx, lease.ID)
	if err != nil {
		return fmt.Errorf("start lease keepalive: %w", err)
	}
	select {
	case response, ok := <-keepAlive:
		if !ok || response == nil || response.TTL <= 0 {
			return fmt.Errorf("initial lease keepalive closed or returned non-positive TTL")
		}
	case <-time.After(cfg.commandTimeout):
		return fmt.Errorf("initial lease keepalive timed out")
	}

	leaseKey := cfg.prefix + "lease"
	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	_, err = client.Put(opCtx, leaseKey, "alive", clientv3.WithLease(lease.ID))
	cancel()
	if err != nil {
		return fmt.Errorf("attach lease key: %w", err)
	}

	watchKey := cfg.prefix + "watch"
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	watch := client.Watch(watchCtx, watchKey, clientv3.WithCreatedNotify())
	select {
	case response, ok := <-watch:
		if !ok || response.Err() != nil || !response.Created {
			return fmt.Errorf("watch creation failed: closed=%t err=%v created=%t", !ok, response.Err(), response.Created)
		}
	case <-time.After(cfg.commandTimeout):
		return fmt.Errorf("watch creation timed out")
	}

	fmt.Println("PROBE_STARTED")
	var maxLatency time.Duration
	lastPDCheck := time.Now()
	for i := 1; i <= cfg.iterations; i++ {
		if time.Since(lastPDCheck) >= 500*time.Millisecond {
			currentPDLeader, pdErr := readPDLeader(ctx, cfg.pdEndpoints, cfg.dialTimeout)
			if pdErr != nil {
				return fmt.Errorf("iteration=%d backend instability: %w", i, pdErr)
			}
			if currentPDLeader != initialPDLeader {
				return fmt.Errorf("iteration=%d backend instability: PD leader changed from %+v to %+v", i, initialPDLeader, currentPDLeader)
			}
			lastPDCheck = time.Now()
		}
		started := time.Now()
		value := strconv.Itoa(i)
		deadline := started.Add(cfg.commandTimeout)
		for {
			opCtx, cancel = context.WithDeadline(ctx, deadline)
			_, putErr := client.Put(opCtx, watchKey, value)
			cancel()
			if putErr == nil {
				break
			}
			opCtx, cancel = context.WithDeadline(ctx, deadline)
			observed, getErr := client.Get(opCtx, watchKey)
			cancel()
			if getErr == nil && len(observed.Kvs) == 1 && string(observed.Kvs[0].Value) == value {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("iteration=%d put unresolved before deadline: put=%v get=%v", i, putErr, getErr)
			}
			time.Sleep(50 * time.Millisecond)
		}

		select {
		case response, ok := <-watch:
			if !ok || response.Err() != nil {
				return fmt.Errorf("iteration=%d watch closed or failed: %v", i, response.Err())
			}
			if len(response.Events) != 1 || string(response.Events[0].Kv.Key) != watchKey || string(response.Events[0].Kv.Value) != value {
				return fmt.Errorf("iteration=%d unexpected watch response: events=%d", i, len(response.Events))
			}
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

		select {
		case response, ok := <-keepAlive:
			if !ok || response == nil || response.TTL <= 0 {
				return fmt.Errorf("iteration=%d lease keepalive closed or returned non-positive TTL", i)
			}
		default:
		}
		time.Sleep(cfg.interval)
	}

	opCtx, cancel = context.WithTimeout(ctx, cfg.commandTimeout)
	ttl, err := client.TimeToLive(opCtx, lease.ID, clientv3.WithAttachedKeys())
	cancel()
	if err != nil {
		return fmt.Errorf("final lease verification failed: %w", err)
	}
	if ttl == nil || ttl.TTL <= 0 || len(ttl.Keys) != 1 || string(ttl.Keys[0]) != leaseKey {
		return fmt.Errorf("final lease verification failed: response=%v", ttl)
	}
	fmt.Printf("PROBE_SUMMARY ok=%d fail=0 total=%d watch=%d lease=alive max_latency_ms=%d\n", cfg.iterations, cfg.iterations, cfg.iterations, maxLatency.Milliseconds())
	return nil
}
