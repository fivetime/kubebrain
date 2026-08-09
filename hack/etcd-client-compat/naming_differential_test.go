package compat

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	etcdresolver "go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	gresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type namingOutcome struct {
	InitialAtomicUpdates []string
	InitialList          []string
	ReplacementUpdates   []string
	ReplacementList      []string
	PrefixIsolated       bool
	LeaseDeleteObserved  bool
	RoundRobinBothReady  bool
	RoundRobinDeleteDone bool
	ResolverInitial      string
	ResolverAfterDelete  string
}

func TestNamingDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run naming differential tests")
	}

	referenceOutcome := runNamingScenario(t, reference, "etcd")
	kubeBrainOutcome := runNamingScenario(t, compatEndpoint(t), "kubebrain")
	require.Equal(t, referenceOutcome, kubeBrainOutcome)
	require.Equal(t, namingOutcome{
		InitialAtomicUpdates: []string{"add:e1:127.0.0.1:2001:metadata-1", "add:e2:127.0.0.1:2002:metadata-2"},
		InitialList:          []string{"e1:127.0.0.1:2001:metadata-1", "e2:127.0.0.1:2002:metadata-2"},
		ReplacementUpdates:   []string{"delete:e1::", "add:e3:127.0.0.1:2003:metadata-3"},
		ReplacementList:      []string{"e2:127.0.0.1:2002:metadata-2", "e3:127.0.0.1:2003:metadata-3"},
		PrefixIsolated:       true,
		LeaseDeleteObserved:  true,
		RoundRobinBothReady:  true,
		RoundRobinDeleteDone: true,
		ResolverInitial:      "SERVING",
		ResolverAfterDelete:  "NOT_SERVING",
	}, kubeBrainOutcome)
}

func TestNamingResolverDoesNotForwardEndpointMetadataToGRPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{compatEndpoint(t)}, DialTimeout: 3 * time.Second, Context: ctx,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	prefix := fmt.Sprintf("/dbaas-naming-metadata/%d", time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	}()

	manager, err := endpoints.NewManager(client, prefix)
	require.NoError(t, err)
	require.NoError(t, manager.AddEndpoint(ctx, prefix+"/ep1", endpoints.Endpoint{
		Addr:     "127.0.0.1:2001",
		Metadata: "user-owned metadata must stay out of grpc resolver addresses",
	}))
	listed, err := manager.List(ctx)
	require.NoError(t, err)
	require.Equal(t, "user-owned metadata must stay out of grpc resolver addresses", listed[prefix+"/ep1"].Metadata)

	builder, err := etcdresolver.NewBuilder(client)
	require.NoError(t, err)
	cc := newCapturingResolverClientConn()
	resolved, err := builder.Build(gresolver.Target{URL: url.URL{Scheme: "etcd", Path: "/" + prefix}}, cc, gresolver.BuildOptions{})
	require.NoError(t, err)
	defer resolved.Close()

	state := cc.waitForState(t, ctx)
	require.Len(t, state.Endpoints, 1)
	require.Len(t, state.Endpoints[0].Addresses, 1)
	require.Equal(t, "127.0.0.1:2001", state.Endpoints[0].Addresses[0].Addr)
	require.Nil(t, state.Endpoints[0].Addresses[0].Metadata)
}

func runNamingScenario(t *testing.T, endpoint, instance string) namingOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Context: ctx,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	base := fmt.Sprintf("/dbaas-naming/%s/%d/", instance, time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, base, clientv3.WithPrefix())
	}()

	managerPrefix := base + "manager"
	manager, err := endpoints.NewManager(client, managerPrefix)
	require.NoError(t, err)
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	updates, err := manager.NewWatchChannel(watchCtx)
	require.NoError(t, err)
	require.NoError(t, manager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewAddUpdateOpts(managerPrefix+"/e1", endpoints.Endpoint{Addr: "127.0.0.1:2001", Metadata: "metadata-1"}),
		endpoints.NewAddUpdateOpts(managerPrefix+"/e2", endpoints.Endpoint{Addr: "127.0.0.1:2002", Metadata: "metadata-2"}),
	}))
	initialAtomicUpdates := namingUpdates(t, receiveNamingUpdates(t, ctx, updates), managerPrefix)
	initialList, err := manager.List(ctx)
	require.NoError(t, err)

	require.NoError(t, manager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewDeleteUpdateOpts(managerPrefix + "/e1"),
		endpoints.NewAddUpdateOpts(managerPrefix+"/e3", endpoints.Endpoint{Addr: "127.0.0.1:2003", Metadata: "metadata-3"}),
	}))
	replacementUpdates := namingUpdates(t, receiveNamingUpdates(t, ctx, updates), managerPrefix)
	replacementList, err := manager.List(ctx)
	require.NoError(t, err)

	otherPrefix := base + "manager-other"
	other, err := endpoints.NewManager(client, otherPrefix)
	require.NoError(t, err)
	require.NoError(t, other.AddEndpoint(ctx, otherPrefix+"/foreign", endpoints.Endpoint{Addr: "127.0.0.1:2999"}))
	mainAfterOther, err := manager.List(ctx)
	require.NoError(t, err)
	otherList, err := other.List(ctx)
	require.NoError(t, err)
	prefixIsolated := len(mainAfterOther) == 2 && len(otherList) == 1

	lease, err := client.Grant(ctx, 10)
	require.NoError(t, err)
	leaseKey := managerPrefix + "/leased"
	require.NoError(t, manager.AddEndpoint(ctx, leaseKey,
		endpoints.Endpoint{Addr: "127.0.0.1:2010", Metadata: "leased"}, clientv3.WithLease(lease.ID)))
	addLeaseUpdate := receiveNamingUpdates(t, ctx, updates)
	require.Len(t, addLeaseUpdate, 1)
	_, err = client.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	deleteLeaseUpdate := receiveNamingUpdates(t, ctx, updates)
	leaseDeleteObserved := len(deleteLeaseUpdate) == 1 &&
		deleteLeaseUpdate[0].Op == endpoints.Delete && deleteLeaseUpdate[0].Key == leaseKey

	servingAddr, stopServing := startNamingHealthServer(t, healthpb.HealthCheckResponse_SERVING)
	defer stopServing()
	notServingAddr, stopNotServing := startNamingHealthServer(t, healthpb.HealthCheckResponse_NOT_SERVING)
	defer stopNotServing()
	resolverPrefix := base + "resolver"
	resolverManager, err := endpoints.NewManager(client, resolverPrefix)
	require.NoError(t, err)
	require.NoError(t, resolverManager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewAddUpdateOpts(resolverPrefix+"/serving", endpoints.Endpoint{Addr: servingAddr}),
		endpoints.NewAddUpdateOpts(resolverPrefix+"/not-serving", endpoints.Endpoint{Addr: notServingAddr}),
	}))
	builder, err := etcdresolver.NewBuilder(client)
	require.NoError(t, err)
	roundRobinConnection, err := grpc.NewClient("etcd:///"+resolverPrefix,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	require.NoError(t, err)
	t.Cleanup(func() { _ = roundRobinConnection.Close() })
	roundRobinClient := healthpb.NewHealthClient(roundRobinConnection)
	roundRobinStatuses := make(map[string]struct{}, 2)
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		response, callErr := roundRobinClient.Check(callCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
		if callErr != nil {
			return false
		}
		roundRobinStatuses[response.Status.String()] = struct{}{}
		return len(roundRobinStatuses) == 2
	}, 5*time.Second, 20*time.Millisecond,
		"round_robin resolver did not route to both READY endpoint subchannels")
	require.NoError(t, resolverManager.DeleteEndpoint(ctx, resolverPrefix+"/not-serving"))
	consecutiveServing := 0
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		response, callErr := roundRobinClient.Check(callCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
		if callErr != nil || response.Status != healthpb.HealthCheckResponse_SERVING {
			consecutiveServing = 0
			return false
		}
		consecutiveServing++
		return consecutiveServing == 20
	}, 5*time.Second, 20*time.Millisecond,
		"round_robin resolver continued routing to an endpoint removed by its watch")
	roundRobinDeleteDone := consecutiveServing == 20
	require.NoError(t, resolverManager.AddEndpoint(ctx, resolverPrefix+"/not-serving",
		endpoints.Endpoint{Addr: notServingAddr}))
	require.NoError(t, roundRobinConnection.Close())
	roundRobinBothReady := len(roundRobinStatuses) == 2

	connection, err := grpc.NewClient("etcd:///"+resolverPrefix,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"pick_first"}`))
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.Close()) }()
	healthClient := healthpb.NewHealthClient(connection)
	initialStatus := namingHealthStatus(t, ctx, healthClient)
	if initialStatus == healthpb.HealthCheckResponse_SERVING.String() {
		require.NoError(t, resolverManager.DeleteEndpoint(ctx, resolverPrefix+"/serving"))
	} else {
		require.NoError(t, resolverManager.DeleteEndpoint(ctx, resolverPrefix+"/not-serving"))
	}
	wantStatus := healthpb.HealthCheckResponse_SERVING.String()
	if initialStatus == wantStatus {
		wantStatus = healthpb.HealthCheckResponse_NOT_SERVING.String()
	}
	afterDelete := ""
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		response, callErr := healthClient.Check(callCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
		if callErr != nil {
			return false
		}
		afterDelete = response.Status.String()
		return afterDelete == wantStatus
	}, 5*time.Second, 20*time.Millisecond)

	// Normalize the initial endpoint selection so map iteration does not make
	// the differential outcome nondeterministic.
	resolverInitial, resolverAfterDelete := initialStatus, afterDelete
	if resolverInitial == healthpb.HealthCheckResponse_NOT_SERVING.String() {
		resolverInitial, resolverAfterDelete = resolverAfterDelete, resolverInitial
	}
	return namingOutcome{
		InitialAtomicUpdates: initialAtomicUpdates,
		InitialList:          namingList(initialList, managerPrefix),
		ReplacementUpdates:   replacementUpdates,
		ReplacementList:      namingList(replacementList, managerPrefix),
		PrefixIsolated:       prefixIsolated,
		LeaseDeleteObserved:  leaseDeleteObserved,
		RoundRobinBothReady:  roundRobinBothReady,
		RoundRobinDeleteDone: roundRobinDeleteDone,
		ResolverInitial:      resolverInitial,
		ResolverAfterDelete:  resolverAfterDelete,
	}
}

func receiveNamingUpdates(t *testing.T, ctx context.Context, updates endpoints.WatchChannel) []*endpoints.Update {
	t.Helper()
	select {
	case update, ok := <-updates:
		require.True(t, ok, "endpoint watch closed")
		return update
	case <-ctx.Done():
		t.Fatalf("endpoint watch update timed out: %v", ctx.Err())
		return nil
	}
}

func namingUpdates(t *testing.T, updates []*endpoints.Update, prefix string) []string {
	t.Helper()
	result := make([]string, 0, len(updates))
	for _, update := range updates {
		op := "add"
		if update.Op == endpoints.Delete {
			op = "delete"
		}
		metadata := ""
		if update.Endpoint.Metadata != nil {
			value, ok := update.Endpoint.Metadata.(string)
			require.True(t, ok, "unexpected endpoint metadata type %T", update.Endpoint.Metadata)
			metadata = value
		}
		result = append(result, fmt.Sprintf("%s:%s:%s:%s",
			op, update.Key[len(prefix)+1:], update.Endpoint.Addr, metadata))
	}
	return result
}

func namingList(values endpoints.Key2EndpointMap, prefix string) []string {
	result := make([]string, 0, len(values))
	for key, endpoint := range values {
		result = append(result, fmt.Sprintf("%s:%s:%v", key[len(prefix)+1:], endpoint.Addr, endpoint.Metadata))
	}
	sort.Strings(result)
	return result
}

func startNamingHealthServer(t *testing.T, status healthpb.HealthCheckResponse_ServingStatus) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", status)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}

func namingHealthStatus(t *testing.T, ctx context.Context, client healthpb.HealthClient) string {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := client.Check(callCtx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)
	return response.Status.String()
}

type capturingResolverClientConn struct {
	states chan gresolver.State
	errors chan error
}

func newCapturingResolverClientConn() *capturingResolverClientConn {
	return &capturingResolverClientConn{
		states: make(chan gresolver.State, 4),
		errors: make(chan error, 4),
	}
}

func (c *capturingResolverClientConn) UpdateState(state gresolver.State) error {
	c.states <- state
	return nil
}

func (c *capturingResolverClientConn) ReportError(err error) {
	c.errors <- err
}

func (c *capturingResolverClientConn) NewAddress([]gresolver.Address) {}

func (c *capturingResolverClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return nil
}

func (c *capturingResolverClientConn) waitForState(t *testing.T, ctx context.Context) gresolver.State {
	t.Helper()
	for {
		select {
		case state := <-c.states:
			if len(state.Endpoints) > 0 {
				return state
			}
		case err := <-c.errors:
			t.Fatalf("resolver reported error: %v", err)
		case <-ctx.Done():
			t.Fatalf("resolver did not publish endpoint state: %v", ctx.Err())
		}
	}
}
