package etcdproxy

import (
	"context"
	"slices"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	proxyRetiredClientTimeout = time.Second
	proxyMaxRetiredClients    = 2
)

type retiredProxyClient struct {
	client *clientv3.Client
	timer  *time.Timer
}

// Core unary admission pins the selected transport under the same lock that
// removes it from routing. A topology change must not itself discard a response
// from an already admitted write. This is not replay or outcome reconciliation:
// actual peer failure, caller deadlines and forced retirement remain ambiguous.
func (e *etcdProxy) readyCoreUnaryClient(ctx context.Context) (*clientv3.Client, string, func(), error) {
	if err := e.waitReady(ctx); err != nil {
		return nil, "", nil, err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	if err := e.readyLocked(); err != nil {
		return nil, "", nil, err
	}
	client := e.client
	if e.unaryPins == nil {
		e.unaryPins = make(map[*clientv3.Client]int)
	}
	e.unaryPins[client]++
	return client, e.curLeader, func() { e.releaseCoreUnaryClient(client) }, nil
}

func (e *etcdProxy) releaseCoreUnaryClient(client *clientv3.Client) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.unaryPins[client]--
	if e.unaryPins[client] != 0 {
		return
	}
	delete(e.unaryPins, client)
	for _, retired := range e.retiredClients {
		if retired.client == client {
			e.closeRetiredClientLocked(retired)
			return
		}
	}
}

// Caller holds lock. Stream generations are signalled separately by resetClient;
// streams do not extend retirement. Limit both time and count, evicting the oldest
// generation on churn rather than retaining unbounded old transports.
func (e *etcdProxy) retireClientLocked(client *clientv3.Client) {
	if e.unaryPins[client] == 0 {
		e.closeClient(client)
		return
	}
	for len(e.retiredClients) >= proxyMaxRetiredClients {
		e.closeRetiredClientLocked(e.retiredClients[0])
	}
	retired := &retiredProxyClient{client: client}
	e.retiredClients = append(e.retiredClients, retired)
	duration := e.retiredClientTimeout
	if duration <= 0 {
		duration = proxyRetiredClientTimeout
	}
	e.retirementWorkers.Add(1)
	retired.timer = time.AfterFunc(duration, func() {
		defer e.retirementWorkers.Done()
		e.lock.Lock()
		defer e.lock.Unlock()
		e.closeRetiredClientLocked(retired)
	})
}

func (e *etcdProxy) closeRetiredClientLocked(target *retiredProxyClient) {
	for index, retired := range e.retiredClients {
		if retired != target {
			continue
		}
		e.retiredClients = slices.Delete(e.retiredClients, index, index+1)
		if retired.timer.Stop() {
			e.retirementWorkers.Done()
		}
		e.closeClient(retired.client)
		return
	}
}
