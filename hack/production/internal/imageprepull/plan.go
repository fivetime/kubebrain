package imageprepull

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Plan reads the exact journal scope and complete hard-placement/Service
// inventories. It creates no Kubernetes resources and never uses current Pod
// locations as a substitute for the eligible pool. Prepare rechecks the plan
// before mutation. Names are random per plan; the journal separately binds its
// unpredictable ownership attempt before CREATE.
func (e Executor) Plan(ctx context.Context, image, clientService string, holdSeconds int64, ttlSeconds int32) ([]JobRequest, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	if e.Journal == nil {
		return nil, errors.New("durable planning requires an explicit scoped recovery journal")
	}
	ctx, cancel := context.WithTimeout(ctx, e.PrepareTimeout)
	defer cancel()
	if err := e.Journal.checkNamespace(ctx, e); err != nil {
		return nil, err
	}
	scope := e.Journal.Scope()
	source, err := e.Client.AppsV1().StatefulSets(scope.Namespace).Get(ctx, scope.SourceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if source.Namespace != scope.Namespace || source.Name != scope.SourceName || source.UID != scope.SourceUID {
		return nil, errors.New("source differs from the explicitly scoped preparation target")
	}
	placement, err := DiscoverPlacement(ctx, e.Client, source)
	if err != nil {
		return nil, err
	}
	services, err := e.Client.CoreV1().Services(scope.Namespace).List(ctx, metav1.ListOptions{Limit: 257})
	if err != nil {
		return nil, err
	}
	if services.Continue != "" || len(services.Items) > 256 {
		return nil, errors.New("preparation Service inventory is incomplete or exceeds its bound")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	requests := make([]JobRequest, 0, len(placement.Nodes))
	for i := range placement.Nodes {
		request := JobRequest{Source: source, Node: &placement.Nodes[i], RuntimeClass: placement.RuntimeClass, PriorityClass: placement.PriorityClass,
			Services: services.Items, ClientServiceName: clientService, Image: image, HoldSeconds: holdSeconds, TTLSeconds: ttlSeconds,
			Name: fmt.Sprintf("kb-prepull-%s-%02d", hex.EncodeToString(nonce[:]), i)}
		if _, err := BuildJob(request); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return requests, nil
}
