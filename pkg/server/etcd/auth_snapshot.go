package etcd

import (
	"context"
	"sync"
)

// authSnapshotCache avoids scanning auth records on every authenticated RPC.
// The persisted config revision is the invalidation signal shared by replicas.
type authSnapshotCache struct {
	repo *authRepository

	mu       sync.Mutex
	snapshot *authSnapshot
}

func newAuthSnapshotCache(backend BackendShim) *authSnapshotCache {
	return &authSnapshotCache{repo: newAuthRepository(backend)}
}

func (c *authSnapshotCache) current(ctx context.Context) (*authSnapshot, error) {
	config, err := c.repo.loadConfig(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshot != nil && c.snapshot.Config == config {
		return c.snapshot, nil
	}
	snapshot, err := c.repo.load(ctx)
	if err != nil {
		return nil, err
	}
	c.snapshot = snapshot
	return snapshot, nil
}
