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

func (c *authSnapshotCache) invalidate() {
	c.mu.Lock()
	c.snapshot = nil
	c.mu.Unlock()
}

func (c *authSnapshotCache) cachedSnapshot() (*authSnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot, c.snapshot != nil
}

func (c *authSnapshotCache) current(ctx context.Context) (*authSnapshot, error) {
	config, exists, err := c.repo.loadConfigState(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshot != nil && c.snapshot.Config == config && c.snapshot.ConfigExists == exists {
		return c.snapshot, nil
	}
	snapshot, err := c.repo.load(ctx)
	if err != nil {
		return nil, err
	}
	c.snapshot = snapshot
	return snapshot, nil
}
