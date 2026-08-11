package nativepitr

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// InspectFencedSourceRevision returns the highest committed etcd revision
// visible in a tenant after its restoration fence has excluded all writers.
// The durable watermark covers compacted/deleted latest versions; the physical
// object scan detects a lagging watermark and rejects unknown encodings.
func InspectFencedSourceRevision(ctx context.Context, store storage.KvStorage, keyspace string) (uint64, error) {
	ks, err := coder.NewKeyspace(keyspace)
	if err != nil {
		return 0, err
	}
	durable, err := backend.ReadDurableRevision(ctx, store, keyspace)
	if err != nil {
		return 0, fmt.Errorf("read source durable revision: %w", err)
	}
	ts, err := store.GetTimestampOracle(ctx)
	if err != nil {
		return 0, fmt.Errorf("get fenced source snapshot TSO: %w", err)
	}
	it, err := store.Iter(ctx, ks.ObjectKeyspaceStart(), ks.ObjectKeyspaceEnd(), ts, 0)
	if err != nil {
		return 0, fmt.Errorf("scan fenced source revisions: %w", err)
	}
	defer it.Close()
	maxRevision := durable
	c := ks.NewCoder()
	for {
		if err := it.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("scan fenced source revisions: %w", err)
		}
		key := append([]byte(nil), it.Key()...)
		if ks.IsInternalStorageKey(key) {
			continue
		}
		_, revision, err := c.Decode(key)
		if err != nil {
			return 0, fmt.Errorf("decode fenced source object key: %w", err)
		}
		if revision > maxRevision {
			maxRevision = revision
		}
	}
	if maxRevision == 0 {
		return 0, errors.New("fenced source revision is zero")
	}
	return maxRevision, nil
}
