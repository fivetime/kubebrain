package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// validateWitnessObjectWindow batches physical reads only. Every historical
// expectation remains checked, including older versions of repeatedly written
// keys. The caller owns witness/compaction/evidence rechecks on corruption.
func (b *backend) validateWitnessObjectWindow(ctx context.Context, expectations []txnRevisionIndexExpectation) (*txnRevisionIndexCorruption, error) {
	keys := make([][]byte, 0, len(expectations))
	seen := make(map[string]bool, len(expectations))
	for _, e := range expectations {
		if e.objectRevision == 0 {
			continue
		}
		key := b.coder.EncodeObjectKey(e.userKey, e.objectRevision)
		if !seen[string(key)] {
			seen[string(key)] = true
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	values, incomplete, err := b.loadEventValues(ctx, keys)
	if err != nil {
		return nil, err
	}
	for _, e := range expectations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.objectRevision == 0 {
			continue
		}
		key := b.coder.EncodeObjectKey(e.userKey, e.objectRevision)
		value, present := values[string(key)]
		if incomplete {
			// loadEventValues may discard partial results on a missing key.
			// Resolve exact evidence, rather than falsely blaming the first key
			// of that batch or caching a missing result across operator repair.
			value, err = b.kv.Get(ctx, key)
			present = err == nil
			if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
				return nil, err
			}
		}
		if !present {
			return &txnRevisionIndexCorruption{key: key, missing: true, revision: e.revision},
				fmt.Errorf("%w: persisted witness at revision %d references a missing object version", ErrTxnWitnessCorrupt, e.revision)
		}
		if err := b.validateEventObjectValue(ctx, e.userKey, e.objectRevision, value); err != nil {
			if !errors.Is(err, ErrInvalidMVCCMetadata) {
				return nil, err
			}
			return &txnRevisionIndexCorruption{key: key, expected: append([]byte(nil), value...), revision: e.revision},
				fmt.Errorf("%w: persisted witness at revision %d references an invalid object value: %v", ErrTxnWitnessCorrupt, e.revision, err)
		}
	}
	return nil, nil
}
