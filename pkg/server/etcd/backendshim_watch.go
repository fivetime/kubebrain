// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"fmt"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

// watchTranslator converts backend proto events/KeyValues into the etcd mvccpb
// wire types the apiserver expects: watchEventToEtcdEvent (a watch event, with
// its PrevKv resolved via the shim's prevKvResolver) and kvToEtcdKv (the shared
// stored-KV -> mvccpb.KeyValue converter used by both the read and watch paths,
// resolving create_revision/version and the attached lease). Extracted from the
// backendShim God object (audit A2); backendShim embeds a *watchTranslator so
// both methods stay promoted. Its collaborators (prev-kv resolution, lease
// lookup) are borrowed from the owning shim.
type watchTranslator struct {
	shim *backendShim
}

func newWatchTranslator(shim *backendShim) *watchTranslator {
	return &watchTranslator{shim: shim}
}

func (wt *watchTranslator) watchEventToEtcdEvent(ctx context.Context, e *proto.Event) (*mvccpb.Event, error) {
	if err := validateBackendWatchEvent(e); err != nil {
		return nil, err
	}
	revision := watchEventRevision(e)
	switch e.Type {
	case proto.Event_CREATE:
		kv, err := wt.kvToEtcdKv(ctx, e.Kv)
		if err != nil {
			return nil, err
		}
		kv.ModRevision = int64(revision)
		wt.shim.noteEvent(e.Kv.Key, revision, kv, false)
		return &mvccpb.Event{
			Type: mvccpb.PUT,
			Kv:   kv,
		}, nil
	case proto.Event_PUT:
		kv, err := wt.kvToEtcdKv(ctx, e.Kv)
		if err != nil {
			return nil, err
		}
		kv.ModRevision = int64(revision)
		prevKv := wt.shim.cachedPreviousEtcdKv(ctx, e.Kv.Key, revision, kv.Version, kv.CreateRevision)
		// A PUT event is an update, never a create, so its CreateRevision must
		// differ from ModRevision (clientv3.Event.IsCreate reports create iff they
		// are equal). Prefer the create_revision the value carries inline (approach
		// A) — it is already set by kvToEtcdKv. Only when it is unknown (legacy
		// events without inline metadata) derive it from the previous version, and
		// as a last resort synthesize a value just below ModRevision. Previously a
		// failed prev-version lookup unconditionally overwrote a correct inline
		// create_revision with ModRevision, misreporting the update as a create and
		// dropping PrevKv (#52).
		if kv.CreateRevision == 0 || kv.CreateRevision == kv.ModRevision {
			if prevKv != nil {
				kv.CreateRevision = prevKv.CreateRevision
				if kv.CreateRevision == 0 {
					kv.CreateRevision = prevKv.ModRevision
				}
			}
			if kv.CreateRevision == 0 || kv.CreateRevision == kv.ModRevision {
				kv.CreateRevision = kv.ModRevision - 1
			}
		}
		wt.shim.noteEvent(e.Kv.Key, revision, kv, false)
		return &mvccpb.Event{
			Type:   mvccpb.PUT,
			Kv:     kv,
			PrevKv: prevKv,
		}, nil
	case proto.Event_DELETE:
		prevKv, err := wt.kvToEtcdKv(ctx, e.Kv)
		if err != nil {
			return nil, err
		}
		kv := &mvccpb.KeyValue{
			ModRevision: int64(revision),
			Key:         e.Kv.Key,
		}
		// etcd's DELETE event Kv contains only key + delete revision. The deleted
		// generation's create/version/value/lease belong exclusively to PrevKv.
		// Copying create_revision here is observably incompatible and makes a
		// tombstone look like a surviving generation to generic watch consumers.
		wt.shim.noteEvent(e.Kv.Key, revision, nil, true)
		return &mvccpb.Event{
			Type:   mvccpb.DELETE,
			Kv:     kv,
			PrevKv: prevKv,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported watch event type %s", e.Type)
	}
}

func validateBackendWatchEvent(e *proto.Event) error {
	if e == nil || e.Kv == nil {
		return fmt.Errorf("invalid nil watch event")
	}
	switch e.Type {
	case proto.Event_CREATE, proto.Event_PUT:
		// A write event's Kv is the newly committed object, so its revision is
		// the event revision. Event.Revision may be zero in legacy producers, in
		// which case watchEventRevision deliberately falls back to Kv.Revision;
		// when both are present they must agree. Without this check the inline
		// metadata was validated against Kv.Revision and ModRevision was then
		// overwritten with Event.Revision, allowing an impossible lifecycle to
		// escape after validation.
		if e.Kv.Revision == 0 {
			return fmt.Errorf("%s event has zero key revision", e.Type)
		}
		if e.Revision != 0 && e.Revision != e.Kv.Revision {
			return fmt.Errorf("%s event revision %d disagrees with key revision %d", e.Type, e.Revision, e.Kv.Revision)
		}
		return nil
	case proto.Event_DELETE:
		// DELETE intentionally differs: Event.Revision is the deletion revision,
		// while Kv.Revision identifies the deleted previous value used for PrevKv.
		// Both must be explicit and the previous value must strictly precede its
		// tombstone; equality is the old "missing previous value" sentinel that
		// would otherwise be translated into a fabricated version-1 PrevKV.
		if e.Revision == 0 {
			return fmt.Errorf("DELETE event has zero deletion revision")
		}
		if e.Kv.Revision == 0 {
			return fmt.Errorf("DELETE event has zero previous-value revision")
		}
		if e.Kv.Revision >= e.Revision {
			return fmt.Errorf("DELETE event revision %d does not follow previous-value revision %d", e.Revision, e.Kv.Revision)
		}
		return nil
	default:
		return fmt.Errorf("unsupported watch event type %s", e.Type)
	}
}

func (wt *watchTranslator) kvToEtcdKv(ctx context.Context, kv *proto.KeyValue) (*mvccpb.KeyValue, error) {
	if kv == nil {
		return nil, nil
	}
	// Approach A: prefer create_revision/version inlined in the stored value —
	// no metadata lookup, and the envelope is stripped so the client gets the
	// raw value. Legacy (un-enveloped) values fall back to the etcdmeta lookup.
	meta, rawValue, inlined, err := backend.DecodeInlineValueChecked(kv.Value)
	if err != nil {
		return nil, err
	}
	if !inlined {
		rawValue = kv.Value
		var err error
		meta, err = wt.shim.cachedMetadata(ctx, kv.Key, kv.Revision)
		if err != nil {
			return nil, err
		}
	} else if validationErr := backend.ValidateEtcdMetadataAtRevision(meta, kv.Revision, "watch response inline value metadata"); validationErr != nil {
		return nil, validationErr
	}
	if meta.CreateRevision == 0 {
		meta.CreateRevision = kv.Revision
	}
	if meta.Version == 0 {
		meta.Version = 1
	}
	out := &mvccpb.KeyValue{
		Key:            kv.Key,
		Value:          rawValue,
		Version:        int64(meta.Version),
		CreateRevision: int64(meta.CreateRevision),
		ModRevision:    int64(kv.Revision),
	}
	// v2/v3 envelopes authoritatively record this version's non-zero/zero lease.
	// Upgrade-era v1 and raw values predate per-version lease provenance. They
	// may use the live attachment only when this is still the key's current
	// object version; a historical v1/raw row must never borrow a newer binding.
	if inlined && backend.InlineValueLeaseKnown(kv.Value) {
		out.Lease = meta.Lease
	} else {
		lease, leaseErr := wt.currentLegacyLease(ctx, kv.Key, kv.Revision)
		if leaseErr != nil {
			return nil, leaseErr
		}
		out.Lease = lease
	}
	return out, nil
}

func (wt *watchTranslator) currentLegacyLease(ctx context.Context, key []byte, revision uint64) (int64, error) {
	if wt.shim.leaseLookup == nil {
		return 0, nil
	}
	lease := wt.shim.leaseLookup(string(key))
	if lease == 0 {
		return 0, nil
	}
	current, err := wt.shim.backend.Get(ctx, &proto.GetRequest{Key: key})
	if err != nil {
		return 0, err
	}
	if current == nil || current.Kv == nil || current.Kv.Revision != revision {
		return 0, nil
	}
	return lease, nil
}
