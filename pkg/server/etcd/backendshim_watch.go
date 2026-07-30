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

	"go.etcd.io/etcd/api/v3/mvccpb"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

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
	if e == nil || e.Kv == nil {
		return nil, fmt.Errorf("invalid nil watch event")
	}
	revision := watchEventRevision(e)
	switch e.Type {
	case proto.Event_CREATE:
		kv := wt.kvToEtcdKv(ctx, e.Kv)
		kv.ModRevision = int64(revision)
		wt.shim.noteEvent(e.Kv.Key, revision, kv, false)
		return &mvccpb.Event{
			Type: mvccpb.PUT,
			Kv:   kv,
		}, nil
	case proto.Event_PUT:
		kv := wt.kvToEtcdKv(ctx, e.Kv)
		kv.ModRevision = int64(revision)
		prevKv := wt.shim.cachedPreviousEtcdKv(e.Kv.Key, revision, kv.Version, kv.CreateRevision)
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
		prevKv := wt.kvToEtcdKv(ctx, e.Kv)
		kv := &mvccpb.KeyValue{
			ModRevision: int64(revision),
			Key:         e.Kv.Key,
		}
		if prevKv != nil {
			kv.CreateRevision = prevKv.CreateRevision
			if kv.CreateRevision == 0 {
				kv.CreateRevision = prevKv.ModRevision
			}
		}
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

func (wt *watchTranslator) kvToEtcdKv(ctx context.Context, kv *proto.KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	// Approach A: prefer create_revision/version inlined in the stored value —
	// no metadata lookup, and the envelope is stripped so the client gets the
	// raw value. Legacy (un-enveloped) values fall back to the etcdmeta lookup.
	meta, rawValue, inlined := backend.DecodeInlineValue(kv.Value)
	if !inlined {
		rawValue = kv.Value
		var err error
		meta, err = wt.shim.cachedMetadata(ctx, kv.Key, kv.Revision)
		if err != nil {
			klog.V(4).InfoS("failed to read etcd metadata", "key", kv.Key, "revision", kv.Revision, "err", err)
			meta = backend.EtcdMetadata{CreateRevision: kv.Revision, Version: 1}
		}
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
	// etcd returns the lease attached to the key on Get/Range and in watch
	// events. A v2 value envelope records the lease of THIS specific version
	// (review #9), so historical reads, prevKv, and delete events report the
	// lease the key held at that revision — use it directly. Legacy/v1 values
	// carry no per-version lease (meta.Lease == 0 and inlined via v1), so fall
	// back to the live keyLeaseIndex (current binding) as before. Only the leader
	// has that index populated; followers proxy reads to it. The fast path inside
	// the resolver skips the lease mutex entirely when no key holds a lease.
	if meta.Lease != 0 {
		out.Lease = meta.Lease
	} else if wt.shim.leaseLookup != nil {
		out.Lease = wt.shim.leaseLookup(string(kv.Key))
	}
	return out
}
