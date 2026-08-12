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

package tso

import (
	"errors"
	"sync/atomic"
)

var ErrRevisionExhausted = errors.New("etcd MVCC revision space exhausted")

// TSO tracks the locally observed and continuously committed revision watermarks.
type TSO interface {
	// Init sets both watermarks to the recovered durable revision.
	Init(start uint64)

	// GetRevision returns the max committed continuous revision grow from init revision
	GetRevision() (maxCommittedRevision uint64)

	// Dealt returns the highest transaction revision observed locally. It may be
	// ahead of the continuously committed watermark while publication is pending.
	Dealt() (revision uint64)

	// AdvanceDealFloor records a transaction-local durable allocation without
	// publishing it as continuously committed.
	AdvanceDealFloor(revision uint64)

	// Commit is used for notifying that txn with revision has been done
	Commit(revision uint64)
}

// todo: implement TSOController refer to tidb placement driver
// 		 https://github.com/tikv/pd/blob/master/server/tso

// naiveTSO is an in-memory projection of durable transaction revision state.
type naiveTSO struct {
	committedRevision uint64
	dealRevision      uint64
}

// GetRevision implement TSO interface
func (n *naiveTSO) GetRevision() (maxCommittedRevision uint64) {
	return atomic.LoadUint64(&n.committedRevision)
}

// Dealt implement TSO interface
func (n *naiveTSO) Dealt() (revision uint64) {
	return atomic.LoadUint64(&n.dealRevision)
}

func (n *naiveTSO) AdvanceDealFloor(revision uint64) {
	for {
		current := atomic.LoadUint64(&n.dealRevision)
		if revision <= current || atomic.CompareAndSwapUint64(&n.dealRevision, current, revision) {
			return
		}
	}
}

// Commit implement TSO interface
func (n *naiveTSO) Commit(revision uint64) {
	// The committed revision is monotonic — it must never move backwards.
	// A plain store let a stale advance (e.g. the event collector racing a
	// watch-overflow reset, or an unsigned-underflow re-trigger) pull the
	// cluster read revision back, corrupting reads and watches.
	for {
		cur := atomic.LoadUint64(&n.committedRevision)
		if revision <= cur {
			break
		}
		if atomic.CompareAndSwapUint64(&n.committedRevision, cur, revision) {
			break
		}
	}
	// in case leader transfer, need to update tso and pre tso
	for {
		preTSO := atomic.LoadUint64(&n.dealRevision)
		if preTSO >= revision {
			break
		}
		if atomic.CompareAndSwapUint64(&n.dealRevision, preTSO, revision) {
			break
		}
	}
}

// Init implement TSO interface
func (n *naiveTSO) Init(start uint64) {
	atomic.StoreUint64(&n.committedRevision, start)
	atomic.StoreUint64(&n.dealRevision, start)
}

func NewTSO() TSO {
	return &naiveTSO{}
}
