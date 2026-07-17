// Copyright 2026 ByteDance and/or its affiliates
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

package backend

import "sync"

// revisionPins protects event-log commit markers while an uncertain multi-key
// transaction is being resolved. Counts make duplicate revisions safe even
// though the TSO normally allocates each revision only once.
type revisionPins struct {
	mu   sync.Mutex
	refs map[uint64]uint
}

func (p *revisionPins) pin(revision uint64) {
	if revision == 0 {
		return
	}
	p.mu.Lock()
	if p.refs == nil {
		p.refs = make(map[uint64]uint)
	}
	p.refs[revision]++
	p.mu.Unlock()
}

func (p *revisionPins) unpin(revision uint64) {
	if revision == 0 {
		return
	}
	p.mu.Lock()
	if p.refs[revision] <= 1 {
		delete(p.refs, revision)
	} else {
		p.refs[revision]--
	}
	p.mu.Unlock()
}

func (p *revisionPins) min() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var minimum uint64
	for revision := range p.refs {
		if minimum == 0 || revision < minimum {
			minimum = revision
		}
	}
	return minimum
}
