// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import "context"

type rangeTxnOwnerKey struct{}

// BeginRangeTxn implements Backend. The ownership marker lets writes issued by
// this transaction reuse the exclusive lock instead of trying to RLock it.
func (b *backend) BeginRangeTxn(ctx context.Context) (context.Context, func()) {
	b.logicalWriteMu.Lock()
	return b.withLogicalWriteOwnership(ctx), b.logicalWriteMu.Unlock
}

func (b *backend) withLogicalWriteOwnership(ctx context.Context) context.Context {
	return context.WithValue(ctx, rangeTxnOwnerKey{}, b)
}

func (b *backend) lockLogicalWrite(ctx context.Context) func() {
	if owner, _ := ctx.Value(rangeTxnOwnerKey{}).(*backend); owner == b {
		return func() {}
	}
	b.logicalWriteMu.RLock()
	return b.logicalWriteMu.RUnlock
}
