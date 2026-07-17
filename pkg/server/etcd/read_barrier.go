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

package etcd

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// readBarrierStatusErr classifies failures to synchronize a follower with the
// leader. Every error produced by revisionSyncer means that a linearizable
// revision fence could not be established, so clients must retry rather than
// receive gRPC Unknown. Preserve context and already-classified status errors.
func readBarrierStatusErr(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unavailable, err.Error())
}
