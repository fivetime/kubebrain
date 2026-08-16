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

package endpoint

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func normalizeContextStatus(err error) error {
	other, contextErr := splitContextError(err)
	if other != nil {
		return other
	}
	if contextErr != nil {
		return status.FromContextError(contextErr).Err()
	}
	return nil
}

func splitContextError(err error) (other, contextErr error) {
	if err == nil {
		return nil, nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			childOther, childContext := splitContextError(child)
			other = errors.Join(other, childOther)
			contextErr = errors.Join(contextErr, childContext)
		}
		return other, contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	return err, nil
}

func normalizeContextStatusUnary(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	response, err := handler(ctx, req)
	return response, normalizeContextStatus(err)
}

func normalizeContextStatusStream(
	srv any,
	stream grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	return normalizeContextStatus(handler(srv, stream))
}
