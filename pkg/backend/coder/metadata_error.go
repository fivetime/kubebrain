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

package coder

import "errors"

// ErrInvalidMVCCMetadata marks a persisted revision index, object key, or
// revision watermark that violates KubeBrain's MVCC storage encoding.
var ErrInvalidMVCCMetadata = errors.New("MVCC metadata is inconsistent")

type invalidMVCCMetadataError struct {
	cause error
}

func (e invalidMVCCMetadataError) Error() string { return e.cause.Error() }

func (e invalidMVCCMetadataError) Unwrap() []error {
	return []error{ErrInvalidMVCCMetadata, e.cause}
}

// MarkInvalidMVCCMetadata classifies a deterministic persisted encoding error
// without changing its diagnostic text or hiding its original cause.
func MarkInvalidMVCCMetadata(err error) error {
	if err == nil || errors.Is(err, ErrInvalidMVCCMetadata) {
		return err
	}
	return invalidMVCCMetadataError{cause: err}
}
