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

import (
	"errors"
	"fmt"
)

// ErrInvalidQuotaMetadata marks a persisted clean quota checkpoint whose usage
// cannot be decoded. Dirty or missing tracking state remains rebuildable and is
// reported separately as ErrQuotaUninitialized.
var ErrInvalidQuotaMetadata = errors.New("quota metadata is inconsistent")

func invalidQuotaMetadataf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidQuotaMetadata, fmt.Sprintf(format, args...))
}
