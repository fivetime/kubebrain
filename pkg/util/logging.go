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

package util

import (
	"crypto/sha256"
	"fmt"
)

const maxLoggedKeyBytes = 256

// LoggedKey returns a bounded diagnostic representation of a client-controlled
// key. Short keys remain readable; large keys retain only a stable fingerprint
// and their original length so logs cannot become a second request payload.
func LoggedKey(key []byte) string {
	if len(key) <= maxLoggedKeyBytes {
		return string(key)
	}
	digest := sha256.Sum256(key)
	return fmt.Sprintf("sha256:%x (length=%d)", digest, len(key))
}
