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

package capability

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSupportedDocumentIsCanonical(t *testing.T) {
	document := SupportedDocument()
	require.NoError(t, document.Validate())
	require.True(t, document.Has(SnapshotDrainPinned))

	document.Capabilities[0] = "mutated"
	require.Equal(t, SnapshotDrainPinned, SupportedDocument().Capabilities[0],
		"callers must not mutate the process-wide capability set")
}

func TestDocumentValidationRejectsNonCanonicalInput(t *testing.T) {
	for name, document := range map[string]Document{
		"wrong format": {Format: "v2"},
		"empty":        {Format: DocumentFormat, Capabilities: []string{""}},
		"uppercase":    {Format: DocumentFormat, Capabilities: []string{"Unsafe"}},
		"control":      {Format: DocumentFormat, Capabilities: []string{"unsafe\nname"}},
		"too long":     {Format: DocumentFormat, Capabilities: []string{"a" + string(make([]byte, 128))}},
		"duplicate":    {Format: DocumentFormat, Capabilities: []string{"a", "a"}},
		"unsorted":     {Format: DocumentFormat, Capabilities: []string{"b", "a"}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, document.Validate())
		})
	}
}
