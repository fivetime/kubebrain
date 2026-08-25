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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEqualEndpointsTreatsAdvertisedEndpointsAsMultiset(t *testing.T) {
	require.True(t, equalEndpoints(
		[]string{"http://node:30079", "http://node:30079", "http://node:30079"},
		[]string{"http://node:30079", "http://node:30079", "http://node:30079"},
	))
	require.True(t, equalEndpoints(
		[]string{"http://b:2379", "http://a:2379", "http://c:2379"},
		[]string{"http://a:2379", "http://c:2379", "http://b:2379"},
	))
	require.False(t, equalEndpoints(
		[]string{"http://node:30079", "http://node:30079", "http://other:30079"},
		[]string{"http://node:30079", "http://node:30079", "http://node:30079"},
	))
}

func TestSplitOptionalDefaultsWithoutAliasing(t *testing.T) {
	fallback := []string{"http://a:2379"}
	require.Equal(t, fallback, splitOptional("BALANCER_SMOKE_UNSET_ENDPOINTS", fallback))
	got := splitOptional("BALANCER_SMOKE_UNSET_ENDPOINTS", fallback)
	got[0] = "changed"
	require.Equal(t, "http://a:2379", fallback[0])
}

func TestSplitOptionalUsesExplicitAdvertisedEndpoints(t *testing.T) {
	t.Setenv("BALANCER_SMOKE_ENDPOINTS", "http://node:30079,http://node:30079,http://node:30079")
	got := splitOptional("BALANCER_SMOKE_ENDPOINTS", []string{"fallback"})
	require.Equal(t, []string{"http://node:30079", "http://node:30079", "http://node:30079"}, got)
	require.Equal(t, 1, uniqueEndpointCount(got))
}
