package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseJSONStringArray(t *testing.T) {
	values, err := parseJSONStringArray(`["kubebrain.logical.v2","kubebrain.object-audit.v1"]`)
	require.NoError(t, err)
	require.Equal(t, []string{"kubebrain.logical.v2", "kubebrain.object-audit.v1"}, values)

	empty, err := parseJSONStringArray(`[]`)
	require.NoError(t, err)
	require.Empty(t, empty)
	require.NotNil(t, empty)
}

func TestParseJSONStringArrayRejectsNullAndTrailingJSON(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "null", raw: `null`, want: "null"},
		{name: "object", raw: `{"format":"kubebrain.logical.v2"}`, want: "[]string"},
		{name: "trailing", raw: `["kubebrain.logical.v2"] {"trailing":true}`, want: "trailing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, err := parseJSONStringArray(tc.raw)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, values)
		})
	}
}
