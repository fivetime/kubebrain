package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseJSONStringArrayAcceptsCanonicalArrays(t *testing.T) {
	values, err := parseJSONStringArray(`["receipt-a.json","receipt-b.json"]`, "RECEIPT_INPUTS_JSON")
	require.NoError(t, err)
	require.Equal(t, []string{"receipt-a.json", "receipt-b.json"}, values)

	empty, err := parseJSONStringArray(`[]`, "RECEIPT_INPUTS_JSON")
	require.NoError(t, err)
	require.Empty(t, empty)
	require.NotNil(t, empty)
}

func TestParseJSONStringArrayRejectsAmbiguousInputs(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty", raw: "", want: "JSON string array"},
		{name: "null", raw: "null", want: "not null"},
		{name: "object", raw: `{"value":["a"]}`, want: "JSON string array"},
		{name: "non string element", raw: `[1]`, want: "JSON string array"},
		{name: "empty string element", raw: `[""]`, want: "[0]"},
		{name: "trailing json", raw: `["a"] {"extra":true}`, want: "single JSON string array"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, err := parseJSONStringArray(tc.raw, "ALLOWED_FORMATS_JSON")
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, values)
		})
	}
}

func TestParseOptionalBoolEnv(t *testing.T) {
	t.Setenv("OBJECTSTORE_BOOL", "")
	value, err := parseOptionalBoolEnv("OBJECTSTORE_BOOL")
	require.NoError(t, err)
	require.False(t, value)

	t.Setenv("OBJECTSTORE_BOOL", "true")
	value, err = parseOptionalBoolEnv("OBJECTSTORE_BOOL")
	require.NoError(t, err)
	require.True(t, value)

	t.Setenv("OBJECTSTORE_BOOL", "0")
	value, err = parseOptionalBoolEnv("OBJECTSTORE_BOOL")
	require.NoError(t, err)
	require.False(t, value)
}

func TestParseOptionalBoolEnvRejectsInvalidValue(t *testing.T) {
	t.Setenv("OBJECTSTORE_BOOL", "truthy")
	value, err := parseOptionalBoolEnv("OBJECTSTORE_BOOL")
	require.ErrorContains(t, err, "OBJECTSTORE_BOOL must be a boolean")
	require.False(t, value)
}
