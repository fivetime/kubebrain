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

func TestParseOptionalBoolEnv(t *testing.T) {
	t.Setenv("METERING_STORAGE_BOOL", "")
	value, err := parseOptionalBoolEnv("METERING_STORAGE_BOOL")
	require.NoError(t, err)
	require.False(t, value)

	t.Setenv("METERING_STORAGE_BOOL", "true")
	value, err = parseOptionalBoolEnv("METERING_STORAGE_BOOL")
	require.NoError(t, err)
	require.True(t, value)

	t.Setenv("METERING_STORAGE_BOOL", "0")
	value, err = parseOptionalBoolEnv("METERING_STORAGE_BOOL")
	require.NoError(t, err)
	require.False(t, value)
}

func TestParseOptionalBoolEnvRejectsInvalidValue(t *testing.T) {
	t.Setenv("METERING_STORAGE_BOOL", "definitely")
	value, err := parseOptionalBoolEnv("METERING_STORAGE_BOOL")
	require.ErrorContains(t, err, "METERING_STORAGE_BOOL must be a boolean")
	require.False(t, value)
}

func TestValidateObjectStoreEndpoint(t *testing.T) {
	require.NoError(t, validateObjectStoreEndpoint("SOURCE_S3_ENDPOINT", "https://s3.example"))
}

func TestValidateObjectStoreEndpointRejectsUnsafeIdentity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "control character", endpoint: "https://s3.example\nother"},
		{name: "DEL", endpoint: "https://s3.example\x7fother"},
		{name: "quote", endpoint: `https://s3.example"other`},
		{name: "backslash", endpoint: `https://s3.example\other`},
	} {
		for _, variable := range []string{"SOURCE_S3_ENDPOINT", "METERING_S3_ENDPOINT"} {
			t.Run(variable+"/"+tc.name, func(t *testing.T) {
				err := validateObjectStoreEndpoint(variable, tc.endpoint)
				require.ErrorContains(t, err, variable+" contains unsupported characters")
			})
		}
	}
}
