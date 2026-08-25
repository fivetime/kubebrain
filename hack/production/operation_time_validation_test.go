package production_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalVersionIDValidationIsPortableBeyondMuslRegexLimit(t *testing.T) {
	script, err := filepath.Abs("operation-time-validation.sh")
	require.NoError(t, err)
	command := `. "$1"
valid_512="a$(printf '%0511d' 0 | tr 0 a)"
invalid_513="a$(printf '%0512d' 0 | tr 0 a)"
operation_is_external_version_id "kms/prod/jwt/versions/42" &&
operation_is_external_version_id "$valid_512" &&
! operation_is_external_version_id "$invalid_513" &&
! operation_is_external_version_id "" &&
! operation_is_external_version_id "/starts-with-slash" &&
! operation_is_external_version_id "kms/version with-space"`
	output, err := runProductionCommand(t, "bash", []string{"-c", command, "validation-test", script}, nil)
	require.NoError(t, err, string(output))
}
