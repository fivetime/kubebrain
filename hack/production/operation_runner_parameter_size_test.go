package production_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationRunnersRejectOversizedParameters(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "backup", run: func(t *testing.T) {
			f := newBackupRunnerFixture(t, false)
			oversizeOperationParameters(t, f.parameters)
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "operation parameters exceed 65536 bytes")
		}},
		{name: "backup deletion", run: func(t *testing.T) {
			f := newBackupDeletionFixture(t)
			oversizeOperationParameters(t, f.parameters)
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "operation parameters exceed 65536 bytes")
		}},
		{name: "restore cutover", run: func(t *testing.T) {
			f := newCutoverRunnerFixture(t)
			oversizeOperationParameters(t, f.parameters)
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "operation parameters exceed 65536 bytes")
		}},
		{name: "post-restore audit", run: func(t *testing.T) {
			f := newOperationRunnerFixture(t)
			oversizeOperationParameters(t, f.parameters)
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "operation parameters exceed 65536 bytes")
		}},
		{name: "post-restore audit managed parameters", run: func(t *testing.T) {
			f := newOperationRunnerFixture(t)
			oversizeOperationParameters(t, f.parameters)
			originalEnv := append([]string{}, f.env...)
			env := f.env[:0]
			for _, value := range originalEnv {
				if !strings.HasPrefix(value, "PARAMETERS_INPUT=") {
					env = append(env, value)
				}
			}
			f.env = env
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "operation parameters exceed 65536 bytes")
		}},
		{name: "certificate rotation", run: func(t *testing.T) {
			f := newRotationRunnerFixture(t)
			oversizeOperationParameters(t, f.parameters)
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "operation parameters exceed 65536 bytes")
		}},
		{name: "destroy", run: func(t *testing.T) {
			f := newDestroyRunnerFixture(t, true)
			oversizeOperationParameters(t, f.parameters)
			f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "operation parameters exceed 65536 bytes")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}

func TestOperationRunnerAcceptsParametersAtSizeLimit(t *testing.T) {
	f := newDestroyRunnerFixture(t, true)
	parameters := mustRead(t, f.parameters)
	require.Less(t, len(parameters), 65536)
	parameters = append(parameters, []byte(strings.Repeat(" ", 65536-len(parameters)))...)
	require.Len(t, parameters, 65536)
	require.NoError(t, os.WriteFile(f.parameters, parameters, 0o600))

	f.run(t, true, "CLAIM_DIGEST="+fileDigest(t, f.parameters))
}

func oversizeOperationParameters(t *testing.T, path string) {
	t.Helper()
	parameters := append(mustRead(t, path), []byte(strings.Repeat(" ", 65536))...)
	require.NoError(t, os.WriteFile(path, parameters, 0o600))
}
