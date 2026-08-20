package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativePITROperationsRejectOversizedParametersBeforeExecution(t *testing.T) {
	tests := []struct {
		name, script, operationType, operationName, requester, action string
	}{
		{"full backup", "run-native-pitr-full-backup-operation.sh", "NativePITRFullBackup", "native-pitr-full-", "platform:native-pitr-full-backup", "retry"},
		{"full restore", "run-native-pitr-full-restore-operation.sh", "NativePITRFullRestore", "native-pitr-restore-", "platform:native-pitr-full-restore", "fail"},
		{"target provisioning", "run-native-pitr-target-provisioning-operation.sh", "NativePITRTargetProvisioning", "native-pitr-provision-", "platform:native-pitr-target-provisioning", "fail"},
		{"target retirement", "run-native-pitr-target-retirement-operation.sh", "NativePITRTargetRetirement", "native-pitr-retire-", "platform:native-pitr-target-retirement", "fail"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			parameters := filepath.Join(dir, "parameters.json")
			data := append([]byte(`{}`), []byte(strings.Repeat(" ", 65535))...)
			require.Len(t, data, 65537)
			require.NoError(t, os.WriteFile(parameters, data, 0o600))
			digest := fmt.Sprintf("%x", sha256.Sum256(data))
			operationName := tt.operationName + digest[:20]
			operationLog := filepath.Join(dir, "operation.log")
			operationctl := filepath.Join(dir, "operationctl")
			claim := fmt.Sprintf(`{"namespace":"kubebrain-operations","name":%q,"operation_id":%q,"instance":"kubebrain","type":%q,"requested_by":%q,"owner":"worker","parameters_secret":%q,"parameters_key":"parameters.json","attempt":1,"parameters_sha256":%q}`, operationName, operationName, tt.operationType, tt.requester, operationName+"-parameters", digest)
			writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf '%%s\\n' \"$*\" >>%q\nif [[ \"$*\" == *\"--action claim\"* ]]; then printf '%%s\\n' %q; fi\n", operationLog, claim))
			producerLog := filepath.Join(dir, "producer.log")
			producer := filepath.Join(dir, "producer")
			writeExecutable(t, producer, fmt.Sprintf("#!/usr/bin/env bash\nprintf called >%q\n", producerLog))
			env := []string{
				"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
				"OPERATIONCTL=" + operationctl, "WORK_DIR=" + dir, "INPUT_ROOT=" + dir,
				"BACKUP_COMMAND=" + producer, "RESTORE_COMMAND=" + producer, "PROVISION=" + producer,
				"BR_BINARY=/bin/true", "RECEIPT_VERIFY=/bin/true",
			}
			output, err := runProductionScriptCommand(t, tt.script, env)
			require.Error(t, err)
			require.Contains(t, string(output), "operation parameters exceed 65536 bytes")
			log, readErr := os.ReadFile(operationLog)
			require.NoError(t, readErr)
			require.Contains(t, string(log), "--action "+tt.action)
			require.NoFileExists(t, producerLog)
		})
	}
}

func TestNativePITRFullBackupRejectsOversizedManagedParameters(t *testing.T) {
	dir := t.TempDir()
	data := append([]byte(`{}`), []byte(strings.Repeat(" ", 65535))...)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	name := "native-pitr-full-" + digest[:20]
	logPath := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	claim := fmt.Sprintf(`{"namespace":"kubebrain-operations","name":%q,"operation_id":%q,"instance":"kubebrain","type":"NativePITRFullBackup","requested_by":"platform:native-pitr-full-backup","owner":"worker","parameters_secret":%q,"parameters_key":"parameters.json","attempt":1,"parameters_sha256":%q}`, name, name, name+"-parameters", digest)
	writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf '%%s\\n' \"$*\" >>%q\nif [[ \"$*\" == *\"--action claim\"* ]]; then printf '%%s\\n' %q; elif [[ \"$*\" == *\"--action parameters\"* ]]; then printf '%%s' %q; fi\n", logPath, claim, string(data)))
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", []string{
		"WORKER_ID=worker", "OPERATIONCTL=" + operationctl, "WORK_DIR=" + dir,
		"BACKUP_COMMAND=/bin/true", "BR_BINARY=/bin/true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "operation parameters exceed 65536 bytes")
	log := string(mustReadProductionFile(t, logPath))
	require.Contains(t, log, "--action parameters")
	require.Contains(t, log, "--action retry")
}

func TestNativePITRFullBackupRechecksFrozenParameterSize(t *testing.T) {
	dir := t.TempDir()
	data := []byte(`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`)
	parameters := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(parameters, data, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	name := "native-pitr-full-" + digest[:20]
	logPath := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	claim := fmt.Sprintf(`{"namespace":"kubebrain-operations","name":%q,"operation_id":%q,"instance":"kubebrain","type":"NativePITRFullBackup","requested_by":"platform:native-pitr-full-backup","owner":"worker","parameters_secret":%q,"parameters_key":"parameters.json","attempt":1,"parameters_sha256":%q}`, name, name, name+"-parameters", digest)
	writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf '%%s\\n' \"$*\" >>%q\nif [[ \"$*\" == *\"--action claim\"* ]]; then printf '%%s\\n' %q; fi\n", logPath, claim))
	cpWrapper := filepath.Join(dir, "cp")
	writeExecutable(t, cpWrapper, "#!/usr/bin/env bash\n/bin/cp \"$@\"\ndest=\"${@: -1}\"\nhead -c 65537 /dev/zero >>\"$dest\"\n")
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"), "WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "WORK_DIR=" + dir, "BACKUP_COMMAND=/bin/true", "BR_BINARY=/bin/true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "operation parameters exceed 65536 bytes")
	require.Contains(t, string(mustReadProductionFile(t, logPath)), "--action retry")
}

func TestNativePITRFullBackupAcceptsParametersAtSizeLimit(t *testing.T) {
	dir := t.TempDir()
	base := []byte(`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`)
	data := append(base, []byte(strings.Repeat(" ", 65536-len(base)))...)
	require.Len(t, data, 65536)
	parameters := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(parameters, data, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	name := "native-pitr-full-" + digest[:20]
	logPath := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	claim := fmt.Sprintf(`{"namespace":"kubebrain-operations","name":%q,"operation_id":%q,"instance":"kubebrain","type":"NativePITRFullBackup","requested_by":"platform:native-pitr-full-backup","owner":"worker","parameters_secret":%q,"parameters_key":"parameters.json","attempt":1,"parameters_sha256":%q}`, name, name, name+"-parameters", digest)
	writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf '%%s\\n' \"$*\" >>%q\nif [[ \"$*\" == *\"--action claim\"* ]]; then printf '%%s\\n' %q; fi\n", logPath, claim))
	producer := filepath.Join(dir, "backup")
	writeExecutable(t, producer, "#!/usr/bin/env bash\nfor arg in \"$@\"; do case \"$arg\" in --attestation-output=*) printf receipt >\"${arg#*=}\";; esac; done\n")
	tlsDir := filepath.Join(dir, "tls")
	require.NoError(t, os.Mkdir(tlsDir, 0o700))
	for _, file := range []string{"ca.crt", "tls.crt", "tls.key"} {
		require.NoError(t, os.WriteFile(filepath.Join(tlsDir, file), []byte("test"), 0o600))
	}
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", []string{
		"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl,
		"WORK_DIR=" + dir, "BACKUP_COMMAND=" + producer, "BR_BINARY=/bin/true", "TLS_DIR=" + tlsDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1",
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(mustReadProductionFile(t, logPath)), "--action succeed")
}
