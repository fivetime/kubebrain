package nativepitr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAES256KeyFileAndPrivateSnapshot(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(source, []byte(strings.Repeat("a", 64)+"\n"), 0o400))
	key, err := ReadAES256KeyFile(source)
	require.NoError(t, err)
	require.Len(t, key, 32)
	staged, cleanup, err := StageAES256KeyFile(key)
	require.NoError(t, err)
	info, err := os.Stat(staged)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	stagedKey, err := ReadAES256KeyFile(staged)
	require.NoError(t, err)
	require.Equal(t, key, stagedKey)
	cleanup()
	require.NoFileExists(t, staged)
}

func TestLegacyPlaintextAndEncryptedPlanRestoreIdentity(t *testing.T) {
	legacyPlan := validReceiptPlan(t)
	legacyPlan.Format, legacyPlan.Full.Encryption = legacyPlanFormat, ""
	require.NoError(t, legacyPlan.Validate())
	legacyRestore := validFullRestoreExecution()
	legacyRestore.Format, legacyRestore.Encryption = legacyFullRestoreExecutionFormat, ""
	require.NoError(t, legacyRestore.Validate())
	require.True(t, planRestoreEncryptionMatches(legacyPlan, legacyRestore))

	plan := validReceiptPlan(t)
	plan.Full.Encryption = CipherMethodAES256CTR
	plan.Full.EncryptionKeyID = "kms/prod/backup/versions/7"
	restore := validFullRestoreExecution()
	restore.Encryption = CipherMethodAES256CTR
	restore.EncryptionKeyID = plan.Full.EncryptionKeyID
	require.True(t, planRestoreEncryptionMatches(plan, restore))
	restore.EncryptionKeyID = "kms/prod/backup/versions/8"
	require.False(t, planRestoreEncryptionMatches(plan, restore))
}
