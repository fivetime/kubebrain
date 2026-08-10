package pitrinventory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const testSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validReceipt() Receipt {
	return Receipt{Format: Format, ObjectStoreID: "store-a", Bucket: "bucket-a", Prefix: "instances/a/log", Entries: []Entry{{Name: "v1/log/1", ObjectKey: "instances/a/log/v1/log/1", VersionID: "version-1", Bytes: 10, SHA256: testSHA, RetentionMode: "COMPLIANCE", RetainUntilUnix: 2_100_000_000}}, ObjectCount: 1, TotalBytes: 10, Pages: 2, PaginationExhausted: true, ExactVersionsVerified: true, MinRetainUntilUnix: 2_050_000_000, CheckedAtUnix: 2_000_000_000}
}

func TestReceiptValidatesAuthoritativeEmptyPrefix(t *testing.T) {
	receipt := validReceipt()
	receipt.Entries = []Entry{}
	receipt.ObjectCount, receipt.TotalBytes = 0, 0
	require.NoError(t, receipt.Validate())
}

func TestReadCanonicalRejectsMalleableOrUnsafeInventory(t *testing.T) {
	receipt := validReceipt()
	dir := t.TempDir()
	path := filepath.Join(dir, "inventory.json")
	b, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(b, '\n'), 0o600))
	got, _, err := ReadCanonical(path)
	require.NoError(t, err)
	require.Equal(t, receipt, got)

	require.NoError(t, os.WriteFile(path, []byte("  "+string(b)+"\n"), 0o600))
	_, _, err = ReadCanonical(path)
	require.ErrorContains(t, err, "not canonical")

	receipt.Entries[0].Name = "../escape"
	b, err = json.Marshal(receipt)
	require.NoError(t, err)
	_, err = Decode(strings.NewReader(string(b)))
	require.ErrorContains(t, err, "invalid")
}
