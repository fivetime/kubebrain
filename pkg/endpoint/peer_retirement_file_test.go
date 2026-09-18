package endpoint

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadPeerRetirementOptions(t *testing.T) {
	valid := `{"scope":"retirement-v1:test","holder_pins":{"local":["` + strings.Repeat("01", 32) + `"],"remote":["` + strings.Repeat("02", 32) + `"]},"endpoint_holders":{"https://remote:3380":"remote"},"read_budget":"1s","operation_budget":"1s","send_budget":"250ms","concurrency":2,"requests_per_second":4}`
	for name, input := range map[string]string{
		"valid":             valid,
		"duplicate":         strings.Replace(valid, `"scope":`, `"scope":"other","scope":`, 1),
		"escaped duplicate": strings.Replace(valid, `"scope":`, `"\u0073cope":"other","scope":`, 1),
		"nested duplicate":  strings.Replace(valid, `"remote":[`, `"remote":[],"remote":[`, 1),
		"unknown field":     strings.Replace(valid, `"scope":`, `"typo":`, 1),
		"wrong case":        strings.Replace(valid, `"scope":`, `"Scope":`, 1),
		"trailing value":    valid + `{}`,
		"null":              `null`,
		"null budget":       strings.Replace(valid, `"read_budget":"1s"`, `"read_budget":null`, 1),
		"negative budget":   strings.Replace(valid, `"250ms"`, `"-1s"`, 1),
		"large budget":      strings.Replace(valid, `"250ms"`, `"61s"`, 1),
		"zero concurrency":  strings.Replace(valid, `"concurrency":2`, `"concurrency":0`, 1),
		"insecure target":   strings.Replace(valid, `https://`, `http://`, 1),
		"target path":       strings.Replace(valid, `remote:3380`, `remote:3380/path`, 1),
		"shared key":        strings.ReplaceAll(valid, strings.Repeat("02", 32), strings.Repeat("01", 32)),
		"unknown holder":    strings.Replace(valid, `3380":"remote"`, `3380":"unknown"`, 1),
		"oversized":         valid + strings.Repeat(" ", 64<<10),
		"deep":              strings.Repeat("[", 12) + "0" + strings.Repeat("]", 12),
		"invalid UTF8":      valid + string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "policy.json")
			require.NoError(t, os.WriteFile(path, []byte(input), 0600))
			policy, err := LoadPeerRetirementOptions(path)
			if name != "valid" {
				require.ErrorIs(t, err, errPeerRetirementFile)
				require.Nil(t, policy)
				return
			}
			require.NoError(t, err)
			require.Equal(t, time.Second, policy.ReadBudget)
			require.Equal(t, time.Second, policy.OperationBudget)
			require.Equal(t, 250*time.Millisecond, policy.SendBudget)
			link := filepath.Join(t.TempDir(), "projected")
			require.NoError(t, os.Symlink(path, link))
			projected, err := LoadPeerRetirementOptions(link)
			require.NoError(t, err)
			require.Equal(t, policy, projected)
		})
	}
}

func TestPeerRetirementPolicyRejectsNonRegularFiles(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	require.NoError(t, syscall.Mkfifo(fifo, 0600))
	for _, path := range []string{dir, fifo, filepath.Join(dir, "missing"), "/dev/null"} {
		policy, err := LoadPeerRetirementOptions(path)
		require.ErrorIs(t, err, errPeerRetirementFile)
		require.Nil(t, policy)
	}
}
