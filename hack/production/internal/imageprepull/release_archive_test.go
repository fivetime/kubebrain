package imageprepull

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReleaseArchiveIsBoundedAndNeverExtracts(t *testing.T) {
	for _, mode := range []string{"valid", "digest", "traversal", "duplicate", "extra", "missing", "symlink", "oversize-member", "index-changed", "corrupt-crc", "not-zip"} {
		t.Run(mode, func(t *testing.T) {
			object, platforms := releaseFixture(t)
			image, index := releaseBytes(t, object)
			wanted := ReleaseIdentity{Source: strings.Repeat("a", 40), RunID: "123", RunAttempt: "1", Image: image}
			receipt, err := json.Marshal(ReleaseEvidence{SchemaVersion: 1, Repository: "fivetime/kubebrain", Source: wanted.Source, RunID: wanted.RunID, RunAttempt: wanted.RunAttempt, Workflow: ".github/workflows/image.yml", Image: image, IndexSHA256: strings.Split(image, "sha256:")[1], Platforms: platforms})
			require.NoError(t, err)
			var buffer bytes.Buffer
			writer := zip.NewWriter(&buffer)
			add := func(name string, data []byte, mode os.FileMode) {
				header := &zip.FileHeader{Name: name, Method: zip.Store}
				header.SetMode(mode)
				stream, err := writer.CreateHeader(header)
				require.NoError(t, err)
				_, err = stream.Write(data)
				require.NoError(t, err)
			}
			indexName, indexBytes, indexMode := "index.json", index, os.FileMode(0600)
			switch mode {
			case "traversal":
				indexName = "../index.json"
			case "duplicate":
				indexName = "release.json"
			case "symlink":
				indexMode = os.ModeSymlink | 0600
			case "oversize-member":
				indexBytes = bytes.Repeat([]byte("a"), (1<<20)+1)
			case "index-changed":
				indexBytes = append(append([]byte(nil), index...), '\n')
			}
			add(indexName, indexBytes, indexMode)
			if mode != "missing" {
				add("release.json", receipt, 0600)
			}
			if mode == "extra" {
				add("other", []byte("x"), 0600)
			}
			require.NoError(t, writer.Close())
			data := buffer.Bytes()
			if mode == "corrupt-crc" {
				at := bytes.Index(data, index)
				require.GreaterOrEqual(t, at, 0)
				data[at] = '!'
			}
			if mode == "not-zip" {
				data = []byte("not a zip")
			}
			digest := fmt.Sprintf("sha256:%x", sha256.Sum256(data))
			if mode == "digest" {
				digest = "sha256:" + strings.Repeat("0", 64)
			}
			got, actualIndex, err := ParseReleaseArchive(data, digest, wanted)
			if mode == "valid" {
				require.NoError(t, err)
				require.Equal(t, index, actualIndex)
				require.Equal(t, platforms, got.Platforms)
			} else {
				require.Error(t, err)
				require.Nil(t, actualIndex)
				require.Equal(t, ReleaseEvidence{}, got)
			}
		})
	}
}
