package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/imageprepull"
	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
)

func TestCommandReleasePreflightRechecksInputs(t *testing.T) {
	for _, mode := range []string{"success", "download-failed", "wrong-source", "plan-changed", "tool-changed"} {
		t.Run(mode, func(t *testing.T) {
			p := serializedCommandFixture(t)
			data, err := json.Marshal(p)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "command.json")
			require.NoError(t, os.WriteFile(path, data, 0600))
			calls := 0
			fetch := func(ctx context.Context, pathArg, digestArg string) (imageprepull.ReleaseEvidence, []byte, error) {
				calls++
				require.Equal(t, "release-plan", pathArg)
				require.Equal(t, "release-digest", digestArg)
				r := imageprepull.ReleaseEvidence{Source: p.Bindings.Source, Image: p.Bindings.Image, Platforms: p.Release.Reviewed}
				switch mode {
				case "download-failed":
					return r, p.Release.Index, errors.New("download failed")
				case "wrong-source":
					r.Source = "different"
				case "plan-changed":
					require.NoError(t, os.WriteFile(path, append(data, '\n'), 0600))
				case "tool-changed":
					require.NoError(t, os.WriteFile(p.ProbeExecutable, []byte("changed"), 0700))
				}
				return r, p.Release.Index, nil
			}
			err = authenticateCommandRelease(t.Context(), path, planinput.SHA256(data), "release-plan", "release-digest", fetch)
			require.Equal(t, 1, calls)
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.NoFileExists(t, filepath.Join(p.OwnerDirectory, commandAttemptFile))
			require.NoFileExists(t, filepath.Join(p.OwnerDirectory, ownerIntentFile))
		})
	}
}

func TestCommandAuthenticatedReleaseBinding(t *testing.T) {
	for _, mode := range []string{"match", "source", "image", "index", "platform", "extra-platform"} {
		t.Run(mode, func(t *testing.T) {
			p := serializedCommandFixture(t)
			r := imageprepull.ReleaseEvidence{Source: p.Bindings.Source, Image: p.Bindings.Image, Platforms: map[string]string{}}
			for platform, digest := range p.Release.Reviewed {
				r.Platforms[platform] = digest
			}
			index := append([]byte(nil), p.Release.Index...)
			switch mode {
			case "source":
				r.Source = "wrong-source"
			case "image":
				r.Image = "wrong-image"
			case "index":
				index = append(index, '\n')
			case "platform":
				r.Platforms["linux/amd64"] = "wrong-digest"
			case "extra-platform":
				r.Platforms["linux/s390x"] = "unapproved"
			}
			err := p.matchAuthenticatedRelease(r, index)
			if mode == "match" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "authenticated release differs")
			}
		})
	}
}

func TestCommandReleasePreflightCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, AuthenticateCommandRelease(ctx, "/absent", "", "/absent", ""), context.Canceled)
	require.Error(t, AuthenticateCommandRelease(nil, "", "", "", ""))
}
