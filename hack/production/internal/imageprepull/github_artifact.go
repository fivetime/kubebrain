package imageprepull

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

type releaseArtifactBinding struct {
	Digest string
	Size   int64
}

func checkReleaseArtifact(raw []byte, id string, wanted ReleaseIdentity) (releaseArtifactBinding, error) {
	var empty releaseArtifactBinding
	if len(raw) == 0 || len(raw) > 1<<20 {
		return empty, errors.New("invalid artifact metadata size")
	}
	if err := uniqueJSON(raw); err != nil {
		return empty, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return empty, err
	}
	if err := exactFieldNames(fields, "id", "name", "size_in_bytes", "digest", "expired", "workflow_run"); err != nil {
		return empty, err
	}
	var runFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["workflow_run"], &runFields); err != nil {
		return empty, err
	}
	if err := exactFieldNames(runFields, "id", "repository_id", "head_repository_id", "head_sha", "head_branch"); err != nil {
		return empty, err
	}
	var artifact struct {
		ID      uint64 `json:"id"`
		Name    string `json:"name"`
		Size    int64  `json:"size_in_bytes"`
		Digest  string `json:"digest"`
		Expired *bool  `json:"expired"`
		Run     struct {
			ID               uint64 `json:"id"`
			RepositoryID     uint64 `json:"repository_id"`
			HeadRepositoryID uint64 `json:"head_repository_id"`
			Source           string `json:"head_sha"`
			Branch           string `json:"head_branch"`
		} `json:"workflow_run"`
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return empty, err
	}
	if strconv.FormatUint(artifact.ID, 10) != id || artifact.Name != "dbaas-release-"+wanted.RunID+"-"+wanted.RunAttempt || artifact.Size <= 0 || artifact.Size > 2<<20 || !validDigest(artifact.Digest) || artifact.Expired == nil || *artifact.Expired || strconv.FormatUint(artifact.Run.ID, 10) != wanted.RunID || artifact.Run.Source != wanted.Source || artifact.Run.Branch != "dbaas" || artifact.Run.RepositoryID != 1285006877 || artifact.Run.HeadRepositoryID != 1285006877 {
		return empty, errors.New("artifact is not the admitted release attempt")
	}
	return releaseArtifactBinding{artifact.Digest, artifact.Size}, nil
}

// FetchGitHubReleaseEvidence checks a successful run attempt, checks artifact
// ownership, downloads by immutable artifact ID, validates ZIP/index/receipt,
// then rechecks artifact metadata and run state before returning. No writes,
// retries, extraction, tag fallback, or inferred deployment authorization.
// admit must pin tools/credentials and approve the workflow source itself.
// retain is mandatory for every response, including failures (ZIP up to 2 MiB).
func FetchGitHubReleaseEvidence(ctx context.Context, gh, configDirectory, artifactID string, wanted ReleaseIdentity, admit func(context.Context) error, retain func(string, []byte, error) error) (ReleaseEvidence, []byte, error) {
	fail := func(err error) (ReleaseEvidence, []byte, error) { return ReleaseEvidence{}, nil, err }
	if !releaseRunID.MatchString(artifactID) || retain == nil || !strings.HasPrefix(wanted.Image, "ghcr.io/fivetime/kubebrain@sha256:") || !pinnedImage.MatchString(wanted.Image) {
		return fail(errors.New("invalid release download binding"))
	}
	keep := func(stage string) func([]byte, error) error {
		return func(data []byte, err error) error { return retain(stage, data, err) }
	}
	if err := VerifyGitHubReleaseRun(ctx, gh, configDirectory, wanted, admit, keep("run-before")); err != nil {
		return fail(err)
	}
	endpoint := "repos/fivetime/kubebrain/actions/artifacts/" + artifactID
	var binding releaseArtifactBinding
	if err := githubReleaseRequest(ctx, gh, configDirectory, endpoint, 1<<20, func(raw []byte) error {
		var err error
		binding, err = checkReleaseArtifact(raw, artifactID, wanted)
		return err
	}, admit, keep("artifact-before")); err != nil {
		return fail(err)
	}
	var receipt ReleaseEvidence
	var index []byte
	if err := githubReleaseRequest(ctx, gh, configDirectory, endpoint+"/zip", 2<<20, func(raw []byte) error {
		if int64(len(raw)) != binding.Size {
			return errors.New("downloaded artifact size differs from metadata")
		}
		var err error
		receipt, index, err = ParseReleaseArchive(raw, binding.Digest, wanted)
		return err
	}, admit, keep("archive")); err != nil {
		return fail(err)
	}
	if err := githubReleaseRequest(ctx, gh, configDirectory, endpoint, 1<<20, func(raw []byte) error {
		after, err := checkReleaseArtifact(raw, artifactID, wanted)
		if err != nil {
			return err
		}
		if after != binding {
			return errors.New("artifact metadata changed during download")
		}
		return nil
	}, admit, keep("artifact-after")); err != nil {
		return fail(err)
	}
	if err := VerifyGitHubReleaseRun(ctx, gh, configDirectory, wanted, admit, keep("run-after")); err != nil {
		return fail(err)
	}
	return receipt, index, nil
}
