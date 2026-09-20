package imageprepull

import (
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"

	strictjson "sigs.k8s.io/json"
)

// ReleaseIdentity must come from independently authenticated successful CI and
// artifact ownership, never from the receipt being parsed. This type by itself
// does not authenticate GitHub, the workflow source, or a downloaded artifact.
type ReleaseIdentity struct {
	Source, RunID, RunAttempt, Image string
}

type ReleaseEvidence struct {
	SchemaVersion int               `json:"schema_version"`
	Repository    string            `json:"repository"`
	Source        string            `json:"source"`
	RunID         string            `json:"run_id"`
	RunAttempt    string            `json:"run_attempt"`
	Workflow      string            `json:"workflow"`
	Image         string            `json:"image"`
	IndexSHA256   string            `json:"index_sha256"`
	Platforms     map[string]string `json:"platforms"`
}

var releaseSource = regexp.MustCompile(`^[a-f0-9]{40}$`)
var releaseRunID = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
var releaseAttempt = regexp.MustCompile(`^[1-9][0-9]{0,9}$`)

// ParseReleaseEvidence checks the bounded, strict formatter output against
// externally established CI identity and exact OCI index bytes. It performs no
// network access and cannot establish CI success or artifact authenticity.
// Unknown/duplicate/case-aliased fields and trailing JSON are rejected. Returned
// platform digests have passed the existing two-platform index selection policy.
func ParseReleaseEvidence(raw, index []byte, wanted ReleaseIdentity) (ReleaseEvidence, error) {
	var receipt ReleaseEvidence
	if !releaseSource.MatchString(wanted.Source) || !releaseRunID.MatchString(wanted.RunID) || !releaseAttempt.MatchString(wanted.RunAttempt) || !strings.HasPrefix(wanted.Image, "ghcr.io/fivetime/kubebrain@sha256:") || !pinnedImage.MatchString(wanted.Image) {
		return receipt, errors.New("invalid independent release identity")
	}
	if len(raw) == 0 || len(raw) > 32<<10 || !utf8.Valid(raw) {
		return receipt, errors.New("invalid release receipt size or encoding")
	}
	if err := uniqueJSON(raw); err != nil {
		return receipt, err
	}
	strict, err := strictjson.UnmarshalStrict(raw, &receipt, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 {
		return ReleaseEvidence{}, errors.New("invalid strict release receipt JSON")
	}
	if receipt.SchemaVersion != 1 || receipt.Repository != "fivetime/kubebrain" || receipt.Workflow != ".github/workflows/image.yml" || receipt.Source != wanted.Source || receipt.RunID != wanted.RunID || receipt.RunAttempt != wanted.RunAttempt || receipt.Image != wanted.Image || !validDigest("sha256:"+receipt.IndexSHA256) || receipt.Image != "ghcr.io/fivetime/kubebrain@sha256:"+receipt.IndexSHA256 {
		return ReleaseEvidence{}, errors.New("release receipt differs from independent CI identity")
	}
	if _, err := ApprovedRuntimeDigests(receipt.Image, index, receipt.Platforms); err != nil {
		return ReleaseEvidence{}, err
	}
	return receipt, nil
}
