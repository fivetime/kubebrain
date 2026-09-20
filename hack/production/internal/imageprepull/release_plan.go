package imageprepull

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	strictjson "sigs.k8s.io/json"
)

// ReleaseDownloadPlan binds tool/config bytes and workflow snapshots to an exact
// release. Hashes alone do not approve workflow contents: the operator must
// review both snapshots from Source before approving the plan digest.
type ReleaseDownloadPlan struct {
	Source             string            `json:"source"`
	Image              string            `json:"image"`
	RunID              string            `json:"run_id"`
	RunAttempt         string            `json:"run_attempt"`
	RegressionRunID    string            `json:"regression_run_id"`
	RegressionAttempt  string            `json:"regression_attempt"`
	ArtifactID         string            `json:"artifact_id"`
	GH                 string            `json:"gh"`
	ConfigDirectory    string            `json:"config_directory"`
	EvidenceDirectory  string            `json:"evidence_directory"`
	ImageWorkflow      string            `json:"image_workflow"`
	RegressionWorkflow string            `json:"regression_workflow"`
	Files              map[string]string `json:"files"`
}

// FetchReleasePlan authenticates both CI runs and the artifact using a pinned,
// independently reviewed plan. The returned raw OCI index can be bound directly
// to an experiment. No Kubernetes operations or deployment occur.
// Evidence is exclusive: never retry into the same evidence directory.
func FetchReleasePlan(ctx context.Context, path, approved string) (ReleaseEvidence, []byte, error) {
	var receipt ReleaseEvidence
	var index []byte
	err := fetchReleasePlan(ctx, path, approved, func(r ReleaseEvidence, raw []byte) {
		receipt, index = r, raw
	})
	return receipt, index, err
}

func fetchReleasePlan(ctx context.Context, path, approved string, accept func(ReleaseEvidence, []byte)) error {
	if !planinput.ValidSHA256(approved) {
		return errors.New("require independently approved release plan SHA256")
	}
	raw, err := planinput.ReadFile(path, true, 1<<20)
	if err != nil {
		return err
	}
	if planinput.SHA256(raw) != approved {
		return errors.New("release plan differs from approved SHA256")
	}
	var p ReleaseDownloadPlan
	strict, err := strictjson.UnmarshalStrict(raw, &p, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 {
		return errors.New("invalid strict release download plan")
	}
	if p.ImageWorkflow == p.RegressionWorkflow || len(p.Files) != 5 {
		return errors.New("require exactly five distinct admitted release inputs")
	}
	for _, dir := range []string{p.ConfigDirectory, p.EvidenceDirectory} {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || dir == "/" {
			return errors.New("require canonical release directories")
		}
	}
	// Require an isolated file-backed gh configuration. With no HOME or token
	// environment inherited, credential helpers/keyrings are not relied upon.
	required := []string{p.GH, filepath.Join(p.ConfigDirectory, "hosts.yml"), filepath.Join(p.ConfigDirectory, "config.yml"), p.ImageWorkflow, p.RegressionWorkflow}
	seen := map[string]bool{}
	for _, file := range required {
		if seen[file] || !planinput.ValidSHA256(p.Files[file]) {
			return errors.New("missing or aliased admitted release input")
		}
		seen[file] = true
	}
	configInfo, err := os.Lstat(p.ConfigDirectory)
	if err != nil || !configInfo.IsDir() || configInfo.Mode().Perm() != 0700 {
		return errors.New("gh configuration directory must be private")
	}
	evidenceInfo, err := os.Lstat(p.EvidenceDirectory)
	if err != nil || !evidenceInfo.IsDir() || evidenceInfo.Mode().Perm() != 0700 {
		return errors.New("release evidence directory must be private")
	}
	if os.SameFile(configInfo, evidenceInfo) {
		return errors.New("credentials and release evidence must use separate directories")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	admit := func(ctx context.Context) error {
		for dir, expected := range map[string]os.FileInfo{p.ConfigDirectory: configInfo, p.EvidenceDirectory: evidenceInfo} {
			actual, err := os.Lstat(dir)
			if err != nil || !actual.IsDir() || actual.Mode().Perm() != 0700 || !os.SameFile(actual, expected) {
				return errors.New("release directory identity or permissions changed")
			}
		}
		data, err := planinput.ReadFile(path, true, 1<<20)
		if err != nil || planinput.SHA256(data) != approved {
			return errors.New("release plan changed")
		}
		for _, file := range required {
			if err := ctx.Err(); err != nil {
				return err
			}
			private := filepath.Dir(file) == p.ConfigDirectory
			limit := int64(1 << 20)
			if file == p.GH {
				limit = 128 << 20
			}
			data, err := planinput.ReadFile(file, private, limit)
			if err != nil || planinput.SHA256(data) != p.Files[file] {
				return errors.New("admitted release input changed or unreadable")
			}
		}
		return ctx.Err()
	}
	receipt, index, err := FetchGitHubReleaseEvidence(ctx, p.GH, p.ConfigDirectory, p.ArtifactID,
		ReleaseIdentity{Source: p.Source, Image: p.Image, RunID: p.RunID, RunAttempt: p.RunAttempt},
		RegressionIdentity{RunID: p.RegressionRunID, RunAttempt: p.RegressionAttempt}, admit,
		func(stage string, data []byte, observed error) error {
			return RetainReleaseResponse(p.EvidenceDirectory, stage, data, observed)
		})
	if err != nil {
		return err
	}
	accept(receipt, index)
	return nil
}
