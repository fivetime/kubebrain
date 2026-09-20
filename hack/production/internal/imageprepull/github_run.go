package imageprepull

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

// VerifyGitHubReleaseRun reads a specific run attempt via an admitted gh binary
// and credential directory. It never retries, writes GitHub state or downloads
// artifacts. Success authenticates run metadata only, not artifact ownership,
// workflow contents or image provenance. Those remain separate admission gates.
// admit must pin the executable and credential configuration. retain must save
// the exact bounded API response even on failure, without publishing credentials.
func VerifyGitHubReleaseRun(ctx context.Context, gh, configDirectory string, wanted ReleaseIdentity, admit func(context.Context) error, retain func([]byte, error) error) error {
	if ctx == nil || admit == nil || retain == nil || !releaseSource.MatchString(wanted.Source) || !releaseRunID.MatchString(wanted.RunID) || !releaseAttempt.MatchString(wanted.RunAttempt) || !filepath.IsAbs(configDirectory) || filepath.Clean(configDirectory) != configDirectory || configDirectory == "/" {
		return errors.New("invalid GitHub release run admission")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("GitHub admission requires a bounded context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := processgroup.ValidateExecutable(gh); err != nil {
		return err
	}
	if err := admit(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(requestCtx, gh, "api", "--hostname", "github.com", "--method", "GET", "repos/fivetime/kubebrain/actions/runs/"+wanted.RunID+"/attempts/"+wanted.RunAttempt)
	// Do not inherit GH_HOST, GH_TOKEN, debug logging or shell startup hooks.
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "GH_CONFIG_DIR=" + configDirectory, "GH_PROMPT_DISABLED=1", "GH_NO_UPDATE_NOTIFIER=1", "GH_NO_EXTENSION_UPDATE_NOTIFIER=1"}
	processgroup.Configure(cmd)
	cmd.WaitDelay = processgroup.DefaultWaitDelay
	raw, observed := processgroup.CombinedOutput(cmd, 1<<20)
	observed = errors.Join(observed, requestCtx.Err())
	requestDeadline, _ := requestCtx.Deadline()
	if !time.Now().Before(requestDeadline) {
		observed = errors.Join(observed, context.DeadlineExceeded)
	}
	if observed == nil {
		observed = checkGitHubReleaseRun(raw, wanted)
	}
	if err := errors.Join(observed, retain(raw, observed), ctx.Err()); err != nil {
		return err
	}
	return errors.Join(admit(ctx), ctx.Err())
}

func checkGitHubReleaseRun(raw []byte, wanted ReleaseIdentity) error {
	if len(raw) == 0 || len(raw) > 1<<20 {
		return errors.New("invalid GitHub run response size")
	}
	if err := uniqueJSON(raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if err := exactFieldNames(fields, "id", "run_attempt", "head_sha", "head_branch", "event", "path", "status", "conclusion", "repository", "head_repository"); err != nil {
		return err
	}
	for _, key := range []string{"repository", "head_repository"} {
		var repoFields map[string]json.RawMessage
		if err := json.Unmarshal(fields[key], &repoFields); err != nil {
			return err
		}
		if err := exactFieldNames(repoFields, "id", "full_name"); err != nil {
			return err
		}
	}
	type repository struct {
		ID       uint64 `json:"id"`
		FullName string `json:"full_name"`
	}
	var run struct {
		ID             uint64     `json:"id"`
		Attempt        uint64     `json:"run_attempt"`
		Source         string     `json:"head_sha"`
		Branch         string     `json:"head_branch"`
		Event          string     `json:"event"`
		Path           string     `json:"path"`
		Status         string     `json:"status"`
		Conclusion     string     `json:"conclusion"`
		Repository     repository `json:"repository"`
		HeadRepository repository `json:"head_repository"`
	}
	if err := json.Unmarshal(raw, &run); err != nil {
		return errors.New("invalid GitHub run JSON")
	}
	expectedRepo := repository{ID: 1285006877, FullName: "fivetime/kubebrain"}
	if strconv.FormatUint(run.ID, 10) != wanted.RunID || strconv.FormatUint(run.Attempt, 10) != wanted.RunAttempt || run.Source != wanted.Source || run.Branch != "dbaas" || (run.Event != "push" && run.Event != "workflow_dispatch") || run.Path != ".github/workflows/image.yml" || run.Status != "completed" || run.Conclusion != "success" || run.Repository != expectedRepo || run.HeadRepository != expectedRepo {
		return errors.New("GitHub run does not match successful admitted release attempt")
	}
	return nil
}
