package leasefault

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

// RunFaultJoin executes one independently audited owner-specific cleanup/join
// script and durably retains its output. Unlike a read-only observer, this may
// stop owned workers; it never retries (including exit 75). The script must check
// process identities and escaped descendants. Exit zero alone is not that proof:
// admit must authenticate this exact script, Bash, tools and owner inputs.
// Recovery and execution entries use the same bounded process-group behavior.
func RunFaultJoin(ctx context.Context, directory, script string, admit func(context.Context) error) error {
	if err := ownerContext(ctx); err != nil {
		return err
	}
	if admit == nil || !filepath.IsAbs(script) || filepath.Clean(script) != script || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == "/" {
		return errors.New("join requires admitted absolute script")
	}
	if _, err := planinput.ReadFile(script, false, 1<<20); err != nil {
		return err
	}
	root, err := recoveryRoot(directory)
	if err != nil {
		return err
	}
	identity, statErr := root.Stat(".")
	closeErr := root.Close()
	if err := errors.Join(statErr, closeErr); err != nil {
		return err
	}
	checkDirectory := func() error {
		current, err := os.Lstat(directory)
		if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
			return errors.New("join owner directory changed")
		}
		return nil
	}
	if err := admit(ctx); err != nil {
		return err
	}
	if err := errors.Join(ctx.Err(), checkDirectory()); err != nil {
		return err
	}
	if err := processgroup.ValidateExecutable("/bin/bash"); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "/bin/bash", script, directory)
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin"}
	processgroup.Configure(cmd)
	cmd.WaitDelay = processgroup.DefaultWaitDelay
	out, observed := processgroup.CombinedOutput(cmd, processgroup.DefaultOutputLimitBytes)
	observed = errors.Join(observed, ctx.Err())
	if err := checkDirectory(); err != nil {
		return errors.Join(observed, err)
	}
	retained := RetainRecoveryObserver(directory, "join", out, observed)
	if err := errors.Join(observed, retained, ctx.Err()); err != nil {
		return err
	}
	// Revalidate admitted files after the script; success cannot mask changes
	// made during cleanup. This does not renew the caller's recovery budget.
	err = admit(ctx)
	return errors.Join(err, ctx.Err(), checkDirectory())
}
