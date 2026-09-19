package metricsworker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

// RunFaultCommand is an Inject adapter for an independently admitted script.
// It appends the original Unix-nanosecond clock as ONE final argument, inherits
// no environment beyond spec.Env, and writes both output streams to spec.Stderr.
// The script must implement the original fault and acceptance checks; it MUST
// NOT restore the experiment. Restoration belongs after the outer Run returns.
// No retry, shell interpolation, deadline extension or acceptance inference is
// performed here. This adapter does not authenticate scripts or their evidence.
func RunFaultCommand(ctx context.Context, spec Command, origin time.Time) error {
	if err := validateFaultBudget(ctx, origin); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	ns := origin.UnixNano()
	return runFaultCommand(ctx, spec, ns, deadline)
}

func validateFaultBudget(ctx context.Context, origin time.Time) error {
	if ctx == nil {
		return errors.New("fault command requires context")
	}
	deadline, ok := ctx.Deadline()
	now, ns := time.Now(), origin.UnixNano()
	if !ok || deadline.After(origin.Add(30*time.Second)) || !deadline.After(now) || origin.After(now) ||
		ns < 1000000000000000000 || ns >= 9000000000000000000 || !time.Unix(0, ns).Equal(origin) {
		return errors.New("fault command requires remaining original fault budget")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func runFaultCommand(ctx context.Context, spec Command, ns int64, deadline time.Time) error {
	if spec.Stderr == nil {
		return errors.New("fault command requires private evidence log")
	}
	info, err := spec.Stderr.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return errors.New("fault command requires private regular evidence log")
	}
	if err := processgroup.ValidateExecutable(spec.Executable); err != nil {
		return err
	}
	args := append(append([]string{}, spec.Args...), strconv.FormatInt(ns, 10))
	cmd := exec.CommandContext(ctx, spec.Executable, args...)
	// nil Env means inheritance to os/exec; an explicit empty slice does not.
	cmd.Env = append([]string{}, spec.Env...)
	cmd.Stdout, cmd.Stderr = spec.Stderr, spec.Stderr
	processgroup.Configure(cmd)
	cmd.WaitDelay = processgroup.DefaultWaitDelay
	// A successful direct child can leave descendants in the same group.
	// Kill those on all paths; escaped groups require external cleanup checks.
	defer func() { _ = cmd.Cancel() }()
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("fault command failed: %w", errors.Join(err, ctx.Err()))
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}
