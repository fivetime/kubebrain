package leasefault

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

// ErrObservationPending is the admitted observer's identity-verified pending
// result (exit 75). It is not success. The caller may observe again only within
// the SAME recovery deadline; fatal errors must not be converted to pending.
var ErrObservationPending = errors.New("recovery observation pending")

// WaitRecoveryObserver connects typed observations to a recovery-stage hook.
// admit must revalidate independent inputs and exclusive ownership before EVERY
// attempt. retain must durably preserve each attempt's output/status in private
// evidence; retention failure is fatal, including after an otherwise matched
// observation. Only exact ErrObservationPending retries, with the original ctx.
// It retries reads, never a mutation; do not pass a fault/cleanup script here.
func WaitRecoveryObserver(ctx context.Context, executable string, args, env []string, admit func(context.Context) error, retain func([]byte, error) error) error {
	if ctx == nil || admit == nil || retain == nil {
		return errors.New("observer wait requires context, admission and evidence retention")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("observer wait requires bounded recovery deadline")
	}
	// Copy caller-owned slices once so every attempt uses the same arguments.
	args, env = append([]string{}, args...), append([]string{}, env...)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := admit(ctx); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		output, observed := RunRecoveryObserver(ctx, executable, args, env)
		// Retain the result even if cancellation raced with the observation.
		if err := retain(output, observed); err != nil {
			return errors.Join(err, observed, ctx.Err())
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, observed)
		}
		if observed != ErrObservationPending {
			return observed
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// RunRecoveryObserver executes one independently admitted read-only observer.
// executable/script hashes, arguments, live resource bindings and private evidence
// paths must be verified by the caller. No shell interpolation, inherited environment,
// retry, deadline extension or acceptance inference is performed. Returned combined
// output is bounded to 1 MiB and must be retained privately by the coordinator.
// Exit 0 means only that observer's documented scope matched, never fault acceptance.
// Cancellation kills the process group and joins the direct child/output readers;
// escaped descendants still require an external lifecycle ownership mechanism.
func RunRecoveryObserver(ctx context.Context, executable string, args, env []string) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("observer requires context")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return nil, errors.New("observer requires bounded recovery deadline")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := processgroup.ValidateExecutable(executable); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, executable, append([]string{}, args...)...)
	cmd.Env = append([]string{}, env...)
	processgroup.Configure(cmd)
	cmd.WaitDelay = processgroup.DefaultWaitDelay
	out, err := processgroup.CombinedOutput(cmd, processgroup.DefaultOutputLimitBytes)
	if ctx.Err() != nil {
		return out, errors.Join(ctx.Err(), err)
	}
	if !time.Now().Before(deadline) {
		return out, errors.Join(context.DeadlineExceeded, err)
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 75 {
		return out, ErrObservationPending
	}
	if err != nil {
		return out, fmt.Errorf("recovery observer failed: %w", err)
	}
	return out, nil
}
