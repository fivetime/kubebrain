package metricsworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

// WithPreparedFault keeps preparation and fault execution in ONE child shell,
// so that shell can wait for the original probe it started before the fault.
// The admitted script emits exactly FAULT_READY\n after preparation, reads one
// original Unix-nanosecond clock from stdin, performs the original fault gates,
// then emits FAULT_DONE\t<same clock>\n and exits. Diagnostics use private stderr.
// Neither the script nor run may restore the experiment. Restoration belongs
// after this function returns and external cleanup checks pass.
//
// run must obey its context and call inject synchronously exactly once (usually
// from Run's Inject hook). Script preparation/exit are NOT proof of admission,
// fault installation or acceptance. The script must implement those checks.
// Cancellation kills its process group and joins the direct child; descendants
// escaping that group require independent cleanup. No script retry is allowed.
func WithPreparedFault(parent context.Context, spec Command, run func(context.Context, func(context.Context, time.Time) error) error) error {
	if parent == nil || run == nil {
		return errors.New("prepared fault requires context and callback")
	}
	if _, ok := parent.Deadline(); !ok {
		return errors.New("prepared fault requires caller deadline")
	}
	if err := parent.Err(); err != nil {
		return err
	}
	if spec.Stderr == nil {
		return errors.New("prepared fault requires private evidence log")
	}
	info, err := spec.Stderr.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return errors.New("prepared fault requires private regular evidence log")
	}
	if err := processgroup.ValidateExecutable(spec.Executable); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	cmd := exec.CommandContext(ctx, spec.Executable, spec.Args...)
	cmd.Env = append([]string{}, spec.Env...)
	cmd.Stderr = spec.Stderr
	processgroup.Configure(cmd)
	cmd.WaitDelay = processgroup.DefaultWaitDelay
	input, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	defer output.Close()
	if err := cmd.Start(); err != nil {
		return err
	}
	// Closing stdout on cancellation prevents escaped descendants holding a
	// pipe from blocking our reader. Wait still reaps the direct child.
	stop := context.AfterFunc(ctx, func() { _ = output.Close() })
	defer stop()
	ready := make(chan error, 1)
	joined := make(chan struct{})
	var childErr error    // Published by closing joined.
	var completion []byte // Also published by closing joined.
	go func() {
		defer close(joined)
		marker := make([]byte, len("FAULT_READY\n"))
		_, readErr := io.ReadFull(output, marker)
		if readErr == nil && string(marker) != "FAULT_READY\n" {
			readErr = errors.New("invalid prepared fault readiness")
		}
		ready <- readErr
		if readErr == nil {
			completion, readErr = io.ReadAll(io.LimitReader(output, 65))
			if len(completion) > 64 {
				readErr = errors.New("oversize prepared fault completion")
			}
		}
		if readErr != nil {
			cancel()
		}
		childErr = errors.Join(readErr, cmd.Wait())
		if childErr != nil {
			cancel()
		}
	}()
	defer func() {
		cancel()
		<-joined
		_ = cmd.Cancel() // Also remove residual members after a successful exit.
	}()
	select {
	case err := <-ready:
		if err != nil {
			return fmt.Errorf("prepare fault: %w", err)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	called := false
	var injectErr error
	inject := func(faultCtx context.Context, origin time.Time) error {
		if called {
			injectErr = errors.New("prepared fault clock already dispatched")
			cancel()
			return injectErr
		}
		called = true
		injectErr = func() error {
			if err := validateFaultBudget(faultCtx, origin); err != nil {
				return err
			}
			deadline, _ := faultCtx.Deadline()
			stopFault := context.AfterFunc(faultCtx, cancel)
			defer stopFault()
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := io.WriteString(input, strconv.FormatInt(origin.UnixNano(), 10)+"\n"); err != nil {
				return err
			}
			if err := input.Close(); err != nil {
				return err
			}
			<-joined
			if string(completion) != "FAULT_DONE\t"+strconv.FormatInt(origin.UnixNano(), 10)+"\n" {
				return errors.Join(childErr, errors.New("invalid prepared fault completion"))
			}
			if !time.Now().Before(deadline) {
				return errors.Join(childErr, context.DeadlineExceeded)
			}
			return errors.Join(childErr, ctx.Err(), faultCtx.Err())
		}()
		// Cancellation commonly truncates the completion record or closes its
		// pipe. Preserve the fault context's cause even on those earlier exits;
		// the session context only reports cancellation, not the fault deadline.
		if faultCtx != nil {
			injectErr = errors.Join(injectErr, faultCtx.Err())
		}
		if injectErr != nil {
			cancel()
		}
		return injectErr
	}
	runErr := run(ctx, inject)
	if !called {
		return errors.Join(runErr, errors.New("prepared fault clock was not dispatched"))
	}
	return errors.Join(runErr, injectErr, ctx.Err())
}
