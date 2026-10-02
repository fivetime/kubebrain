package leasefault

import (
	"context"
	"errors"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
)

// WaitObservation describes one independently admitted protected-wait-worker.
// Admit must bind the actual command/arguments, receipt directory, tool hashes,
// source/image approval and live ownership/Pod/term to this observation. Retain
// durably records the joined receipt/result, including receipt validation errors.
type WaitObservation struct {
	Command                       metricsworker.Command
	Directory, Source, SourceHash string
	Count                         int
	Admit                         func(context.Context) error
	Retain                        func(context.Context, WaitReceipt, error) error
	// Concrete runtime only: one fresh source bracket around this synchronous
	// read-only observation; every original live admission remains inside it.
	observeTools, observeAdmit func(context.Context) error
}

// WithWaitObservation prepares a protected-stack child before running the
// controller. The controller uses observe inside OriginalProbe.Pending (count=1)
// or after independently observing demotion (count=0). observe sends the SAME
// original clock, joins the actual child, then validates and retains its receipts
// inside that original budget. No recovery, retries, RPC renewals or clock reset.
// Controller and admission callbacks must honor cancellation; escaped processes
// and filesystem provenance remain the caller's independent responsibility.
// The observe function is single-consumer and must not be called concurrently.
func WithWaitObservation(ctx context.Context, o WaitObservation, run func(context.Context, func(context.Context, time.Time) error) error) error {
	if ctx == nil || o.Admit == nil || o.Retain == nil || run == nil || o.Count < 0 || o.Count > 1 || o.Directory == "" || o.Source == "" || o.SourceHash == "" {
		return errors.New("incomplete wait observation")
	}
	if (o.observeTools == nil) != (o.observeAdmit == nil) {
		return errors.New("incomplete protected observation source boundary")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("wait observation needs preparation deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.Command.Args = append([]string{}, o.Command.Args...)
	o.Command.Env = append([]string{}, o.Command.Env...)
	if err := o.Admit(ctx); err != nil {
		return err
	}
	return metricsworker.WithPreparedFault(ctx, o.Command, func(runCtx context.Context, capture func(context.Context, time.Time) error) error {
		var observationErr error
		var observationDeadline time.Time
		called := false
		observe := func(faultCtx context.Context, origin time.Time) error {
			if called {
				observationErr = errors.Join(observationErr, errors.New("wait observation already consumed"))
				return observationErr
			}
			called = true
			observationErr = func() (err error) {
				if faultCtx == nil {
					return errors.New("wait observation needs original context")
				}
				deadline, ok := faultCtx.Deadline()
				if !ok || origin.IsZero() || origin.After(time.Now()) || deadline.After(origin.Add(30*time.Second)) {
					return errors.New("wait observation needs original fault budget")
				}
				observationDeadline = deadline
				admit := o.Admit
				if o.observeTools != nil {
					if err := o.observeTools(faultCtx); err != nil {
						return err
					}
					defer func() { err = errors.Join(err, o.observeTools(faultCtx), runCtx.Err(), faultCtx.Err()) }()
					admit = o.observeAdmit
				}
				check := func() error {
					if err := runCtx.Err(); err != nil {
						return err
					}
					if err := faultCtx.Err(); err != nil {
						return err
					}
					if !time.Now().Before(deadline) {
						return context.DeadlineExceeded
					}
					if err := admit(faultCtx); err != nil {
						return err
					}
					if !time.Now().Before(deadline) {
						return context.DeadlineExceeded
					}
					return errors.Join(runCtx.Err(), faultCtx.Err())
				}
				if err := check(); err != nil {
					return err
				}
				if err := capture(faultCtx, origin); err != nil {
					return err
				}
				// capture success means the actual child has been reaped, not just
				// that a marker appeared. Its EXIT cleanup has written exit-code.
				if err := check(); err != nil {
					return err
				}
				receipt, validationErr := LoadWaitReceipt(o.Directory, o.Source, o.SourceHash, o.Count, origin)
				if err := check(); err != nil {
					return errors.Join(validationErr, err)
				}
				retainErr := o.Retain(faultCtx, receipt, validationErr)
				return errors.Join(validationErr, retainErr, check())
			}()
			return observationErr
		}
		runErr := run(runCtx, observe)
		if !called {
			return errors.Join(runErr, errors.New("wait observation not requested"))
		}
		if !observationDeadline.IsZero() && !time.Now().Before(observationDeadline) {
			observationErr = errors.Join(observationErr, context.DeadlineExceeded)
		}
		return errors.Join(runErr, observationErr)
	})
}
