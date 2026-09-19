package leasefault

import (
	"context"
	"errors"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
)

// OriginalObservation assembles one original probe and two independently
// prepared protected-stack sessions. The caller still owns fault activation,
// drop/successor verification, outcome reads, metrics, recovery and admission.
type OriginalObservation struct {
	Probe         metricsworker.Command
	Initial       Binding // Independently admitted identity; Origin/SuccessorTerm must be zero.
	Before, After WaitObservation
	Admit         func(context.Context) error
	Retain        func(context.Context, string, Binding, []byte, error) error
}

// ObservedOriginal is single-controller, valid only inside WithOriginalObservation.
// Pending must succeed before activation. Finish is called only after independent
// drop/successor observations, never using a term taken from the response itself.
type ObservedOriginal struct {
	config                    OriginalObservation
	probe                     *OriginalProbe
	before, after             func(context.Context, time.Time) error
	binding                   Binding
	pending, finished, closed bool
	err                       error
	log                       []byte
}

func WithOriginalObservation(ctx context.Context, o OriginalObservation, run func(context.Context, *ObservedOriginal) error) error {
	if ctx == nil || run == nil {
		return errors.New("original observation needs context and controller")
	}
	if err := o.validate(); err != nil {
		return err
	}
	if err := o.Admit(ctx); err != nil {
		return err
	}
	return o.withPrepared(ctx, run)
}

func (o OriginalObservation) validate() error {
	if o.Admit == nil || o.Retain == nil || o.Initial.LeaseID == 0 || o.Initial.ClusterID == 0 || o.Initial.InitialMemberID == 0 || o.Initial.InitialTerm == 0 ||
		!o.Initial.Origin.IsZero() || o.Initial.SuccessorTerm != 0 || o.Before.Count != 1 || o.After.Count != 0 || o.Before.Directory == o.After.Directory || o.Before.Source != o.After.Source || o.Before.SourceHash != o.After.SourceHash {
		return errors.New("incomplete original observation")
	}
	for _, w := range []WaitObservation{o.Before, o.After} {
		if w.Admit == nil || w.Retain == nil || w.Directory == "" || w.Source == "" || w.SourceHash == "" {
			return errors.New("incomplete stack observation")
		}
	}
	return nil
}

func (o OriginalObservation) withPrepared(ctx context.Context, run func(context.Context, *ObservedOriginal) error) error {
	return WithWaitObservation(ctx, o.Before, func(beforeCtx context.Context, before func(context.Context, time.Time) error) error {
		return WithWaitObservation(beforeCtx, o.After, func(afterCtx context.Context, after func(context.Context, time.Time) error) error {
			return WithOriginalProbe(afterCtx, o.Probe, func(p *OriginalProbe) error {
				if _, err := p.AwaitRequest(afterCtx, o.Initial); err != nil {
					return err
				}
				if err := o.Admit(afterCtx); err != nil {
					return err
				}
				h := &ObservedOriginal{config: o, probe: p, before: before, after: after, binding: o.Initial}
				defer func() { h.closed = true }()
				err := run(afterCtx, h)
				if !h.finished {
					err = errors.Join(err, errors.New("original observation did not finish"))
				}
				return errors.Join(err, h.err)
			})
		})
	})
}

func (o *ObservedOriginal) check(ctx context.Context, origin time.Time) error {
	if ctx == nil || o.closed || origin.IsZero() || origin.After(time.Now()) {
		return errors.New("inactive original observation")
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(origin.Add(30*time.Second)) {
		return errors.New("observation requires original fault budget")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	if err := o.config.Admit(ctx); err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}

func (o *ObservedOriginal) Pending(ctx context.Context, origin time.Time) error {
	if o.err != nil {
		return o.err
	}
	if !o.binding.Origin.IsZero() {
		o.err = errors.New("original pending observation already consumed")
		return o.err
	}
	o.binding.Origin = origin
	o.err = func() error {
		if err := o.check(ctx, origin); err != nil {
			return err
		}
		data, err := o.probe.Pending(ctx, o.binding, func(ctx context.Context) error { return o.before(ctx, origin) })
		retained := o.config.Retain(ctx, "pending", o.binding, append([]byte(nil), data...), err)
		return errors.Join(err, retained, o.check(ctx, origin))
	}()
	o.pending = o.err == nil
	return o.err
}

func (o *ObservedOriginal) Finish(ctx context.Context, origin time.Time, successor uint64) error {
	if o.err != nil {
		return o.err
	}
	o.err = func() error {
		if !o.pending || o.finished || !o.binding.Origin.Equal(origin) || successor <= o.binding.InitialTerm {
			return errors.New("invalid original completion order or successor")
		}
		if err := o.check(ctx, origin); err != nil {
			return err
		}
		o.binding.SuccessorTerm = successor
		if err := o.after(ctx, origin); err != nil {
			return err
		}
		data, err := o.probe.Finish(ctx, origin)
		o.log = append([]byte(nil), data...)
		if err == nil {
			_, err = ValidateOriginalResponse(data, o.binding)
		}
		retained := o.config.Retain(ctx, "response", o.binding, append([]byte(nil), data...), err)
		return errors.Join(err, retained, o.check(ctx, origin))
	}()
	o.finished = o.err == nil
	return o.err
}

// Evidence returns a copy only after the original child and both stack workers
// have joined successfully. This still does not prove the whole fault gates.
func (o *ObservedOriginal) Evidence(ctx context.Context) (Binding, []byte, error) {
	if o.err != nil {
		return Binding{}, nil, o.err
	}
	if !o.finished {
		o.err = errors.New("original observation incomplete")
		return Binding{}, nil, o.err
	}
	if err := o.check(ctx, o.binding.Origin); err != nil {
		o.err = err
		return Binding{}, nil, err
	}
	return o.binding, append([]byte(nil), o.log...), nil
}
