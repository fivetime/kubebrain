package leasefault

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"sync"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

// OriginalProbe owns one admitted lease-term-probe child and its bounded stdout.
// Methods are for a single controller; the output reader is synchronized. Never
// reconstruct this handle from a PID file or restart its request after failure.
type OriginalProbe struct {
	ctx      context.Context
	cancel   context.CancelFunc
	joined   chan struct{}
	err      error // Published by closing joined.
	mu       sync.Mutex
	data     []byte
	over     bool
	finished bool // Controller-only; true only after successful Finish.
}

// WithOriginalProbe starts exactly once, runs the controller, then cancels and
// joins the child before returning, even on controller failure. The executable,
// arguments, full environment and private stderr must be independently admitted.
// It supplies no credentials. Descendants escaping the process group still need
// the lifecycle's independent Join audit. Controller callbacks must honor ctx.
func WithOriginalProbe(ctx context.Context, spec metricsworker.Command, run func(*OriginalProbe) error) error {
	if ctx == nil || run == nil || spec.Stderr == nil {
		return errors.New("incomplete original probe configuration")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 10*time.Minute {
		return errors.New("original probe needs a bounded preparation deadline")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := processgroup.ValidateExecutable(spec.Executable); err != nil {
		return err
	}
	st, err := spec.Stderr.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return errors.New("original probe requires a private regular stderr file")
	}
	p := &OriginalProbe{joined: make(chan struct{})}
	p.ctx, p.cancel = context.WithCancel(ctx)
	defer p.cancel()
	cmd := exec.CommandContext(p.ctx, spec.Executable, spec.Args...)
	cmd.Env = append([]string{}, spec.Env...)
	cmd.Stdout, cmd.Stderr = probeOutput{p}, spec.Stderr
	processgroup.Configure(cmd)
	cmd.WaitDelay = processgroup.DefaultWaitDelay
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() {
		p.err = cmd.Wait()
		_ = cmd.Cancel() // Kill residual members even after normal direct-child exit.
		close(p.joined)
	}()
	defer func() { p.cancel(); <-p.joined }()
	if err := run(p); err != nil {
		return err
	}
	if !p.finished {
		return errors.New("original probe controller returned without successful join")
	}
	return ctx.Err()
}

type probeOutput struct{ p *OriginalProbe }

func (w probeOutput) Write(data []byte) (int, error) {
	p := w.p
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.over || len(data) > (12<<10)-len(p.data) {
		p.over = true
		p.cancel()
		return 0, errors.New("original probe output exceeds limit")
	}
	p.data = append(p.data, data...)
	return len(data), nil
}

// AwaitRequest waits only for the original request's two complete events under
// the preparation deadline. b supplies independently admitted initial identity;
// its Origin/SuccessorTerm are unused here, before the fault clock is chosen.
// This does NOT establish server blocking or authorize FAULT_READY on its own.
func (p *OriginalProbe) AwaitRequest(ctx context.Context, b Binding) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("request readiness requires context")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("request readiness requires deadline")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !time.Now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		if err := p.live(ctx); err != nil {
			return nil, err
		}
		data, err := p.snapshot()
		if err != nil {
			return data, err
		}
		if bytes.Count(data, []byte{'\n'}) >= 2 {
			b.Origin = time.Now() // Parsing cutoff only, never the fault origin.
			if err := ValidateOriginalPendingPrefix(data, b); err != nil {
				return data, err
			}
			return data, p.live(ctx)
		}
		select {
		case <-ctx.Done():
			return data, ctx.Err()
		case <-p.ctx.Done():
			return data, p.ctx.Err()
		case <-p.joined:
			return data, errors.Join(errors.New("original probe exited before readiness"), p.err)
		case <-ticker.C:
		}
	}
}

func (p *OriginalProbe) snapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.over {
		return nil, errors.New("original probe output exceeded limit")
	}
	return append([]byte(nil), p.data...), nil
}

func (p *OriginalProbe) live(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	select {
	case <-p.joined:
		return errors.Join(errors.New("original probe already exited"), p.err)
	default:
		return nil
	}
}

// Pending is the live process/log portion of OriginalPending. observe must
// capture and validate fresh source-bound blocked-stack, same-term/process and
// ownership evidence. The two-event log must be unchanged across that capture.
// This is not atomic with remote activation and is not a fault acceptance gate
// by itself. The caller must retain the returned snapshot even on failure.
func (p *OriginalProbe) Pending(ctx context.Context, b Binding, observe func(context.Context) error) ([]byte, error) {
	if ctx == nil || observe == nil {
		return nil, errors.New("pending observation requires context and stack observer")
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(b.Origin.Add(30*time.Second)) || b.Origin.After(time.Now()) {
		return nil, errors.New("pending observation requires original fault budget")
	}
	check := func() error {
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return p.live(ctx)
	}
	if err := check(); err != nil {
		return nil, err
	}
	before, err := p.snapshot()
	if err != nil {
		return before, err
	}
	if err := ValidateOriginalPendingPrefix(before, b); err != nil {
		return before, err
	}
	if err := observe(ctx); err != nil {
		return before, err
	}
	after, err := p.snapshot()
	if err != nil {
		return after, err
	}
	if !bytes.Equal(before, after) {
		return after, errors.New("original probe changed during blocked-stack capture")
	}
	return after, check()
}

// Finish joins this exact child inside the original fault context, returning its
// complete bounded stdout only after exit. Call ValidateOriginalResponse and all
// remaining gates separately. On timeout the enclosing function cancels/joins;
// no alternate request or fresh response budget is created.
func (p *OriginalProbe) Finish(ctx context.Context, origin time.Time) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("finish requires context")
	}
	deadline, ok := ctx.Deadline()
	if !ok || origin.IsZero() || origin.After(time.Now()) || deadline.After(origin.Add(30*time.Second)) {
		return nil, errors.New("finish requires original fault budget")
	}
	select {
	case <-p.joined:
	case <-ctx.Done():
		data, err := p.snapshot()
		return data, errors.Join(err, ctx.Err())
	case <-p.ctx.Done():
		data, err := p.snapshot()
		return data, errors.Join(err, p.ctx.Err())
	}
	data, err := p.snapshot()
	if !time.Now().Before(deadline) {
		return data, context.DeadlineExceeded
	}
	err = errors.Join(err, p.err, ctx.Err(), p.ctx.Err())
	if err == nil {
		p.finished = true
	}
	return data, err
}
