package metricsworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

// Command must refer to an independently admitted worker/tool bundle. Env is
// the complete environment, including the owner's bindings and unique ports.
// The supervisor does not authorize commands or supply deployment credentials.
type Command struct {
	Executable string
	Args, Env  []string
	// Production callers should supply a private evidence log. A file avoids
	// an unbounded Go stderr buffer or a writer goroutine blocking cleanup.
	Stderr *os.File
}

type Result struct {
	Ready    Ready
	Captured Captured
}

// Hooks must honor cancellation and must not restore the experiment. Validators
// must independently verify artifacts, not just trust protocol paths. Inject
// returns the ORIGINAL fault timestamp, never a new timestamp after injection.
// Restore is permitted only after Run returns and external cleanup checks pass.
type Hooks struct {
	Baseline  func(context.Context, int, Ready) error
	Inject    func(context.Context) (time.Time, error)
	Completed func(context.Context, int, Result, time.Time) error
}

type event struct {
	index  int
	ready  *Ready
	result Result
	err    error
}
type child struct {
	input io.WriteCloser
}

// Run owns all started direct children, cancels their process groups on any
// failure, and waits for their reader/wait goroutines before returning. It is
// Linux/Unix tooling, not a service endpoint. Successful protocol and exit alone
// are insufficient: the supplied validators must check evidence and receipts.
// Descendants escaping the process group are outside this mechanism's scope.
func Run(parent context.Context, owner string, commands []Command, hooks Hooks) ([]Result, error) {
	if _, ok := parent.Deadline(); !ok {
		return nil, errors.New("supervisor requires caller deadline")
	}
	if len(commands) == 0 || len(commands) > 16 || hooks.Baseline == nil || hooks.Inject == nil || hooks.Completed == nil {
		return nil, errors.New("invalid supervisor configuration")
	}
	// Validate all static inputs before starting any process.
	if _, err := NewProtocol(nilReader{}, owner); err != nil {
		return nil, err
	}
	for _, command := range commands {
		if err := processgroup.ValidateExecutable(command.Executable); err != nil {
			return nil, err
		}
	}
	guard, err := bindOwner(owner)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	children := make([]child, 0, len(commands))
	defer func() {
		cancel()
		// Do not race EOF-triggered EXIT cleanup with the cancellation TERM.
		// Keep stdin open until the worker's signal/exit path has joined.
		wg.Wait()
		for _, c := range children {
			_ = c.input.Close()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if guard.check() != nil {
					cancel()
					return
				}
			}
		}
	}()
	events := make(chan event, 2*len(commands))
	for i, spec := range commands {
		cmd := exec.CommandContext(ctx, spec.Executable, spec.Args...)
		cmd.Env = spec.Env
		if spec.Stderr != nil {
			cmd.Stderr = spec.Stderr
		}
		processgroup.Configure(cmd)
		hardCancel := cmd.Cancel
		var escalation *time.Timer
		var escalated chan struct{}
		cmd.Cancel = func() error {
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			if err == nil {
				escalated = make(chan struct{})
				escalation = time.AfterFunc(5*time.Second, func() { defer close(escalated); _ = hardCancel() })
			}
			return err
		}
		cmd.WaitDelay = 6 * time.Second
		input, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		output, err := cmd.StdoutPipe()
		if err != nil {
			_ = input.Close()
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			_ = input.Close()
			_ = output.Close()
			return nil, err
		}
		children = append(children, child{input: input})
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// Unblock protocol reads even if a descendant retains the pipe.
			stop := context.AfterFunc(ctx, func() { _ = output.Close() })
			defer stop()
			p, _ := NewProtocol(output, owner)
			r, readErr := p.ReadReady()
			if readErr == nil {
				events <- event{index: index, ready: &r}
			}
			var captured Captured
			if readErr == nil {
				captured, readErr = p.ReadCaptured()
			}
			if readErr == nil {
				readErr = p.Finish()
			}
			if readErr != nil {
				cancel()
			}
			waitErr := cmd.Wait()
			// Wait joins os/exec's cancellation watcher before reading its timer.
			if escalation != nil && !escalation.Stop() {
				<-escalated
			}
			// A direct child can exit while descendants retain its group.
			_ = hardCancel()
			if waitErr != nil {
				cancel()
			}
			events <- event{index: index, result: Result{r, captured}, err: errors.Join(readErr, waitErr)}
		}(i)
	}
	results := make([]Result, len(commands))
	seenPaths := make(map[string]bool)
	for count := 0; count < len(commands); count++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case e := <-events:
			if e.ready == nil {
				return nil, errors.New("worker exited before fault barrier")
			}
			if seenPaths[e.ready.Worker] || seenPaths[e.ready.Baseline] {
				return nil, errors.New("workers reused evidence paths")
			}
			seenPaths[e.ready.Worker], seenPaths[e.ready.Baseline] = true, true
			if err := hooks.Baseline(ctx, e.index, *e.ready); err != nil {
				return nil, err
			}
			if err := guard.check(); err != nil {
				return nil, err
			}
			results[e.index].Ready = *e.ready
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := guard.check(); err != nil {
		return nil, err
	}
	origin, err := hooks.Inject(ctx)
	if err != nil {
		return nil, err
	}
	if err := guard.check(); err != nil {
		return nil, err
	}
	now := time.Now()
	if origin.After(now) || !origin.Add(30*time.Second).After(now) {
		return nil, errors.New("invalid original fault clock")
	}
	// The returned clock can only shorten the caller budget, never reset it.
	timer := time.AfterFunc(time.Until(origin.Add(30*time.Second)), cancel)
	defer timer.Stop()
	for _, c := range children {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := fmt.Fprintf(c.input, "%d\n", origin.UnixNano()); err != nil {
			return nil, err
		}
		if err := c.input.Close(); err != nil {
			return nil, err
		}
	}
	for count := 0; count < len(commands); count++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case e := <-events:
			if e.ready != nil || e.err != nil {
				return nil, errors.Join(errors.New("worker completion failed"), e.err)
			}
			if seenPaths[e.result.Captured.Capture] || seenPaths[e.result.Captured.Schedule] {
				return nil, errors.New("workers reused completion evidence paths")
			}
			seenPaths[e.result.Captured.Capture], seenPaths[e.result.Captured.Schedule] = true, true
			if err := hooks.Completed(ctx, e.index, e.result, origin); err != nil {
				return nil, err
			}
			if err := guard.check(); err != nil {
				return nil, err
			}
			results[e.index] = e.result
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := guard.check(); err != nil {
		return nil, err
	}
	if !time.Now().Before(origin.Add(30 * time.Second)) {
		return nil, errors.New("original fault deadline exceeded")
	}
	return results, nil
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }
