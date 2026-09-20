package leasefault

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
)

type ReapedJoinChild struct {
	PID    int `json:"pid"`
	Status int `json:"wait_status"`
}

// ReapIsolatedJoinZombies is only for the admitted long-lived PID-1 driver,
// AFTER all managed exec.Cmd.Wait calls and command-producing goroutines have
// finished. Never run it concurrently with Go's child waiters: wait4(-1) could
// otherwise consume their exit status. It sends no signals and does not wait
// for live children to exit. Context, identity or live-child failures stop it.
//
// It returns every exit status consumed, even on partial failure; the caller
// must retain these and must not erase a prior managed-worker failure. Nil means
// no waitable children remain, NOT fault acceptance. RunFaultJoin must still
// independently check the complete namespace inventory afterward. The future
// executor must enforce this sequencing; this is not automatically wired into
// the generic Join runner, which may run outside an isolated PID namespace.
func ReapIsolatedJoinZombies(ctx context.Context, directory, digest string) ([]ReapedJoinChild, error) {
	var reaped []ReapedJoinChild
	if err := ownerContext(ctx); err != nil {
		return reaped, err
	}
	if os.Getpid() != 1 {
		return reaped, errors.New("isolated Join reaping requires PID 1")
	}
	check := func() error {
		if err := VerifyJoinInputs(ctx, directory, IsolatedJoinScript, map[string]string{filepath.Join(directory, IsolatedJoinIdentity): digest}); err != nil {
			return err
		}
		owner, err := os.Lstat(directory)
		if err != nil {
			return err
		}
		current, err := currentIsolatedJoinRecord(owner)
		if err != nil {
			return err
		}
		if planinput.SHA256(current) != digest {
			return errors.New("live isolated Join identity differs from startup admission")
		}
		return ctx.Err()
	}
	for {
		if err := check(); err != nil {
			return reaped, err
		}
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
		if errors.Is(err, syscall.ECHILD) {
			return reaped, check()
		}
		if err != nil {
			return reaped, err
		}
		if pid == 0 {
			return reaped, errors.New("isolated Join still has live children; none were signalled")
		}
		reaped = append(reaped, ReapedJoinChild{PID: pid, Status: int(status)})
		if !status.Exited() && !status.Signaled() {
			return reaped, errors.New("isolated Join observed a nonterminal child status")
		}
		if len(reaped) == 4096 {
			return reaped, errors.New("isolated Join reaping exceeded bounded child count")
		}
	}
}
