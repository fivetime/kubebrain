package leasefault

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// CommandArtifacts owns only the log descriptors it created. Close after all
// children join. Neither Close nor a construction failure removes evidence or
// releases cluster ownership. A failed partial setup needs a fresh attempt.
type CommandArtifacts struct {
	Plan     ObservationCommandPlan
	Targets  []MetricCommandTarget
	files    []*os.File
	once     sync.Once
	closeErr error
}

func (a *CommandArtifacts) Close() error {
	a.once.Do(func() {
		for _, f := range a.files {
			a.closeErr = errors.Join(a.closeErr, f.Sync(), f.Close())
		}
	})
	return a.closeErr
}

// PrepareArtifacts creates the two empty private stack-worker directories that
// protected-wait-worker requires and independent private stderr files. The
// metrics worker creates its own receipt directory later. OwnerDirectory must
// already be private; snapshots/tools may exist, but no claim/attempt markers
// or these artifact names may exist. No tools, hooks or cluster calls run here.
func (p ObservationCommandPlan) PrepareArtifacts(metricExecutable string, targets []MetricCommandTarget) (*CommandArtifacts, error) {
	if p.ProbeLog != nil || p.BeforeLog != nil || p.AfterLog != nil || len(targets) != len(p.Bindings.Metrics) {
		return nil, errors.New("artifacts require unset logs and matching metric target count")
	}
	if err := p.Bindings.Validate(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(p.OwnerDirectory) || filepath.Clean(p.OwnerDirectory) != p.OwnerDirectory || p.OwnerDirectory == "/" {
		return nil, errors.New("invalid artifact owner directory")
	}
	dirs := []string{p.BeforeDirectory, p.AfterDirectory}
	if dirs[0] == dirs[1] {
		return nil, errors.New("stack artifact directories must differ")
	}
	for _, dir := range dirs {
		if filepath.Clean(dir) != dir || filepath.Dir(dir) != p.OwnerDirectory || !stackWorkerName.MatchString(filepath.Base(dir)) {
			return nil, errors.New("invalid stack artifact directory")
		}
	}
	for _, target := range targets {
		if target.Stderr != nil {
			return nil, errors.New("metric artifact logs must be unset")
		}
	}
	root, err := recoveryRoot(p.OwnerDirectory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	identity, err := root.Stat(".")
	if err != nil {
		return nil, err
	}
	names := []string{"probe.stderr", "stack-before.stderr", "stack-after.stderr"}
	for i := range targets {
		names = append(names, fmt.Sprintf("metric-%02d.stderr", i))
	}
	unused := append([]string{"deployment-claimed", "HOLD", "final-exit-code", ownerIntentFile, ownerReceiptFile, faultOriginFile, filepath.Base(dirs[0]), filepath.Base(dirs[1])}, names...)
	for _, name := range unused {
		if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("command artifacts require an unused attempt")
		}
	}
	a := &CommandArtifacts{Plan: p, Targets: append([]MetricCommandTarget(nil), targets...)}
	fail := func(err error) (*CommandArtifacts, error) { return nil, errors.Join(err, a.Close()) }
	for _, dir := range dirs {
		if err := root.Mkdir(filepath.Base(dir), 0700); err != nil {
			return fail(err)
		}
	}
	for _, name := range names {
		f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
		if err != nil {
			return fail(err)
		}
		a.files = append(a.files, f)
	}
	a.Plan.ProbeLog, a.Plan.BeforeLog, a.Plan.AfterLog = a.files[0], a.files[1], a.files[2]
	for i := range a.Targets {
		a.Targets[i].Stderr = a.files[i+3]
	}
	if _, err := a.Plan.MetricCommands(metricExecutable, a.Targets); err != nil {
		return fail(err)
	}
	for _, f := range a.files {
		if err := f.Sync(); err != nil {
			return fail(err)
		}
	}
	dir, err := root.Open(".")
	if err != nil {
		return fail(err)
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return fail(err)
	}
	current, err := os.Lstat(p.OwnerDirectory)
	if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
		return fail(errors.New("artifact owner directory changed"))
	}
	return a, nil
}
