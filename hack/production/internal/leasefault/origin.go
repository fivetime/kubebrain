package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
)

const faultOriginFile = "fault-origin.json"

type faultOriginRecord struct {
	Version        int    `json:"version"`
	Owner          string `json:"owner"`
	BindingsSHA256 string `json:"bindings_sha256"`
	OriginNS       string `json:"origin_unix_nano"`
	DeadlineNS     string `json:"deadline_unix_nano"`
}

// NewDurableFaultOrigin freezes the admitted bindings and returns the actual
// supervisor clock hook. Construction performs no IO. Invocation creates and
// syncs a single receipt before returning the original monotonic clock; fsync
// and the final ownership check consume, never restart, its 30-second budget.
// Existing or partial receipts forbid another attempt, including after restart.
// This is neither ownership acquisition nor proof of source/runtime admission.
func NewDurableFaultOrigin(directory string, bindings NativeExperimentBindings, own func(context.Context) error) (func(context.Context) (time.Time, error), error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == "/" || own == nil {
		return nil, errors.New("invalid durable origin directory or ownership check")
	}
	if err := bindings.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(bindings)
	if err != nil || len(raw) > 1<<20 {
		return nil, errors.New("invalid durable origin bindings")
	}
	digest, owner := planinput.SHA256(raw), bindings.Network.Owner
	return func(ctx context.Context) (time.Time, error) {
		fail := func(err error) (time.Time, error) { return time.Time{}, err }
		if ctx == nil {
			return fail(errors.New("origin requires bounded context"))
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 5*time.Minute {
			return fail(errors.New("origin requires bounded context"))
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if err := own(ctx); err != nil {
			return fail(err)
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		root, err := recoveryRoot(directory)
		if err != nil {
			return fail(err)
		}
		defer root.Close()
		identity, err := root.Stat(".")
		if err != nil {
			return fail(err)
		}
		// O_EXCL is the one-shot fence across concurrent hooks/processes. Never
		// remove a partial file: any ambiguous persistence requires reconciliation.
		f, err := root.OpenFile(faultOriginFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
		if err != nil {
			return fail(err)
		}
		origin := time.Now()
		faultCtx, cancel := context.WithDeadline(ctx, origin.Add(30*time.Second))
		defer cancel()
		record := faultOriginRecord{Version: 1, Owner: owner, BindingsSHA256: digest,
			OriginNS: strconv.FormatInt(origin.UnixNano(), 10), DeadlineNS: strconv.FormatInt(origin.Add(30*time.Second).UnixNano(), 10)}
		writeErr := json.NewEncoder(f).Encode(record)
		syncErr := f.Sync()
		closeErr := f.Close()
		if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
			return fail(err)
		}
		dir, err := root.Open(".")
		if err != nil {
			return fail(err)
		}
		syncErr = dir.Sync()
		closeErr = dir.Close()
		if err := errors.Join(syncErr, closeErr, faultCtx.Err()); err != nil {
			return fail(err)
		}
		if err := own(faultCtx); err != nil {
			return fail(err)
		}
		current, err := os.Lstat(directory)
		if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
			return fail(errors.New("origin owner directory changed"))
		}
		if err := faultCtx.Err(); err != nil {
			return fail(err)
		}
		return origin, nil
	}, nil
}
