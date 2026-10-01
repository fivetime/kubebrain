package leasefault

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"time"
)

// NoncesSafe supplies FaultPreparation.NoncesSafe before reservation and between
// preparation writes. Unlike Prepared it needs no reservation receipt: the first
// scan must precede CREATE. Admit must authenticate the same fixed test cluster,
// claim, explicit environment and pinned collector/classifier/tool dependencies.
// This is one snapshot attempt, not a continuous-absence or enforcement proof.
func (o NetworkObserver) NoncesSafe(ctx context.Context) (err error) {
	if ctx == nil || o.Client == nil || o.Admit == nil || o.Retain == nil || !filepath.IsAbs(o.ScriptDirectory) || filepath.Clean(o.ScriptDirectory) != o.ScriptDirectory {
		return errors.New("incomplete nonce observer binding")
	}
	if o.nonceTools != nil {
		if err := o.nonceTools(ctx); err != nil {
			return err
		}
		defer func() { err = errors.Join(err, o.nonceTools(ctx), ctx.Err()) }()
	}
	o.Network.PodBefore = bytes.Clone(o.Network.PodBefore)
	o.Network.ApprovedPolicy = bytes.Clone(o.Network.ApprovedPolicy)
	admit := func(ctx context.Context) error {
		if err := CheckNetworkIdentity(ctx, o.Client, o.Network, o.StatefulSetName, NetworkLabelRecovery, o.Admit); err != nil {
			return err
		}
		r, err := recoveryRoot(o.Directory)
		if err != nil {
			return err
		}
		defer r.Close()
		f, err := openRecoveryRecord(r, "observer-pod.json")
		if err != nil {
			return err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil || st.Mode().Perm() != 0600 || st.Size() > networkRecoveryLimit {
			return errors.New("invalid nonce observer Pod input")
		}
		data, err := io.ReadAll(io.LimitReader(f, networkRecoveryLimit+1))
		if err != nil {
			return err
		}
		if !bytes.Equal(data, o.Network.PodBefore) {
			return errors.New("nonce observer Pod input differs from admission")
		}
		return ctx.Err()
	}
	if err := admit(ctx); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline() // CheckNetworkIdentity requires a bounded deadline.
	args := []string{filepath.Join(o.ScriptDirectory, "observe-local-nonces.sh"), o.Directory,
		filepath.Join(o.Directory, "observer-pod.json"), o.Network.Nonce, o.Network.ReservedNonce,
		strconv.FormatInt(deadline.UnixNano(), 10)}
	out, observed := RunRecoveryObserver(ctx, "/bin/bash", args, append([]string{}, o.Env...))
	// Preserve even a failed observation. Exit 75 is not retried by this adapter.
	if err := o.Retain("nonces", out, observed); err != nil {
		return errors.Join(err, observed, ctx.Err())
	}
	if err := errors.Join(observed, ctx.Err()); err != nil {
		return err
	}
	if err := admit(ctx); err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
