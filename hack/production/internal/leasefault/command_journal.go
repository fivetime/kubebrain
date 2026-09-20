package leasefault

import (
	"encoding/json"
	"errors"
	"os"
	"syscall"
	"time"
)

const commandAttemptFile = "command-attempt.json"
const commandReturnFile = "command-return.json"

// beginCommandJournal reserves one local execution attempt, not a cluster claim
// or a fault clock. Records stay on failure; a missing return means unknown,
// never success. All writes use the original private directory handle.
func beginCommandJournal(p NativeCommandPlan, digest string) (func(ClaimedCommandResult, error) error, error) {
	root, err := recoveryRoot(p.OwnerDirectory)
	if err != nil {
		return nil, err
	}
	identity, err := root.Stat(".")
	if err != nil {
		root.Close()
		return nil, err
	}
	check := func() error {
		current, err := os.Lstat(p.OwnerDirectory)
		if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
			return errors.New("command journal owner directory changed")
		}
		return nil
	}
	write := func(name string, value any) error {
		if err := check(); err != nil {
			return err
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if len(data) > 128<<10 {
			return errors.New("command journal record too large")
		}
		f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
		if err != nil {
			return err
		}
		_, writeErr := f.Write(data)
		err = errors.Join(writeErr, f.Sync(), f.Close())
		if err != nil {
			return err
		}
		d, err := root.Open(".")
		if err != nil {
			return err
		}
		return errors.Join(d.Sync(), d.Close(), check())
	}
	// Reject old/partial attempts even if a previous command failed before
	// artifacts or claim acquisition. Never overwrite evidence to retry.
	for _, name := range []string{commandAttemptFile, commandReturnFile, ownerIntentFile, ownerReceiptFile, "deployment-claimed", faultOriginFile, "HOLD", "final-exit-code"} {
		if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			root.Close()
			return nil, errors.New("command journal requires a fresh attempt")
		}
	}
	start := struct {
		PlanSHA256 string    `json:"plan_sha256"`
		Owner      string    `json:"owner"`
		At         time.Time `json:"at"`
		Scope      string    `json:"scope"`
	}{digest, p.Bindings.Network.Owner, time.Now().UTC(), "local command invocation; not cluster ownership or fault origin"}
	if err := write(commandAttemptFile, start); err != nil {
		root.Close()
		return nil, err
	}
	return func(result ClaimedCommandResult, observed error) error {
		defer root.Close()
		message := ""
		if observed != nil {
			message = observed.Error()
		}
		if len(message) > 64<<10 {
			return errors.New("command return error too large")
		}
		uid := ""
		if result.Owner != nil {
			uid = result.Owner.uid
		}
		return write(commandReturnFile, struct {
			PlanSHA256             string    `json:"plan_sha256"`
			At                     time.Time `json:"at"`
			Error                  string    `json:"error"`
			ClaimUID               string    `json:"known_claim_uid"`
			RecoveryAttempted      bool      `json:"recovery_attempted"`
			ActivationAcknowledged bool      `json:"activation_acknowledged"`
			Scope                  string    `json:"scope"`
		}{digest, time.Now().UTC(), message, uid, result.Lifecycle.RecoveryAttempted, result.Lifecycle.ActivationAcknowledged, "command return only; empty claim UID is not proof of no mutation; not acceptance or release approval"})
	}, nil
}
