package leasefault

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

var observerEvidenceStage = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

// RetainRecoveryObserver durably preserves one bounded observer result. The
// observation's error is data, not a retention error; callers must independently
// reject failed observations. This neither admits evidence nor proves recovery.
// All file operations use one directory handle. A replaced owner path fails
// closed after persistence; preserve partial records on any error.
func RetainRecoveryObserver(directory, stage string, output []byte, observed error) error {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == "/" || !observerEvidenceStage.MatchString(stage) || len(output) > processgroup.DefaultOutputLimitBytes {
		return errors.New("invalid recovery observer evidence scope or size")
	}
	root, err := recoveryRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	identity, err := root.Stat(".")
	if err != nil {
		return err
	}
	message := ""
	if observed != nil {
		message = observed.Error()
	}
	if len(message) > 64<<10 {
		return errors.New("oversized recovery observer error")
	}
	data, err := json.Marshal(struct {
		Output []byte    `json:"output"`
		Error  string    `json:"error"`
		At     time.Time `json:"at"`
	}{output, message, time.Now().UTC()})
	if err != nil {
		return err
	}
	name := "recovery-" + stage + "." + rand.Text() + ".json"
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(data, '\n'))
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	syncErr = dir.Sync()
	closeErr = dir.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return err
	}
	current, err := os.Lstat(directory)
	if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(identity, current) {
		return errors.New("recovery observer owner directory changed")
	}
	return nil
}
