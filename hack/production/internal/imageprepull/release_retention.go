package imageprepull

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// RetainReleaseResponse durably records one download phase in a caller-created
// private directory. Fixed exclusive names prohibit overwriting or reusing an
// attempt. Failure preserves partial evidence; callers must not delete/retry it.
// observed is recorded data, not the retention result. The caller must still
// reject failed observations. Output bytes are losslessly JSON/base64 encoded.
func RetainReleaseResponse(directory, stage string, output []byte, observed error) error {
	limit := 1 << 20
	switch stage {
	case "run-before", "run-after", "artifact-before", "artifact-after", "regression-before", "regression-after":
	case "archive":
		limit = 2 << 20
	default:
		return errors.New("invalid release evidence stage")
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == "/" || len(output) > limit {
		return errors.New("invalid release evidence scope or size")
	}
	message := ""
	if observed != nil {
		message = observed.Error()
	}
	if len(message) > 64<<10 {
		return errors.New("oversized release observation error")
	}
	before, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	if !before.IsDir() || before.Mode().Perm() != 0700 {
		return errors.New("release evidence directory must be private")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	opened, err := dir.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return errors.New("release evidence directory changed while opening")
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &st); err != nil {
		return err
	}
	if st.Uid != uint32(os.Geteuid()) {
		return errors.New("release evidence directory belongs to another user")
	}
	data, err := json.Marshal(struct {
		Stage  string    `json:"stage"`
		Output []byte    `json:"output"`
		Error  string    `json:"error"`
		At     time.Time `json:"at"`
	}{stage, output, message, time.Now().UTC()})
	if err != nil {
		return err
	}
	file, err := root.OpenFile("github-"+stage+".json", os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(data, '\n'))
	syncErr := file.Sync()
	if err := errors.Join(writeErr, syncErr, file.Close()); err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		return err
	}
	after, err := os.Lstat(directory)
	if err != nil || !after.IsDir() || after.Mode().Perm() != 0700 || !os.SameFile(opened, after) {
		return errors.New("release evidence directory changed after persistence")
	}
	return nil
}
