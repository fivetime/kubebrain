package leasefault

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	strictjson "sigs.k8s.io/json"
)

type WaitReceipt struct{ Capture, Classification string }

// LoadWaitReceipt checks the completed protected-wait-worker's local evidence
// chain against the independently supplied original clock/source/count. Call
// only AFTER joining the actual admitted worker. This is NOT proof of process
// join, image/source provenance, live Pod identity, current term or lease wait.
// It does not re-run the classifier: the authenticated, pinned worker does that.
// Caller owns exclusive directories and must enforce the original deadline.
func LoadWaitReceipt(worker, source, sourceHash string, count int, origin time.Time) (WaitReceipt, error) {
	bad := errors.New("invalid protected wait worker receipt")
	var result WaitReceipt
	hexOK := func(s string, n int) bool {
		b, err := hex.DecodeString(s)
		return err == nil && len(b)*2 == n && len(s) == n && strings.ToLower(s) == s
	}
	if !hexOK(source, 40) || !hexOK(sourceHash, 64) || count < 0 || count > 1 || origin.UnixNano() < 1000000000000000000 || origin.UnixNano() >= 9000000000000000000 || !time.Unix(0, origin.UnixNano()).Equal(origin) {
		return result, bad
	}
	owner := filepath.Dir(worker)
	checkDir := func(dir, prefix string) error {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || owner == "/" || filepath.Dir(dir) != owner ||
			!regexp.MustCompile("^"+regexp.QuoteMeta(prefix)+`\.[A-Za-z0-9]{8}$`).MatchString(filepath.Base(dir)) {
			return bad
		}
		st, err := os.Lstat(dir)
		if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
			return bad
		}
		return nil
	}
	read := func(dir, name string, limit int64) ([]byte, error) {
		r, err := os.OpenRoot(dir)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		f, err := openRecoveryRecord(r, name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil || st.Mode().Perm()&0077 != 0 || st.Size() > limit {
			return nil, bad
		}
		data, err := io.ReadAll(io.LimitReader(f, limit+1))
		if int64(len(data)) > limit {
			return nil, bad
		}
		return data, err
	}
	if err := checkDir(worker, "stack-worker"); err != nil {
		return result, err
	}
	receipts := make(map[string][]byte)
	for _, name := range []string{"capture-path", "classification-path", "origin-ns", "exit-code"} {
		data, err := read(worker, name, 16<<10)
		if err != nil {
			return result, err
		}
		receipts[name] = data
	}
	if string(receipts["exit-code"]) != "0\n" || string(receipts["origin-ns"]) != strconv.FormatInt(origin.UnixNano(), 10)+"\n" {
		return result, bad
	}
	result.Capture = strings.TrimSuffix(string(receipts["capture-path"]), "\n")
	result.Classification = strings.TrimSuffix(string(receipts["classification-path"]), "\n")
	if string(receipts["capture-path"]) != result.Capture+"\n" || string(receipts["classification-path"]) != result.Classification+"\n" ||
		checkDir(result.Capture, "stack") != nil || checkDir(result.Classification, "expired-wait") != nil {
		return WaitReceipt{}, bad
	}
	files := make(map[string][]byte)
	for dir, names := range map[string][]string{
		result.Capture:        {"namespace.json", "sts.json", "pod-before.json", "pod-after.json", "probe.json", "goroutines.txt", "timing.tsv", "probe.stderr", "COMPLETE", "evidence.sha256"},
		result.Classification: {"input.json", "frames.json", "COMPLETE", "evidence.sha256"},
	} {
		for _, name := range names {
			data, err := read(dir, name, 8<<20)
			if err != nil {
				return WaitReceipt{}, err
			}
			files[filepath.Join(dir, name)] = data
		}
	}
	if string(files[filepath.Join(result.Capture, "COMPLETE")]) != "CAPTURE_COMPLETE_WITHIN_CALLER_BUDGET\n" || string(files[filepath.Join(result.Classification, "COMPLETE")]) != "SOURCE_BOUND_WAIT_CANDIDATES_NOT_LEASE_OR_FAULT_PROOF\n" {
		return WaitReceipt{}, bad
	}
	checkManifest := func(dir string, paths []string) bool {
		manifest := string(files[filepath.Join(dir, "evidence.sha256")])
		lines := strings.Split(strings.TrimSuffix(manifest, "\n"), "\n")
		if !strings.HasSuffix(manifest, "\n") || len(lines) != len(paths) {
			return false
		}
		expected := make(map[string]bool)
		for _, path := range paths {
			hash := sha256.Sum256(files[path])
			expected[hex.EncodeToString(hash[:])+"  "+path] = true
		}
		for _, line := range lines {
			if !expected[line] {
				return false
			}
			delete(expected, line)
		}
		return len(expected) == 0
	}
	var captured []string
	for _, name := range []string{"namespace.json", "sts.json", "pod-before.json", "pod-after.json", "probe.json", "goroutines.txt", "timing.tsv", "probe.stderr"} {
		captured = append(captured, filepath.Join(result.Capture, name))
	}
	if !checkManifest(result.Capture, captured) || !checkManifest(result.Classification, []string{filepath.Join(result.Classification, "input.json"), filepath.Join(result.Classification, "frames.json"), filepath.Join(result.Capture, "evidence.sha256")}) {
		return WaitReceipt{}, bad
	}
	var input struct {
		Capture    string `json:"capture"`
		Source     string `json:"source"`
		SourceHash string `json:"source_file_sha256"`
		Clock      string `json:"clock"`
		Count      string `json:"expected_candidates"`
		Lease      *bool  `json:"lease_identity_proven"`
		Acceptance *bool  `json:"fault_acceptance_proven"`
	}
	strict, err := strictjson.UnmarshalStrict(files[filepath.Join(result.Classification, "input.json")], &input, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 || input.Capture != result.Capture || input.Source != source || input.SourceHash != sourceHash || input.Clock != strconv.FormatInt(origin.UnixNano(), 10) || input.Count != strconv.Itoa(count) || input.Lease == nil || *input.Lease || input.Acceptance == nil || *input.Acceptance {
		return WaitReceipt{}, bad
	}
	var frames []json.RawMessage
	if json.Unmarshal(files[filepath.Join(result.Classification, "frames.json")], &frames) != nil || frames == nil || len(frames) != count {
		return WaitReceipt{}, bad
	}
	for name, before := range receipts {
		after, err := read(worker, name, 16<<10)
		if err != nil || string(before) != string(after) {
			return WaitReceipt{}, bad
		}
	}
	return result, nil
}
