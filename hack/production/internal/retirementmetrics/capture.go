package retirementmetrics

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	strictjson "sigs.k8s.io/json"
)

// CaptureBinding comes from the experiment's independently saved admission.
// SpecSHA256 is the hash of encoding/json's serialization of the decoded
// StatefulSet spec (case-sensitive JSON with integers preserved).
type CaptureBinding struct {
	NamespaceUID, StatefulSetUID, PodUID, SpecSHA256 string
	Cluster                                          string
}

// LoadCapture reads the protected-stack-session metrics directory without
// contacting Kubernetes. It verifies internal consistency, not the authenticity
// of the local filesystem or the experiment's original admission.
func LoadCapture(dir string, expected CaptureBinding) (Sample, error) {
	bad := func() (Sample, error) { return Sample{}, errors.New("invalid metrics capture evidence") }
	if expected.NamespaceUID == "" || expected.StatefulSetUID == "" || expected.PodUID == "" || expected.Cluster == "" || len(expected.SpecSHA256) != 64 {
		return bad()
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return bad()
	}
	info, err := os.Lstat(abs)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return bad()
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return bad()
	}
	defer root.Close()
	read := func(name string, limit int64) ([]byte, error) {
		original, err := root.Lstat(name)
		if err != nil || !original.Mode().IsRegular() || original.Size() > limit {
			return nil, errors.New("invalid capture file")
		}
		f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil || !st.Mode().IsRegular() || st.Size() > limit || !os.SameFile(original, st) {
			return nil, errors.New("invalid capture file")
		}
		data, err := io.ReadAll(io.LimitReader(f, limit+1))
		if int64(len(data)) > limit {
			return nil, errors.New("oversize capture file")
		}
		return data, err
	}
	complete, err := read("COMPLETE", 128)
	if err != nil || string(complete) != "CAPTURE_COMPLETE_WITHIN_CALLER_BUDGET\n" {
		return bad()
	}
	manifest, err := read("evidence.sha256", 64<<10)
	if err != nil {
		return bad()
	}
	names := []string{"namespace.json", "sts.json", "pod-before.json", "pod-after.json", "probe.json", "metrics.txt", "timing.tsv", "probe.stderr"}
	files := make(map[string][]byte, len(names))
	for _, name := range names {
		data, err := read(name, 8<<20)
		if err != nil {
			return bad()
		}
		files[name] = data
	}
	// Accept exactly the generated manifest, never follow filenames supplied by
	// it. Paths must still refer to this capture, not another successful sample.
	lines := strings.Split(strings.TrimSuffix(string(manifest), "\n"), "\n")
	if len(lines) != len(names) {
		return bad()
	}
	seen := map[string]bool{}
	for _, line := range lines {
		if len(line) < 67 || line[64:66] != "  " {
			return bad()
		}
		path := line[66:]
		name := filepath.Base(path)
		data, ok := files[name]
		if !ok || seen[name] || path != filepath.Join(abs, name) {
			return bad()
		}
		sum := sha256.Sum256(data)
		if line[:64] != hex.EncodeToString(sum[:]) {
			return bad()
		}
		seen[name] = true
	}
	documents := map[string]map[string]any{}
	for _, name := range []string{"namespace.json", "sts.json", "pod-before.json", "pod-after.json"} {
		var doc map[string]any
		strictErrors, err := strictjson.UnmarshalStrict(files[name], &doc, strictjson.DisallowDuplicateFields)
		if err != nil || len(strictErrors) > 0 || doc == nil {
			return bad()
		}
		documents[name] = doc
	}
	ns, sts := documents["namespace.json"], documents["sts.json"]
	before, after := documents["pod-before.json"], documents["pod-after.json"]
	if at(ns, "metadata", "uid") != expected.NamespaceUID || at(ns, "metadata", "deletionTimestamp") != nil || at(sts, "metadata", "uid") != expected.StatefulSetUID || at(sts, "metadata", "deletionTimestamp") != nil {
		return bad()
	}
	gen, ok := at(sts, "metadata", "generation").(int64)
	if !ok || gen < 1 || at(sts, "status", "observedGeneration") != gen {
		return bad()
	}
	spec, err := json.Marshal(at(sts, "spec"))
	if err != nil {
		return bad()
	}
	sum := sha256.Sum256(spec)
	if hex.EncodeToString(sum[:]) != expected.SpecSHA256 {
		return bad()
	}
	process, err := captureProcess(before, expected.PodUID, expected.StatefulSetUID)
	if err != nil {
		return bad()
	}
	afterProcess, err := captureProcess(after, expected.PodUID, expected.StatefulSetUID)
	if err != nil || afterProcess != process {
		return bad()
	}
	process.Cluster = expected.Cluster
	beforeStatuses := at(before, "status", "containerStatuses").([]any)
	afterStatuses := at(after, "status", "containerStatuses").([]any)
	if beforeStatuses[0].(map[string]any)["imageID"] != afterStatuses[0].(map[string]any)["imageID"] {
		return bad()
	}
	for _, path := range [][]string{{"metadata", "name"}, {"metadata", "namespace"}, {"spec"}, {"status", "podIP"}} {
		if !reflect.DeepEqual(at(before, path...), at(after, path...)) {
			return bad()
		}
	}
	if at(before, "metadata", "namespace") != at(ns, "metadata", "name") || at(sts, "metadata", "namespace") != at(ns, "metadata", "name") || !reflect.DeepEqual(at(before, "spec", "containers"), at(sts, "spec", "template", "spec", "containers")) {
		return bad()
	}
	sample, err := NewSample(files["metrics.txt"], files["probe.json"], process)
	if err != nil {
		return Sample{}, err
	}
	identity, err := json.Marshal([]any{expected.NamespaceUID, expected.StatefulSetUID, expected.SpecSHA256, at(before, "metadata", "name"), at(before, "metadata", "namespace"), at(before, "spec"), at(before, "status", "podIP"), beforeStatuses[0].(map[string]any)["imageID"]})
	if err != nil {
		return bad()
	}
	identityHash := sha256.Sum256(identity)
	sample.captureIdentity = hex.EncodeToString(identityHash[:])
	// Every captured stage must have succeeded in the expected order. Parsing
	// wall-clock stage durations is deliberately not an event-latency claim.
	stages := []string{"verify-inputs", "snapshot-before", "identity-before", "protected-probe", "snapshot-after", "identity-after"}
	trace := bytes.Split(bytes.TrimSuffix(files["timing.tsv"], []byte("\n")), []byte("\n"))
	if len(trace) == 14 {
		stages = append(stages, "rearm-anonymous")
	}
	if len(trace) != 2*len(stages) {
		return bad()
	}
	var previous time.Time
	for i, stage := range stages {
		for j, suffix := range []string{"\t" + stage + "\tstart\t-", "\t" + stage + "\tend\t0"} {
			if !strings.HasSuffix(string(trace[2*i+j]), suffix) {
				return bad()
			}
			stamp := strings.TrimSuffix(string(trace[2*i+j]), suffix)
			parts := strings.Split(stamp, ".")
			if len(parts) != 2 || len(parts[1]) != 6 {
				return bad()
			}
			seconds, e1 := strconv.ParseInt(parts[0], 10, 64)
			micros, e2 := strconv.ParseInt(parts[1], 10, 64)
			if e1 != nil || e2 != nil || seconds <= 0 || micros < 0 || micros >= 1000000 {
				return bad()
			}
			current := time.Unix(seconds, micros*1000)
			if !previous.IsZero() && current.Before(previous) {
				return bad()
			}
			if stage == "protected-probe" && ((j == 0 && current.After(sample.started)) || (j == 1 && current.Before(sample.completed))) {
				return bad()
			}
			previous = current
		}
	}
	return sample, nil
}

func at(doc map[string]any, path ...string) any {
	var value any = doc
	for _, key := range path {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = object[key]
	}
	return value
}

func captureProcess(pod map[string]any, uid, owner string) (Process, error) {
	bad := func() (Process, error) { return Process{}, errors.New("invalid captured Pod process") }
	if at(pod, "apiVersion") != "v1" || at(pod, "kind") != "Pod" || at(pod, "metadata", "uid") != uid || at(pod, "metadata", "deletionTimestamp") != nil {
		return bad()
	}
	for _, path := range [][]string{{"metadata", "name"}, {"metadata", "namespace"}, {"spec", "nodeName"}, {"status", "podIP"}} {
		if value, ok := at(pod, path...).(string); !ok || value == "" {
			return bad()
		}
	}
	owners, ok := at(pod, "metadata", "ownerReferences").([]any)
	if !ok {
		return bad()
	}
	owned := false
	for _, item := range owners {
		ref, ok := item.(map[string]any)
		if ok && ref["kind"] == "StatefulSet" && ref["uid"] == owner && ref["controller"] == true {
			owned = true
		}
	}
	if !owned {
		return bad()
	}
	containers, ok := at(pod, "spec", "containers").([]any)
	if !ok || len(containers) != 1 {
		return bad()
	}
	statuses, ok := at(pod, "status", "containerStatuses").([]any)
	if !ok || len(statuses) != 1 {
		return bad()
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return bad()
	}
	status, ok := statuses[0].(map[string]any)
	if !ok {
		return bad()
	}
	name, ok := container["name"].(string)
	if !ok || name == "" || status["name"] != name {
		return bad()
	}
	if image, ok := status["imageID"].(string); !ok || image == "" {
		return bad()
	}
	state, ok := status["state"].(map[string]any)
	if !ok || len(state) != 1 {
		return bad()
	}
	id, _ := status["containerID"].(string)
	started, _ := at(state, "running", "startedAt").(string)
	restarts, ok := status["restartCount"].(int64)
	if !ok || restarts < 0 || restarts > 2147483647 {
		return bad()
	}
	p := Process{PodUID: uid, ContainerID: id, StartedAt: started, RestartCount: int32(restarts)}
	if !p.valid() {
		return bad()
	}
	return p, nil
}
