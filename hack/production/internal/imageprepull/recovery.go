package imageprepull

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"

	"golang.org/x/sys/unix"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

const recoveryLimit = 64 << 10

var attemptPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// RecoveryScope binds the receipt to one namespace incarnation and source.
// Use an explicitly selected cluster client; the receipt contains no credentials.
type RecoveryScope struct {
	Namespace    string    `json:"namespace"`
	NamespaceUID types.UID `json:"namespaceUID"`
	SourceName   string    `json:"sourceName"`
	SourceUID    types.UID `json:"sourceUID"`
}

type recoveryTarget struct {
	Name      string    `json:"name"`
	Attempted bool      `json:"attempted"`
	UID       types.UID `json:"uid,omitempty"`
	Removed   bool      `json:"removed"`
}

type recoveryRecord struct {
	Version     int                  `json:"version"`
	Scope       RecoveryScope        `json:"scope"`
	Attempt     string               `json:"attempt,omitempty"`
	Targets     []recoveryTarget     `json:"targets,omitempty"`
	Preparation *preparationSnapshot `json:"preparation,omitempty"`
}

// RecoveryJournal always supports cleanup and may contain a prepared snapshot.
// Neither is standalone rollout authorization: RestoreVerified must revalidate
// all live evidence. Keep it open through Prepare and
// Cleanup. A stable advisory lock excludes cooperating processes even when the
// JSON snapshot is atomically replaced. Use a private local filesystem directory
// with durable fsync/rename semantics; do not share it with untrusted writers.
// Close releases the lock, but intentionally retains the receipt and lock file.
type RecoveryJournal struct {
	root   *os.Root
	lock   *os.File
	name   string
	record recoveryRecord
	closed bool
}

// Scope returns a value copy for an explicitly scoped recovery CLI to compare
// with its requested namespace/source identities before contacting Kubernetes.
func (j *RecoveryJournal) Scope() RecoveryScope { return j.record.Scope }

// CreateRecoveryJournal never overwrites an existing receipt. The caller creates
// the directory with mode 0700; the receipt and persistent lock use mode 0600.
func CreateRecoveryJournal(directory, name string, scope RecoveryScope) (*RecoveryJournal, error) {
	if err := validateRecoveryRecord(recoveryRecord{Version: 1, Scope: scope}); err != nil {
		return nil, err
	}
	j, err := lockRecoveryJournal(directory, name)
	if err != nil {
		return nil, err
	}
	j.record = recoveryRecord{Version: 1, Scope: scope}
	file, err := j.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		data, marshalErr := json.Marshal(j.record)
		err = marshalErr
		if err == nil {
			_, err = file.Write(append(data, '\n'))
		}
		if err == nil {
			err = file.Sync()
		}
		err = errors.Join(err, file.Close())
		if err == nil {
			err = j.syncDirectory()
		}
	}
	if err != nil {
		return nil, errors.Join(err, j.Close())
	}
	return j, nil
}

func OpenRecoveryJournal(directory, name string) (*RecoveryJournal, error) {
	j, err := lockRecoveryJournal(directory, name)
	if err != nil {
		return nil, err
	}
	file, err := openRecoveryFile(j.root, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(err, j.Close())
	}
	info, err := file.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > recoveryLimit) {
		err = errors.New("pre-pull recovery receipt must be a bounded regular 0600 file")
	}
	if err == nil {
		err = checkRecoveryFileOwner(file)
	}
	var data []byte
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(file, recoveryLimit+1))
	}
	err = errors.Join(err, file.Close())
	if err == nil && len(data) > recoveryLimit {
		err = errors.New("pre-pull recovery receipt exceeds its byte limit")
	}
	if err == nil {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&j.record)
		if err == nil {
			// Canonical snapshots also reject duplicate JSON fields and trailing
			// data instead of silently choosing an interpretation of corruption.
			canonical, marshalErr := json.Marshal(j.record)
			err = marshalErr
			if err == nil && !bytes.Equal(data, append(canonical, '\n')) {
				err = errors.New("pre-pull recovery receipt is noncanonical or truncated")
			}
		}
	}
	if err == nil {
		err = validateRecoveryRecord(j.record)
	}
	if err != nil {
		return nil, errors.Join(err, j.Close())
	}
	return j, nil
}

func lockRecoveryJournal(directory, name string) (*RecoveryJournal, error) {
	if len(validation.IsDNS1123Label(name)) != 0 {
		return nil, errors.New("pre-pull recovery receipt name must be a DNS label")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*RecoveryJournal, error) { return nil, errors.Join(err, root.Close()) }
	info, err := root.Stat(".")
	if err != nil {
		return fail(err)
	}
	if info.Mode().Perm() != 0700 {
		return fail(errors.New("pre-pull recovery directory must have mode 0700"))
	}
	dir, err := root.Open(".")
	if err != nil {
		return fail(err)
	}
	var directoryStat unix.Stat_t
	err = unix.Fstat(int(dir.Fd()), &directoryStat)
	err = errors.Join(err, dir.Close())
	if err != nil {
		return fail(err)
	}
	if directoryStat.Uid != uint32(os.Geteuid()) {
		return fail(errors.New("pre-pull recovery directory is not owned by the current user"))
	}
	lock, err := openRecoveryFile(root, name+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fail(err)
	}
	j := &RecoveryJournal{root: root, lock: lock, name: name}
	lockInfo, err := lock.Stat()
	if err == nil && (!lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != 0600) {
		err = errors.New("pre-pull recovery lock must be a regular 0600 file")
	}
	if err == nil {
		err = checkRecoveryFileOwner(lock)
	}
	if err == nil {
		err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("lock pre-pull recovery receipt: %w", err), j.Close())
	}
	return j, nil
}

func checkRecoveryFileOwner(file *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	if stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return errors.New("pre-pull recovery files must be owned by the current user and have no hard links")
	}
	return nil
}

func openRecoveryFile(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	// os.Root keeps path traversal inside the root, but resolves symlinks there.
	// Reject all non-regular leaf objects before opening, including FIFOs, and
	// compare the opened identity to the checked object. The 0700 owned directory
	// and advisory lock are still required; this is not an untrusted-writer API.
	before, err := root.Lstat(name)
	if err != nil && !(errors.Is(err, os.ErrNotExist) && flags&os.O_CREATE != 0) {
		return nil, err
	}
	if err == nil && !before.Mode().IsRegular() {
		return nil, errors.New("pre-pull recovery paths must not be symlinks or special files")
	}
	file, err := root.OpenFile(name, flags|unix.O_NONBLOCK, mode)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err == nil && (!after.Mode().IsRegular() || (before != nil && !os.SameFile(before, after))) {
		err = errors.New("pre-pull recovery file identity changed while opening")
	}
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

func validateRecoveryRecord(record recoveryRecord) error {
	scope := record.Scope
	if record.Version != 1 || len(validation.IsDNS1123Label(scope.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(scope.SourceName)) != 0 || !validIdentity(string(scope.NamespaceUID)) || !validIdentity(string(scope.SourceUID)) {
		return errors.New("invalid pre-pull recovery version or scope identity")
	}
	if record.Attempt == "" && len(record.Targets) == 0 {
		if record.Preparation != nil {
			return errors.New("prepared snapshot has no attempt or targets")
		}
		return nil
	}
	if !attemptPattern.MatchString(record.Attempt) || len(record.Targets) < 1 || len(record.Targets) > 32 {
		return errors.New("invalid pre-pull recovery attempt or target count")
	}
	names, uids := map[string]bool{}, map[types.UID]bool{}
	for _, target := range record.Targets {
		if len(validation.IsDNS1123Label(target.Name)) != 0 || names[target.Name] ||
			(target.UID != "" && (!validIdentity(string(target.UID)) || uids[target.UID])) ||
			(!target.Attempted && (target.UID != "" || target.Removed)) || (target.Removed && target.UID == "") {
			return errors.New("invalid pre-pull recovery target state or identity")
		}
		names[target.Name], uids[target.UID] = true, true
	}
	return validatePreparation(record.Preparation, record.Targets)
}

func (j *RecoveryJournal) syncDirectory() error {
	dir, err := j.root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (j *RecoveryJournal) persist(next recoveryRecord) error {
	if j == nil || j.closed {
		return errors.New("pre-pull recovery journal is closed")
	}
	if err := validateRecoveryRecord(next); err != nil {
		return err
	}
	data, err := json.Marshal(next)
	if err != nil || len(data)+1 > recoveryLimit {
		return errors.New("pre-pull recovery snapshot cannot be encoded within its bound")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	tempName := j.name + ".tmp-" + hex.EncodeToString(random[:])
	file, err := j.root.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer j.root.Remove(tempName) // Only the uniquely created, owned temporary file.
	_, err = file.Write(append(data, '\n'))
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err == nil {
		err = j.root.Rename(tempName, j.name)
	}
	if err == nil {
		err = j.syncDirectory()
	}
	if err == nil {
		j.record = next
	}
	return err
}

func (j *RecoveryJournal) bind(session *Session) error {
	if j.closed || j.record.Attempt != "" {
		return errors.New("pre-pull recovery journal is closed or already bound to an attempt")
	}
	first := session.entries[0]
	if first.wanted.Namespace != j.record.Scope.Namespace || first.request.Source.Name != j.record.Scope.SourceName || first.request.Source.UID != j.record.Scope.SourceUID {
		return errors.New("pre-pull recovery scope does not match the source")
	}
	next := j.record
	next.Attempt = first.wanted.Annotations[attemptAnnotation]
	for _, entry := range session.entries {
		next.Targets = append(next.Targets, recoveryTarget{Name: entry.wanted.Name})
	}
	return j.persist(next)
}

func (j *RecoveryJournal) checkSession(session *Session) error {
	if j == nil || j.closed {
		return errors.New("pre-pull recovery journal is closed")
	}
	if session == nil || len(session.entries) != len(j.record.Targets) || len(session.entries) == 0 {
		return errors.New("pre-pull session and recovery receipt belong to a different attempt")
	}
	for i, entry := range session.entries {
		target := j.record.Targets[i]
		if entry.wanted.Namespace != j.record.Scope.Namespace || entry.wanted.Name != target.Name ||
			entry.wanted.Annotations[attemptAnnotation] != j.record.Attempt ||
			entry.request.Source.Name != j.record.Scope.SourceName || entry.request.Source.UID != j.record.Scope.SourceUID ||
			(target.UID != "" && entry.uid != target.UID) {
			return errors.New("pre-pull session and recovery receipt belong to a different attempt")
		}
	}
	return nil
}

func (j *RecoveryJournal) update(name string, attempted bool, uid types.UID, removed bool) error {
	next := j.record
	next.Targets = append([]recoveryTarget(nil), next.Targets...)
	for i, target := range next.Targets {
		if target.Name != name {
			continue
		}
		if (target.UID != "" && uid != "" && target.UID != uid) || (target.Removed && !removed) {
			return errors.New("pre-pull recovery target identity or terminal state would regress")
		}
		target.Attempted = target.Attempted || attempted
		if uid != "" {
			target.UID = uid
		}
		target.Removed = removed
		if removed {
			next.Preparation = nil
		}
		next.Targets[i] = target
		return j.persist(next)
	}
	return errors.New("pre-pull recovery target is not in the durable plan")
}

func (j *RecoveryJournal) Close() error {
	if j == nil || j.closed {
		return nil
	}
	j.closed = true
	return errors.Join(j.lock.Close(), j.root.Close())
}

func (j *RecoveryJournal) checkNamespace(ctx context.Context, e Executor) error {
	if j == nil || j.closed {
		return errors.New("pre-pull recovery journal is closed")
	}
	namespace, err := e.Client.CoreV1().Namespaces().Get(ctx, j.record.Scope.Namespace, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if namespace.Name != j.record.Scope.Namespace || namespace.UID != j.record.Scope.NamespaceUID || namespace.DeletionTimestamp != nil {
		return errors.New("pre-pull recovery namespace incarnation changed or is terminating")
	}
	return ctx.Err()
}

// RecoverCleanup never resumes a rollout. It checks namespace identity and
// cleans each durable target independently. A pending CREATE without a known
// UID requires the original unpredictable marker and source owner. NotFound is
// NOT proof that an ambiguous request cannot still commit later, so that case
// remains an explicit error with its receipt retained for another recovery.
func (e Executor) RecoverCleanup(ctx context.Context, journal *RecoveryJournal) error {
	if err := e.validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, e.CleanupTimeout)
	defer cancel()
	if err := journal.checkNamespace(ctx, e); err != nil {
		return err
	}
	if err := journal.invalidatePreparation(); err != nil {
		return err
	}
	var failures []error
	for i := len(journal.record.Targets) - 1; i >= 0; i-- {
		target := journal.record.Targets[i]
		if !target.Attempted || target.Removed {
			continue
		}
		scope := journal.record.Scope
		entry := preparedEntry{uid: target.UID, wanted: &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: target.Name, Namespace: scope.Namespace,
			Annotations: map[string]string{attemptAnnotation: journal.record.Attempt},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: scope.SourceName,
				UID: scope.SourceUID, Controller: ptr.To(false), BlockOwnerDeletion: ptr.To(false)}},
		}}}
		if entry.uid == "" {
			job, err := e.Client.BatchV1().Jobs(scope.Namespace).Get(ctx, target.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				err = errors.New("CREATE outcome remains unknown; current absence does not prove no delayed creation")
			} else if err == nil && !ownsAttempt(&entry, job) {
				err = errors.New("ambiguous CREATE target is unowned; leaving it untouched")
			}
			if err != nil {
				failures = append(failures, fmt.Errorf("recover %s: %w", target.Name, err))
				continue
			}
			entry.uid = job.UID
			if err := journal.update(target.Name, true, entry.uid, false); err != nil {
				failures = append(failures, fmt.Errorf("record recovered UID %s: %w", target.Name, err))
				continue
			}
		}
		if err := e.cleanupOne(ctx, &entry); err != nil {
			failures = append(failures, fmt.Errorf("recover cleanup %s: %w", target.Name, err))
			continue
		}
		if err := journal.update(target.Name, true, entry.uid, true); err != nil {
			failures = append(failures, fmt.Errorf("record cleanup %s: %w", target.Name, err))
		}
	}
	return errors.Join(append(failures, ctx.Err())...)
}
