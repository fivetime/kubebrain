package imageprepull

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func journalFixture(t *testing.T, f *executorFixture) (string, *RecoveryJournal) {
	t.Helper()
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0700))
	scope := RecoveryScope{Namespace: "kubebrain-test", NamespaceUID: "namespace-uid", SourceName: "kubebrain", SourceUID: f.requests[0].Source.UID}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: scope.Namespace, UID: scope.NamespaceUID}}
	require.NoError(t, f.client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("namespaces"), ns, ""))
	j, err := CreateRecoveryJournal(directory, "attempt", scope)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.Close()) })
	f.executor.Journal = j
	return directory, j
}

func TestRecoveryJournalPrecedesMutationAndContainsNoSourcePayload(t *testing.T) {
	f := newExecutorFixture(t, 2)
	directory, j := journalFixture(t, f)
	f.beforeCreate = func(job *batchv1.Job) error {
		data, err := os.ReadFile(filepath.Join(directory, "attempt"))
		require.NoError(t, err)
		require.Contains(t, string(data), `"attempted":true`)
		require.Contains(t, string(data), job.Annotations[attemptAnnotation])
		require.NotContains(t, string(data), "not-for-prepull")
		require.NotContains(t, string(data), "registry-credential")
		require.NotContains(t, string(data), "server-tls")
		var diskRecord recoveryRecord
		require.NoError(t, json.Unmarshal(data, &diskRecord))
		found := false
		for _, target := range diskRecord.Targets {
			if target.Name == job.Name {
				require.True(t, target.Attempted)
				found = true
			}
		}
		require.True(t, found, "this exact target's intent must be on disk before CREATE")
		return nil
	}
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	for _, target := range j.record.Targets {
		require.NotEmpty(t, target.UID)
		require.False(t, target.Removed)
	}
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	for _, target := range j.record.Targets {
		require.True(t, target.Removed)
	}
	require.NoError(t, j.Close())
	reopened, err := OpenRecoveryJournal(directory, "attempt")
	require.NoError(t, err)
	defer reopened.Close()
	require.NoError(t, f.executor.RecoverCleanup(context.Background(), reopened))
	f.assertEmpty(t)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 2, "only the durable receipt and persistent lock should remain")
}

func TestRecoveryReopensWithoutOriginalSession(t *testing.T) {
	f := newExecutorFixture(t, 2)
	directory, j := journalFixture(t, f)
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	require.NoError(t, j.Close()) // Simulates loss of the process's in-memory Session.
	f.executor.Journal = nil
	reopened, err := OpenRecoveryJournal(directory, "attempt")
	require.NoError(t, err)
	defer reopened.Close()
	require.NoError(t, f.executor.RecoverCleanup(context.Background(), reopened))
	require.Len(t, f.deletes, 2)
	f.assertEmpty(t)
}

func TestRecoveryReconcilesLostUIDReceipt(t *testing.T) {
	f := newExecutorFixture(t, 1)
	directory, j := journalFixture(t, f)
	f.afterCreate = func(*batchv1.Job, *corev1.Pod) error {
		// Intent is durable but the created UID cannot be persisted. The API
		// object is then present when a new recovery process opens the receipt.
		require.NoError(t, j.Close())
		return nil
	}
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.ErrorContains(t, err, "persist pre-pull created UID")
	require.Nil(t, session)
	require.Empty(t, f.deletes)
	reopened, err := OpenRecoveryJournal(directory, "attempt")
	require.NoError(t, err)
	defer reopened.Close()
	require.True(t, reopened.record.Targets[0].Attempted)
	require.Empty(t, reopened.record.Targets[0].UID)
	require.NoError(t, f.executor.RecoverCleanup(context.Background(), reopened))
	require.NotEmpty(t, reopened.record.Targets[0].UID)
	require.True(t, reopened.record.Targets[0].Removed)
	f.assertEmpty(t)
}

func TestRecoveryRetainsUnknownOrUnownedCreate(t *testing.T) {
	for _, mode := range []string{"absent", "foreign marker", "foreign owner"} {
		t.Run(mode, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			directory, j := journalFixture(t, f)
			if mode == "absent" {
				f.beforeCreate = func(*batchv1.Job) error { return errors.New("unknown network outcome") }
			} else {
				f.afterCreate = func(job *batchv1.Job, _ *corev1.Pod) error {
					if mode == "foreign marker" {
						job.Annotations[attemptAnnotation] = strings.Repeat("0", 32)
					} else {
						job.OwnerReferences[0].UID = "foreign-source"
					}
					return errors.New("lost response")
				}
			}
			_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.Error(t, err)
			require.Empty(t, f.deletes)
			require.NoError(t, j.Close())
			reopened, err := OpenRecoveryJournal(directory, "attempt")
			require.NoError(t, err)
			defer reopened.Close()
			err = f.executor.RecoverCleanup(context.Background(), reopened)
			if mode == "absent" {
				require.ErrorContains(t, err, "outcome remains unknown")
			} else {
				require.ErrorContains(t, err, "unowned")
			}
			require.Empty(t, f.deletes)
			require.False(t, reopened.record.Targets[0].Removed)
		})
	}
}

func TestRecoveryRefusesReplacementButCleansIndependentTargets(t *testing.T) {
	f := newExecutorFixture(t, 2)
	directory, j := journalFixture(t, f)
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	job, err := f.client.BatchV1().Jobs("kubebrain-test").Get(context.Background(), "prepull-1", metav1.GetOptions{})
	require.NoError(t, err)
	job.UID = "replaced-job"
	require.NoError(t, f.client.Tracker().Update(jobsResource, job, job.Namespace))
	require.NoError(t, j.Close())
	reopened, err := OpenRecoveryJournal(directory, "attempt")
	require.NoError(t, err)
	defer reopened.Close()
	require.ErrorContains(t, f.executor.RecoverCleanup(context.Background(), reopened), "refusing to delete replaced")
	require.Len(t, f.deletes, 1)
	require.True(t, reopened.record.Targets[0].Removed)
	require.False(t, reopened.record.Targets[1].Removed)
	job, err = f.client.BatchV1().Jobs("kubebrain-test").Get(context.Background(), "prepull-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.EqualValues(t, "replaced-job", job.UID)
}

func TestRecoveryNamespaceFenceAndPreWriteFailure(t *testing.T) {
	for _, mode := range []string{"namespace", "closed journal", "reuse journal"} {
		t.Run(mode, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			_, j := journalFixture(t, f)
			if mode == "namespace" {
				ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kubebrain-test", UID: "replacement-namespace"}}
				require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("namespaces"), ns, ""))
			} else if mode == "closed journal" {
				require.NoError(t, j.Close())
			} else {
				_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
				require.NoError(t, err)
				f.client.ClearActions()
			}
			_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.Error(t, err)
			for _, action := range f.client.Actions() {
				require.NotEqual(t, "create", action.GetVerb())
				require.NotEqual(t, "delete", action.GetVerb(), "failed rebind must not clean an earlier attempt")
			}
		})
	}
}

func TestRecoveryFileBoundsLockAndNoOverwrite(t *testing.T) {
	f := newExecutorFixture(t, 1)
	directory, j := journalFixture(t, f)
	_, err := OpenRecoveryJournal(directory, "attempt")
	require.Error(t, err, "exclusive lock must survive snapshot replacement")
	_, err = f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	_, err = OpenRecoveryJournal(directory, "attempt")
	require.Error(t, err)
	scope := j.record.Scope
	require.NoError(t, j.Close())
	_, err = CreateRecoveryJournal(directory, "attempt", scope)
	require.ErrorIs(t, err, os.ErrExist)
	for name, data := range map[string]string{
		"truncated": `{`,
		"oversized": strings.Repeat("x", recoveryLimit+1),
		"duplicate": `{"version":1,"version":1}`,
		"unknown":   `{"extra":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(filepath.Join(directory, name), []byte(data), 0600))
			_, err := OpenRecoveryJournal(directory, name)
			require.Error(t, err)
		})
	}
	require.NoError(t, os.Symlink("attempt", filepath.Join(directory, "symlink")))
	_, err = OpenRecoveryJournal(directory, "symlink")
	require.Error(t, err)
	require.NoError(t, unix.Mkfifo(filepath.Join(directory, "fifo"), 0600))
	_, err = OpenRecoveryJournal(directory, "fifo")
	require.Error(t, err, "special files must not block recovery reads")
	_, err = OpenRecoveryJournal(directory, "../escape")
	require.Error(t, err)
	require.NoError(t, os.Chmod(directory, 0755))
	_, err = OpenRecoveryJournal(directory, "attempt")
	require.ErrorContains(t, err, "0700")
}

func TestRecoveryReconcilesCanceledCreationUsingIndependentContext(t *testing.T) {
	f := newExecutorFixture(t, 1)
	_, j := journalFixture(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.afterCreate = func(*batchv1.Job, *corev1.Pod) error { cancel(); return errors.New("lost canceled response") }
	// The immediate reconciliation GET obeys cancellation; recovery must use
	// its independent compensation context to find the persisted object.
	f.client.PrependReactor("get", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		// fake actions do not carry contexts, so reject the first read after
		// cancellation and let independent recovery reads use normal tracking.
		if ctx.Err() != nil {
			f.client.ReactionChain = f.client.ReactionChain[1:]
			return true, nil, context.Canceled
		}
		return false, nil, nil
	})
	_, err := f.executor.Prepare(ctx, f.requests, f.approved)
	require.Error(t, err)
	require.True(t, j.record.Targets[0].Removed)
	f.assertEmpty(t)
}

func TestRecoveryRejectsSessionFromAnotherAttempt(t *testing.T) {
	f := newExecutorFixture(t, 1)
	directory, firstJournal := journalFixture(t, f)
	firstSession, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	secondJournal, err := CreateRecoveryJournal(directory, "another-attempt", firstJournal.record.Scope)
	require.NoError(t, err)
	defer secondJournal.Close()
	f.executor.Journal = secondJournal
	f.requests[0].Name = "prepull-next"
	secondSession, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	require.ErrorContains(t, f.executor.Cleanup(context.Background(), firstSession), "different attempt")
	require.Empty(t, f.deletes, "a mismatched cleanup call must not mutate either attempt")
	require.ErrorContains(t, f.executor.Verify(context.Background(), firstSession), "different attempt")
	require.NoError(t, f.executor.Cleanup(context.Background(), secondSession))
	f.executor.Journal = firstJournal
	require.NoError(t, f.executor.Cleanup(context.Background(), firstSession))
	f.assertEmpty(t)
}

func TestRecoveryCanResolvePreviouslyAbsentDelayedCreate(t *testing.T) {
	f := newExecutorFixture(t, 1)
	directory, j := journalFixture(t, f)
	f.beforeCreate = func(*batchv1.Job) error { return errors.New("unknown outcome") }
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.Error(t, err)
	require.NoError(t, j.Close())
	reopened, err := OpenRecoveryJournal(directory, "attempt")
	require.NoError(t, err)
	defer reopened.Close()
	require.ErrorContains(t, f.executor.RecoverCleanup(context.Background(), reopened), "outcome remains unknown")
	// The original attempted object becomes visible only after the first
	// recovery failed. Do not create a new attempt to hide this ambiguity.
	f.beforeCreate = nil
	job, err := BuildJob(f.requests[0])
	require.NoError(t, err)
	job.Annotations[attemptAnnotation] = reopened.record.Attempt
	_, err = f.client.BatchV1().Jobs(job.Namespace).Create(context.Background(), job, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, f.executor.RecoverCleanup(context.Background(), reopened))
	require.True(t, reopened.record.Targets[0].Removed)
	f.assertEmpty(t)
}

func TestRecoveryNamespaceReplacementBlocksDeletion(t *testing.T) {
	f := newExecutorFixture(t, 1)
	_, j := journalFixture(t, f)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kubebrain-test", UID: "replacement-namespace"}}
	require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("namespaces"), ns, ""))
	require.ErrorContains(t, f.executor.Verify(context.Background(), session), "namespace incarnation")
	require.ErrorContains(t, f.executor.Cleanup(context.Background(), session), "namespace incarnation")
	require.ErrorContains(t, f.executor.RecoverCleanup(context.Background(), j), "namespace incarnation")
	require.Empty(t, f.deletes)
}

func TestRecoveryPersistsSuccessfulUIDBeforeAdmissionRejection(t *testing.T) {
	f := newExecutorFixture(t, 1)
	_, j := journalFixture(t, f)
	f.afterCreate = func(job *batchv1.Job, _ *corev1.Pod) error {
		job.OwnerReferences[0].UID = "admission-mutated-owner"
		job.Annotations[attemptAnnotation] = "admission-mutated-marker"
		return nil
	}
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.ErrorContains(t, err, "creation identity mismatch")
	require.NotEmpty(t, j.record.Targets[0].UID)
	require.True(t, j.record.Targets[0].Removed)
	f.assertEmpty(t)
}

func TestRecoveryRejectsHardLinkedFilesAndInvalidTransitions(t *testing.T) {
	f := newExecutorFixture(t, 1)
	directory, j := journalFixture(t, f)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	require.Error(t, j.update("prepull-0", true, "another-job-uid", false))
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	require.Error(t, j.update("prepull-0", true, "", false))
	require.NoError(t, j.Close())
	require.NoError(t, os.Link(filepath.Join(directory, "attempt"), filepath.Join(directory, "hard-link")))
	_, err = OpenRecoveryJournal(directory, "hard-link")
	require.ErrorContains(t, err, "hard links")
	require.NoError(t, os.Symlink("attempt.lock", filepath.Join(directory, "lock-alias.lock")))
	_, err = OpenRecoveryJournal(directory, "lock-alias")
	require.ErrorContains(t, err, "symlinks")
}
