package imageprepull

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func reopenPrepared(t *testing.T, f *executorFixture, directory string, journal *RecoveryJournal) *RecoveryJournal {
	t.Helper()
	require.NoError(t, journal.Close())
	reopened, err := OpenRecoveryJournal(directory, "attempt")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	f.executor.Journal = reopened
	return reopened
}

func TestPreparedSnapshotRestoresOnlyAfterFreshReadOnlyEvidence(t *testing.T) {
	f := newExecutorFixture(t, 2)
	directory, journal := journalFixture(t, f)
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	require.NotNil(t, journal.record.Preparation)
	data, err := os.ReadFile(filepath.Join(directory, "attempt"))
	require.NoError(t, err)
	for _, private := range []string{"not-for-prepull", "registry-credential", "server-tls", "imagePullSecrets", "source-puller"} {
		require.NotContains(t, string(data), private)
	}
	reopened := reopenPrepared(t, f, directory, journal)
	f.client.ClearActions()
	session, err := f.executor.RestoreVerified(context.Background(), f.requests[0].Image, f.approved)
	require.NoError(t, err)
	require.NotNil(t, session)
	for _, action := range f.client.Actions() {
		require.Contains(t, []string{"get", "list"}, action.GetVerb())
	}
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	require.Nil(t, reopened.record.Preparation)
	_, err = f.executor.RestoreVerified(context.Background(), f.requests[0].Image, f.approved)
	require.ErrorContains(t, err, "no active prepared snapshot")
	f.assertEmpty(t)
}

func TestPreparedSnapshotRejectsChangedEvidenceWithoutRepairingIt(t *testing.T) {
	for _, change := range []string{"source spec", "source revision", "runtime policy", "service selector", "extra node", "node identity", "job identity", "pod identity", "expired hold", "wrong image", "wrong approval", "policy fingerprint"} {
		t.Run(change, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			directory, j := journalFixture(t, f)
			_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.NoError(t, err)
			j = reopenPrepared(t, f, directory, j)
			image := f.requests[0].Image
			approved := f.approved
			switch change {
			case "source spec", "source revision":
				source := f.requests[0].Source.DeepCopy()
				if change == "source spec" {
					source.Spec.Template.Spec.Containers[0].Image = "changed:image"
				} else {
					source.Status.CurrentRevision, source.Status.UpdateRevision = "new-revision", "new-revision"
				}
				require.NoError(t, f.client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), source, source.Namespace))
			case "runtime policy":
				class := f.requests[0].RuntimeClass.DeepCopy()
				class.Handler = "sandbox"
				require.NoError(t, f.client.Tracker().Update(nodev1.SchemeGroupVersion.WithResource("runtimeclasses"), class, ""))
			case "service selector":
				service := f.requests[0].Services[0].DeepCopy()
				service.Spec.Selector = map[string]string{"another": "business"}
				require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("services"), service, service.Namespace))
			case "extra node", "node identity":
				node := f.requests[0].Node.DeepCopy()
				node.UID = "new-node-uid"
				if change == "extra node" {
					node.Name = "extra-node"
					require.NoError(t, f.client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
				} else {
					require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
				}
			case "job identity", "expired hold":
				job, err := f.client.BatchV1().Jobs("kubebrain-test").Get(context.Background(), "prepull-0", metav1.GetOptions{})
				require.NoError(t, err)
				if change == "job identity" {
					job.UID = "replacement-job"
				} else {
					job.Status.StartTime = ptr.To(metav1.NewTime(time.Now().Add(-time.Hour)))
				}
				require.NoError(t, f.client.Tracker().Update(jobsResource, job, job.Namespace))
			case "pod identity":
				pod, err := f.client.CoreV1().Pods("kubebrain-test").Get(context.Background(), "prepull-0-pod", metav1.GetOptions{})
				require.NoError(t, err)
				pod.UID = "replacement-pod"
				require.NoError(t, f.client.Tracker().Update(podsResource, pod, pod.Namespace))
			case "wrong image":
				image = "another/image@sha256:" + strings.Repeat("a", 64)
			case "wrong approval":
				approved = map[string][]string{"linux/amd64": {"sha256:" + strings.Repeat("b", 64)}}
			case "policy fingerprint":
				j.record.Preparation.Nodes[0].JobSHA256 = "sha256:" + strings.Repeat("b", 64)
			}
			f.client.ClearActions()
			session, err := f.executor.RestoreVerified(context.Background(), image, approved)
			require.Error(t, err)
			require.Nil(t, session)
			for _, action := range f.client.Actions() {
				require.Contains(t, []string{"get", "list"}, action.GetVerb())
			}
			require.Empty(t, f.deletes)
		})
	}
}

func TestPreparedSnapshotIgnoresOnlyHarmlessMetadataAndListOrder(t *testing.T) {
	f := newExecutorFixture(t, 1)
	directory, j := journalFixture(t, f)
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	reopenPrepared(t, f, directory, j)
	class := f.requests[0].RuntimeClass.DeepCopy()
	class.ResourceVersion, class.Labels = "new-rv", map[string]string{"audit": "updated"}
	require.NoError(t, f.client.Tracker().Update(nodev1.SchemeGroupVersion.WithResource("runtimeclasses"), class, ""))
	f.client.PrependReactor("list", "services", func(clienttesting.Action) (bool, runtime.Object, error) {
		list := &corev1.ServiceList{Items: []corev1.Service{*f.requests[0].Services[1].DeepCopy(), *f.requests[0].Services[0].DeepCopy()}}
		for i := range list.Items {
			list.Items[i].ResourceVersion = "new-rv"
			list.Items[i].Labels = map[string]string{"audit": "updated"}
		}
		return true, list, nil
	})
	session, err := f.executor.RestoreVerified(context.Background(), f.requests[0].Image, f.approved)
	require.NoError(t, err)
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	f.assertEmpty(t)
}

func TestCleanupInvalidatesPreparedSnapshotEvenWhenObjectsRemain(t *testing.T) {
	f := newExecutorFixture(t, 1)
	directory, j := journalFixture(t, f)
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	f.keepDependents = true
	f.executor.CleanupTimeout = 30 * time.Millisecond
	require.Error(t, f.executor.RecoverCleanup(context.Background(), j))
	require.Nil(t, j.record.Preparation)
	j = reopenPrepared(t, f, directory, j)
	require.Nil(t, j.record.Preparation)
	_, err = f.executor.RestoreVerified(context.Background(), f.requests[0].Image, f.approved)
	require.ErrorContains(t, err, "no active prepared snapshot")
}

func TestPreparedSnapshotRejectsCleanupOnlyAndMalformedReceipts(t *testing.T) {
	f := newExecutorFixture(t, 1)
	_, j := journalFixture(t, f)
	_, err := f.executor.RestoreVerified(context.Background(), f.requests[0].Image, f.approved)
	require.ErrorContains(t, err, "cleanup receipts alone")
	_, err = f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	original := *j.record.Preparation
	for _, change := range []string{"missing pod uid", "wrong job name", "missing fingerprint", "invalid runtime", "wrong count"} {
		t.Run(change, func(t *testing.T) {
			proof := original
			proof.Nodes = append([]preparationNode(nil), original.Nodes...)
			switch change {
			case "missing pod uid":
				proof.Nodes[0].PodUID = ""
			case "wrong job name":
				proof.Nodes[0].JobName = "other-job"
			case "missing fingerprint":
				proof.SourceSpecSHA256 = ""
			case "invalid runtime":
				proof.Nodes[0].Runtime = ""
			case "wrong count":
				proof.Nodes = nil
			}
			record := j.record
			record.Preparation = &proof
			require.Error(t, j.persist(record))
		})
	}
}

func TestCleanupInvalidationAlsoRejectsOriginalInMemorySession(t *testing.T) {
	f := newExecutorFixture(t, 1)
	_, j := journalFixture(t, f)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	f.client.PrependReactor("delete", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("injected delete unavailable")
	})
	require.Error(t, f.executor.Cleanup(context.Background(), session))
	require.Nil(t, j.record.Preparation)
	// The original holders are still healthy, but cleanup has irrevocably
	// invalidated readiness for this attempt. Do not resurrect it from memory.
	require.ErrorContains(t, f.executor.Verify(context.Background(), session), "no active prepared snapshot")
}
