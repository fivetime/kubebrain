package imageprepull

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedbatch "k8s.io/client-go/kubernetes/typed/batch/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

var jobsResource = batchv1.SchemeGroupVersion.WithResource("jobs")
var podsResource = corev1.SchemeGroupVersion.WithResource("pods")

type executorFixture struct {
	executor       Executor
	client         *fake.Clientset
	requests       []JobRequest
	approved       map[string][]string
	beforeCreate   func(*batchv1.Job) error
	afterCreate    func(*batchv1.Job, *corev1.Pod) error
	keepDependents bool
	deletes        []metav1.DeleteOptions
}

func newExecutorFixture(t *testing.T, count int) *executorFixture {
	t.Helper()
	fixture := &executorFixture{approved: map[string][]string{"linux/amd64": {"sha256:" + strings.Repeat("a", 64)}}}
	base := requestFixture()
	base.Source.Spec.Replicas = ptr.To(int32(3))
	base.Source.Status.ReadyReplicas = 3
	base.Source.Generation, base.Source.Status.ObservedGeneration = 1, 1
	base.Source.Status.CurrentRevision, base.Source.Status.UpdateRevision = "source-revision", "source-revision"
	objects := []runtime.Object{base.Source.DeepCopy(), base.RuntimeClass.DeepCopy(), base.PriorityClass.DeepCopy()}
	for i := range base.Services {
		objects = append(objects, base.Services[i].DeepCopy())
	}
	for i := 0; i < count; i++ {
		r := base
		r.Node = base.Node.DeepCopy()
		r.Node.Name, r.Node.UID = fmt.Sprintf("worker-%d", i), types.UID(fmt.Sprintf("node-%d", i))
		r.Name = fmt.Sprintf("prepull-%d", i)
		fixture.requests = append(fixture.requests, r)
		objects = append(objects, r.Node.DeepCopy())
	}
	fixture.client = fake.NewSimpleClientset(objects...)
	// Success-path policy/recovery tests perform real journal fsyncs under
	// -race. Give them scheduling/I/O headroom on shared CI runners; tests
	// about timeout behavior set their own short phase budget explicitly.
	fixture.executor = Executor{Client: fixture.client, PrepareTimeout: 30 * time.Second,
		CleanupTimeout: 30 * time.Second, PollInterval: time.Millisecond, MinRemainingHold: time.Minute}
	fixture.client.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		job := action.(clienttesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		if fixture.beforeCreate != nil {
			if err := fixture.beforeCreate(job); err != nil {
				return true, nil, err
			}
		}
		job.UID, job.ResourceVersion = types.UID("job-"+job.Name), "1"
		job.Status.StartTime, job.Status.Active = ptr.To(metav1.Now()), 1
		job.Spec.Template.Labels["batch.kubernetes.io/controller-uid"] = string(job.UID)
		job.Spec.Template.Labels["batch.kubernetes.io/job-name"] = job.Name
		job.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"batch.kubernetes.io/controller-uid": string(job.UID)}}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: job.Namespace, Name: job.Name + "-pod", UID: types.UID("pod-" + job.Name), ResourceVersion: "2",
			Labels:          job.Spec.Template.DeepCopy().Labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}},
		}, Spec: *job.Spec.Template.Spec.DeepCopy()}
		for _, r := range fixture.requests {
			if r.Name == job.Name {
				pod.Spec.NodeName = r.Node.Name
			}
		}
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning,
			Conditions:            []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "verify-image", ImageID: fixture.approved["linux/amd64"][0], State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}},
			ContainerStatuses:     []corev1.ContainerStatus{{Name: "hold-image", ImageID: fixture.approved["linux/amd64"][0], Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
		}
		var responseErr error
		if fixture.afterCreate != nil {
			responseErr = fixture.afterCreate(job, pod)
		}
		if err := fixture.client.Tracker().Create(jobsResource, job, job.Namespace); err != nil {
			return true, nil, err
		}
		if err := fixture.client.Tracker().Create(podsResource, pod, pod.Namespace); err != nil {
			return true, nil, err
		}
		if responseErr != nil {
			return true, nil, responseErr
		}
		return true, job.DeepCopy(), nil
	})
	fixture.client.PrependReactor("delete", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		request := action.(clienttesting.DeleteAction)
		object, err := fixture.client.Tracker().Get(jobsResource, action.GetNamespace(), request.GetName())
		if err != nil {
			return true, nil, err
		}
		job := object.(*batchv1.Job)
		options := request.GetDeleteOptions()
		if options.Preconditions == nil || ptr.Deref(options.Preconditions.UID, types.UID("")) != job.UID || ptr.Deref(options.Preconditions.ResourceVersion, "") != job.ResourceVersion {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "jobs"}, job.Name, errors.New("precondition mismatch"))
		}
		require.Equal(t, metav1.DeletePropagationForeground, *options.PropagationPolicy)
		fixture.deletes = append(fixture.deletes, options)
		if !fixture.keepDependents {
			require.NoError(t, fixture.client.Tracker().Delete(podsResource, job.Namespace, job.Name+"-pod"))
		}
		return true, nil, fixture.client.Tracker().Delete(jobsResource, job.Namespace, job.Name)
	})
	return fixture
}

func (f *executorFixture) assertEmpty(t *testing.T) {
	t.Helper()
	jobs, err := f.client.BatchV1().Jobs("kubebrain-test").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, jobs.Items)
	pods, err := f.client.CoreV1().Pods("kubebrain-test").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, pods.Items)
}

func TestExecutorFixtureAllowsSlowSuccessfulPreparationButHonorsExplicitDeadline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		timeout  time.Duration
		delay    time.Duration
		deadline bool
	}{
		{name: "successful setup beyond former one second fixture limit", delay: 1100 * time.Millisecond},
		{name: "explicit short deadline remains enforced", timeout: 50 * time.Millisecond, delay: 100 * time.Millisecond, deadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			_, journal := journalFixture(t, f)
			if tc.timeout != 0 {
				f.executor.PrepareTimeout = tc.timeout
			}
			// Model a slow but successful API response without imposing a host
			// speed requirement on unrelated identity/policy assertions. The
			// explicit-deadline case still requires compensation after CREATE.
			f.beforeCreate = func(*batchv1.Job) error {
				select {
				case <-time.After(tc.delay):
					return nil
				case <-t.Context().Done():
					return t.Context().Err()
				}
			}
			session, err := f.executor.Prepare(t.Context(), f.requests, f.approved)
			if tc.deadline {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, session)
			} else {
				require.NoError(t, err)
				require.NotNil(t, journal.record.Preparation)
				require.NoError(t, f.executor.Verify(t.Context(), session))
				require.NoError(t, f.executor.Cleanup(t.Context(), session))
			}
			require.Len(t, f.deletes, 1)
			f.assertEmpty(t)
		})
	}
}

func TestExecutorPreparesVerifiesAndCleansOwnedJobs(t *testing.T) {
	f := newExecutorFixture(t, 2)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	require.Len(t, session.entries, 2)
	require.NoError(t, f.executor.Verify(context.Background(), session))
	for _, action := range f.client.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "delete" || action.GetVerb() == "update" || action.GetVerb() == "patch" {
			require.Equal(t, "jobs", action.GetResource().Resource, "pre-pull must never mutate the business StatefulSet, Node, or credentials")
		}
	}
	// Mutating caller-owned input must not change a prepared session.
	f.requests[0].Source.Spec.ServiceName = "caller-changed"
	f.approved["linux/amd64"][0] = "caller-changed"
	require.NoError(t, f.executor.Verify(context.Background(), session))
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	require.Len(t, f.deletes, 2)
	f.assertEmpty(t)
}

func TestExecutorCompensatesFailedPreparation(t *testing.T) {
	for name, inject := range map[string]func(*executorFixture){
		"successful create with changed owner marker": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				j.Annotations[attemptAnnotation] = "admission-changed"
				return nil
			}
		},
		"successful create with changed source owner": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error { j.OwnerReferences[0].UID = "admission-changed"; return nil }
		},
		"second create denied": func(f *executorFixture) {
			f.beforeCreate = func(j *batchv1.Job) error {
				if j.Name == "prepull-1" {
					return errors.New("create denied")
				}
				return nil
			}
		},
		"second create response lost": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				if j.Name == "prepull-1" {
					return errors.New("response lost")
				}
				return nil
			}
		},
		"job admission changes command": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				j.Spec.Template.Spec.Containers[0].Command = []string{"unsafe"}
				return nil
			}
		},
		"pod admission adds sidecar": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "unexpected"})
				return nil
			}
		},
		"wrong runtime digest": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				p.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("b", 64)
				return nil
			}
		},
		"wrong node": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error { p.Spec.NodeName = "other-node"; return nil }
		},
		"failed version": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				p.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 17
				return nil
			}
		},
		"cold image pull failure": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				p.Status.Phase = corev1.PodPending
				p.Status.InitContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}
				return nil
			}
		},
		"expired hold budget": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				j.Status.StartTime = ptr.To(metav1.NewTime(time.Now().Add(-time.Hour)))
				return nil
			}
		},
		"holder already succeeded": func(f *executorFixture) {
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error { p.Status.Phase = corev1.PodSucceeded; return nil }
		},
		"holder never ready": func(f *executorFixture) {
			f.executor.PrepareTimeout = 50 * time.Millisecond
			f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
				p.Status.Conditions[0].Status = corev1.ConditionFalse
				return nil
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newExecutorFixture(t, 2)
			inject(f)
			session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.Error(t, err)
			require.Nil(t, session)
			require.NotEmpty(t, f.deletes, "preparation failure must compensate owned Jobs")
			f.assertEmpty(t)
		})
	}
}

func TestExecutorRefusesReplacementAndConfigurationDrift(t *testing.T) {
	for _, change := range []string{"job", "pod", "source", "node", "service"} {
		t.Run(change, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.NoError(t, err)
			ctx := context.Background()
			switch change {
			case "job":
				job, err := f.client.BatchV1().Jobs("kubebrain-test").Get(ctx, "prepull-0", metav1.GetOptions{})
				require.NoError(t, err)
				job.UID = "replacement-job"
				require.NoError(t, f.client.Tracker().Update(jobsResource, job, job.Namespace))
			case "pod":
				pod, err := f.client.CoreV1().Pods("kubebrain-test").Get(ctx, "prepull-0-pod", metav1.GetOptions{})
				require.NoError(t, err)
				pod.UID = "replacement-pod"
				require.NoError(t, f.client.Tracker().Update(podsResource, pod, pod.Namespace))
			case "source":
				source := f.requests[0].Source.DeepCopy()
				source.Spec.ServiceName = "concurrent-change"
				require.NoError(t, f.client.Tracker().Update(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}, source, source.Namespace))
			case "node":
				node := f.requests[0].Node.DeepCopy()
				node.UID = "replacement-node"
				require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
			case "service":
				service := f.requests[0].Services[0].DeepCopy()
				service.Spec.Selector = map[string]string{"kubebrain.io/role": "image-prepull"}
				require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("services"), service, service.Namespace))
			}
			require.Error(t, f.executor.Verify(ctx, session))
			if change == "job" {
				require.ErrorContains(t, f.executor.Cleanup(ctx, session), "refusing to delete replaced")
				require.Empty(t, f.deletes)
			} else {
				require.NoError(t, f.executor.Cleanup(ctx, session))
				f.assertEmpty(t)
			}
		})
	}
}

func TestExecutorCancellationAndCleanupEvidence(t *testing.T) {
	f := newExecutorFixture(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.executor.Prepare(ctx, f.requests, f.approved)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.client.Actions())
	f = newExecutorFixture(t, 1)
	f.keepDependents = true
	f.executor.CleanupTimeout = 30 * time.Millisecond
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	require.ErrorIs(t, f.executor.Cleanup(context.Background(), session), context.DeadlineExceeded)
	require.Len(t, f.deletes, 1, "accepted deletion must still wait for dependent-Pod absence")
	// A successful Job DELETE is not proof that its Pod was deleted.
	pods, err := f.client.CoreV1().Pods("kubebrain-test").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
}

func TestImageIDRequiresReviewedDigestAndKnownIdentityForm(t *testing.T) {
	image := "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("a", 64)
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, value := range []string{digest, "containerd://" + digest, "cri-o://" + digest, "docker://" + digest, "ghcr.io/fivetime/kubebrain@" + digest} {
		require.True(t, imageIDMatches(image, value, []string{digest}), value)
	}
	for _, value := range []string{"untrusted-prefix" + digest, "unknown://" + digest, "other.example/image@" + digest, "sha256:" + strings.Repeat("c", 64)} {
		require.False(t, imageIDMatches(image, value, []string{digest}), value)
	}
}

func TestExecutorRejectsInvalidPlanBeforeAPI(t *testing.T) {
	for name, mutate := range map[string]func(*executorFixture){
		"no targets":              func(f *executorFixture) { f.requests = nil },
		"duplicate node name":     func(f *executorFixture) { f.requests[1].Node.Name = f.requests[0].Node.Name },
		"duplicate node uid":      func(f *executorFixture) { f.requests[1].Node.UID = f.requests[0].Node.UID },
		"duplicate job":           func(f *executorFixture) { f.requests[1].Name = f.requests[0].Name },
		"unreviewed architecture": func(f *executorFixture) { f.approved = nil },
		"duplicate digest": func(f *executorFixture) {
			f.approved["linux/amd64"] = append(f.approved["linux/amd64"], f.approved["linux/amd64"][0])
		},
		"invalid digest": func(f *executorFixture) { f.approved["linux/amd64"][0] = "sha256:invalid" },
		"mixed source": func(f *executorFixture) {
			f.requests[1].Source = f.requests[1].Source.DeepCopy()
			f.requests[1].Source.UID = "other-source"
		},
		"mixed candidate":       func(f *executorFixture) { f.requests[1].Image = "other@sha256:" + strings.Repeat("b", 64) },
		"insufficient hold":     func(f *executorFixture) { f.executor.MinRemainingHold = 30 * time.Minute },
		"invalid poll budget":   func(f *executorFixture) { f.executor.PollInterval = 0 },
		"unbounded preparation": func(f *executorFixture) { f.executor.PrepareTimeout = 2 * time.Hour },
	} {
		t.Run(name, func(t *testing.T) {
			f := newExecutorFixture(t, 2)
			mutate(f)
			_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.Error(t, err)
			require.Empty(t, f.client.Actions())
		})
	}
}

func TestExecutorCancellationAfterCreateStillCompensates(t *testing.T) {
	f := newExecutorFixture(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error { cancel(); return nil }
	// This wrapper, unlike the fake object tracker, honors canceled DELETE
	// contexts and proves compensation does not reuse the canceled context.
	f.executor.Client = contextCheckingClient{Interface: f.client}
	_, err := f.executor.Prepare(ctx, f.requests, f.approved)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, f.deletes, 1)
	f.assertEmpty(t)
}

type contextCheckingClient struct{ kubernetes.Interface }

func (c contextCheckingClient) BatchV1() typedbatch.BatchV1Interface {
	return contextCheckingBatch{BatchV1Interface: c.Interface.BatchV1()}
}

type contextCheckingBatch struct{ typedbatch.BatchV1Interface }

func (c contextCheckingBatch) Jobs(namespace string) typedbatch.JobInterface {
	return contextCheckingJobs{JobInterface: c.BatchV1Interface.Jobs(namespace)}
}

type contextCheckingJobs struct{ typedbatch.JobInterface }

func (c contextCheckingJobs) Delete(ctx context.Context, name string, options metav1.DeleteOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.JobInterface.Delete(ctx, name, options)
}

func TestExecutorPreservesUnownedCreateRace(t *testing.T) {
	f := newExecutorFixture(t, 1)
	f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
		j.Annotations[attemptAnnotation] = "other-attempt"
		return apierrors.NewAlreadyExists(schema.GroupResource{Group: "batch", Resource: "jobs"}, j.Name)
	}
	_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.ErrorContains(t, err, "unowned Job at creation target left untouched")
	require.Empty(t, f.deletes)
	jobs, err := f.client.BatchV1().Jobs("kubebrain-test").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, jobs.Items, 1)
}

func TestExecutorChecksAPIDefaultsAndServiceAdmissionLabels(t *testing.T) {
	for _, bad := range []bool{false, true} {
		f := newExecutorFixture(t, 1)
		f.afterCreate = func(j *batchv1.Job, p *corev1.Pod) error {
			j.Spec.ManualSelector = ptr.To(false)
			j.Spec.Suspend = ptr.To(false)
			j.Spec.CompletionMode = ptr.To(batchv1.NonIndexedCompletion)
			j.Spec.PodReplacementPolicy = ptr.To(batchv1.TerminatingOrFailed)
			p.Spec.DNSPolicy = corev1.DNSClusterFirst
			p.Spec.DeprecatedServiceAccount = p.Spec.ServiceAccountName
			p.Spec.Priority = ptr.To(int32(1000000)) // Computed from source-priority, not the global default.
			for _, key := range []string{"node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"} {
				p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(300))})
			}
			p.Spec.Containers[0].TerminationMessagePath = "/dev/termination-log"
			p.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
			if bad {
				p.Labels["app"] = "kubebrain"
			}
			return nil
		}
		session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
		if bad {
			require.ErrorContains(t, err, "would match Service")
		} else {
			require.NoError(t, err)
			require.NoError(t, f.executor.Cleanup(context.Background(), session))
		}
		f.assertEmpty(t)
	}
}

func TestExecutorRejectsCancellationAtFinalEvidenceBoundary(t *testing.T) {
	f := newExecutorFixture(t, 1)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.client.PrependReactor("list", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) { cancel(); return false, nil, nil })
	require.ErrorIs(t, f.executor.Verify(ctx, session), context.Canceled)
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	f.assertEmpty(t)
}

func TestExecutorRechecksOldestHoldAfterSlowEvidence(t *testing.T) {
	f := newExecutorFixture(t, 1)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	f.executor.PrepareTimeout = 3 * time.Second
	f.executor.MinRemainingHold = 100 * time.Millisecond
	job, err := f.client.BatchV1().Jobs("kubebrain-test").Get(context.Background(), "prepull-0", metav1.GetOptions{})
	require.NoError(t, err)
	job.Status.StartTime = ptr.To(metav1.NewTime(time.Now().Add(-1800*time.Second + 2*time.Second + 300*time.Millisecond)))
	require.NoError(t, f.client.Tracker().Update(jobsResource, job, job.Namespace))
	f.client.PrependReactor("list", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		time.Sleep(300 * time.Millisecond)
		return false, nil, nil
	})
	require.ErrorContains(t, f.executor.Verify(context.Background(), session), "hold window")
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
}

func TestExecutorRetriesDeleteConflictWithFreshResourceVersion(t *testing.T) {
	f := newExecutorFixture(t, 1)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	attempts := 0
	f.client.PrependReactor("delete", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts > 1 {
			return false, nil, nil
		}
		job, err := f.client.Tracker().Get(jobsResource, "kubebrain-test", "prepull-0")
		require.NoError(t, err)
		changed := job.(*batchv1.Job).DeepCopy()
		changed.ResourceVersion = "2"
		require.NoError(t, f.client.Tracker().Update(jobsResource, changed, changed.Namespace))
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "jobs"}, changed.Name, errors.New("controller status update"))
	})
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	require.Equal(t, 2, attempts)
	require.Equal(t, "2", *f.deletes[0].Preconditions.ResourceVersion)
	f.assertEmpty(t)
}
