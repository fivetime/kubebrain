package imageprepull

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestAdmissionChecksBothKindsWithoutPersistingOrPreparing(t *testing.T) {
	for _, mode := range []string{"success", "job drift", "pod drift", "cancel final"} {
		t.Run(mode, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			_, j := journalFixture(t, f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			jobs, pods := 0, 0
			f.client.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
				a := action.(clienttesting.CreateAction)
				options := action.(interface{ GetCreateOptions() metav1.CreateOptions }).GetCreateOptions()
				require.Equal(t, []string{metav1.DryRunAll}, options.DryRun)
				require.Equal(t, metav1.FieldValidationStrict, options.FieldValidation)
				job := a.GetObject().(*batchv1.Job).DeepCopy()
				job.UID = types.UID("dry-job-uid")
				jobs++
				if mode == "job drift" {
					job.Spec.Template.Spec.Containers[0].Command = []string{"changed"}
				}
				return true, job, nil
			})
			f.client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				a := action.(clienttesting.CreateAction)
				options := action.(interface{ GetCreateOptions() metav1.CreateOptions }).GetCreateOptions()
				require.Equal(t, []string{metav1.DryRunAll}, options.DryRun)
				require.Equal(t, metav1.FieldValidationStrict, options.FieldValidation)
				pod := a.GetObject().(*corev1.Pod).DeepCopy()
				pod.UID = types.UID("dry-pod-uid")
				pods++
				if mode == "pod drift" {
					pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "injected"})
				}
				if mode == "cancel final" {
					cancel()
				}
				return true, pod, nil
			})
			err := f.executor.CheckAdmission(ctx, f.requests)
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, 1, jobs)
			if mode == "job drift" {
				require.Zero(t, pods)
			} else {
				require.Equal(t, 1, pods)
			}
			require.Empty(t, j.record.Targets)
			require.Empty(t, j.record.Attempt)
			require.Nil(t, j.record.Preparation)
			for _, action := range f.client.Actions() {
				require.Contains(t, []string{"get", "list", "create"}, action.GetVerb())
			}
			f.assertEmpty(t)
		})
	}
}

func TestNonPreemptingPriorityClassIsRequiredAndPreserved(t *testing.T) {
	r := requestFixture()
	job, err := BuildJob(r)
	require.NoError(t, err)
	require.Equal(t, r.PriorityClass.Name, job.Spec.Template.Spec.PriorityClassName)
	require.Equal(t, r.PriorityClass.Value, *job.Spec.Template.Spec.Priority)
	*job.Spec.Template.Spec.Priority = 1
	require.EqualValues(t, 1000000, r.PriorityClass.Value)
	for name, mutate := range map[string]func(*JobRequest){
		"missing snapshot":      func(r *JobRequest) { r.PriorityClass = nil },
		"unnamed source":        func(r *JobRequest) { r.Source.Spec.Template.Spec.PriorityClassName = "" },
		"different name":        func(r *JobRequest) { r.PriorityClass.Name = "other" },
		"missing UID":           func(r *JobRequest) { r.PriorityClass.UID = "" },
		"missing RV":            func(r *JobRequest) { r.PriorityClass.ResourceVersion = "" },
		"terminating":           func(r *JobRequest) { r.PriorityClass.DeletionTimestamp = ptr.To(metav1.Now()) },
		"preempting":            func(r *JobRequest) { r.PriorityClass.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority) },
		"default policy":        func(r *JobRequest) { r.PriorityClass.PreemptionPolicy = nil },
		"reserved priority":     func(r *JobRequest) { r.PriorityClass.Value = 1000000001 },
		"source value conflict": func(r *JobRequest) { r.Source.Spec.Template.Spec.Priority = ptr.To(int32(0)) },
		"source policy conflict": func(r *JobRequest) {
			r.Source.Spec.Template.Spec.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority)
		},
	} {
		t.Run(name, func(t *testing.T) { r := requestFixture(); mutate(&r); _, err := BuildJob(r); require.Error(t, err) })
	}
}

func TestPreparedPriorityClassIdentityAndPolicyAreRevalidated(t *testing.T) {
	for _, change := range []string{"metadata", "uid", "value", "preemption"} {
		t.Run(change, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			directory, j := journalFixture(t, f)
			_, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.NoError(t, err)
			reopenPrepared(t, f, directory, j)
			class := f.requests[0].PriorityClass.DeepCopy()
			class.ResourceVersion = "updated"
			class.Description = "metadata only"
			switch change {
			case "uid":
				class.UID = "replacement-priority"
			case "value":
				class.Value++
			case "preemption":
				class.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority)
			}
			require.NoError(t, f.client.Tracker().Update(schedulingv1.SchemeGroupVersion.WithResource("priorityclasses"), class, ""))
			_, err = f.executor.RestoreVerified(context.Background(), f.requests[0].Image, f.approved)
			if change == "metadata" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.NoError(t, f.executor.RecoverCleanup(context.Background(), f.executor.Journal))
			f.assertEmpty(t)
		})
	}
}
