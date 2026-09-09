package imageprepull

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func checkJob(entry *preparedEntry, job *batchv1.Job, remaining time.Duration) error {
	if job.DeletionTimestamp != nil || job.Status.Failed != 0 || job.Status.Succeeded != 0 || job.Status.Active > 1 {
		return errors.New("pre-pull Job terminated, failed, or has multiple active Pods")
	}
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue &&
			(condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed ||
				condition.Type == batchv1.JobFailureTarget || condition.Type == batchv1.JobSuccessCriteriaMet) {
			return errors.New("pre-pull Job has a terminal or terminating condition")
		}
	}
	wanted, actual := entry.wanted.Spec.DeepCopy(), job.Spec.DeepCopy()
	if err := checkLabels(entry, actual.Template.Labels); err != nil {
		return err
	}
	// Only controller-generated selector terms are admitted. Never ignore a
	// manual selector that could adopt unrelated Pods.
	if actual.Selector != nil {
		if len(actual.Selector.MatchExpressions) != 0 || len(actual.Selector.MatchLabels) == 0 {
			return errors.New("pre-pull Job selector is invalid")
		}
		for key, value := range actual.Selector.MatchLabels {
			if (key != "controller-uid" && key != "batch.kubernetes.io/controller-uid") || value != string(job.UID) {
				return errors.New("pre-pull Job selector is not bound to its UID")
			}
		}
	}
	actual.Selector = nil
	actual.Template.Labels = wanted.Template.Labels
	normalizeJob(wanted)
	normalizeJob(actual)
	if !reflect.DeepEqual(wanted, actual) {
		return errors.New("pre-pull Job spec changed during admission or observation")
	}
	if job.Status.StartTime != nil {
		now := time.Now()
		if job.Status.StartTime.After(now.Add(2*time.Second)) ||
			job.Status.StartTime.Add(time.Duration(*wanted.ActiveDeadlineSeconds)*time.Second).Sub(now) < remaining+2*time.Second {
			return errors.New("pre-pull Job lacks the required remaining hold window")
		}
	}
	return nil
}

func normalizeJob(spec *batchv1.JobSpec) {
	if spec.ManualSelector == nil {
		spec.ManualSelector = ptr.To(false)
	}
	if spec.Suspend == nil {
		spec.Suspend = ptr.To(false)
	}
	if spec.CompletionMode == nil {
		spec.CompletionMode = ptr.To(batchv1.NonIndexedCompletion)
	}
	if spec.PodReplacementPolicy == nil {
		spec.PodReplacementPolicy = ptr.To(batchv1.TerminatingOrFailed)
	}
	normalizePod(&spec.Template.Spec)
}

func normalizePod(spec *corev1.PodSpec) {
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSClusterFirst
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = corev1.DefaultSchedulerName
	}
	if spec.ServiceAccountName == "" {
		spec.ServiceAccountName = "default"
	}
	if spec.DeprecatedServiceAccount == "" {
		spec.DeprecatedServiceAccount = spec.ServiceAccountName
	}
	if spec.Priority != nil && *spec.Priority == 0 {
		spec.Priority = nil
	}
	for _, containers := range [][]corev1.Container{spec.InitContainers, spec.Containers} {
		for i := range containers {
			if containers[i].TerminationMessagePath == "" {
				containers[i].TerminationMessagePath = corev1.TerminationMessagePathDefault
			}
			if containers[i].TerminationMessagePolicy == "" {
				containers[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
			}
		}
	}
}

func checkLabels(entry *preparedEntry, actual map[string]string) error {
	for key, value := range entry.wanted.Spec.Template.Labels {
		if actual[key] != value {
			return errors.New("pre-pull Pod role labels changed")
		}
	}
	return checkServices(entry.request, actual)
}

func checkPod(entry *preparedEntry, pod *corev1.Pod) (bool, error) {
	if pod.Namespace != entry.wanted.Namespace || pod.DeletionTimestamp != nil || len(pod.OwnerReferences) != 1 {
		return false, errors.New("pre-pull Pod identity or lifecycle is invalid")
	}
	if err := checkLabels(entry, pod.Labels); err != nil {
		return false, err
	}
	if pod.Spec.NodeName != "" && pod.Spec.NodeName != entry.request.Node.Name {
		return false, errors.New("pre-pull Pod scheduled on a different Node")
	}
	wanted, actual := entry.wanted.Spec.Template.Spec.DeepCopy(), pod.Spec.DeepCopy()
	actual.NodeName = ""
	// Admit only the two standard 300-second NoExecute tolerations added by
	// DefaultTolerationSeconds, and only when not explicitly present in policy.
	filtered := actual.Tolerations[:0]
	for _, toleration := range actual.Tolerations {
		if defaultNoExecute(toleration) && !containsToleration(wanted.Tolerations, toleration) {
			continue
		}
		filtered = append(filtered, toleration)
	}
	actual.Tolerations = filtered
	if len(actual.Tolerations) == 0 {
		actual.Tolerations = nil
	}
	normalizePod(wanted)
	normalizePod(actual)
	if !reflect.DeepEqual(wanted, actual) {
		return false, errors.New("pre-pull Pod spec changed during admission or observation")
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodUnknown {
		return false, errors.New("pre-pull holder is no longer active")
	}
	if len(pod.Status.InitContainerStatuses) > 1 || len(pod.Status.ContainerStatuses) > 1 {
		return false, errors.New("pre-pull container status inventory is duplicated or unexpected")
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			if status.Name == "hold-image" && status.State.Terminated != nil {
				return false, errors.New("pre-pull holder container terminated")
			}
			if status.RestartCount != 0 {
				return false, errors.New("pre-pull container restarted")
			}
			if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
				return false, fmt.Errorf("pre-pull container %s failed: exit=%d", status.Name, status.State.Terminated.ExitCode)
			}
			if status.State.Waiting != nil {
				switch status.State.Waiting.Reason {
				case "ErrImagePull", "ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError", "RunContainerError", "CrashLoopBackOff":
					return false, fmt.Errorf("pre-pull container %s cannot start: %s", status.Name, status.State.Waiting.Reason)
				}
			}
		}
	}
	if pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" ||
		len(pod.Status.InitContainerStatuses) != 1 || len(pod.Status.ContainerStatuses) != 1 {
		return false, nil
	}
	init, hold := pod.Status.InitContainerStatuses[0], pod.Status.ContainerStatuses[0]
	if init.Name != "verify-image" || hold.Name != "hold-image" {
		return false, errors.New("pre-pull container status identities are invalid")
	}
	if init.State.Terminated == nil || init.State.Running != nil || init.State.Waiting != nil ||
		hold.State.Running == nil || hold.State.Terminated != nil || hold.State.Waiting != nil || !hold.Ready {
		return false, nil
	}
	if !imageIDMatches(entry.request.Image, init.ImageID, entry.digests) || !imageIDMatches(entry.request.Image, hold.ImageID, entry.digests) {
		return false, errors.New("pre-pull runtime image digest or identity form is not approved for the Node platform")
	}
	ready := 0
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			ready++
			if condition.Status != corev1.ConditionTrue {
				return false, nil
			}
		}
	}
	if ready > 1 {
		return false, errors.New("pre-pull Pod Ready condition is duplicated")
	}
	return ready == 1, nil
}

func defaultNoExecute(toleration corev1.Toleration) bool {
	return (toleration.Key == "node.kubernetes.io/not-ready" || toleration.Key == "node.kubernetes.io/unreachable") &&
		toleration.Operator == corev1.TolerationOpExists && toleration.Effect == corev1.TaintEffectNoExecute &&
		toleration.Value == "" && ptr.Deref(toleration.TolerationSeconds, int64(-1)) == 300
}

func containsToleration(values []corev1.Toleration, wanted corev1.Toleration) bool {
	for _, value := range values {
		if reflect.DeepEqual(value, wanted) {
			return true
		}
	}
	return false
}
