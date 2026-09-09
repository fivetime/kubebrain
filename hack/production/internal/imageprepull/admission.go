package imageprepull

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// CheckAdmission submits only DryRunAll requests, then compares the resulting
// Job and Pod policies. It does not bind a creation attempt, publish preparation
// evidence, run a container, or prove image pullability. In particular, a Job
// template dry-run alone does not exercise Pod admission, so both are checked.
func (e Executor) CheckAdmission(ctx context.Context, requests []JobRequest) error {
	if err := e.validate(); err != nil {
		return err
	}
	if e.Journal == nil || len(requests) < 1 || len(requests) > 32 {
		return errors.New("admission check requires a scoped journal and bounded node plan")
	}
	ctx, cancel := context.WithTimeout(ctx, e.PrepareTimeout)
	defer cancel()
	if err := e.Journal.checkNamespace(ctx, e); err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	session := &Session{}
	scope := e.Journal.Scope()
	names, nodes := map[string]bool{}, map[string]bool{}
	for _, request := range requests {
		wanted, err := BuildJob(request)
		if err != nil {
			return err
		}
		if request.Source.Namespace != scope.Namespace || request.Source.Name != scope.SourceName || request.Source.UID != scope.SourceUID || names[request.Name] || nodes[request.Node.Name] {
			return errors.New("admission plan differs from its scope or duplicates a target")
		}
		names[request.Name], nodes[request.Node.Name] = true, true
		wanted.Annotations[attemptAnnotation] = hex.EncodeToString(nonce[:])
		session.entries = append(session.entries, preparedEntry{request: request, wanted: wanted})
	}
	if err := e.checkPlacement(ctx, session); err != nil {
		return err
	}
	for i := range session.entries {
		entry := &session.entries[i]
		if err := e.checkSource(ctx, entry.request); err != nil {
			return err
		}
		job, err := e.Client.BatchV1().Jobs(scope.Namespace).Create(ctx, entry.wanted.DeepCopy(), metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}, FieldValidation: metav1.FieldValidationStrict})
		if err != nil {
			return fmt.Errorf("Job dry-run for %s: %w", entry.request.Node.Name, err)
		}
		if !ownsAttempt(entry, job) {
			return errors.New("dry-run Job identity or owner changed")
		}
		entry.uid = job.UID
		if err := checkJob(entry, job, e.MinRemainingHold); err != nil {
			return fmt.Errorf("Job admission for %s: %w", entry.request.Node.Name, err)
		}
		pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{
			Name: job.Name + "-check", Namespace: scope.Namespace, Labels: job.Spec.Template.DeepCopy().Labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}},
		}, Spec: *job.Spec.Template.Spec.DeepCopy()}
		actual, err := e.Client.CoreV1().Pods(scope.Namespace).Create(ctx, pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}, FieldValidation: metav1.FieldValidationStrict})
		if err != nil {
			return fmt.Errorf("Pod dry-run for %s: %w", entry.request.Node.Name, err)
		}
		if actual.Name != pod.Name || actual.Namespace != pod.Namespace || !validIdentity(string(actual.UID)) || !reflect.DeepEqual(actual.OwnerReferences, pod.OwnerReferences) {
			return errors.New("dry-run Pod identity or owner changed")
		}
		if _, err := checkPod(entry, actual); err != nil {
			return fmt.Errorf("Pod admission for %s: %w", entry.request.Node.Name, err)
		}
	}
	if err := e.checkPlacement(ctx, session); err != nil {
		return err
	}
	return ctx.Err()
}
