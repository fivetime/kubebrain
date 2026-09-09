// Package imageprepull builds isolated, bounded image-holding Jobs. Building a
// Job is not proof of a complete eligible-node inventory or a successful pull;
// the rollout controller must independently verify both before mutation.
package imageprepull

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

var pinnedImage = regexp.MustCompile(`^[^[:space:]@]+@sha256:[a-f0-9]{64}$`)

type JobRequest struct {
	Source            *appsv1.StatefulSet
	Node              *corev1.Node
	RuntimeClass      *nodev1.RuntimeClass // Required snapshot when source names a RuntimeClass.
	PriorityClass     *schedulingv1.PriorityClass
	Services          []corev1.Service // Complete namespace Service inventory.
	ClientServiceName string
	Name              string
	Image             string
	HoldSeconds       int64
	TTLSeconds        int32
}

func validIdentity(value string) bool {
	return value != "" && len(value) <= 128 && !strings.ContainsFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
}

// BuildJob never changes its inputs, reads credentials, or contacts Kubernetes.
// Keep the returned Job alive through the rollout, verify its actual Pod and
// image identities, and delete it with UID preconditions after use. The Job
// deadline and TTL supplement that cleanup; neither proves timely deletion.
func BuildJob(r JobRequest) (*batchv1.Job, error) {
	if r.Source == nil || r.Node == nil {
		return nil, fmt.Errorf("source StatefulSet and target Node are required")
	}
	source, node := r.Source, r.Node
	if err := checkPriority(&source.Spec.Template.Spec, r.PriorityClass); err != nil {
		return nil, err
	}
	if len(validation.IsDNS1123Label(source.Namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(source.Name)) != 0 ||
		!validIdentity(string(source.UID)) || !validIdentity(source.ResourceVersion) || source.DeletionTimestamp != nil {
		return nil, fmt.Errorf("source StatefulSet identity is invalid")
	}
	if len(validation.IsDNS1123Label(r.Name)) != 0 ||
		len(validation.IsDNS1123Subdomain(node.Name)) != 0 || !validIdentity(string(node.UID)) {
		return nil, fmt.Errorf("pre-pull Job or Node identity is invalid")
	}
	if len(r.Image) > 2048 || !pinnedImage.MatchString(r.Image) || strings.ContainsFunc(r.Image, unicode.IsControl) {
		return nil, fmt.Errorf("pre-pull image must be pinned by a lowercase sha256 digest")
	}
	if r.HoldSeconds < 60 || r.HoldSeconds > 86400 || r.TTLSeconds < 1 || r.TTLSeconds > 3600 {
		return nil, fmt.Errorf("pre-pull hold must be 60..86400 seconds and TTL 1..3600 seconds")
	}
	if node.Spec.Unschedulable || node.DeletionTimestamp != nil ||
		!validIdentity(node.Status.NodeInfo.ContainerRuntimeVersion) ||
		node.Status.NodeInfo.OperatingSystem != "linux" || node.Labels[corev1.LabelOSStable] != "linux" ||
		(node.Status.NodeInfo.Architecture != "amd64" && node.Status.NodeInfo.Architecture != "arm64") ||
		node.Labels[corev1.LabelArchStable] != node.Status.NodeInfo.Architecture {
		return nil, fmt.Errorf("pre-pull Node scheduling or platform identity is invalid")
	}
	ready, disk := 0, 0
	for _, condition := range node.Status.Conditions {
		switch condition.Type {
		case corev1.NodeReady:
			if condition.Status != corev1.ConditionTrue {
				return nil, fmt.Errorf("pre-pull Node is not Ready")
			}
			ready++
		case corev1.NodeDiskPressure:
			if condition.Status != corev1.ConditionFalse {
				return nil, fmt.Errorf("pre-pull Node has disk pressure or unknown disk state")
			}
			disk++
		}
	}
	if ready != 1 || disk != 1 {
		return nil, fmt.Errorf("pre-pull Node health evidence is missing or duplicated")
	}
	podLabels := map[string]string{"app.kubernetes.io/name": "kubebrain-image-prepull", "kubebrain.io/role": "image-prepull"}
	if err := checkServices(r, podLabels); err != nil {
		return nil, err
	}
	spec, err := effectivePlacement(&source.Spec.Template.Spec, r.RuntimeClass)
	if err != nil {
		return nil, err
	}
	if spec.NodeName != "" && spec.NodeName != node.Name {
		return nil, fmt.Errorf("pre-pull Node does not match source fixed nodeName")
	}
	if !labels.SelectorFromSet(spec.NodeSelector).Matches(labels.Set(node.Labels)) {
		return nil, fmt.Errorf("pre-pull Node does not match source nodeSelector")
	}
	var affinity corev1.NodeAffinity
	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil {
		affinity = *spec.Affinity.NodeAffinity.DeepCopy()
	}
	hadRequiredAffinity := affinity.RequiredDuringSchedulingIgnoredDuringExecution != nil
	if !hadRequiredAffinity {
		affinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{}}}
	} else {
		matched := false
		for _, term := range affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			ok, err := nodeMatchesTerm(node, term)
			if err != nil {
				return nil, err
			}
			matched = matched || ok
		}
		if !matched {
			return nil, fmt.Errorf("pre-pull Node does not match source required node affinity")
		}
	}
	// Add the pin to every OR term, not as another term that would widen the
	// source's required node affinity. Do not bypass the scheduler with nodeName.
	for i := range affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		term := &affinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
		if hadRequiredAffinity && len(term.MatchExpressions)+len(term.MatchFields) == 0 {
			continue // Adding a field would turn a no-match alternative into a match.
		}
		term.MatchFields = append(term.MatchFields, corev1.NodeSelectorRequirement{
			Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node.Name},
		})
	}
	encoded, err := json.Marshal(source.Spec)
	if err != nil {
		return nil, fmt.Errorf("encode source spec: %w", err)
	}
	container := corev1.Container{
		Image: r.Image, ImagePullPolicy: corev1.PullIfNotPresent,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
		},
	}
	verify, hold := *container.DeepCopy(), *container.DeepCopy()
	verify.Name, verify.Command, verify.Args = "verify-image", []string{"/usr/local/bin/kube-brain"}, []string{"--version"}
	hold.Name, hold.Command, hold.Args = "hold-image", []string{"/bin/sleep"}, []string{strconv.FormatInt(r.HoldSeconds, 10)}
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: source.Namespace, Name: r.Name,
			Annotations: map[string]string{
				"kubebrain.io/prepull-node-uid":           string(node.UID),
				"kubebrain.io/prepull-source-spec-sha256": fmt.Sprintf("%x", sha256.Sum256(encoded)),
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: source.Name,
				UID: source.UID, Controller: ptr.To(false), BlockOwnerDeletion: ptr.To(false)}},
		},
		Spec: batchv1.JobSpec{
			Parallelism: ptr.To(int32(1)), Completions: ptr.To(int32(1)), BackoffLimit: ptr.To(int32(0)),
			ActiveDeadlineSeconds: ptr.To(r.HoldSeconds), TTLSecondsAfterFinished: ptr.To(r.TTLSeconds),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: ptr.To(false),
					EnableServiceLinks: ptr.To(false), PreemptionPolicy: ptr.To(corev1.PreemptNever),
					PriorityClassName: r.PriorityClass.Name, Priority: ptr.To(r.PriorityClass.Value),
					ServiceAccountName: spec.ServiceAccountName, ImagePullSecrets: spec.ImagePullSecrets,
					SchedulerName: spec.SchedulerName, OS: &corev1.PodOS{Name: corev1.Linux},
					RuntimeClassName: spec.RuntimeClassName, NodeSelector: spec.NodeSelector, Tolerations: spec.Tolerations,
					Overhead: spec.Overhead,
					Affinity: &corev1.Affinity{NodeAffinity: &affinity}, TerminationGracePeriodSeconds: ptr.To(int64(5)),
					SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(65532)),
						RunAsGroup: ptr.To(int64(65532)), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					InitContainers: []corev1.Container{verify}, Containers: []corev1.Container{hold},
				},
			},
		},
	}, nil
}

func checkServices(r JobRequest, podLabels map[string]string) error {
	if r.ClientServiceName == "" || r.Source.Spec.ServiceName == "" {
		return fmt.Errorf("client and peer Service identities are required")
	}
	seen := make(map[string]bool)
	for _, service := range r.Services {
		if service.Namespace != r.Source.Namespace {
			return fmt.Errorf("Service inventory crosses the source namespace")
		}
		if seen[service.Name] || !validIdentity(string(service.UID)) || len(validation.IsDNS1123Label(service.Name)) != 0 {
			return fmt.Errorf("Service inventory identity is missing or duplicated")
		}
		if (service.Name == r.ClientServiceName || service.Name == r.Source.Spec.ServiceName) && service.DeletionTimestamp != nil {
			return fmt.Errorf("source Service %s is terminating", service.Name)
		}
		seen[service.Name] = true
		if len(service.Spec.Selector) > 0 && labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(podLabels)) {
			return fmt.Errorf("pre-pull Pod would match Service %s", service.Name)
		}
	}
	if !seen[r.ClientServiceName] || !seen[r.Source.Spec.ServiceName] {
		return fmt.Errorf("source client or peer Service is missing from inventory")
	}
	return nil
}

func nodeMatchesTerm(node *corev1.Node, term corev1.NodeSelectorTerm) (bool, error) {
	if len(term.MatchExpressions)+len(term.MatchFields) == 0 {
		return false, nil // An explicitly empty required term selects no nodes.
	}
	matched := true
	for _, field := range term.MatchFields {
		if field.Key != "metadata.name" || len(field.Values) != 1 ||
			(field.Operator != corev1.NodeSelectorOpIn && field.Operator != corev1.NodeSelectorOpNotIn) {
			return false, fmt.Errorf("unsupported or malformed node affinity field requirement")
		}
		equal := node.Name == field.Values[0]
		matched = matched && (equal == (field.Operator == corev1.NodeSelectorOpIn))
	}
	operators := map[corev1.NodeSelectorOperator]selection.Operator{
		corev1.NodeSelectorOpIn: selection.In, corev1.NodeSelectorOpNotIn: selection.NotIn,
		corev1.NodeSelectorOpExists: selection.Exists, corev1.NodeSelectorOpDoesNotExist: selection.DoesNotExist,
		corev1.NodeSelectorOpGt: selection.GreaterThan, corev1.NodeSelectorOpLt: selection.LessThan,
	}
	for _, expression := range term.MatchExpressions {
		op, ok := operators[expression.Operator]
		if !ok {
			return false, fmt.Errorf("unsupported node affinity operator %q", expression.Operator)
		}
		requirement, err := labels.NewRequirement(expression.Key, op, expression.Values)
		if err != nil {
			return false, fmt.Errorf("invalid node affinity requirement: %w", err)
		}
		matched = matched && requirement.Matches(labels.Set(node.Labels))
	}
	return matched, nil
}
