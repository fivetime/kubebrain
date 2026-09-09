package imageprepull

import (
	"errors"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

// Preserve the source's explicitly named, non-preempting PriorityClass. Setting
// Never alone on a Pod without that class is rejected by Priority admission.
// Never create/change a cluster-wide PriorityClass as part of pre-pulling.
func checkPriority(spec *corev1.PodSpec, class *schedulingv1.PriorityClass) error {
	if class == nil || spec.PriorityClassName == "" || class.Name != spec.PriorityClassName ||
		len(validation.IsDNS1123Subdomain(class.Name)) != 0 || !validIdentity(string(class.UID)) || !validIdentity(class.ResourceVersion) || class.DeletionTimestamp != nil ||
		ptr.Deref(class.PreemptionPolicy, corev1.PreemptLowerPriority) != corev1.PreemptNever || class.Value > 1000000000 {
		return errors.New("pre-pull requires the source's existing named non-preempting user PriorityClass with verified identity")
	}
	if (spec.Priority != nil && *spec.Priority != class.Value) || (spec.PreemptionPolicy != nil && *spec.PreemptionPolicy != corev1.PreemptNever) {
		return errors.New("source priority or preemption policy conflicts with its PriorityClass")
	}
	return nil
}

func samePriorityClass(first, second *schedulingv1.PriorityClass) bool {
	return first != nil && second != nil && first.Name == second.Name && first.UID == second.UID && second.DeletionTimestamp == nil &&
		first.Value == second.Value && first.GlobalDefault == second.GlobalDefault && reflect.DeepEqual(first.PreemptionPolicy, second.PreemptionPolicy)
}

func priorityFingerprint(class *schedulingv1.PriorityClass) (string, error) {
	if class == nil {
		return fingerprint(nil)
	}
	return fingerprint(struct {
		Name       string
		UID        string
		Value      int32
		Default    bool
		Preemption *corev1.PreemptionPolicy
	}{
		class.Name, string(class.UID), class.Value, class.GlobalDefault, class.PreemptionPolicy})
}
