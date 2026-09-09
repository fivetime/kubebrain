package imageprepull

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// Placement is a read-only snapshot of the entire hard-placement pool, not a
// scheduler simulation. We deliberately do not narrow it by current Pod
// locations, resource availability, taints, readiness, or soft constraints:
// those can change while the rollout is in progress. Every matching node must
// be healthy and run a verified holder before rollout. An untolerated taint or
// custom scheduler can therefore block preparation, never silently remove a
// possible future target from coverage. Re-read immediately before mutation;
// this snapshot alone does not fence later cluster topology changes.
type Placement struct {
	Nodes         []corev1.Node
	RuntimeClass  *nodev1.RuntimeClass
	PriorityClass *schedulingv1.PriorityClass
}

// DiscoverPlacement uses an unfiltered, bounded LIST. A continuation token is
// an incomplete inventory, never success. The caller supplies a bounded context
// and transport; this helper performs no mutations and reads no Secrets.
func DiscoverPlacement(ctx context.Context, client kubernetes.Interface, source *appsv1.StatefulSet) (*Placement, error) {
	if client == nil || source == nil {
		return nil, errors.New("placement requires a client and source StatefulSet")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source.Spec.Template.Spec.PriorityClassName == "" {
		return nil, errors.New("source must name an existing non-preempting PriorityClass")
	}
	priority, err := client.SchedulingV1().PriorityClasses().Get(ctx, source.Spec.Template.Spec.PriorityClassName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := checkPriority(&source.Spec.Template.Spec, priority); err != nil {
		return nil, err
	}
	var runtimeClass *nodev1.RuntimeClass
	if source.Spec.Template.Spec.RuntimeClassName != nil {
		var err error
		runtimeClass, err = client.NodeV1().RuntimeClasses().Get(ctx, *source.Spec.Template.Spec.RuntimeClassName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("read pre-pull RuntimeClass: %w", err)
		}
	}
	spec, err := effectivePlacement(&source.Spec.Template.Spec, runtimeClass)
	if err != nil {
		return nil, err
	}
	list, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 257})
	if err != nil {
		return nil, fmt.Errorf("read complete pre-pull Node inventory: %w", err)
	}
	if list.Continue != "" || len(list.Items) > 256 {
		return nil, errors.New("pre-pull Node inventory is incomplete or exceeds 256 nodes")
	}
	placement := &Placement{RuntimeClass: runtimeClass.DeepCopy(), PriorityClass: priority.DeepCopy()}
	names, uids := map[string]bool{}, map[types.UID]bool{}
	for i := range list.Items {
		node := &list.Items[i]
		if len(validation.IsDNS1123Subdomain(node.Name)) != 0 || !validIdentity(string(node.UID)) || names[node.Name] || uids[node.UID] {
			return nil, errors.New("pre-pull Node inventory has missing or duplicate identities")
		}
		names[node.Name], uids[node.UID] = true, true
		matched, err := matchesPlacement(spec, node)
		if err != nil {
			return nil, err
		}
		if matched {
			placement.Nodes = append(placement.Nodes, *node.DeepCopy())
		}
	}
	if len(placement.Nodes) < 1 || len(placement.Nodes) > 32 {
		return nil, errors.New("pre-pull hard-placement pool must contain 1..32 nodes")
	}
	sort.Slice(placement.Nodes, func(i, j int) bool { return placement.Nodes[i].Name < placement.Nodes[j].Name })
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return placement, nil
}

func matchesPlacement(spec *corev1.PodSpec, node *corev1.Node) (bool, error) {
	matched := (spec.NodeName == "" || spec.NodeName == node.Name) && labels.SelectorFromSet(spec.NodeSelector).Matches(labels.Set(node.Labels))
	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil && spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		affinityMatch := false
		for _, term := range spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			ok, err := nodeMatchesTerm(node, term)
			if err != nil {
				return false, err
			}
			affinityMatch = affinityMatch || ok
		}
		matched = matched && affinityMatch
	}
	return matched, nil
}

// Resolve RuntimeClass admission's selector intersection, toleration union,
// and fixed overhead without modifying the source StatefulSet snapshot.
// Reference: Kubernetes v1.36.2 plugin/pkg/admission/runtimeclass/admission.go.
func effectivePlacement(original *corev1.PodSpec, runtimeClass *nodev1.RuntimeClass) (*corev1.PodSpec, error) {
	spec := original.DeepCopy()
	if spec.OS != nil && spec.OS.Name != corev1.Linux {
		return nil, errors.New("pre-pull source must use Linux containers")
	}
	if spec.RuntimeClassName == nil {
		if runtimeClass != nil || spec.Overhead != nil {
			return nil, errors.New("unexpected RuntimeClass or overhead without runtimeClassName")
		}
		return spec, nil
	}
	if runtimeClass == nil || runtimeClass.Name != *spec.RuntimeClassName ||
		len(validation.IsDNS1123Subdomain(runtimeClass.Name)) != 0 || !validIdentity(string(runtimeClass.UID)) ||
		!validIdentity(runtimeClass.ResourceVersion) || runtimeClass.DeletionTimestamp != nil ||
		len(validation.IsDNS1123Label(runtimeClass.Handler)) != 0 {
		return nil, errors.New("pre-pull RuntimeClass identity or handler is invalid")
	}
	if runtimeClass.Overhead == nil {
		if spec.Overhead != nil {
			return nil, errors.New("source overhead has no corresponding RuntimeClass overhead")
		}
	} else {
		if len(spec.Overhead) > 0 && !apiequality.Semantic.DeepEqual(spec.Overhead, runtimeClass.Overhead.PodFixed) {
			return nil, errors.New("source overhead conflicts with RuntimeClass")
		}
		spec.Overhead = runtimeClass.Overhead.DeepCopy().PodFixed
	}
	if runtimeClass.Scheduling != nil {
		if spec.NodeSelector == nil && len(runtimeClass.Scheduling.NodeSelector) > 0 {
			spec.NodeSelector = map[string]string{}
		}
		for key, value := range runtimeClass.Scheduling.NodeSelector {
			if existing, ok := spec.NodeSelector[key]; ok && existing != value {
				return nil, fmt.Errorf("source nodeSelector conflicts with RuntimeClass at %s", key)
			}
			spec.NodeSelector[key] = value
		}
		var err error
		spec.Tolerations, err = mergeRuntimeTolerations(spec.Tolerations, runtimeClass.Scheduling.Tolerations)
		if err != nil {
			return nil, err
		}
	}
	return spec, nil
}

// Match the admission union's redundancy removal, including NoExecute duration.
// Comparison tolerations need a separately verified feature-gate contract; do
// not guess whether they are enabled on the target cluster.
func mergeRuntimeTolerations(first, second []corev1.Toleration) ([]corev1.Toleration, error) {
	all := make([]corev1.Toleration, 0, len(first)+len(second))
	for _, set := range [][]corev1.Toleration{first, second} {
		for _, item := range set {
			if item.Operator != "" && item.Operator != corev1.TolerationOpEqual && item.Operator != corev1.TolerationOpExists {
				return nil, fmt.Errorf("unverified RuntimeClass toleration operator %q", item.Operator)
			}
			all = append(all, *item.DeepCopy())
		}
	}
	var result []corev1.Toleration
	for i, item := range all {
		covered := false
		for _, other := range result {
			if tolerationCovers(other, item) {
				covered = true
				break
			}
		}
		if !covered {
			for _, other := range all[i+1:] {
				if !reflect.DeepEqual(item, other) && tolerationCovers(other, item) {
					covered = true
					break
				}
			}
		}
		if !covered {
			result = append(result, item)
		}
	}
	return result, nil
}

func tolerationCovers(broad, narrow corev1.Toleration) bool {
	if reflect.DeepEqual(broad, narrow) {
		return true
	}
	if broad.Key != narrow.Key && (broad.Key != "" || broad.Operator != corev1.TolerationOpExists) {
		return false
	}
	if broad.Effect != "" && broad.Effect != narrow.Effect {
		return false
	}
	if broad.Effect == corev1.TaintEffectNoExecute && broad.TolerationSeconds != nil &&
		(narrow.TolerationSeconds == nil || *broad.TolerationSeconds < *narrow.TolerationSeconds) {
		return false
	}
	return broad.Operator == corev1.TolerationOpExists ||
		((broad.Operator == "" || broad.Operator == corev1.TolerationOpEqual) && narrow.Operator == corev1.TolerationOpEqual && broad.Value == narrow.Value)
}

func sameRuntimeClass(first, second *nodev1.RuntimeClass) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return first.Name == second.Name && first.UID == second.UID && second.DeletionTimestamp == nil &&
		first.Handler == second.Handler && reflect.DeepEqual(first.Scheduling, second.Scheduling) && reflect.DeepEqual(first.Overhead, second.Overhead)
}

func (e Executor) checkPlacement(ctx context.Context, session *Session) error {
	first := session.entries[0].request
	placement, err := DiscoverPlacement(ctx, e.Client, first.Source)
	if err != nil {
		return err
	}
	if !sameRuntimeClass(first.RuntimeClass, placement.RuntimeClass) || !samePriorityClass(first.PriorityClass, placement.PriorityClass) || len(placement.Nodes) != len(session.entries) {
		return errors.New("pre-pull plan does not cover the current RuntimeClass/PriorityClass and complete hard-placement pool")
	}
	for _, node := range placement.Nodes {
		found := false
		for _, entry := range session.entries {
			if node.Name == entry.request.Node.Name && node.UID == entry.request.Node.UID {
				request := entry.request
				if node.Status.NodeInfo.ContainerRuntimeVersion != request.Node.Status.NodeInfo.ContainerRuntimeVersion ||
					node.Status.NodeInfo.Architecture != request.Node.Status.NodeInfo.Architecture {
					return errors.New("pre-pull pool runtime changed")
				}
				request.Node, request.RuntimeClass = &node, placement.RuntimeClass
				request.PriorityClass = placement.PriorityClass
				if _, err := BuildJob(request); err != nil {
					return err
				}
				found = true
			}
		}
		if !found {
			return errors.New("pre-pull hard-placement pool Node identity changed")
		}
	}
	return ctx.Err()
}
