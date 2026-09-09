package imageprepull

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestBuildJobAppliesRuntimeClassAdmission(t *testing.T) {
	r := requestFixture()
	r.RuntimeClass.Scheduling = &nodev1.Scheduling{
		NodeSelector: map[string]string{"zone": "a"},
		Tolerations:  []corev1.Toleration{{Key: "runtime", Operator: corev1.TolerationOpExists}},
	}
	r.RuntimeClass.Overhead = &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Mi")}}
	original, class := r.Source.DeepCopy(), r.RuntimeClass.DeepCopy()
	job, err := BuildJob(r)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"pool": "workers", "zone": "a"}, job.Spec.Template.Spec.NodeSelector)
	require.Len(t, job.Spec.Template.Spec.Tolerations, 2)
	require.Equal(t, r.RuntimeClass.Overhead.PodFixed, job.Spec.Template.Spec.Overhead)
	job.Spec.Template.Spec.NodeSelector["zone"] = "changed"
	job.Spec.Template.Spec.Overhead[corev1.ResourceMemory] = resource.MustParse("32Mi")
	job.Spec.Template.Spec.Tolerations[1].Key = "changed"
	require.Equal(t, original, r.Source)
	require.Equal(t, class, r.RuntimeClass)
	r.RuntimeClass.Scheduling.NodeSelector["pool"] = "other"
	_, err = BuildJob(r)
	require.ErrorContains(t, err, "conflicts")
}

func TestExecutorRejectsIncompletePoolBeforeMutation(t *testing.T) {
	f := newExecutorFixture(t, 2)
	session, err := f.executor.Prepare(context.Background(), f.requests[:1], f.approved)
	require.ErrorContains(t, err, "complete hard-placement pool")
	require.Nil(t, session)
	for _, action := range f.client.Actions() {
		require.NotEqual(t, "create", action.GetVerb())
	}
	f.assertEmpty(t)
}

func TestExecutorRejectsPoolExpansionAtFinalEvidence(t *testing.T) {
	f := newExecutorFixture(t, 1)
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	f.client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		node := f.requests[0].Node.DeepCopy()
		node.Name, node.UID = "new-node", "new-node-uid"
		require.NoError(t, f.client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
		return false, nil, nil
	})
	require.ErrorContains(t, f.executor.Verify(context.Background(), session), "complete hard-placement pool")
	// Disable the one-shot injection before dependent-Pod cleanup reads.
	f.client.ReactionChain = f.client.ReactionChain[1:]
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	f.assertEmpty(t)
}

func TestDiscoverPlacementUsesFullHardPool(t *testing.T) {
	f := newExecutorFixture(t, 3)
	// Ready/taints/cordon do not hide a hard-matching future target. The
	// preparation gate must refuse unhealthy targets, not call a subset complete.
	node := f.requests[2].Node.DeepCopy()
	node.Spec.Unschedulable = true
	node.Status.Conditions[0].Status = corev1.ConditionFalse
	node.Spec.Taints = []corev1.Taint{{Key: "maintenance", Effect: corev1.TaintEffectNoSchedule}}
	require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
	p, err := DiscoverPlacement(context.Background(), f.client, f.requests[0].Source)
	require.NoError(t, err)
	require.Len(t, p.Nodes, 3)
	require.Equal(t, "worker-2", p.Nodes[2].Name)
	_, err = f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.Error(t, err)
	f.assertEmpty(t)
	// RuntimeClass selects a narrower hard pool; matching is an intersection.
	class := f.requests[0].RuntimeClass.DeepCopy()
	class.Scheduling = &nodev1.Scheduling{NodeSelector: map[string]string{"zone": "b"}}
	require.NoError(t, f.client.Tracker().Update(nodev1.SchemeGroupVersion.WithResource("runtimeclasses"), class, ""))
	node = f.requests[1].Node.DeepCopy()
	node.Labels["zone"] = "b"
	require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
	p, err = DiscoverPlacement(context.Background(), f.client, f.requests[0].Source)
	require.NoError(t, err)
	require.Len(t, p.Nodes, 1)
	require.Equal(t, "worker-1", p.Nodes[0].Name)
	// Mutating returned objects does not alter the tracker or source.
	p.Nodes[0].Labels["zone"] = "mutated"
	p.RuntimeClass.Handler = "mutated"
	p, err = DiscoverPlacement(context.Background(), f.client, f.requests[0].Source)
	require.NoError(t, err)
	require.Equal(t, "b", p.Nodes[0].Labels["zone"])
	require.Equal(t, "runc", p.RuntimeClass.Handler)
}

func TestDiscoverPlacementRejectsIncompleteOrInvalidEvidence(t *testing.T) {
	for _, name := range []string{"pagination", "too many", "duplicate name", "duplicate uid", "missing uid", "no matches", "over 32 targets", "api failure", "final cancellation"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.client.PrependReactor("list", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
				options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
				require.Empty(t, options.LabelSelector)
				require.Empty(t, options.FieldSelector)
				require.EqualValues(t, 257, options.Limit)
				list := &corev1.NodeList{Items: []corev1.Node{*f.requests[0].Node.DeepCopy()}}
				switch name {
				case "pagination":
					list.Continue = "more"
				case "too many", "over 32 targets":
					count := 33
					if name == "too many" {
						count = 257
					}
					for i := 1; i < count; i++ {
						node := f.requests[0].Node.DeepCopy()
						node.Name, node.UID = fmt.Sprintf("worker-%d", i), types.UID(fmt.Sprintf("uid-%d", i))
						list.Items = append(list.Items, *node)
					}
				case "duplicate name", "duplicate uid":
					node := f.requests[0].Node.DeepCopy()
					if name == "duplicate name" {
						node.UID = "other-uid"
					} else {
						node.Name = "other-node"
					}
					list.Items = append(list.Items, *node)
				case "missing uid":
					list.Items[0].UID = ""
				case "no matches":
					list.Items[0].Labels["pool"] = "other"
				case "api failure":
					return true, nil, errors.New("injected list failure")
				case "final cancellation":
					cancel()
				}
				return true, list, nil
			})
			p, err := DiscoverPlacement(ctx, f.client, f.requests[0].Source)
			require.Error(t, err)
			require.Nil(t, p)
		})
	}
}

func TestRuntimeTolerationUnion(t *testing.T) {
	narrow := corev1.Toleration{Key: "runtime", Operator: corev1.TolerationOpEqual, Value: "sandbox", Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(30))}
	broad := corev1.Toleration{Key: "runtime", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute}
	other := corev1.Toleration{Key: "other", Operator: corev1.TolerationOpExists}
	result, err := mergeRuntimeTolerations([]corev1.Toleration{narrow, other}, []corev1.Toleration{broad, broad})
	require.NoError(t, err)
	require.Equal(t, []corev1.Toleration{other, broad}, result)
	again, err := mergeRuntimeTolerations(result, []corev1.Toleration{broad, broad})
	require.NoError(t, err)
	require.Equal(t, result, again, "RuntimeClass admission must be idempotent on generated Jobs")
	broad.TolerationSeconds = ptr.To(int64(10))
	result, err = mergeRuntimeTolerations([]corev1.Toleration{narrow}, []corev1.Toleration{broad})
	require.NoError(t, err)
	require.Len(t, result, 2, "shorter NoExecute tolerance cannot subsume the longer one")
	_, err = mergeRuntimeTolerations(nil, []corev1.Toleration{{Operator: corev1.TolerationOpGt}})
	require.Error(t, err)
}

func TestRuntimeClassIdentityAndConflicts(t *testing.T) {
	for name, mutate := range map[string]func(*JobRequest){
		"missing":           func(r *JobRequest) { r.RuntimeClass = nil },
		"windows source":    func(r *JobRequest) { r.Source.Spec.Template.Spec.OS = &corev1.PodOS{Name: corev1.Windows} },
		"wrong name":        func(r *JobRequest) { r.RuntimeClass.Name = "another" },
		"missing uid":       func(r *JobRequest) { r.RuntimeClass.UID = "" },
		"missing rv":        func(r *JobRequest) { r.RuntimeClass.ResourceVersion = "" },
		"deleting":          func(r *JobRequest) { r.RuntimeClass.DeletionTimestamp = ptr.To(metav1.Now()) },
		"missing handler":   func(r *JobRequest) { r.RuntimeClass.Handler = "" },
		"unrequested class": func(r *JobRequest) { r.Source.Spec.Template.Spec.RuntimeClassName = nil },
		"orphan overhead": func(r *JobRequest) {
			r.Source.Spec.Template.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")}
		},
		"selector conflict": func(r *JobRequest) {
			r.RuntimeClass.Scheduling = &nodev1.Scheduling{NodeSelector: map[string]string{"pool": "other"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := requestFixture()
			mutate(&r)
			_, err := BuildJob(r)
			require.Error(t, err)
		})
	}
}

func TestExecutorRuntimeClassDriftAndPoolIdentity(t *testing.T) {
	for _, change := range []string{"class uid", "class handler", "class scheduling", "class overhead", "class deleted", "node uid", "node labels", "node architecture", "node runtime"} {
		t.Run(change, func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.NoError(t, err)
			class, node := f.requests[0].RuntimeClass.DeepCopy(), f.requests[0].Node.DeepCopy()
			switch change {
			case "class uid":
				class.UID = "replacement-runtime"
			case "class handler":
				class.Handler = "sandbox"
			case "class scheduling":
				class.Scheduling = &nodev1.Scheduling{Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}}}
			case "class overhead":
				class.Overhead = &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Mi")}}
			case "class deleted":
				class.DeletionTimestamp = ptr.To(metav1.Now())
			case "node uid":
				node.UID = "replacement-node"
			case "node labels":
				node.Labels["pool"] = "other"
			case "node architecture":
				node.Status.NodeInfo.Architecture, node.Labels[corev1.LabelArchStable] = "arm64", "arm64"
			case "node runtime":
				node.Status.NodeInfo.ContainerRuntimeVersion = "cri-o://changed"
			}
			require.NoError(t, f.client.Tracker().Update(nodev1.SchemeGroupVersion.WithResource("runtimeclasses"), class, ""))
			require.NoError(t, f.client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
			require.Error(t, f.executor.Verify(context.Background(), session))
			require.NoError(t, f.executor.Cleanup(context.Background(), session))
			f.assertEmpty(t)
		})
	}
}

func TestExecutorStopsAndCleansWhenPoolChangesBetweenCreates(t *testing.T) {
	f := newExecutorFixture(t, 2)
	created := 0
	f.afterCreate = func(*batchv1.Job, *corev1.Pod) error {
		created++
		node := f.requests[0].Node.DeepCopy()
		node.Name, node.UID = "new-node", "new-node-uid"
		return f.client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("nodes"), node, "")
	}
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.ErrorContains(t, err, "complete hard-placement pool")
	require.Nil(t, session)
	require.Equal(t, 1, created, "pool change must stop further creation")
	require.Len(t, f.deletes, 1)
	f.assertEmpty(t)
}

func TestExecutorRuntimeClassAdmissionEndToEnd(t *testing.T) {
	f := newExecutorFixture(t, 1)
	class := f.requests[0].RuntimeClass
	class.Scheduling = &nodev1.Scheduling{NodeSelector: map[string]string{"zone": "a"},
		Tolerations: []corev1.Toleration{{Key: "pool", Operator: corev1.TolerationOpExists}}}
	class.Overhead = &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Mi")}}
	require.NoError(t, f.client.Tracker().Update(nodev1.SchemeGroupVersion.WithResource("runtimeclasses"), class.DeepCopy(), ""))
	f.afterCreate = func(_ *batchv1.Job, pod *corev1.Pod) error {
		// Explicit expected post-admission policy, independently of the builder.
		pod.Spec.NodeSelector = map[string]string{"pool": "workers", "zone": "a"}
		pod.Spec.Tolerations = []corev1.Toleration{{Key: "pool", Operator: corev1.TolerationOpExists}}
		pod.Spec.Overhead = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Mi")}
		return nil
	}
	session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
	require.NoError(t, err)
	// Harmless API metadata changes do not require a new preparation attempt.
	actual := class.DeepCopy()
	actual.ResourceVersion = "2"
	actual.Labels = map[string]string{"audit": "updated"}
	require.NoError(t, f.client.Tracker().Update(nodev1.SchemeGroupVersion.WithResource("runtimeclasses"), actual, ""))
	class.Handler = "caller-mutation"
	require.NoError(t, f.executor.Verify(context.Background(), session))
	require.NoError(t, f.executor.Cleanup(context.Background(), session))
	f.assertEmpty(t)
}

func TestDiscoverPlacementHardAffinityAndNilRuntime(t *testing.T) {
	f := newExecutorFixture(t, 3)
	source := f.requests[0].Source.DeepCopy()
	source.Spec.Template.Spec.RuntimeClassName = nil
	source.Spec.Template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{}, // An empty alternative must not widen the pool.
			{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"worker-1"}}}},
		}},
	}
	p, err := DiscoverPlacement(context.Background(), f.client, source)
	require.NoError(t, err)
	require.Nil(t, p.RuntimeClass)
	require.Len(t, p.Nodes, 1)
	require.Equal(t, "worker-1", p.Nodes[0].Name)
	source.Spec.Template.Spec.NodeName = "worker-2"
	_, err = DiscoverPlacement(context.Background(), f.client, source)
	require.Error(t, err)
	source.Spec.Template.Spec.NodeName = ""
	source.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[1].MatchFields[0].Key = "unsupported"
	_, err = DiscoverPlacement(context.Background(), f.client, source)
	require.ErrorContains(t, err, "field requirement")
	for _, action := range f.client.Actions() {
		require.NotEqual(t, "runtimeclasses", action.GetResource().Resource)
	}
}

func TestRuntimePlacementDefaultsAndSemanticOverhead(t *testing.T) {
	r := requestFixture()
	r.Source.Spec.Template.Spec.NodeSelector = nil
	r.RuntimeClass.Scheduling = &nodev1.Scheduling{}
	spec, err := effectivePlacement(&r.Source.Spec.Template.Spec, r.RuntimeClass)
	require.NoError(t, err)
	require.Nil(t, spec.NodeSelector, "empty admission settings must not introduce JSON-omitted empty maps")
	r.RuntimeClass.Overhead = &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
	r.Source.Spec.Template.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1000m")}
	_, err = effectivePlacement(&r.Source.Spec.Template.Spec, r.RuntimeClass)
	require.NoError(t, err)
	r.Source.Spec.Template.Spec.Overhead[corev1.ResourceCPU] = resource.MustParse("2")
	_, err = effectivePlacement(&r.Source.Spec.Template.Spec, r.RuntimeClass)
	require.ErrorContains(t, err, "overhead conflicts")
}

func TestExecutorPolicySurvivesJSONRoundTrip(t *testing.T) {
	for _, withRuntime := range []bool{false, true} {
		t.Run(fmt.Sprint(withRuntime), func(t *testing.T) {
			f := newExecutorFixture(t, 1)
			if !withRuntime {
				f.requests[0].RuntimeClass = nil
				f.requests[0].Source.Spec.Template.Spec.RuntimeClassName = nil
				require.NoError(t, f.client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("statefulsets"), f.requests[0].Source.DeepCopy(), "kubebrain-test"))
			}
			f.afterCreate = func(job *batchv1.Job, pod *corev1.Pod) error {
				data, err := json.Marshal(job)
				require.NoError(t, err)
				*job = batchv1.Job{}
				require.NoError(t, json.Unmarshal(data, job))
				data, err = json.Marshal(pod)
				require.NoError(t, err)
				*pod = corev1.Pod{}
				require.NoError(t, json.Unmarshal(data, pod))
				return nil
			}
			session, err := f.executor.Prepare(context.Background(), f.requests, f.approved)
			require.NoError(t, err)
			require.NoError(t, f.executor.Verify(context.Background(), session))
			require.NoError(t, f.executor.Cleanup(context.Background(), session))
			f.assertEmpty(t)
		})
	}
}
