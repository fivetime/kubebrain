package imageprepull

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func requestFixture() JobRequest {
	return JobRequest{
		Name: "rollout-prepull-0", Image: "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("a", 64),
		HoldSeconds: 1800, TTLSeconds: 300, ClientServiceName: "kubebrain-client",
		RuntimeClass:  &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "source-runtime", UID: "runtime-uid", ResourceVersion: "1"}, Handler: "runc"},
		PriorityClass: &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "source-priority", UID: "priority-uid", ResourceVersion: "1"}, Value: 1000000, PreemptionPolicy: ptr.To(corev1.PreemptNever)},
		Source: &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "kubebrain", Namespace: "kubebrain-test", UID: "source-uid", ResourceVersion: "42"},
			Spec: appsv1.StatefulSetSpec{ServiceName: "kubebrain-peer", Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "kubebrain"}},
				Spec: corev1.PodSpec{
					PriorityClassName:  "source-priority",
					ServiceAccountName: "source-puller", RuntimeClassName: ptr.To("source-runtime"),
					SchedulerName: "source-scheduler", NodeSelector: map[string]string{"pool": "workers"},
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry-credential"}},
					Tolerations:      []corev1.Toleration{{Key: "pool", Operator: corev1.TolerationOpEqual, Value: "workers", Effect: corev1.TaintEffectNoSchedule}},
					Volumes:          []corev1.Volume{{Name: "server-keys", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "server-tls"}}}},
					Containers:       []corev1.Container{{Name: "kubebrain", Image: "old:image", Env: []corev1.EnvVar{{Name: "PRIVATE", Value: "not-for-prepull"}}}},
					Affinity:         &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname"}}}},
				},
			}},
		},
		Node: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-0", UID: "node-uid", Labels: map[string]string{
				"pool": "workers", "zone": "a", corev1.LabelOSStable: "linux", corev1.LabelArchStable: "amd64",
			}},
			Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{OperatingSystem: "linux", Architecture: "amd64", ContainerRuntimeVersion: "cri-o://1.35.3"},
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}, {Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse}}},
		},
		Services: []corev1.Service{
			{ObjectMeta: metav1.ObjectMeta{Name: "kubebrain-client", Namespace: "kubebrain-test", UID: "client-uid"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "kubebrain"}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "kubebrain-peer", Namespace: "kubebrain-test", UID: "peer-uid"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "kubebrain"}}},
		},
	}
}

func TestBuildJobIsolatedAndBounded(t *testing.T) {
	r := requestFixture()
	original := r.Source.DeepCopy()
	job, err := BuildJob(r)
	require.NoError(t, err)
	require.Equal(t, original, r.Source)
	require.Equal(t, r.Source.UID, job.OwnerReferences[0].UID)
	require.Equal(t, string(r.Node.UID), job.Annotations["kubebrain.io/prepull-node-uid"])
	require.Regexp(t, `^[a-f0-9]{64}$`, job.Annotations["kubebrain.io/prepull-source-spec-sha256"])
	require.EqualValues(t, 1800, *job.Spec.ActiveDeadlineSeconds)
	require.EqualValues(t, 300, *job.Spec.TTLSecondsAfterFinished)
	require.Zero(t, *job.Spec.BackoffLimit)
	require.EqualValues(t, 1, *job.Spec.Parallelism)
	require.EqualValues(t, 1, *job.Spec.Completions)
	pod := &job.Spec.Template.Spec
	require.Equal(t, corev1.RestartPolicyNever, pod.RestartPolicy)
	require.False(t, *pod.AutomountServiceAccountToken)
	require.False(t, *pod.EnableServiceLinks)
	require.Equal(t, corev1.PreemptNever, *pod.PreemptionPolicy)
	require.True(t, *pod.SecurityContext.RunAsNonRoot)
	require.EqualValues(t, 65532, *pod.SecurityContext.RunAsUser)
	require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, pod.SecurityContext.SeccompProfile.Type)
	require.Empty(t, pod.NodeName, "the scheduler must still enforce admission/placement")
	require.Equal(t, "source-scheduler", pod.SchedulerName)
	require.Equal(t, "source-puller", pod.ServiceAccountName)
	require.Equal(t, "source-runtime", *pod.RuntimeClassName)
	require.Equal(t, original.Spec.Template.Spec.ImagePullSecrets, pod.ImagePullSecrets)
	require.Equal(t, original.Spec.Template.Spec.Tolerations, pod.Tolerations)
	require.Empty(t, pod.Volumes)
	require.Nil(t, pod.Affinity.PodAntiAffinity, "warm holders must coexist with business Pods")
	require.Len(t, pod.InitContainers, 1)
	require.Len(t, pod.Containers, 1)
	require.Equal(t, []string{"/usr/local/bin/kube-brain"}, pod.InitContainers[0].Command)
	require.Equal(t, []string{"--version"}, pod.InitContainers[0].Args)
	require.Equal(t, []string{"/bin/sleep"}, pod.Containers[0].Command)
	require.Equal(t, []string{"1800"}, pod.Containers[0].Args)
	for _, container := range append(pod.InitContainers, pod.Containers...) {
		require.Equal(t, r.Image, container.Image)
		require.Equal(t, corev1.PullIfNotPresent, container.ImagePullPolicy)
		require.True(t, *container.SecurityContext.ReadOnlyRootFilesystem)
		require.False(t, *container.SecurityContext.AllowPrivilegeEscalation)
		require.Equal(t, []corev1.Capability{"ALL"}, container.SecurityContext.Capabilities.Drop)
		require.Empty(t, container.Env)
		require.Empty(t, container.VolumeMounts)
		require.Empty(t, container.Ports)
		require.NotEmpty(t, container.Resources.Requests)
		require.NotEmpty(t, container.Resources.Limits)
	}
	// Returned policy objects and source objects must not alias each other.
	pod.NodeSelector["pool"] = "changed"
	pod.ImagePullSecrets[0].Name = "changed"
	*pod.RuntimeClassName = "changed"
	pod.Tolerations[0].Key = "changed"
	pod.InitContainers[0].SecurityContext.Capabilities.Drop[0] = "changed"
	require.Equal(t, []corev1.Capability{"ALL"}, pod.Containers[0].SecurityContext.Capabilities.Drop)
	require.Equal(t, original, r.Source)
}

func TestBuildJobRejectsUnsafeOrIncompleteEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*JobRequest){
		"missing source":           func(r *JobRequest) { r.Source = nil },
		"missing node":             func(r *JobRequest) { r.Node = nil },
		"missing source uid":       func(r *JobRequest) { r.Source.UID = "" },
		"missing resource version": func(r *JobRequest) { r.Source.ResourceVersion = "" },
		"missing node uid":         func(r *JobRequest) { r.Node.UID = "" },
		"bad namespace":            func(r *JobRequest) { r.Source.Namespace = "../another" },
		"bad job name":             func(r *JobRequest) { r.Name = strings.Repeat("a", 64) },
		"mutable image":            func(r *JobRequest) { r.Image = "ghcr.io/fivetime/kubebrain:dbaas" },
		"uppercase digest":         func(r *JobRequest) { r.Image = strings.ReplaceAll(r.Image, "a", "A") },
		"image control":            func(r *JobRequest) { r.Image = "registry\x00/image@sha256:" + strings.Repeat("a", 64) },
		"short hold":               func(r *JobRequest) { r.HoldSeconds = 59 },
		"unbounded hold":           func(r *JobRequest) { r.HoldSeconds = 86401 },
		"missing ttl":              func(r *JobRequest) { r.TTLSeconds = 0 },
		"unbounded ttl":            func(r *JobRequest) { r.TTLSeconds = 3601 },
		"cordoned node":            func(r *JobRequest) { r.Node.Spec.Unschedulable = true },
		"deleting node":            func(r *JobRequest) { r.Node.DeletionTimestamp = ptr.To(metav1.Now()) },
		"node not ready":           func(r *JobRequest) { r.Node.Status.Conditions[0].Status = corev1.ConditionFalse },
		"disk pressure":            func(r *JobRequest) { r.Node.Status.Conditions[1].Status = corev1.ConditionTrue },
		"unknown disk":             func(r *JobRequest) { r.Node.Status.Conditions[1].Status = corev1.ConditionUnknown },
		"missing health":           func(r *JobRequest) { r.Node.Status.Conditions = nil },
		"duplicate health": func(r *JobRequest) {
			r.Node.Status.Conditions = append(r.Node.Status.Conditions, r.Node.Status.Conditions[0])
		},
		"windows":                    func(r *JobRequest) { r.Node.Status.NodeInfo.OperatingSystem = "windows" },
		"unknown architecture":       func(r *JobRequest) { r.Node.Status.NodeInfo.Architecture = "riscv64" },
		"architecture label drift":   func(r *JobRequest) { r.Node.Labels[corev1.LabelArchStable] = "arm64" },
		"node selector mismatch":     func(r *JobRequest) { r.Node.Labels["pool"] = "another" },
		"source fixed to other node": func(r *JobRequest) { r.Source.Spec.Template.Spec.NodeName = "worker-1" },
		"missing services":           func(r *JobRequest) { r.Services = nil },
		"missing peer service":       func(r *JobRequest) { r.Services = r.Services[:1] },
		"missing service uid":        func(r *JobRequest) { r.Services[0].UID = "" },
		"cross namespace service":    func(r *JobRequest) { r.Services[0].Namespace = "another" },
		"duplicate service":          func(r *JobRequest) { r.Services = append(r.Services, r.Services[0]) },
		"business service selects warm pod": func(r *JobRequest) {
			r.Services[0].Spec.Selector = map[string]string{"kubebrain.io/role": "image-prepull"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := requestFixture()
			mutate(&r)
			job, err := BuildJob(r)
			require.Error(t, err)
			require.Nil(t, job)
		})
	}
}

func TestBuildJobPreservesRequiredAffinityWithoutWideningEmptyTerms(t *testing.T) {
	r := requestFixture()
	terms := []corev1.NodeSelectorTerm{
		{},
		{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}},
		{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}}},
	}
	r.Source.Spec.Template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms},
	}
	original := r.Source.DeepCopy()
	job, err := BuildJob(r)
	require.NoError(t, err)
	require.Equal(t, original, r.Source)
	actual := job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	require.Equal(t, corev1.NodeSelectorTerm{}, actual[0], "a previously empty alternative must not become a match-all-node-labels pin")
	for _, zone := range []string{"a", "b", "c"} {
		for _, name := range []string{"worker-0", "worker-1"} {
			node := r.Node.DeepCopy()
			node.Name, node.Labels["zone"] = name, zone
			matched := false
			for _, term := range actual {
				ok, err := nodeMatchesTerm(node, term)
				require.NoError(t, err)
				matched = matched || ok
			}
			require.Equal(t, name == "worker-0" && zone != "c", matched, "name=%s zone=%s", name, zone)
		}
	}
}

func TestBuildJobRequiredNodeAffinityOperators(t *testing.T) {
	for _, tc := range []struct {
		name string
		term corev1.NodeSelectorTerm
		want bool
	}{
		{name: "in", term: affinityTerm("pool", corev1.NodeSelectorOpIn, "workers"), want: true},
		{name: "not in", term: affinityTerm("pool", corev1.NodeSelectorOpNotIn, "other"), want: true},
		{name: "exists", term: affinityTerm("zone", corev1.NodeSelectorOpExists), want: true},
		{name: "does not exist", term: affinityTerm("absent", corev1.NodeSelectorOpDoesNotExist), want: true},
		{name: "greater than exact int64", term: affinityTerm("rank", corev1.NodeSelectorOpGt, "9007199254740992"), want: true},
		{name: "less than exact int64", term: affinityTerm("rank", corev1.NodeSelectorOpLt, "9007199254740994"), want: true},
		{name: "greater than equal", term: affinityTerm("rank", corev1.NodeSelectorOpGt, "9007199254740993")},
		{name: "numeric overflow", term: affinityTerm("rank", corev1.NodeSelectorOpGt, "9223372036854775808")},
		{name: "empty term"},
		{name: "label mismatch", term: affinityTerm("zone", corev1.NodeSelectorOpIn, "other")},
		{name: "unknown operator", term: affinityTerm("pool", corev1.NodeSelectorOperator("Bogus"), "workers")},
		{name: "invalid value count", term: affinityTerm("pool", corev1.NodeSelectorOpExists, "workers")},
		{name: "name in", term: corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"worker-0"}}}}, want: true},
		{name: "name not in", term: corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"worker-1"}}}}, want: true},
		{name: "name mismatch", term: corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"worker-1"}}}}},
		{name: "unsupported field", term: corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.uid", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-uid"}}}}},
		{name: "empty name values", term: corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn}}}},
		{name: "multiple name values", term: corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"worker-0", "worker-1"}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := requestFixture()
			r.Node.Labels["rank"] = "9007199254740993"
			r.Source.Spec.Template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{tc.term}},
			}
			job, err := BuildJob(r)
			if tc.want {
				require.NoError(t, err)
				require.NotNil(t, job)
			} else {
				require.Error(t, err)
				require.Nil(t, job)
			}
		})
	}
}

func affinityTerm(key string, operator corev1.NodeSelectorOperator, values ...string) corev1.NodeSelectorTerm {
	return corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: operator, Values: values}}}
}

func TestBuildJobPlatformBoundsAndDeletionEvidence(t *testing.T) {
	for _, architecture := range []string{"amd64", "arm64"} {
		for _, hold := range []int64{60, 86400} {
			r := requestFixture()
			r.Node.Status.NodeInfo.Architecture, r.Node.Labels[corev1.LabelArchStable] = architecture, architecture
			r.HoldSeconds, r.TTLSeconds = hold, 3600
			r.Source.Spec.Template.Spec.NodeName = r.Node.Name
			job, err := BuildJob(r)
			require.NoError(t, err)
			require.Equal(t, hold, *job.Spec.ActiveDeadlineSeconds)
			require.Equal(t, corev1.Linux, job.Spec.Template.Spec.OS.Name)
		}
	}
	for name, mutate := range map[string]func(*JobRequest){
		"deleting source":         func(r *JobRequest) { r.Source.DeletionTimestamp = ptr.To(metav1.Now()) },
		"deleting client service": func(r *JobRequest) { r.Services[0].DeletionTimestamp = ptr.To(metav1.Now()) },
		"deleting peer service":   func(r *JobRequest) { r.Services[1].DeletionTimestamp = ptr.To(metav1.Now()) },
		"empty required terms": func(r *JobRequest) {
			r.Source.Spec.Template.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{}}
		},
		"missing service names": func(r *JobRequest) { r.ClientServiceName = "" },
		"bad service name":      func(r *JobRequest) { r.Services[0].Name = "../client" },
	} {
		t.Run(name, func(t *testing.T) {
			r := requestFixture()
			mutate(&r)
			_, err := BuildJob(r)
			require.Error(t, err)
		})
	}
}
