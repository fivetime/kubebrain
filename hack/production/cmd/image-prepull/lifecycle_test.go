package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
)

type failedOutput struct{}

func (failedOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// Exercises native HTTPS and Kubernetes request serialization across separate
// CLI invocations. This finite fixture simulates admission/Job execution/GC;
// it does not claim to be a real apiserver or a real image pull.
func TestCommandPrepareVerifyAndCleanupLifecycle(t *testing.T) {
	for _, mode := range []string{"normal", "output failure", "unconfirmed"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			require.NoError(t, os.Chmod(directory, 0700))
			amd64, arm64 := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
			index := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":10,"platform":{"os":"linux","architecture":"amd64"}},{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":20,"platform":{"os":"linux","architecture":"arm64"}}]}`, amd64, arm64))
			image := fmt.Sprintf("ghcr.io/fivetime/kubebrain@sha256:%x", sha256.Sum256(index))
			indexFile := filepath.Join(directory, "index")
			require.NoError(t, os.WriteFile(indexFile, index, 0600))
			namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kubebrain-test", UID: "namespace-uid"}}
			priority := &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "source-priority", UID: "priority-uid", ResourceVersion: "1"}, Value: 1000000, PreemptionPolicy: ptr.To(corev1.PreemptNever)}
			source := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "kubebrain", Namespace: namespace.Name, UID: "source-uid", ResourceVersion: "1", Generation: 1},
				Spec:   appsv1.StatefulSetSpec{Replicas: ptr.To(int32(1)), ServiceName: "peer", Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "business"}}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"pool": "workers"}, Containers: []corev1.Container{{Name: "kubebrain", Image: "old:image", Env: []corev1.EnvVar{{Name: "PRIVATE", Value: "fixture-business-private"}}}}}}},
				Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 1, CurrentRevision: "revision", UpdateRevision: "revision"}}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", UID: "node-uid", ResourceVersion: "1", Labels: map[string]string{"pool": "workers", corev1.LabelOSStable: "linux", corev1.LabelArchStable: "amd64"}},
				Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{OperatingSystem: "linux", Architecture: "amd64", ContainerRuntimeVersion: "cri-o://1.35.3"}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}, {Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse}}}}
			services := &corev1.ServiceList{}
			source.Spec.Template.Spec.PriorityClassName = priority.Name
			for _, name := range []string{"client", "peer"} {
				services.Items = append(services.Items, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace.Name, UID: types.UID(name + "-uid")}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "business"}}})
			}
			var mu sync.Mutex
			var job *batchv1.Job
			var pod *corev1.Pod
			creates, deletes, unexpected, mutations := 0, 0, 0, 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Method != http.MethodGet {
					mutations++
				}
				w.Header().Set("Content-Type", "application/json")
				write := func(value any) {
					if err := json.NewEncoder(w).Encode(value); err != nil {
						t.Errorf("encode fixture response: %v", err)
					}
				}
				fail := func(code int, reason metav1.StatusReason) {
					w.WriteHeader(code)
					write(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: reason, Code: int32(code)})
				}
				path := r.URL.Path
				jobsPath := "/apis/batch/v1/namespaces/" + namespace.Name + "/jobs"
				switch {
				case r.Method == "GET" && path == "/api/v1/namespaces/"+namespace.Name:
					write(namespace)
				case r.Method == "GET" && path == "/apis/apps/v1/namespaces/"+namespace.Name+"/statefulsets/kubebrain":
					write(source)
				case r.Method == "GET" && path == "/api/v1/nodes":
					write(&corev1.NodeList{Items: []corev1.Node{*node}})
				case r.Method == "GET" && path == "/apis/scheduling.k8s.io/v1/priorityclasses/source-priority":
					write(priority)
				case r.Method == "GET" && path == "/api/v1/nodes/worker-0":
					write(node)
				case r.Method == "GET" && path == "/api/v1/namespaces/"+namespace.Name+"/services":
					write(services)
				case r.Method == "GET" && path == "/api/v1/namespaces/"+namespace.Name+"/pods":
					list := &corev1.PodList{}
					if pod != nil && r.URL.Query().Get("labelSelector") == "batch.kubernetes.io/controller-uid="+string(job.UID) {
						list.Items = append(list.Items, *pod.DeepCopy())
					}
					write(list)
				case r.Method == "GET" && strings.HasPrefix(path, jobsPath+"/"):
					if job == nil || path != jobsPath+"/"+job.Name {
						fail(404, metav1.StatusReasonNotFound)
					} else {
						write(job)
					}
				case r.Method == "POST" && path == jobsPath:
					body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
					created := &batchv1.Job{}
					if err != nil || runtime.DecodeInto(scheme.Codecs.UniversalDeserializer(), body, created) != nil || job != nil {
						fail(400, metav1.StatusReasonBadRequest)
						return
					}
					creates++
					created.UID, created.ResourceVersion = types.UID("job-"+created.Name), "1"
					created.Spec.Template.Labels["batch.kubernetes.io/controller-uid"] = string(created.UID)
					created.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"batch.kubernetes.io/controller-uid": string(created.UID)}}
					created.Status.StartTime, created.Status.Active = ptr.To(metav1.Now()), 1
					job = created
					pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-pod", Namespace: namespace.Name, UID: types.UID("pod-" + job.Name), Labels: job.Spec.Template.DeepCopy().Labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: ptr.To(true)}}}, Spec: *job.Spec.Template.Spec.DeepCopy(),
						Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "verify-image", ImageID: amd64, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "hold-image", ImageID: amd64, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
					pod.Spec.NodeName = node.Name
					w.WriteHeader(201)
					write(job)
				case r.Method == "DELETE" && job != nil && path == jobsPath+"/"+job.Name:
					body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
					options := &metav1.DeleteOptions{}
					if err != nil || runtime.DecodeInto(scheme.Codecs.UniversalDeserializer(), body, options) != nil || options.Preconditions == nil || ptr.Deref(options.Preconditions.UID, types.UID("")) != job.UID || ptr.Deref(options.Preconditions.ResourceVersion, "") != job.ResourceVersion || ptr.Deref(options.PropagationPolicy, metav1.DeletionPropagation("")) != metav1.DeletePropagationForeground {
						fail(409, metav1.StatusReasonConflict)
						return
					}
					deletes++
					job, pod = nil, nil
					write(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess, Code: 200})
				default:
					unexpected++
					fail(400, metav1.StatusReasonBadRequest)
				}
			}))
			defer server.Close()
			config := clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"cluster": {Server: server.URL, CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"identity": {}}, Contexts: map[string]*clientcmdapi.Context{"explicit": {Cluster: "cluster", AuthInfo: "identity"}}}
			configBytes, err := clientcmd.Write(config)
			require.NoError(t, err)
			configFile := filepath.Join(directory, "kubeconfig")
			require.NoError(t, os.WriteFile(configFile, configBytes, 0600))
			base := []string{"--receipt-directory=" + directory, "--receipt-name=attempt", "--namespace=kubebrain-test", "--namespace-uid=namespace-uid", "--statefulset=kubebrain", "--statefulset-uid=source-uid", "--kubeconfig=" + configFile, "--context=explicit", "--index-file=" + indexFile, "--image=" + image, "--amd64-digest=" + amd64, "--arm64-digest=" + arm64, "--client-service=client", "--hold-seconds=300", "--minimum-remaining-hold=1m", "--prepare-timeout=10s", "--cleanup-timeout=3s", "--verify-timeout=3s"}
			args := append([]string{"--mode=prepare"}, base...)
			if mode != "unconfirmed" {
				args = append(args, "--confirm-create-isolated-jobs")
			}
			var output bytes.Buffer
			var destination io.Writer = &output
			if mode == "output failure" {
				destination = failedOutput{}
			}
			err = run(context.Background(), args, destination)
			if mode == "normal" {
				require.NoError(t, err)
				require.Equal(t, "PREPULL_READY\n", output.String())
				proof, err := os.ReadFile(filepath.Join(directory, "attempt"))
				require.NoError(t, err)
				require.Contains(t, string(proof), `"preparation":`)
				require.NotContains(t, string(proof), "fixture-business-private")
				output.Reset()
				require.NoError(t, run(context.Background(), append([]string{"--mode=verify"}, base...), &output))
				require.Equal(t, "PREPULL_VERIFIED\n", output.String())
				mu.Lock()
				pod.UID = "replacement-pod"
				mu.Unlock()
				output.Reset()
				require.Error(t, run(context.Background(), append([]string{"--mode=verify"}, base...), &output))
				require.Empty(t, output.String())
				output.Reset()
				require.NoError(t, run(context.Background(), append([]string{"--mode=recover-cleanup"}, base...), &output))
				require.Equal(t, "PREPULL_CLEANUP_CONFIRMED\n", output.String())
				output.Reset()
				require.Error(t, run(context.Background(), append([]string{"--mode=verify"}, base...), &output))
				require.Empty(t, output.String())
			} else if mode == "output failure" {
				require.True(t, errors.Is(err, io.ErrClosedPipe), err)
			} else {
				require.ErrorContains(t, err, "confirm-create")
			}
			mu.Lock()
			defer mu.Unlock()
			require.Nil(t, job)
			require.Nil(t, pod)
			require.Zero(t, unexpected)
			if mode == "unconfirmed" {
				require.Zero(t, mutations)
				require.Zero(t, creates)
				require.Zero(t, deletes)
			} else {
				require.Equal(t, 2, mutations, "only the initial CREATE and final DELETE may mutate API state")
				require.Equal(t, 1, creates)
				require.Equal(t, 1, deletes)
			}
		})
	}
}
