package leasefault

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestCommandControllerScope(t *testing.T) {
	for _, role := range []string{"original", "observer", "metrics"} {
		for _, mode := range []string{"valid", "uid", "name", "kind", "api", "missing", "multiple", "not-controller", "namespace", "non-controller-owner"} {
			t.Run(role+"/"+mode, func(t *testing.T) {
				p := serializedCommandFixture(t)
				raw := &p.Bindings.Network.PodBefore
				if role == "observer" {
					raw = &p.Processes.Observer
				} else if role == "metrics" {
					raw = &p.Processes.Metrics[0]
				}
				var pod corev1.Pod
				require.NoError(t, json.Unmarshal(*raw, &pod))
				switch mode {
				case "uid":
					pod.OwnerReferences[0].UID = "foreign"
				case "name":
					pod.OwnerReferences[0].Name = "foreign"
				case "kind":
					pod.OwnerReferences[0].Kind = "ReplicaSet"
				case "api":
					pod.OwnerReferences[0].APIVersion = "example/v1"
				case "missing":
					pod.OwnerReferences = nil
				case "multiple":
					pod.OwnerReferences = append(pod.OwnerReferences, pod.OwnerReferences[0])
				case "not-controller":
					pod.OwnerReferences[0].Controller = nil
				case "namespace":
					pod.Namespace = "foreign"
				case "non-controller-owner":
					ref := pod.OwnerReferences[0]
					ref.Name, ref.UID, ref.Controller = "other", "other", nil
					pod.OwnerReferences = append(pod.OwnerReferences, ref)
				}
				data, err := json.Marshal(pod)
				require.NoError(t, err)
				*raw = data
				err = p.CheckLocal()
				if mode == "valid" || mode == "non-controller-owner" {
					require.NoError(t, err)
				} else {
					require.Error(t, err, "serialized command must reject the wrong initial controller")
				}
			})
		}
	}
}
