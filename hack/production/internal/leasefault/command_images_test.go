package leasefault

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func commandReleaseFixture(t *testing.T) (string, CommandRelease) {
	t.Helper()
	r := CommandRelease{Container: "brain", Reviewed: map[string]string{"linux/amd64": "sha256:" + strings.Repeat("a", 64), "linux/arm64": "sha256:" + strings.Repeat("b", 64)}, Platforms: map[string]string{"pod-uid": "linux/amd64", "observer-uid": "linux/amd64"}}
	r.Index = []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":123,"platform":{"os":"linux","architecture":"amd64"}},{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":%q,"size":123,"platform":{"os":"linux","architecture":"arm64"}}]}`, r.Reviewed["linux/amd64"], r.Reviewed["linux/arm64"]))
	return "ghcr.io/fivetime/kubebrain@sha256:" + planinput.SHA256(r.Index), r
}

func TestCommandProcessImages(t *testing.T) {
	for _, mode := range []string{"valid", "index-id", "wrong-architecture", "tag", "wrong-index", "unknown-platform", "extra-pod", "missing-container", "duplicate-status", "not-running", "metric-image", "duplicate-json"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			image, release := commandReleaseFixture(t)
			p.Bindings.Image = image
			pod := &unstructured.Unstructured{}
			require.NoError(t, pod.UnmarshalJSON([]byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test-ns","uid":"pod-uid"},"spec":{"nodeName":"worker1","containers":[{"name":"brain","image":"placeholder"}]},"status":{"containerStatuses":[{"name":"brain","containerID":"containerd://one","imageID":"placeholder","state":{"running":{"startedAt":"2026-09-20T00:00:00Z"}}}]}}`)))
			containers := pod.Object["spec"].(map[string]any)["containers"].([]any)
			statuses := pod.Object["status"].(map[string]any)["containerStatuses"].([]any)
			containers[0].(map[string]any)["image"] = image
			status := statuses[0].(map[string]any)
			status["imageID"] = release.Reviewed["linux/amd64"]
			switch mode {
			case "index-id":
				status["imageID"] = "cri-o://" + strings.Split(image, "@")[1]
			case "wrong-architecture":
				status["imageID"] = release.Reviewed["linux/arm64"]
			case "tag":
				containers[0].(map[string]any)["image"] = "ghcr.io/fivetime/kubebrain:dbaas"
			case "wrong-index":
				release.Index = append(release.Index, '\n')
			case "unknown-platform":
				release.Platforms["pod-uid"] = "linux/riscv64"
			case "extra-pod":
				release.Platforms["extra"] = "linux/amd64"
			case "missing-container":
				release.Container = "other"
			case "duplicate-status":
				pod.Object["status"].(map[string]any)["containerStatuses"] = append(statuses, status)
			case "not-running":
				status["state"] = map[string]any{"waiting": map[string]any{}}
			}
			var err error
			p.Bindings.Network.PodBefore, err = pod.MarshalJSON()
			require.NoError(t, err)
			pod.SetUID("observer-uid")
			observer, err := pod.MarshalJSON()
			require.NoError(t, err)
			metric := append([]byte(nil), p.Bindings.Network.PodBefore...)
			if mode == "metric-image" {
				metric = []byte(strings.Replace(string(metric), release.Reviewed["linux/amd64"], release.Reviewed["linux/arm64"], 1))
			}
			if mode == "duplicate-json" {
				observer = []byte(strings.Replace(string(observer), `"kind":"Pod"`, `"kind":"Pod","kind":"Pod"`, 1))
			}
			err = p.CheckProcessImages(CommandProcessInputs{Observer: observer, Metrics: []json.RawMessage{metric}}, release)
			if mode == "valid" || mode == "index-id" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
