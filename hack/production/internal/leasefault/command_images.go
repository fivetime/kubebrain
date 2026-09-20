package leasefault

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/kubewharf/kubebrain/hack/production/internal/imageprepull"
	corev1 "k8s.io/api/core/v1"
	strictjson "sigs.k8s.io/json"
)

// CommandRelease contains independently authenticated release inputs. Platforms
// maps Pod UIDs to platforms established from their admitted Nodes. AdmitTools
// must authenticate this mapping and CI source binding; arbitrary JSON is not
// CI approval. Container identifies the KubeBrain container in every snapshot.
type CommandRelease struct {
	Index     []byte            `json:"index"`
	Reviewed  map[string]string `json:"reviewed"`
	Platforms map[string]string `json:"platforms"`
	NodeUIDs  map[string]string `json:"node_uids"` // independently admitted node name -> UID
	Container string            `json:"container"`
}

// CheckProcessImages binds command snapshots to an immutable release and its
// platform children. Live process checks subsequently reject image changes.
// This offline check does not prove source, TLS, live state or successful CI.
func (p ObservationCommandPlan) CheckProcessImages(inputs CommandProcessInputs, release CommandRelease) error {
	if release.Container == "" || len(release.Container) > 63 || len(release.Platforms) == 0 || len(release.Platforms) > 18 || len(inputs.Metrics) == 0 || len(inputs.Metrics) > 16 || len(inputs.Metrics) != len(p.Bindings.Metrics) || !experimentImage.MatchString(p.Bindings.Image) {
		return errors.New("incomplete command image bindings")
	}
	approved, err := imageprepull.ApprovedRuntimeDigests(p.Bindings.Image, release.Index, release.Reviewed)
	if err != nil {
		return err
	}
	snapshots := append([]json.RawMessage{p.Bindings.Network.PodBefore, inputs.Observer}, inputs.Metrics...)
	seen := map[string]bool{}
	nodes := map[string]string{}
	repository, _, _ := strings.Cut(p.Bindings.Image, "@")
	for _, raw := range snapshots {
		var pod corev1.Pod
		if len(raw) == 0 || len(raw) > 256<<10 {
			return errors.New("invalid image snapshot size")
		}
		strict, err := strictjson.UnmarshalStrict(raw, &pod, strictjson.DisallowDuplicateFields)
		if err != nil || len(strict) != 0 || pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.UID == "" || pod.Spec.NodeName == "" {
			return errors.New("invalid image snapshot")
		}
		uid := string(pod.UID)
		digests := approved[release.Platforms[uid]]
		if len(digests) == 0 {
			return errors.New("missing admitted Pod platform")
		}
		seen[uid] = true
		if release.NodeUIDs[pod.Spec.NodeName] == "" {
			return errors.New("missing admitted Node UID")
		}
		platform := release.Platforms[uid]
		if previous, ok := nodes[pod.Spec.NodeName]; ok && previous != platform {
			return errors.New("contradictory platforms for one Node")
		}
		nodes[pod.Spec.NodeName] = platform
		containers, statuses := 0, 0
		for _, c := range pod.Spec.Containers {
			if c.Name == release.Container {
				containers++
				if c.Image != p.Bindings.Image {
					return errors.New("Pod image differs from admitted release")
				}
			}
		}
		for _, c := range pod.Status.ContainerStatuses {
			if c.Name != release.Container {
				continue
			}
			statuses++
			matched := false
			for _, digest := range digests {
				if c.ImageID == digest || c.ImageID == repository+"@"+digest || c.ImageID == "containerd://"+digest || c.ImageID == "cri-o://"+digest || c.ImageID == "docker://"+digest {
					matched = true
				}
			}
			if !matched || c.ContainerID == "" || c.State.Running == nil || c.State.Running.StartedAt.IsZero() || c.State.Waiting != nil || c.State.Terminated != nil {
				return errors.New("runtime image or running process differs from admission")
			}
		}
		if containers != 1 || statuses != 1 {
			return errors.New("missing or ambiguous image container")
		}
	}
	if len(seen) != len(release.Platforms) {
		return errors.New("platform map contains unrelated Pods")
	}
	if len(nodes) != len(release.NodeUIDs) {
		return errors.New("Node map contains unrelated Nodes")
	}
	return nil
}
