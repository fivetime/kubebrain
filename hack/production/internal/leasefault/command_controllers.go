package leasefault

import (
	"encoding/json"
	"errors"

	corev1 "k8s.io/api/core/v1"
	strictjson "sigs.k8s.io/json"
)

// CheckCommandControllers binds every admitted Pod snapshot to the intended
// StatefulSet. Live process gates subsequently require ownerReferences to stay
// unchanged. This static check neither authenticates snapshots nor acquires a
// fault claim, and is not a substitute for the live StatefulSet UID/spec check.
func (p ObservationCommandPlan) CheckCommandControllers(inputs CommandProcessInputs, name string) error {
	if err := p.Bindings.Validate(); err != nil {
		return err
	}
	if !recoveryIdentity.MatchString(name) || len(inputs.Metrics) == 0 || len(inputs.Metrics) > 16 {
		return errors.New("invalid command controller scope")
	}
	snapshots := append([]json.RawMessage{p.Bindings.Network.PodBefore, inputs.Observer}, inputs.Metrics...)
	for _, raw := range snapshots {
		var pod corev1.Pod
		if len(raw) == 0 || len(raw) > 256<<10 {
			return errors.New("invalid controller snapshot size")
		}
		strict, err := strictjson.UnmarshalStrict(raw, &pod, strictjson.DisallowDuplicateFields)
		if err != nil || len(strict) != 0 || pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Namespace != p.Bindings.Network.Namespace || pod.Name == "" || pod.UID == "" || pod.DeletionTimestamp != nil {
			return errors.New("invalid command controller snapshot")
		}
		controllers := 0
		for _, ref := range pod.OwnerReferences {
			if ref.Controller == nil || !*ref.Controller {
				continue
			}
			controllers++
			if ref.APIVersion != "apps/v1" || ref.Kind != "StatefulSet" || ref.Name != name || string(ref.UID) != p.Bindings.Network.StatefulSetUID {
				return errors.New("Pod controller differs from admitted StatefulSet")
			}
		}
		if controllers != 1 {
			return errors.New("command Pod requires exactly one admitted controller")
		}
	}
	return nil
}
