package leasefault

import (
	"errors"
	"net"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	strictjson "sigs.k8s.io/json"
)

// CheckCommandEndpoints binds the actual lazy transports to the admitted Pod
// snapshots and the original probe argv. This is a local consistency check;
// live process and authenticated member checks remain mandatory before claim.
func (p ObservationCommandPlan) CheckCommandEndpoints(r MeasuredNetworkFaultRuntime, inputs CommandProcessInputs) error {
	type targeted interface{ Target() string }
	original, ok := r.Network.Lifecycle.Preparation.Connection.(targeted)
	if !ok {
		return errors.New("original transport must expose its actual target")
	}
	observer, ok := r.Network.SuccessorConnection.(targeted)
	if !ok {
		return errors.New("observer transport must expose its actual target")
	}
	decode := func(raw []byte) (corev1.Pod, error) {
		var pod corev1.Pod
		if len(raw) == 0 || len(raw) > 256<<10 {
			return pod, errors.New("invalid endpoint snapshot size")
		}
		strict, err := strictjson.UnmarshalStrict(raw, &pod, strictjson.DisallowDuplicateFields)
		if err != nil || len(strict) != 0 || pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Namespace != p.Bindings.Network.Namespace || pod.Name == "" || pod.UID == "" || pod.Spec.HostNetwork || net.ParseIP(pod.Status.PodIP) == nil {
			return pod, errors.New("invalid endpoint Pod snapshot")
		}
		return pod, nil
	}
	a, err := decode(p.Bindings.Network.PodBefore)
	if err != nil {
		return err
	}
	b, err := decode(inputs.Observer)
	if err != nil {
		return err
	}
	if a.Name != p.Bindings.Network.PodName || string(a.UID) != p.Bindings.Network.PodUID || a.Name == b.Name || a.UID == b.UID || net.ParseIP(a.Status.PodIP).Equal(net.ParseIP(b.Status.PodIP)) {
		return errors.New("endpoint Pods must match original binding and be independent")
	}
	check := func(target, ip string) error {
		const prefix = "passthrough:///"
		if len(target) <= len(prefix) || target[:len(prefix)] != prefix {
			return errors.New("command transport must use literal passthrough target")
		}
		host, port, err := net.SplitHostPort(target[len(prefix):])
		n, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || n < 1 || n > 65535 || port != strconv.Itoa(n) || net.ParseIP(host) == nil || !net.ParseIP(host).Equal(net.ParseIP(ip)) {
			return errors.New("transport target differs from admitted Pod IP or has invalid port")
		}
		return nil
	}
	if original.Target() != "passthrough:///"+p.Endpoint {
		return errors.New("original transport differs from probe endpoint")
	}
	return errors.Join(check(original.Target(), a.Status.PodIP), check(observer.Target(), b.Status.PodIP))
}
