package etcd

import (
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// These paired observations cover only local Put requests that reach the
// backend call. Pre-backend includes admission, leadership, auth, quota and
// lease-lock acquisition. Backend includes shim reads/CAS and any leased
// atomic write; it is NOT pure TiKV commit latency. Early rejects and follower
// proxy calls emit neither sample. Both samples carry the backend outcome,
// allowing comparisons over the same population without logging request data.
func emitPutBackendPhaseDurations(metricCli metrics.Metrics, before, backend time.Duration, err error) {
	if metricCli == nil {
		return
	}
	tags := []metrics.T{metrics.Tag("method", "put"), getSuccessMetricTagByErr(err)}
	_ = metricCli.EmitHistogram("write.pre_backend.latency", before.Seconds(), tags...)
	_ = metricCli.EmitHistogram("write.backend.latency", backend.Seconds(), tags...)
}
