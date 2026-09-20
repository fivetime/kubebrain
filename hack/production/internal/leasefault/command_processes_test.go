package leasefault

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

func TestCommandProcessAdmission(t *testing.T) {
	for _, mode := range []string{"bound", "metric-uid", "observer-original", "contradictory-snapshot", "missing-admission", "missing-jq", "missing-predicate"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			pod := &unstructured.Unstructured{}
			require.NoError(t, pod.UnmarshalJSON([]byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test-ns","uid":"pod-uid"},"spec":{"nodeName":"worker1","containers":[{"name":"brain","image":"pinned"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","containerID":"containerd://one","imageID":"sha256:one","restartCount":0,"state":{"running":{"startedAt":"2026-09-20T00:00:00Z"}}}]}}`)))
			pod.SetResourceVersion("1")
			pod.Object["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "brain", "uid": "sts-uid", "controller": true}}
			var err error
			p.Bindings.Network.PodBefore, err = pod.MarshalJSON()
			require.NoError(t, err)
			observer := pod.DeepCopy()
			observer.SetName("brain-1")
			observer.SetUID("observer-uid")
			observerRaw, err := observer.MarshalJSON()
			require.NoError(t, err)
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), pod, observer)
			calls := 0
			admit := func(context.Context) error { calls++; return nil }
			r := MeasuredNetworkFaultRuntime{Network: NetworkFaultRuntime{Lifecycle: FaultLifecycle{Preparation: FaultPreparation{Client: client, StatefulSetName: "brain"}, OutcomeAdmit: admit}, AdmitSuccessor: admit}, AdmitMetrics: admit}
			h := ObservationHooks{AdmitOriginal: admit, AdmitStack: func(context.Context, string) error { calls++; return nil }}
			predicate, err := filepath.Abs("../../same-pod-process.jq")
			require.NoError(t, err)
			inputs := CommandProcessInputs{JQ: "/usr/bin/jq", Predicate: predicate, Observer: observerRaw, Metrics: []json.RawMessage{append([]byte(nil), p.Bindings.Network.PodBefore...)}}
			targets := metricTargets(t, p)
			switch mode {
			case "metric-uid":
				targets[0].PodUID = "other"
			case "observer-original":
				inputs.Observer = p.Bindings.Network.PodBefore
			case "contradictory-snapshot":
				changed := pod.DeepCopy()
				require.NoError(t, unstructured.SetNestedField(changed.Object, "other", "spec", "nodeName"))
				inputs.Metrics[0], err = changed.MarshalJSON()
				require.NoError(t, err)
			case "missing-admission":
				r.AdmitMetrics = nil
			case "missing-jq":
				inputs.JQ = "/missing/jq"
			case "missing-predicate":
				inputs.Predicate = "/missing/predicate"
			}
			bound, hooks, err := p.BindProcessAdmission(r, h, targets, inputs)
			require.Zero(t, calls)
			require.Empty(t, client.Actions())
			if mode != "bound" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			// Subsequent caller mutation must not change the captured snapshots.
			inputs.Observer[0], inputs.Metrics[0][0], p.Bindings.Network.PodBefore[0] = '!', '!', '!'
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, hooks.AdmitOriginal(ctx))
			require.NoError(t, hooks.AdmitStack(ctx, "before"))
			require.NoError(t, hooks.AdmitStack(ctx, "after"))
			require.NoError(t, bound.AdmitMetrics(ctx))
			require.NoError(t, bound.Network.AdmitSuccessor(ctx))
			require.NoError(t, bound.Network.Lifecycle.OutcomeAdmit(ctx))
			require.Equal(t, 12, calls, "preserve each base admission before and after GET/comparison")
			require.Len(t, client.Actions(), 6)
			files, err := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-process-*.json"))
			require.NoError(t, err)
			require.Len(t, files, 6)
			require.Nil(t, hooks.RetainOriginal, "ClaimAndRun still installs its own evidence callbacks")
		})
	}
}
