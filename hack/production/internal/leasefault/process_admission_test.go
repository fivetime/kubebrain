package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

func TestLiveProcessAdmissionCanonicalPredicate(t *testing.T) {
	for _, mode := range []string{"same", "readiness", "restart", "image", "spec", "uid", "api-error", "retention-error", "post-admission"} {
		t.Run(mode, func(t *testing.T) {
			expected := []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test","uid":"pod-uid"},"spec":{"nodeName":"worker1","containers":[{"name":"brain","image":"pinned"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","containerID":"containerd://one","imageID":"sha256:one","restartCount":0,"ready":true,"state":{"running":{"startedAt":"2026-09-20T00:00:00Z"}}}]}}`)
			current := &unstructured.Unstructured{}
			require.NoError(t, current.UnmarshalJSON(expected))
			statuses, _, err := unstructured.NestedSlice(current.Object, "status", "containerStatuses")
			require.NoError(t, err)
			status := statuses[0].(map[string]any)
			switch mode {
			case "readiness":
				status["ready"] = false
			case "restart":
				status["restartCount"] = int64(1)
			case "image":
				status["imageID"] = "sha256:other"
			case "spec":
				require.NoError(t, unstructured.SetNestedField(current.Object, "worker2", "spec", "nodeName"))
			case "uid":
				current.SetUID("other")
			}
			require.NoError(t, unstructured.SetNestedSlice(current.Object, statuses, "status", "containerStatuses"))
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), current)
			if mode == "api-error" {
				client = fake.NewSimpleDynamicClient(runtime.NewScheme())
			}
			predicate, err := filepath.Abs("../../same-pod-process.jq")
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			admissions, retained := 0, 0
			err = CheckLivePodProcess(ctx, client, expected, "/usr/bin/jq", predicate, func(context.Context) error {
				admissions++
				if mode == "post-admission" && admissions == 2 {
					return errors.New("source changed")
				}
				return nil
			}, func(data []byte, observed error) error {
				retained++
				var record struct {
					Input  json.RawMessage
					Output []byte
				}
				require.NoError(t, json.Unmarshal(data, &record))
				require.NotEmpty(t, record.Input)
				if mode == "same" || mode == "readiness" || mode == "retention-error" || mode == "post-admission" {
					require.NoError(t, observed)
					require.Equal(t, "true\n", string(record.Output))
				} else {
					require.Error(t, observed)
				}
				if mode == "retention-error" {
					return errors.New("disk failure")
				}
				return nil
			})
			if mode == "same" || mode == "readiness" {
				require.NoError(t, err)
				require.Equal(t, 2, admissions)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, 1, retained)
			require.Len(t, client.Actions(), 1)
			require.Equal(t, "get", client.Actions()[0].GetVerb())
		})
	}
}
