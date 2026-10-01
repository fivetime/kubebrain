package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestClaimingCommandPreflightAndRetainedOwnership(t *testing.T) {
	for _, mode := range []string{"execution-refused", "execution-retention-failed", "ambiguous-create", "invalid-tool", "invalid-recovery", "existing-marker", "existing-intent", "preclaim-rejected", "preclaim-cancelled", "preclaim-held", "preclaim-directory-replaced", "missing-preclaim", "existing-owner", "custom-retention"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			p.ProbeExecutable, p.StackExecutable = "/bin/bash", "/bin/bash"
			n := p.Bindings.Network
			ns := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": n.Namespace, "uid": n.NamespaceUID, "resourceVersion": "1"}}}
			sts := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": map[string]interface{}{"name": "brain", "namespace": n.Namespace, "uid": n.StatefulSetUID, "resourceVersion": "1"}}}
			client := fake.NewSimpleDynamicClient(runtime.NewScheme(), ns, sts)
			creates, preclaims, liveChecks := 0, 0, 0
			client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				creates++
				require.Equal(t, 1, preclaims)
				require.FileExists(t, filepath.Join(p.OwnerDirectory, ownerIntentFile))
				u := a.(ktesting.CreateAction).GetObject().(*unstructured.Unstructured).DeepCopy()
				u.SetUID("claim-uid")
				u.SetResourceVersion("1")
				require.NoError(t, client.Tracker().Create(ownerResource, u, n.Namespace))
				if mode == "ambiguous-create" {
					return true, nil, errors.New("response lost")
				}
				return true, u, nil
			})
			check := func(context.Context) error { return nil }
			refuse := func(context.Context) error {
				liveChecks++
				if mode == "execution-retention-failed" {
					require.NoError(t, os.Chmod(p.OwnerDirectory, 0755))
				}
				return errors.New("live preparation refused")
			}
			conn := &successorConnection{}
			r := MeasuredNetworkFaultRuntime{Network: NetworkFaultRuntime{
				Lifecycle: FaultLifecycle{
					Preparation:  FaultPreparation{Directory: p.OwnerDirectory, StatefulSetName: "brain", Network: n, Protocol: p.Bindings.Protocol, Client: client, Connection: conn, Own: refuse},
					OutcomeAdmit: check, Join: check, RecoveryConnection: conn, RecoveryTimeout: time.Second,
				},
				ScriptDirectory: "/bin", TargetsSHA256: strings.Repeat("a", 64), AdmitNetwork: check,
				SuccessorConnection: conn, AdmitSuccessor: check, CaptureSeconds: 1,
			}, AdmitMetrics: check}
			h := ObservationHooks{AdmitOriginal: check, AdmitStack: func(context.Context, string) error { return nil }}
			// Ownership/retention assertions are functional, not a five-second
			// filesystem SLA. Explicit cancellation remains exercised below.
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			admit := func(context.Context) error {
				preclaims++
				if mode == "preclaim-rejected" {
					return errors.New("image not admitted")
				}
				if mode == "preclaim-cancelled" {
					cancel()
				}
				if mode == "preclaim-held" {
					require.NoError(t, os.WriteFile(filepath.Join(p.OwnerDirectory, "HOLD"), nil, 0600))
				}
				if mode == "preclaim-directory-replaced" {
					moved := filepath.Join(t.TempDir(), "retained")
					require.NoError(t, os.Rename(p.OwnerDirectory, moved))
					require.NoError(t, os.Mkdir(p.OwnerDirectory, 0700))
				}
				return nil
			}
			switch mode {
			case "invalid-tool":
				p.ProbeExecutable = "/missing/probe"
			case "invalid-recovery":
				r.Network.Lifecycle.RecoveryConnection = nil
			case "existing-marker":
				require.NoError(t, os.Mkdir(filepath.Join(p.OwnerDirectory, "deployment-claimed"), 0700))
			case "existing-intent":
				require.NoError(t, os.WriteFile(filepath.Join(p.OwnerDirectory, ownerIntentFile), []byte("partial"), 0600))
			case "missing-preclaim":
				admit = nil
			case "existing-owner":
				r.Network.Lifecycle.Owner = &FaultOwner{}
			case "custom-retention":
				r.Network.RetainStatus = func(context.Context, SuccessorSample) error { return nil }
			}
			result, err := p.ClaimAndRun(ctx, r, h, "/bin/bash", metricTargets(t, p), admit)
			require.Error(t, err)
			if strings.HasPrefix(mode, "execution-") || mode == "ambiguous-create" {
				require.Equal(t, 1, creates)
				_, getErr := client.Resource(ownerResource).Namespace(n.Namespace).Get(context.Background(), faultOwnerName, metav1.GetOptions{})
				require.NoError(t, getErr, "claim must remain on any failure")
				if strings.HasPrefix(mode, "execution-") {
					require.NotNil(t, result.Owner, "claim return error: %v; context: %v; preclaims=%d creates=%d liveChecks=%d", err, ctx.Err(), preclaims, creates, liveChecks)
					require.Error(t, result.Lifecycle.ExecutionError)
					require.Error(t, result.Lifecycle.RecoveryError)
					require.True(t, result.Lifecycle.RecoveryAttempted)
					require.Positive(t, liveChecks)
					require.DirExists(t, filepath.Join(p.OwnerDirectory, "deployment-claimed"))
					files, globErr := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-lifecycle.*.json"))
					require.NoError(t, globErr)
					if mode == "execution-refused" {
						require.Len(t, files, 1)
						data, readErr := os.ReadFile(files[0])
						require.NoError(t, readErr)
						var record struct {
							Output []byte
							Error  string
						}
						require.NoError(t, json.Unmarshal(data, &record))
						require.Contains(t, record.Error, "live preparation refused")
						var payload struct {
							ClaimUID          string `json:"claim_uid"`
							RecoveryAttempted bool   `json:"recovery_attempted"`
							ExecutionError    string `json:"execution_error"`
							RecoveryError     string `json:"recovery_error"`
						}
						require.NoError(t, json.Unmarshal(record.Output, &payload))
						require.Equal(t, "claim-uid", payload.ClaimUID)
						require.True(t, payload.RecoveryAttempted)
						require.NotEmpty(t, payload.ExecutionError)
						require.NotEmpty(t, payload.RecoveryError)
					} else {
						require.Empty(t, files)
						require.Contains(t, err.Error(), "live preparation refused")
						require.Contains(t, err.Error(), "recovery owner must be a private directory")
					}
				} else {
					require.Nil(t, result.Owner)
					require.False(t, result.Lifecycle.RecoveryAttempted)
					require.Zero(t, liveChecks)
				}
			} else {
				require.Nil(t, result.Owner)
				require.False(t, result.Lifecycle.RecoveryAttempted)
				require.Zero(t, creates)
				require.Zero(t, liveChecks)
				require.Empty(t, client.Actions())
			}
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb())
			}
		})
	}
}
