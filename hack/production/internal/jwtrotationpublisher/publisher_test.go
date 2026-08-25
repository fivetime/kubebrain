package jwtrotationpublisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublisherLifecycleAndIdempotentTakeover(t *testing.T) {
	f, options := newFixture(t)
	a, err := Run(t.Context(), f, options, fixedNow(100))
	require.NoError(t, err)
	require.Equal(t, "phase-a", a.Phase)
	require.Equal(t, "rev-phase-a", a.AfterRevision)
	require.False(t, a.ReconciledExisting)
	require.Equal(t, options.OldKeySHA256, a.OldKeySHA256)
	require.Equal(t, options.NewKeySHA256, a.NewKeySHA256)
	require.Equal(t, 1, f.creates)
	require.Equal(t, 1, f.patches)
	require.NotContains(t, mustRead(t, options.ReceiptOutput), string(f.oldKey))

	// An identical takeover proves the online state again without another mutation.
	got, err := Run(t.Context(), f, options, fixedNow(101))
	require.NoError(t, err)
	require.Equal(t, a, got)
	require.Equal(t, 1, f.patches)

	bOptions := options
	bOptions.Phase = "phase-b"
	bOptions.PreviousReceipt = options.ReceiptOutput
	bOptions.ReceiptOutput = filepath.Join(f.dir, "phase-b.json")
	b, err := Run(t.Context(), f, bOptions, fixedNow(200))
	require.NoError(t, err)
	require.Equal(t, "rev-phase-a", b.BeforeRevision)
	require.Equal(t, "rev-phase-b", b.AfterRevision)
	require.Equal(t, digestFile(t, options.ReceiptOutput), b.PreviousReceiptSHA256)
	require.NoError(t, os.Remove(bOptions.ReceiptOutput))
	recoveredB, err := Run(t.Context(), f, bOptions, fixedNow(201))
	require.NoError(t, err)
	require.True(t, recoveredB.ReconciledExisting)
	require.Equal(t, a.AfterRevision, recoveredB.BeforeRevision)
	b = recoveredB

	// A failed phase C rollout may be conservatively restored to the receipted
	// phase B without publishing or replacing any phase receipt.
	candidateC := bOptions
	candidateC.Phase = "phase-c"
	candidateC.PreviousReceipt = bOptions.ReceiptOutput
	candidateC.ReceiptOutput = filepath.Join(f.dir, "phase-c.json")
	phaseCArg, _ := desiredAuthArgs(candidateC)
	containers := nested(f.statefulSet, "spec", "template", "spec")["containers"].([]any)
	containers[0].(map[string]any)["args"].([]any)[0] = phaseCArg
	annotations := nested(f.statefulSet, "spec", "template", "metadata")["annotations"].(map[string]any)
	annotations["dbaas.kubebrain.io/jwt-key-rotation-phase"] = "phase-c"
	status := f.statefulSet["status"].(map[string]any)
	status["updateRevision"] = "rev-phase-c-partial"
	require.NoError(t, RollbackPhaseCToB(t.Context(), f, candidateC))
	require.Equal(t, "phase-b", nested(f.statefulSet, "spec", "template", "metadata")["annotations"].(map[string]any)["dbaas.kubebrain.io/jwt-key-rotation-phase"])
	require.NoFileExists(t, candidateC.ReceiptOutput)
	require.Equal(t, 3, f.patches)
	require.NoError(t, RollbackPhaseCToB(t.Context(), f, candidateC))
	require.Equal(t, 3, f.patches)

	cOptions := bOptions
	cOptions.Phase = "phase-c"
	cOptions.PreviousReceipt = bOptions.ReceiptOutput
	cOptions.ReceiptOutput = filepath.Join(f.dir, "phase-c.json")
	c, err := Run(t.Context(), f, cOptions, fixedNow(300))
	require.NoError(t, err)
	require.Equal(t, "rev-phase-c", c.AfterRevision)
	require.Equal(t, a.TemplateBaselineSHA, b.TemplateBaselineSHA)
	require.Equal(t, b.TemplateBaselineSHA, c.TemplateBaselineSHA)
	require.Equal(t, digestFile(t, bOptions.ReceiptOutput), c.PreviousReceiptSHA256)
	require.Equal(t, 4, f.patches)
	for _, path := range []string{options.ReceiptOutput, bOptions.ReceiptOutput, cOptions.ReceiptOutput} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	require.ErrorContains(t, RollbackPhaseCToB(t.Context(), f, cOptions), "receipted phase C")
}

func TestPublisherFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*testing.T, *fakeKube, *Options)
		want    string
	}{
		{
			name: "phase B without secret",
			prepare: func(t *testing.T, f *fakeKube, o *Options) {
				o.Phase = "phase-b"
				o.PreviousReceipt = writeReceipt(t, f.dir, validPrevious(*o, "phase-a"))
			},
			want: "immutable key Secret is missing",
		},
		{
			name: "insecure key source",
			prepare: func(t *testing.T, _ *fakeKube, o *Options) {
				require.NoError(t, os.Chmod(o.OldKeySource, 0o644))
			},
			want: "inaccessible to group/other",
		},
		{
			name: "duplicate auth argument",
			prepare: func(_ *testing.T, f *fakeKube, _ *Options) {
				containers := nested(f.statefulSet, "spec", "template", "spec")["containers"].([]any)
				args := containers[0].(map[string]any)["args"].([]any)
				containers[0].(map[string]any)["args"] = append(args, args[0])
			},
			want: "duplicate auth-token arguments",
		},
		{
			name: "wrong TTL",
			prepare: func(_ *testing.T, f *fakeKube, _ *Options) {
				containers := nested(f.statefulSet, "spec", "template", "spec")["containers"].([]any)
				containers[0].(map[string]any)["args"].([]any)[1] = "--auth-token-ttl=91"
			},
			want: "TTL argument",
		},
		{
			name:    "concurrent patch",
			prepare: func(_ *testing.T, f *fakeKube, _ *Options) { f.patchErr = errors.New("conflict") },
			want:    "patch StatefulSet",
		},
		{
			name: "source digest mismatch",
			prepare: func(_ *testing.T, _ *fakeKube, o *Options) {
				o.NewKeySHA256 = strings.Repeat("0", 64)
			},
			want: "key source digest",
		},
		{
			name: "rollout already unhealthy",
			prepare: func(_ *testing.T, f *fakeKube, _ *Options) {
				f.statefulSet["status"].(map[string]any)["readyReplicas"] = float64(2)
			},
			want: "fully rolled out",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, options := newFixture(t)
			tc.prepare(t, f, &options)
			_, err := Run(t.Context(), f, options, fixedNow(100))
			require.ErrorContains(t, err, tc.want)
			require.NoFileExists(t, options.ReceiptOutput)
		})
	}
}

func TestPublisherRejectsDriftAndNonCanonicalReceipt(t *testing.T) {
	f, options := newFixture(t)
	_, err := Run(t.Context(), f, options, fixedNow(100))
	require.NoError(t, err)

	t.Run("template drift", func(t *testing.T) {
		b := options
		b.Phase, b.PreviousReceipt, b.ReceiptOutput = "phase-b", options.ReceiptOutput, filepath.Join(f.dir, "drift.json")
		containers := nested(f.statefulSet, "spec", "template", "spec")["containers"].([]any)
		containers[0].(map[string]any)["image"] = "unapproved:image"
		_, err := Run(t.Context(), f, b, fixedNow(200))
		require.ErrorContains(t, err, "template baseline changed")
	})

	t.Run("unknown receipt field", func(t *testing.T) {
		f2, o2 := newFixture(t)
		_, err := Run(t.Context(), f2, o2, fixedNow(100))
		require.NoError(t, err)
		data := strings.TrimSpace(mustRead(t, o2.ReceiptOutput))
		data = strings.TrimSuffix(data, "}") + `,"shadow":true}` + "\n"
		require.NoError(t, os.WriteFile(o2.ReceiptOutput, []byte(data), 0o600))
		b := o2
		b.Phase, b.PreviousReceipt, b.ReceiptOutput = "phase-b", o2.ReceiptOutput, filepath.Join(f2.dir, "b.json")
		_, err = Run(t.Context(), f2, b, fixedNow(200))
		require.ErrorContains(t, err, "unknown field")
	})

	t.Run("secret content drift", func(t *testing.T) {
		f3, o3 := newFixture(t)
		_, err := Run(t.Context(), f3, o3, fixedNow(100))
		require.NoError(t, err)
		f3.secret["data"].(map[string]any)[o3.NewKeyField] = "ZHJpZnQ="
		b := o3
		b.Phase, b.PreviousReceipt, b.ReceiptOutput = "phase-b", o3.ReceiptOutput, filepath.Join(f3.dir, "b.json")
		_, err = Run(t.Context(), f3, b, fixedNow(200))
		require.ErrorContains(t, err, "does not match approved source")
	})

	t.Run("permissive previous receipt", func(t *testing.T) {
		f4, o4 := newFixture(t)
		_, err := Run(t.Context(), f4, o4, fixedNow(100))
		require.NoError(t, err)
		require.NoError(t, os.Chmod(o4.ReceiptOutput, 0o640))
		b := o4
		b.Phase, b.PreviousReceipt, b.ReceiptOutput = "phase-b", o4.ReceiptOutput, filepath.Join(f4.dir, "b.json")
		_, err = Run(t.Context(), f4, b, fixedNow(200))
		require.ErrorContains(t, err, "0600 regular")
	})

	t.Run("statefulset UID replacement", func(t *testing.T) {
		f5, o5 := newFixture(t)
		_, err := Run(t.Context(), f5, o5, fixedNow(100))
		require.NoError(t, err)
		f5.statefulSet["metadata"].(map[string]any)["uid"] = "replacement-uid"
		b := o5
		b.Phase, b.PreviousReceipt, b.ReceiptOutput = "phase-b", o5.ReceiptOutput, filepath.Join(f5.dir, "b.json")
		_, err = Run(t.Context(), f5, b, fixedNow(200))
		require.ErrorContains(t, err, "UID changed")
	})

	t.Run("managed annotation drift on takeover", func(t *testing.T) {
		f6, o6 := newFixture(t)
		_, err := Run(t.Context(), f6, o6, fixedNow(100))
		require.NoError(t, err)
		annotations := nested(f6.statefulSet, "spec", "template", "metadata")["annotations"].(map[string]any)
		annotations["dbaas.kubebrain.io/jwt-key-secret-sha256"] = strings.Repeat("0", 64)
		_, err = Run(t.Context(), f6, o6, fixedNow(101))
		require.ErrorContains(t, err, "existing publish receipt does not match")
	})
}

type fakeKube struct {
	dir                 string
	statefulSet, secret map[string]any
	oldKey, newKey      []byte
	creates, patches    int
	patchErr            error
}

func newFixture(t *testing.T) (*fakeKube, Options) {
	t.Helper()
	dir := t.TempDir()
	oldPath, newPath := filepath.Join(dir, "old"), filepath.Join(dir, "new")
	require.NoError(t, os.WriteFile(oldPath, []byte("old-secret-material"), 0o600))
	require.NoError(t, os.WriteFile(newPath, []byte("new-secret-material"), 0o600))
	o := Options{
		Phase: "phase-a", OperationID: "jwt-key-rotate-0123456789abcdefabcd", Instance: "instance-a",
		Namespace: "instance-a", StatefulSet: "kubebrain", Secret: "kubebrain-jwt-rotation",
		OldKeyField: "old-key", NewKeyField: "new-key", OldKeySource: oldPath, NewKeySource: newPath,
		OldKeySHA256: digestBytes([]byte("old-secret-material")), NewKeySHA256: digestBytes([]byte("new-secret-material")),
		KeyVolume: "jwt-keys", KeyMountDir: "/etc/kubebrain-jwt", SignMethod: "HS256",
		ExpectedReplicas: 3, JWTTokenTTL: 90, ReceiptOutput: filepath.Join(dir, "phase-a.json"),
		PollInterval: time.Millisecond, RolloutTimeout: time.Second,
	}
	oldOnly := "--auth-token=jwt,sign-method=HS256,priv-key=/etc/kubebrain-jwt/old-key"
	statefulSet := map[string]any{
		"apiVersion": "apps/v1", "kind": "StatefulSet",
		"metadata": map[string]any{"uid": "sts-uid", "resourceVersion": "10", "generation": float64(1)},
		"spec": map[string]any{"replicas": float64(3), "template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{"example.com/profile": "stable"}},
			"spec": map[string]any{
				"containers": []any{map[string]any{"name": "kubebrain", "image": "kubebrain:exact", "args": []any{oldOnly, "--auth-token-ttl=90", "--port=2379"}, "volumeMounts": []any{map[string]any{"name": "jwt-keys", "mountPath": "/etc/kubebrain-jwt", "readOnly": true}}}},
				"volumes":    []any{map[string]any{"name": "jwt-keys", "secret": map[string]any{"secretName": "kubebrain-jwt-rotation"}}},
			},
		}},
		"status": map[string]any{"observedGeneration": float64(1), "readyReplicas": float64(3), "updatedReplicas": float64(3), "currentRevision": "rev-old", "updateRevision": "rev-old"},
	}
	return &fakeKube{dir: dir, statefulSet: statefulSet, oldKey: []byte("old-secret-material"), newKey: []byte("new-secret-material")}, o
}

func (f *fakeKube) Get(_ context.Context, _, resource, _ string) ([]byte, bool, error) {
	switch resource {
	case "secret":
		if f.secret == nil {
			return nil, false, nil
		}
		data, _ := json.Marshal(f.secret)
		return data, true, nil
	case "statefulset":
		data, _ := json.Marshal(f.statefulSet)
		return data, true, nil
	default:
		return nil, false, fmt.Errorf("unexpected resource %s", resource)
	}
}

func (f *fakeKube) Create(_ context.Context, _ string, object []byte) error {
	f.creates++
	if f.secret != nil {
		return errors.New("already exists")
	}
	requireJSON := map[string]any{}
	if err := json.Unmarshal(object, &requireJSON); err != nil {
		return err
	}
	metadata := requireJSON["metadata"].(map[string]any)
	metadata["uid"], metadata["resourceVersion"] = "secret-uid", "20"
	f.secret = requireJSON
	return nil
}

func (f *fakeKube) Patch(_ context.Context, _, resource, _ string, patch []byte) error {
	if f.patchErr != nil {
		return f.patchErr
	}
	if resource != "statefulset" {
		return errors.New("unexpected patch resource")
	}
	var operations []map[string]any
	if err := json.Unmarshal(patch, &operations); err != nil {
		return err
	}
	metadata := f.statefulSet["metadata"].(map[string]any)
	if operations[0]["value"] != metadata["resourceVersion"] {
		return errors.New("resourceVersion test failed")
	}
	containers := nested(f.statefulSet, "spec", "template", "spec")["containers"].([]any)
	container := containers[0].(map[string]any)
	args := container["args"].([]any)
	if operations[1]["value"] != args[0] {
		return errors.New("auth-token test failed")
	}
	args[0] = operations[2]["value"]
	nested(f.statefulSet, "spec", "template", "metadata")["annotations"] = operations[3]["value"]
	f.patches++
	phase := operations[3]["value"].(map[string]any)["dbaas.kubebrain.io/jwt-key-rotation-phase"].(string)
	metadata["generation"] = metadata["generation"].(float64) + 1
	metadata["resourceVersion"] = fmt.Sprintf("%d", 10+f.patches)
	status := f.statefulSet["status"].(map[string]any)
	status["observedGeneration"] = metadata["generation"]
	status["currentRevision"], status["updateRevision"] = "rev-"+phase, "rev-"+phase
	return nil
}

func nested(value map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		value = value[key].(map[string]any)
	}
	return value
}

func fixedNow(unix int64) func() time.Time { return func() time.Time { return time.Unix(unix, 0) } }
func mustRead(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}
func digestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return digestBytes(data)
}
func writeReceipt(t *testing.T, dir string, receipt Receipt) string {
	t.Helper()
	path := filepath.Join(dir, "previous.json")
	data, err := json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0o600))
	return path
}
func validPrevious(o Options, phase string) Receipt {
	return Receipt{Format: ReceiptFormat, Phase: phase, OperationID: o.OperationID, Instance: o.Instance, Namespace: o.Namespace, StatefulSet: o.StatefulSet, Secret: o.Secret, SecretUID: "secret-uid", SecretResourceVersion: "20", SecretDataSHA256: strings.Repeat("a", 64), TemplateBaselineSHA: strings.Repeat("b", 64), AfterRevision: "rev-phase-a", Replicas: o.ExpectedReplicas, RolledOut: true}
}
