package testcluster_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

func TestExperimentalPeerMountPatchPreservesBaselineAndSeparatesMembers(t *testing.T) {
	data, err := os.ReadFile("kubebrain-local.json")
	require.NoError(t, err)
	var list struct {
		Items []json.RawMessage `json:"items"`
	}
	require.NoError(t, json.Unmarshal(data, &list))
	var baseline appsv1.StatefulSet
	var original []byte
	for _, item := range list.Items {
		var object struct {
			Kind string `json:"kind"`
		}
		require.NoError(t, json.Unmarshal(item, &object))
		if object.Kind == "StatefulSet" {
			original = item
			require.NoError(t, json.Unmarshal(item, &baseline))
		}
	}
	require.NotEmpty(t, original)
	patch, err := os.ReadFile("peer-retirement-mounts.patch.json")
	require.NoError(t, err)
	merged, err := strategicpatch.StrategicMergePatch(original, patch, appsv1.StatefulSet{})
	require.NoError(t, err)
	var candidate appsv1.StatefulSet
	require.NoError(t, json.Unmarshal(merged, &candidate))
	expected := baseline.DeepCopy()
	mode := int32(0440)
	optional := false
	var items []corev1.KeyToPath
	for i := 0; i < 3; i++ {
		member := fmt.Sprintf("kubebrain-local-%d", i)
		for _, file := range []string{"tls.crt", "tls.key", "ca.crt", "policy.json"} {
			items = append(items, corev1.KeyToPath{Key: member + "." + file, Path: member + "/" + file})
		}
	}
	mounts, volumes := 0, 0
	for i := range expected.Spec.Template.Spec.Volumes {
		v := &expected.Spec.Template.Spec.Volumes[i]
		if v.Name == "peer-tls" {
			volumes++
			v.Secret = &corev1.SecretVolumeSource{SecretName: "kubebrain-local-peer-retirement-v1", DefaultMode: &mode, Optional: &optional, Items: items}
		}
	}
	for i := range expected.Spec.Template.Spec.Containers {
		c := &expected.Spec.Template.Spec.Containers[i]
		for j := range c.VolumeMounts {
			m := &c.VolumeMounts[j]
			if m.Name == "peer-tls" {
				mounts++
				require.Equal(t, "kubebrain", c.Name)
				require.Equal(t, "/etc/kubebrain/peer-tls", m.MountPath)
				m.SubPathExpr = "$(POD_NAME)"
				m.ReadOnly = true
			}
		}
	}
	require.Equal(t, 1, mounts)
	require.Equal(t, 1, volumes)
	require.Equal(t, *expected, candidate, "patch must not change image, args, workload identity, TLS elsewhere, storage, resources, or expose the bundle root")
	require.Len(t, candidate.Spec.Template.Spec.Containers, 1)
	require.Empty(t, candidate.Spec.Template.Spec.InitContainers)
	require.Empty(t, candidate.Spec.Template.Spec.EphemeralContainers)
	var podName *corev1.EnvVar
	for _, env := range candidate.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "POD_NAME" {
			value := env
			podName = &value
		}
	}
	require.NotNil(t, podName)
	require.Empty(t, podName.Value)
	require.NotNil(t, podName.ValueFrom)
	require.NotNil(t, podName.ValueFrom.FieldRef)
	require.Equal(t, "metadata.name", podName.ValueFrom.FieldRef.FieldPath)
	require.NotNil(t, candidate.Spec.Template.Spec.AutomountServiceAccountToken)
	require.False(t, *candidate.Spec.Template.Spec.AutomountServiceAccountToken)
}
