package leasefault

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func reservationFixture() json.RawMessage {
	p := networkPlan()
	raw := strings.Replace(string(p.ApprovedPolicy), `"active"`, `"reserved"`, 1)
	return json.RawMessage(strings.Replace(raw, `"name":"fault-policy"`, `"uid":"created-uid","resourceVersion":"18446744073709551615","name":"fault-policy"`, 1))
}

func TestNetworkReservationDurableDeletePlan(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	p := networkPlan()
	raw := reservationFixture()
	require.Error(t, SaveNetworkReservation(dir, p, raw)) // intent must precede CREATE
	require.NoError(t, ArmNetworkRecovery(dir, p))
	require.NoError(t, SaveNetworkReservation(dir, p, raw))
	require.Error(t, SaveNetworkReservation(dir, p, raw))
	saved, err := LoadNetworkReservation(dir, p)
	require.NoError(t, err)
	require.Equal(t, raw, saved)
	st, err := os.Stat(filepath.Join(dir, networkReceiptFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), st.Mode().Perm())
	input := map[string]interface{}{
		"binding": map[string]string{"namespace": p.Namespace, "namespaceUID": p.NamespaceUID,
			"name": p.PolicyName, "nonce": p.Nonce, "reservedNonce": p.ReservedNonce},
		"namespace": map[string]interface{}{"apiVersion": "v1", "kind": "Namespace",
			"metadata": map[string]string{"name": p.Namespace, "uid": p.NamespaceUID}},
		"approved": p.ApprovedPolicy, "expected": saved,
		"current": json.RawMessage(strings.Replace(string(saved), `"reserved"`, `"active"`, 1)),
	}
	data, err := json.Marshal(input)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "jq", "-e", "-f", "../../fault-policy-delete-plan.jq")
	cmd.Stdin = bytes.NewReader(data)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	require.JSONEq(t, `{"apiVersion":"cilium.io/v2","resource":"ciliumnetworkpolicies","namespace":"test-ns","name":"fault-policy","uid":"created-uid","resourceVersion":"18446744073709551615"}`, string(output))
	p.NamespaceUID = "replacement"
	_, err = LoadNetworkReservation(dir, p)
	require.Error(t, err)
}

func TestNetworkReservationRejectsInvalidReceipt(t *testing.T) {
	for _, pair := range [][2]string{
		{`"reserved"`, `"active"`}, {`"created-uid"`, `""`},
		{`"uid"`, `"UID"`}, {`"resourceVersion":"18446744073709551615"`, `"resourceVersion":18446744073709551615`},
		{`"test-ns"`, `"other-ns"`}, {`"fault-policy"`, `"other-policy"`},
		{`"egressDeny"`, `"ingressDeny"`}, {`"10.0.0.1/32"`, `"10.0.0.2/32"`},
		{`"uid":"created-uid"`, `"uid":"created-uid","uid":"replacement"`},
		{`"uid":"created-uid"`, `"uid":"created-uid","deletionTimestamp":"2026-09-19T00:00:00Z"`},
		{`"spec":`, `"specs":[],"spec":`},
	} {
		t.Run(pair[1], func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			p := networkPlan()
			require.NoError(t, ArmNetworkRecovery(dir, p))
			bad := json.RawMessage(strings.Replace(string(reservationFixture()), pair[0], pair[1], 1))
			require.Error(t, SaveNetworkReservation(dir, p, bad))
			_, err := os.Lstat(filepath.Join(dir, networkReceiptFile))
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestNetworkReservationRejectsUnsafeSavedRecord(t *testing.T) {
	for _, mode := range []string{"missing", "truncated", "public", "symlink", "fifo", "oversize", "active"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			p := networkPlan()
			require.NoError(t, ArmNetworkRecovery(dir, p))
			path := filepath.Join(dir, networkReceiptFile)
			switch mode {
			case "missing":
			case "truncated":
				require.NoError(t, os.WriteFile(path, []byte(`{"metadata":`), 0600))
			case "public":
				require.NoError(t, SaveNetworkReservation(dir, p, reservationFixture()))
				require.NoError(t, os.Chmod(path, 0644))
			case "symlink":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "target"), reservationFixture(), 0600))
				require.NoError(t, os.Symlink("target", path))
			case "fifo":
				require.NoError(t, syscall.Mkfifo(path, 0600))
			case "oversize":
				require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte(" "), networkRecoveryLimit+1), 0600))
			case "active":
				require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(reservationFixture()), `"reserved"`, `"active"`, 1)), 0600))
			}
			got, err := LoadNetworkReservation(dir, p)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
