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

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

func TestCommandEvidenceCallbacks(t *testing.T) {
	p := commandPlan(t)
	require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
	r, h, err := p.BindEvidence(MeasuredNetworkFaultRuntime{}, ObservationHooks{})
	require.NoError(t, err)
	ctx := context.Background()
	observed := errors.New("rpc response lost")
	require.NoError(t, h.RetainOriginal(ctx, "pending", p.Bindings.Initial(), []byte{0, 255}, observed))
	require.NoError(t, h.RetainStack(ctx, "before", WaitReceipt{Capture: "capture"}, observed))
	require.NoError(t, r.Network.RetainNetwork("drops", []byte("network"), observed))
	require.NoError(t, r.Network.RetainStatus(ctx, SuccessorSample{Error: observed}))
	require.NoError(t, r.RetainMetrics(ctx, 0, retirementmetrics.WorkerMeasurement{Count: 1}))
	files, err := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-*.json"))
	require.NoError(t, err)
	require.Len(t, files, 5)
	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		var record struct {
			Output []byte
			Error  string
		}
		require.NoError(t, json.Unmarshal(data, &record))
		var payload struct {
			Owner          string          `json:"owner"`
			BindingsSHA256 string          `json:"bindings_sha256"`
			Payload        json.RawMessage `json:"payload"`
		}
		require.NoError(t, json.Unmarshal(record.Output, &payload))
		require.Equal(t, p.Bindings.Network.Owner, payload.Owner)
		require.Len(t, payload.BindingsSHA256, 64)
		require.NotEmpty(t, payload.Payload)
		if strings.HasPrefix(filepath.Base(file), "experiment-metrics") {
			require.Empty(t, record.Error)
		} else {
			require.Equal(t, observed.Error(), record.Error)
		}
		if strings.HasPrefix(filepath.Base(file), "experiment-original") {
			var original struct {
				Binding Binding
				Output  []byte
			}
			require.NoError(t, json.Unmarshal(payload.Payload, &original))
			require.Equal(t, p.Bindings.Initial(), original.Binding)
			require.Equal(t, []byte{0, 255}, original.Output)
		}
	}
	_, _, err = p.BindEvidence(r, h)
	require.Error(t, err, "do not silently overwrite callbacks")
	require.Error(t, h.RetainOriginal(ctx, "../invalid", Binding{}, nil, nil))
	require.Error(t, h.RetainStack(ctx, "invalid", WaitReceipt{}, nil))
	require.Error(t, r.RetainMetrics(ctx, -1, retirementmetrics.WorkerMeasurement{}))
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, h.RetainStack(cancelled, "after", WaitReceipt{}, observed), context.Canceled)
	files, err = filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-stack-after.*.json"))
	require.NoError(t, err)
	require.Len(t, files, 1, "preserve cancelled observations before reporting cancellation")
	moved := filepath.Join(t.TempDir(), "retained")
	require.NoError(t, os.Rename(p.OwnerDirectory, moved))
	require.NoError(t, os.Mkdir(p.OwnerDirectory, 0700))
	require.ErrorContains(t, r.Network.RetainStatus(ctx, SuccessorSample{}), "directory changed")
	entries, err := os.ReadDir(p.OwnerDirectory)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestCommandEvidenceRealSuccessorRPC(t *testing.T) {
	p := commandPlan(t)
	require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
	r, _, err := p.BindEvidence(MeasuredNetworkFaultRuntime{}, ObservationHooks{})
	require.NoError(t, err)
	conn := recoveryGRPCFixtureAtTerm(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initial, err := pb.NewMaintenanceClient(conn).Status(ctx, &pb.StatusRequest{})
	require.NoError(t, err)
	old := initial.Leader + 1
	if old == 0 {
		old = 1
	}
	result, err := ObserveSuccessor(ctx, conn, SuccessorBinding{
		ClusterID: initial.Header.ClusterId, ObserverMemberID: initial.Header.MemberId,
		OldLeaderID: old, OldTerm: 2, Origin: time.Now(),
	}, func(context.Context) error { return nil }, r.Network.RetainStatus)
	require.NoError(t, err)
	files, err := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-successor.*.json"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	var record struct {
		Output []byte
		Error  string
	}
	require.NoError(t, json.Unmarshal(data, &record))
	require.Empty(t, record.Error)
	var envelope struct {
		Payload struct {
			Started, Completed time.Time
			Status             *pb.StatusResponse
		} `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(record.Output, &envelope))
	require.True(t, proto.Equal(result, envelope.Payload.Status))
	require.False(t, envelope.Payload.Started.IsZero())
	require.False(t, envelope.Payload.Completed.Before(envelope.Payload.Started))
}
