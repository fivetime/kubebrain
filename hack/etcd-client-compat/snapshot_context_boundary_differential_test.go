package compat

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type snapshotContextBoundaryOutcome struct {
	Name            string
	ReturnedReader  bool
	ReturnedVersion bool
	ErrorCode       string
	ContextCanceled bool
	ContextDeadline bool
	BytesReadable   int
	FirstResponse   bool
	FirstBlob       bool
	TerminalAllowed bool
}

func TestSnapshotContextBoundaryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := []snapshotContextBoundaryOutcome{
		{Name: "snapshot-canceled", ErrorCode: "Unknown", ContextCanceled: true},
		{Name: "snapshot-with-version-canceled", ErrorCode: "Unknown", ContextCanceled: true},
		{Name: "snapshot-deadline", ErrorCode: "Unknown", ContextDeadline: true},
		{Name: "snapshot-with-version-deadline", ErrorCode: "Unknown", ContextDeadline: true},
		{Name: "raw-cancel-after-first", FirstResponse: true, FirstBlob: true, TerminalAllowed: true},
	}
	referenceOutcome := runSnapshotContextBoundaryScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runSnapshotContextBoundaryScenario(t, compatEndpoint(t)))
}

func runSnapshotContextBoundaryScenario(t *testing.T, endpoint string) []snapshotContextBoundaryOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Logger: zap.NewNop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	type snapshotCall struct {
		name        string
		withVersion bool
		ctx         func() (context.Context, context.CancelFunc)
	}
	canceled := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}
	deadline := func() (context.Context, context.CancelFunc) {
		return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	}
	calls := []snapshotCall{
		{name: "snapshot-canceled", ctx: canceled},
		{name: "snapshot-with-version-canceled", withVersion: true, ctx: canceled},
		{name: "snapshot-deadline", ctx: deadline},
		{name: "snapshot-with-version-deadline", withVersion: true, ctx: deadline},
	}
	outcomes := make([]snapshotContextBoundaryOutcome, 0, len(calls))
	for _, call := range calls {
		ctx, cancel := call.ctx()
		var reader io.ReadCloser
		var version string
		var callErr error
		if call.withVersion {
			response, err := cli.SnapshotWithVersion(ctx)
			callErr = err
			if response != nil {
				reader = response.Snapshot
				version = response.Version
			}
		} else {
			reader, callErr = cli.Snapshot(ctx)
		}
		cancel()
		outcome := snapshotContextBoundaryOutcome{
			Name: call.name, ReturnedReader: reader != nil, ReturnedVersion: version != "",
			ErrorCode: status.Code(callErr).String(), ContextCanceled: errors.Is(callErr, context.Canceled),
			ContextDeadline: errors.Is(callErr, context.DeadlineExceeded),
		}
		if reader != nil {
			contents, readErr := io.ReadAll(io.LimitReader(reader, 64))
			outcome.BytesReadable = len(contents)
			if callErr == nil {
				callErr = readErr
				outcome.ErrorCode = status.Code(readErr).String()
				outcome.ContextCanceled = errors.Is(readErr, context.Canceled)
				outcome.ContextDeadline = errors.Is(readErr, context.DeadlineExceeded)
			}
			require.NoError(t, reader.Close())
		}
		require.Error(t, callErr, call.name)
		outcomes = append(outcomes, outcome)
	}

	rawCtx, rawCancel := context.WithCancel(context.Background())
	stream, err := etcdserverpb.NewMaintenanceClient(cli.ActiveConnection()).Snapshot(
		rawCtx, &etcdserverpb.SnapshotRequest{},
	)
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	rawCancel()
	var terminalErr error
	for terminalErr == nil {
		_, terminalErr = stream.Recv()
	}
	outcomes = append(outcomes, snapshotContextBoundaryOutcome{
		Name: "raw-cancel-after-first", FirstResponse: first != nil,
		FirstBlob:       first != nil && len(first.Blob) > 0,
		TerminalAllowed: errors.Is(terminalErr, io.EOF) || status.Code(terminalErr) == codes.Canceled,
	})
	return outcomes
}
