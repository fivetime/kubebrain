package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestInitialMemberAdmission(t *testing.T) {
	for _, mode := range []string{"valid", "cluster", "member", "leader", "term", "health", "rpc", "observer", "cancelled-response", "pre-admission", "post-admission", "directory-replaced"} {
		t.Run(mode, func(t *testing.T) {
			p := commandPlan(t)
			require.NoError(t, os.Chmod(p.OwnerDirectory, 0700))
			// The outer bound covers both members and durable evidence. Each
			// Status request must still obey the independently asserted 5s limit.
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			makeConnection := func(member uint64, original bool) *successorConnection {
				return &successorConnection{read: func(ctx context.Context, count int) (*pb.StatusResponse, error) {
					require.Equal(t, 1, count, "initial admission must never retry")
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
					r := &pb.StatusResponse{Header: &pb.ResponseHeader{ClusterId: p.Bindings.Protocol.ClusterID, MemberId: member, RaftTerm: p.Bindings.InitialTerm}, Leader: p.Bindings.Protocol.AlarmMemberID}
					if !original {
						if mode == "observer" {
							r.Header.MemberId = p.Bindings.Protocol.AlarmMemberID
						}
						return r, nil
					}
					switch mode {
					case "cluster":
						r.Header.ClusterId--
					case "member":
						r.Header.MemberId--
					case "leader":
						r.Leader = 0
					case "term":
						r.Header.RaftTerm++
					case "health":
						r.Errors = []string{"unhealthy"}
					case "rpc":
						return nil, errors.New("response lost")
					case "cancelled-response":
						cancel()
					}
					return r, nil
				}}
			}
			original, observer := makeConnection(p.Bindings.Protocol.AlarmMemberID, true), makeConnection(p.Bindings.ObserverMemberID, false)
			admissions := 0
			err := p.VerifyInitialMembers(ctx, original, observer, func(context.Context) error {
				admissions++
				if mode == "directory-replaced" {
					require.NoError(t, os.Rename(p.OwnerDirectory, filepath.Join(t.TempDir(), "moved")))
					require.NoError(t, os.Mkdir(p.OwnerDirectory, 0700))
				}
				if mode == "pre-admission" || (mode == "post-admission" && admissions == 2) {
					return errors.New("admission lost")
				}
				return nil
			})
			if mode == "valid" {
				require.NoError(t, err)
				require.Equal(t, 4, admissions)
			} else {
				require.Error(t, err)
			}
			files, globErr := filepath.Glob(filepath.Join(p.OwnerDirectory, "experiment-initial-*.json"))
			require.NoError(t, globErr)
			require.Len(t, files, original.calls+observer.calls)
			if mode == "valid" || mode == "observer" {
				require.Equal(t, 1, observer.calls)
			} else {
				require.Zero(t, observer.calls)
			}
			for _, file := range files {
				data, readErr := os.ReadFile(file)
				require.NoError(t, readErr)
				var record struct {
					Output []byte
					Error  string
				}
				require.NoError(t, json.Unmarshal(data, &record))
				var payload struct {
					Cluster uint64 `json:"expected_cluster,string"`
					Status  *pb.StatusResponse
				}
				require.NoError(t, json.Unmarshal(record.Output, &payload))
				require.Equal(t, p.Bindings.Protocol.ClusterID, payload.Cluster, "retain full-width IDs")
				if mode == "valid" {
					require.Empty(t, record.Error)
					require.NotNil(t, payload.Status)
				}
			}
		})
	}
}
