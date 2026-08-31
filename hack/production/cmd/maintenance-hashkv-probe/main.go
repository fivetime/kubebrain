package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

type hashKVProbeHeader struct {
	ClusterID uint64 `json:"cluster_id"`
	MemberID  uint64 `json:"member_id"`
	Revision  int64  `json:"revision"`
	RaftTerm  uint64 `json:"raft_term"`
}

type hashKVProbeResult struct {
	Header          hashKVProbeHeader `json:"header"`
	Hash            uint32            `json:"hash"`
	CompactRevision int64             `json:"compact_revision"`
	HashRevision    int64             `json:"hash_revision"`
}

func main() {
	if err := run(os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(stdout io.Writer) (retErr error) {
	if os.Getenv("ENDPOINT") == "" {
		return errors.New("ENDPOINT is required")
	}
	timeout, err := etcdutil.TimeoutFromEnv()
	if err != nil {
		return err
	}
	client, err := etcdutil.NewClientFromEnv()
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, client.Close())
		}
	}()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootCtx, timeout)
	defer cancel()
	response, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).HashKV(ctx, &etcdserverpb.HashKVRequest{})
	if err != nil {
		return err
	}
	result, err := validateHashKVResponse(response)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if err := client.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	return nil
}

func validateHashKVResponse(response *etcdserverpb.HashKVResponse) (hashKVProbeResult, error) {
	if response == nil {
		return hashKVProbeResult{}, errors.New("maintenance HashKV returned a nil response")
	}
	header := response.GetHeader()
	if header == nil {
		return hashKVProbeResult{}, errors.New("maintenance HashKV response header is missing")
	}
	if header.GetClusterId() == 0 {
		return hashKVProbeResult{}, errors.New("maintenance HashKV cluster ID must be positive")
	}
	if header.GetMemberId() == 0 {
		return hashKVProbeResult{}, errors.New("maintenance HashKV member ID must be positive")
	}
	if header.GetRevision() < 0 {
		return hashKVProbeResult{}, fmt.Errorf("maintenance HashKV revision must be non-negative, got %d", header.GetRevision())
	}
	if header.GetRaftTerm() == 0 {
		return hashKVProbeResult{}, errors.New("maintenance HashKV raft term must be positive")
	}
	if response.GetHashRevision() < 0 {
		return hashKVProbeResult{}, fmt.Errorf("maintenance HashKV hash revision must be non-negative, got %d", response.GetHashRevision())
	}
	if response.GetCompactRevision() < -1 {
		return hashKVProbeResult{}, fmt.Errorf("maintenance HashKV compact revision must be at least -1, got %d", response.GetCompactRevision())
	}
	return hashKVProbeResult{
		Header: hashKVProbeHeader{
			ClusterID: header.GetClusterId(),
			MemberID:  header.GetMemberId(),
			Revision:  header.GetRevision(),
			RaftTerm:  header.GetRaftTerm(),
		},
		Hash:            response.GetHash(),
		CompactRevision: response.GetCompactRevision(),
		HashRevision:    response.GetHashRevision(),
	}, nil
}
