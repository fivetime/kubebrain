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

type hashProbeHeader struct {
	ClusterID uint64 `json:"cluster_id"`
	MemberID  uint64 `json:"member_id"`
	Revision  int64  `json:"revision"`
	RaftTerm  uint64 `json:"raft_term"`
}

type hashProbeResult struct {
	Header hashProbeHeader `json:"header"`
	Hash   uint32          `json:"hash"`
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
	response, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Hash(ctx, &etcdserverpb.HashRequest{})
	if err != nil {
		return err
	}
	result, err := validateHashResponse(response)
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

func validateHashResponse(response *etcdserverpb.HashResponse) (hashProbeResult, error) {
	if response == nil {
		return hashProbeResult{}, errors.New("maintenance Hash returned a nil response")
	}
	header := response.GetHeader()
	if header == nil {
		return hashProbeResult{}, errors.New("maintenance Hash response header is missing")
	}
	if header.GetClusterId() == 0 {
		return hashProbeResult{}, errors.New("maintenance Hash cluster ID must be positive")
	}
	if header.GetMemberId() == 0 {
		return hashProbeResult{}, errors.New("maintenance Hash member ID must be positive")
	}
	if header.GetRevision() < 0 {
		return hashProbeResult{}, fmt.Errorf("maintenance Hash revision must be non-negative, got %d", header.GetRevision())
	}
	if header.GetRaftTerm() == 0 {
		return hashProbeResult{}, errors.New("maintenance Hash raft term must be positive")
	}
	return hashProbeResult{
		Header: hashProbeHeader{
			ClusterID: header.GetClusterId(),
			MemberID:  header.GetMemberId(),
			Revision:  header.GetRevision(),
			RaftTerm:  header.GetRaftTerm(),
		},
		Hash: response.GetHash(),
	}, nil
}
