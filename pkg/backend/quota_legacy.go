package backend

import (
	"context"
	"errors"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func (b *backend) quotaCreate(ctx context.Context, put *proto.CreateRequest) (*proto.CreateResponse, error) {
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: put.Key, Value: put.Value, Lease: put.Lease,
	}}, []TxnGuard{{Key: put.Key, Absent: true}})
	if errors.Is(err, ErrTxnGuardConflict) {
		return &proto.CreateResponse{
			Header: responseHeader(b.GetCurrentRevision()), Succeeded: false,
		}, nil
	}
	if err != nil {
		return nil, err
	}
	b.waitCommittedRevision(ctx, revision)
	return &proto.CreateResponse{Header: responseHeader(revision), Succeeded: true}, nil
}

func (b *backend) quotaUpdate(ctx context.Context, request *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	key := request.GetKv().GetKey()
	if request.GetKv().GetRevision() == 0 {
		created, err := b.quotaCreate(ctx, &proto.CreateRequest{
			Key: key, Value: request.GetKv().GetValue(), Lease: request.Lease,
		})
		if err != nil {
			return nil, err
		}
		return &proto.UpdateResponse{Header: created.Header, Succeeded: created.Succeeded}, nil
	}
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: key, Value: request.GetKv().GetValue(), Lease: request.Lease,
	}}, []TxnGuard{{Key: key, Revision: request.GetKv().GetRevision()}})
	if errors.Is(err, ErrTxnGuardConflict) {
		response := &proto.UpdateResponse{
			Header: responseHeader(b.GetCurrentRevision()), Succeeded: false,
		}
		value, modRevision, getErr := b.get(ctx, key, 0)
		if getErr == nil {
			response.Header.Revision = maxUint64(response.Header.Revision, modRevision)
			response.Kv = &proto.KeyValue{Key: key, Value: value, Revision: modRevision}
		}
		return response, nil
	}
	if err != nil {
		return nil, err
	}
	b.waitCommittedRevision(ctx, revision)
	return &proto.UpdateResponse{Header: responseHeader(revision), Succeeded: true}, nil
}

func (b *backend) quotaDelete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	guards := []TxnGuard(nil)
	if request.Revision > 0 {
		guards = []TxnGuard{{Key: request.Key, Revision: request.Revision}}
	}
	results, revision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: request.Key, Delete: true,
	}}, guards)
	if errors.Is(err, ErrTxnGuardConflict) {
		response := &proto.DeleteResponse{
			Header: responseHeader(b.GetCurrentRevision()), Succeeded: false,
		}
		value, modRevision, getErr := b.get(ctx, request.Key, 0)
		if getErr == nil {
			response.Header.Revision = maxUint64(response.Header.Revision, modRevision)
			response.Kv = &proto.KeyValue{Key: request.Key, Value: value, Revision: modRevision}
		}
		return response, nil
	}
	if err != nil {
		return nil, err
	}
	b.waitCommittedRevision(ctx, revision)
	response := &proto.DeleteResponse{Header: responseHeader(revision), Succeeded: true}
	if len(results) == 1 && results[0].Deleted {
		response.Kv = &proto.KeyValue{
			Key: results[0].Key, Value: results[0].PrevValue, Revision: results[0].PrevRevision,
		}
	}
	return response, nil
}
