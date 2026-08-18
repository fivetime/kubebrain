package backend

import (
	"context"
	"errors"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

func (b *backend) transactionalCreate(ctx context.Context, put *proto.CreateRequest) (*proto.CreateResponse, error) {
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
	return &proto.CreateResponse{Header: responseHeader(revision), Succeeded: true}, nil
}

func (b *backend) transactionalUpdate(ctx context.Context, request *proto.UpdateRequest) (*proto.UpdateResponse, error) {
	return b.transactionalUpdateOnce(ctx, request, true)
}

func (b *backend) transactionalUpdateOnce(ctx context.Context, request *proto.UpdateRequest, allowHeal bool) (*proto.UpdateResponse, error) {
	key := request.GetKv().GetKey()
	if request.GetKv().GetRevision() == 0 {
		created, err := b.transactionalCreate(ctx, &proto.CreateRequest{
			Key: key, Value: request.GetKv().GetValue(), Lease: request.Lease,
		})
		if err != nil {
			return nil, err
		}
		response := &proto.UpdateResponse{Header: created.Header, Succeeded: created.Succeeded}
		if !created.Succeeded {
			value, modRevision, getErr := b.get(ctx, key, 0)
			if getErr == nil {
				response.Header.Revision = maxUint64(response.Header.Revision, modRevision)
				response.Kv = &proto.KeyValue{Key: key, Value: value, Revision: modRevision}
			}
		}
		return response, nil
	}
	op := TxnWriteOp{Key: key, Value: request.GetKv().GetValue(), Lease: request.Lease}
	if previousLease, known := previousLeaseFromContext(ctx); known {
		op.PrevLeaseKnown, op.PrevLease = true, previousLease
	}
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
		op,
	}, []TxnGuard{{Key: key, Revision: request.GetKv().GetRevision()}})
	if errors.Is(err, ErrTxnGuardConflict) {
		if allowHeal {
			healed, healErr := b.healOrphanIndex(ctx, key)
			if healErr != nil {
				return nil, healErr
			}
			if healed {
				return b.transactionalUpdateOnce(ctx, request, false)
			}
		}
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
	return &proto.UpdateResponse{Header: responseHeader(revision), Succeeded: true}, nil
}

func (b *backend) transactionalDelete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	return b.transactionalDeleteOnce(ctx, request, true)
}

func (b *backend) transactionalDeleteOnce(ctx context.Context, request *proto.DeleteRequest, allowHeal bool) (*proto.DeleteResponse, error) {
	guards := []TxnGuard(nil)
	if request.Revision > 0 {
		guards = []TxnGuard{{Key: request.Key, Revision: request.Revision}}
	}
	op := TxnWriteOp{Key: request.Key, Delete: true}
	if previousLease, known := previousLeaseFromContext(ctx); known {
		op.PrevLeaseKnown, op.PrevLease = true, previousLease
	}
	results, revision, err := b.TxnApply(ctx, []TxnWriteOp{op}, guards)
	if errors.Is(err, ErrTxnGuardConflict) {
		if allowHeal {
			healed, healErr := b.healOrphanIndex(ctx, request.Key)
			if healErr != nil {
				return nil, healErr
			}
			if healed {
				return b.transactionalDeleteOnce(ctx, request, false)
			}
		}
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
	if allowHeal && (len(results) != 1 || !results[0].Deleted) {
		healed, healErr := b.healOrphanIndex(ctx, request.Key)
		if healErr != nil {
			return nil, healErr
		}
		if healed {
			return b.transactionalDeleteOnce(ctx, request, false)
		}
	}
	response := &proto.DeleteResponse{Header: responseHeader(revision)}
	if len(results) == 1 && results[0].Deleted {
		response.Succeeded = true
		response.Kv = &proto.KeyValue{
			Key: results[0].Key, Value: results[0].PrevValue, Revision: results[0].PrevRevision,
		}
	}
	return response, nil
}
