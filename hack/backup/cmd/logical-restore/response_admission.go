package main

import (
	"errors"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type restoreResponseAdmission struct {
	clusterID uint64
	revision  int64
}

func (a *restoreResponseAdmission) admitHeader(header *etcdserverpb.ResponseHeader, mutation bool) (int64, error) {
	if header == nil || header.ClusterId == 0 || header.MemberId == 0 || header.Revision <= 0 || header.Revision < a.revision {
		return 0, errors.New("response returned an invalid or stale header")
	}
	if a.clusterID != 0 && header.ClusterId != a.clusterID {
		return 0, fmt.Errorf("response cluster ID changed from %d to %d", a.clusterID, header.ClusterId)
	}
	if mutation && header.Revision <= a.revision {
		return 0, errors.New("mutation response revision did not advance")
	}
	if a.clusterID == 0 {
		a.clusterID = header.ClusterId
	}
	a.revision = header.Revision
	return header.Revision, nil
}

func (a *restoreResponseAdmission) admitNestedHeader(header *etcdserverpb.ResponseHeader, memberID uint64, revision int64) error {
	if header == nil {
		return nil
	}
	if header.Revision != revision {
		return errors.New("nested response revision differs from transaction header")
	}
	if header.ClusterId == 0 && header.MemberId == 0 {
		return nil
	}
	if header.ClusterId != a.clusterID || header.MemberId != memberID {
		return errors.New("nested response identity differs from transaction header")
	}
	return nil
}

func (a *restoreResponseAdmission) admitGrant(response *clientv3.LeaseGrantResponse) error {
	if response == nil {
		return errors.New("target returned an empty lease grant response")
	}
	_, err := a.admitHeader(response.ResponseHeader, false)
	return err
}

func (a *restoreResponseAdmission) admitPreflight(response *clientv3.TxnResponse) error {
	if response == nil {
		return errors.New("target preflight returned an empty transaction response")
	}
	revision, err := a.admitHeader(response.Header, false)
	if err != nil {
		return err
	}
	for i, op := range response.Responses {
		if op == nil || op.GetResponseRange() == nil {
			return fmt.Errorf("target preflight response %d is not a range response", i)
		}
		if err := a.admitNestedHeader(op.GetResponseRange().Header, response.Header.MemberId, revision); err != nil {
			return fmt.Errorf("target preflight range response %d: %w", i, err)
		}
	}
	return nil
}

func (a *restoreResponseAdmission) admitPut(response *clientv3.TxnResponse) (int64, error) {
	if response == nil {
		return 0, errors.New("restore batch returned an empty transaction response")
	}
	revision, err := a.admitHeader(response.Header, true)
	if err != nil {
		return 0, err
	}
	for i, op := range response.Responses {
		if op == nil || op.GetResponsePut() == nil {
			return 0, fmt.Errorf("restore batch response %d is not a put response", i)
		}
		if err := a.admitNestedHeader(op.GetResponsePut().Header, response.Header.MemberId, revision); err != nil {
			return 0, fmt.Errorf("restore batch put response %d: %w", i, err)
		}
	}
	return revision, nil
}

func (a *restoreResponseAdmission) admitRollback(response *clientv3.TxnResponse) error {
	if response == nil {
		return errors.New("restore rollback returned an empty transaction response")
	}
	revision, err := a.admitHeader(response.Header, true)
	if err != nil {
		return err
	}
	for i, op := range response.Responses {
		if op == nil || op.GetResponseDeleteRange() == nil {
			return fmt.Errorf("restore rollback response %d is not a delete response", i)
		}
		if err := a.admitNestedHeader(op.GetResponseDeleteRange().Header, response.Header.MemberId, revision); err != nil {
			return fmt.Errorf("restore rollback delete response %d: %w", i, err)
		}
	}
	return nil
}

func (a *restoreResponseAdmission) admitRevoke(response *clientv3.LeaseRevokeResponse) error {
	if response == nil {
		return errors.New("lease cleanup returned an empty revoke response")
	}
	_, err := a.admitHeader(response.Header, false)
	return err
}

func validateAndAdmitRestorePut(response *clientv3.TxnResponse, expectedPuts int, keys []string, recordForRollback bool, committed *[]committedBatch, admission *restoreResponseAdmission) (int64, error) {
	revision, err := validateRestorePutTxnResponse(response, expectedPuts)
	if err != nil {
		return 0, err
	}
	if recordForRollback {
		*committed = append(*committed, committedBatch{keys: append([]string(nil), keys...), revision: revision})
	}
	admittedRevision, err := admission.admitPut(response)
	if err != nil {
		return 0, fmt.Errorf("restore batch response admission: %w", err)
	}
	return admittedRevision, nil
}
