package leasefault

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"syscall"
	"unicode/utf8"

	strictjson "sigs.k8s.io/json"
)

const protocolRecoveryFile = "protocol-recovery.json"

// ProtocolRecovery is armed before lease grant, key put, or alarm activation.
// Recovery must conservatively reconcile all three even if the child never
// reported attempting them. It contains no credentials or saved process IDs.
// Network-policy and Pod-label recovery require separate identity records.
type ProtocolRecovery struct {
	Owner          string `json:"owner"`
	NamespaceUID   string `json:"namespace_uid"`
	StatefulSetUID string `json:"statefulset_uid"`
	ClusterID      uint64 `json:"cluster_id,string"`
	AlarmMemberID  uint64 `json:"alarm_member_id,string"`
	LeaseID        int64  `json:"lease_id,string"`
	Key            string `json:"key"`
}

var recoveryIdentity = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func (p ProtocolRecovery) valid() bool {
	return recoveryIdentity.MatchString(p.Owner) && recoveryIdentity.MatchString(p.NamespaceUID) &&
		recoveryIdentity.MatchString(p.StatefulSetUID) && p.ClusterID != 0 && p.AlarmMemberID != 0 && p.LeaseID != 0 &&
		strings.HasPrefix(p.Key, "/acceptance/") && len(p.Key) <= 1024 && utf8.ValidString(p.Key) && !strings.ContainsAny(p.Key, "\x00\r\n")
}

func recoveryRoot(dir string) (*os.Root, error) {
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("recovery owner must be a private directory")
	}
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	opened, err := r.Stat(".")
	if err != nil || !os.SameFile(st, opened) {
		r.Close()
		return nil, errors.New("recovery owner changed")
	}
	return r, nil
}

// ArmProtocolRecovery durably writes a create-once intent in an independently
// admitted private owner directory. Callers MUST NOT mutate the cluster unless
// this returns nil. On any error, preserve the file and stop: never overwrite or
// infer that a failed write means an earlier attempt performed no mutations.
// Successful persistence is not admission, a lock, or proof of recovery.
func ArmProtocolRecovery(dir string, plan ProtocolRecovery) error {
	if !plan.valid() {
		return errors.New("invalid protocol recovery identity")
	}
	r, err := recoveryRoot(dir)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := r.OpenFile(protocolRecoveryFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	writeErr := json.NewEncoder(f).Encode(plan)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	d, err := r.Open(".")
	if err != nil {
		return err
	}
	err = d.Sync()
	return errors.Join(err, d.Close())
}

// LoadProtocolRecovery checks the record against independently retained
// admission. Do not derive expected from this record or from a response under
// test. A malformed/missing record requires stopping for manual reconciliation,
// not skipping recovery. A valid record must still be checked against live
// cluster identities before any RPC; restore only after fault workers join.
func LoadProtocolRecovery(dir string, expected ProtocolRecovery) (ProtocolRecovery, error) {
	bad := errors.New("invalid protocol recovery record")
	if !expected.valid() {
		return ProtocolRecovery{}, bad
	}
	r, err := recoveryRoot(dir)
	if err != nil {
		return ProtocolRecovery{}, err
	}
	defer r.Close()
	f, err := r.OpenFile(protocolRecoveryFile, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return ProtocolRecovery{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 4096 {
		return ProtocolRecovery{}, bad
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(data) > 4096 {
		return ProtocolRecovery{}, bad
	}
	var plan ProtocolRecovery
	strict, err := strictjson.UnmarshalStrict(data, &plan, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil || len(strict) != 0 || !plan.valid() || plan != expected {
		return ProtocolRecovery{}, bad
	}
	return plan, nil
}
