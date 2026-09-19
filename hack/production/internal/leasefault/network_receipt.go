package leasefault

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"syscall"

	strictjson "sigs.k8s.io/json"
)

const networkReceiptFile = "network-reservation.json"

// os.Root may resolve an in-root symlink even with O_NOFOLLOW. Explicitly
// require the named entry to be regular and bind the opened file to that inode.
// This supplements, but does not replace, exclusive private-directory ownership.
func openRecoveryRecord(r *os.Root, name string) (*os.File, error) {
	before, err := r.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("non-regular recovery record")
	}
	f, err := r.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	opened, statErr := f.Stat()
	after, lstatErr := r.Lstat(name)
	if statErr != nil || lstatErr != nil || !after.Mode().IsRegular() ||
		!os.SameFile(before, opened) || !os.SameFile(opened, after) {
		f.Close()
		return nil, errors.New("recovery record changed while opening")
	}
	return f, nil
}

// validateReservation checks an actual inactive CREATE response against admission.
// It cannot establish provenance: callers must pass the authenticated API response,
// never a GET of a same-name object, dry-run response, or synthesized receipt.
func validateReservation(plan NetworkRecovery, raw json.RawMessage) error {
	bad := errors.New("invalid network reservation receipt")
	if _, err := plan.encoded(); err != nil {
		return err
	}
	if len(raw) == 0 || len(raw) > networkRecoveryLimit {
		return bad
	}
	var receipt, approved map[string]interface{}
	strict, err := strictjson.UnmarshalStrict(raw, &receipt, strictjson.DisallowDuplicateFields)
	if err != nil || len(strict) != 0 || receipt == nil {
		return bad
	}
	if err := strictjson.UnmarshalCaseSensitivePreserveInts(plan.ApprovedPolicy, &approved); err != nil {
		return bad
	}
	metadata, ok := receipt["metadata"].(map[string]interface{})
	if !ok {
		return bad
	}
	uid, uidOK := metadata["uid"].(string)
	rv, rvOK := metadata["resourceVersion"].(string)
	if receipt["apiVersion"] != "cilium.io/v2" || receipt["kind"] != "CiliumNetworkPolicy" ||
		metadata["namespace"] != plan.Namespace || metadata["name"] != plan.PolicyName ||
		!uidOK || uid == "" || !rvOK || rv == "" || metadata["deletionTimestamp"] != nil || receipt["specs"] != nil {
		return bad
	}
	// The intent validator guarantees this shape; retain checked assertions here
	// so future schema changes cannot introduce a panic in the recovery path.
	spec, ok := approved["spec"].(map[string]interface{})
	if !ok {
		return bad
	}
	selector, ok := spec["endpointSelector"].(map[string]interface{})
	if !ok {
		return bad
	}
	labels, ok := selector["matchLabels"].(map[string]interface{})
	if !ok {
		return bad
	}
	labels["kubebrain.io/fault-owner"] = plan.ReservedNonce
	if !reflect.DeepEqual(receipt["spec"], spec) {
		return bad
	}
	return nil
}

// SaveNetworkReservation must succeed after inactive CREATE and before activation.
// A failed/ambiguous CREATE or failed save requires reconciliation, not a retry
// with a fabricated receipt or an unconditional same-name delete. The parent must
// hold exclusive lifecycle ownership throughout these calls and all API writes.
func SaveNetworkReservation(dir string, expected NetworkRecovery, receipt json.RawMessage) error {
	if _, err := LoadNetworkRecovery(dir, expected); err != nil {
		return err
	}
	if err := validateReservation(expected, receipt); err != nil {
		return err
	}
	r, err := recoveryRoot(dir)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := r.OpenFile(networkReceiptFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(receipt)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	d, err := r.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// LoadNetworkReservation validates the private receipt and the independently bound
// intent. Fresh namespace/policy identity and resourceVersion observations are
// still required for a preconditioned delete; this is not cleanup authorization.
func LoadNetworkReservation(dir string, expected NetworkRecovery) (json.RawMessage, error) {
	if _, err := LoadNetworkRecovery(dir, expected); err != nil {
		return nil, err
	}
	r, err := recoveryRoot(dir)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	f, err := openRecoveryRecord(r, networkReceiptFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > networkRecoveryLimit {
		return nil, errors.New("unsafe network reservation record")
	}
	data, err := io.ReadAll(io.LimitReader(f, networkRecoveryLimit+1))
	if err != nil {
		return nil, err
	}
	if err := validateReservation(expected, data); err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}
