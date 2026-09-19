package leasefault

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"syscall"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	strictjson "sigs.k8s.io/json"
)

const faultOwnerName = "kubebrain-fault-owner"
const ownerIntentFile = "fault-owner-intent.json"
const ownerReceiptFile = "fault-owner-receipt.json"

var ownerResource = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

type FaultOwnerBinding struct {
	Owner           string `json:"owner"`
	Namespace       string `json:"namespace"`
	NamespaceUID    string `json:"namespace_uid"`
	StatefulSetName string `json:"statefulset_name"`
	StatefulSetUID  string `json:"statefulset_uid"`
}

// FaultOwner is a non-expiring cooperative cluster claim, not a process lock or
// protection against administrators. All dedicated-test controllers must use the
// same fixed namespace-scoped name. No age-based takeover, adoption or renewal is
// provided; crashes leave the claim for explicit post-join reconciliation.
type FaultOwner struct {
	client    dynamic.Interface
	directory string
	binding   FaultOwnerBinding
	uid       string
}

func (b FaultOwnerBinding) valid() bool {
	for _, s := range []string{b.Owner, b.Namespace, b.NamespaceUID, b.StatefulSetName, b.StatefulSetUID} {
		if !recoveryIdentity.MatchString(s) {
			return false
		}
	}
	return true
}
func (b FaultOwnerBinding) data() map[string]interface{} {
	return map[string]interface{}{"owner": b.Owner, "namespace_uid": b.NamespaceUID, "statefulset_name": b.StatefulSetName, "statefulset_uid": b.StatefulSetUID}
}
func ownerContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("owner requires bounded context")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Minute {
		return errors.New("owner requires bounded context")
	}
	return ctx.Err()
}
func ownerScope(ctx context.Context, c dynamic.Interface, b FaultOwnerBinding) error {
	if c == nil || !b.valid() {
		return errors.New("invalid owner binding")
	}
	if err := ownerContext(ctx); err != nil {
		return err
	}
	for _, scope := range []struct {
		resource                 schema.GroupVersionResource
		ns, name, uid, kind, api string
	}{
		{schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, "", b.Namespace, b.NamespaceUID, "Namespace", "v1"},
		{schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}, b.Namespace, b.StatefulSetName, b.StatefulSetUID, "StatefulSet", "apps/v1"},
	} {
		u, err := c.Resource(scope.resource).Namespace(scope.ns).Get(ctx, scope.name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if u == nil || u.GetAPIVersion() != scope.api || u.GetKind() != scope.kind || u.GetNamespace() != scope.ns || u.GetName() != scope.name || string(u.GetUID()) != scope.uid || u.GetResourceVersion() == "" || u.GetDeletionTimestamp() != nil {
			return errors.New("owner scope changed")
		}
	}
	return ctx.Err()
}

// AcquireFaultOwner persists intent before exactly one CREATE, then its actual
// receipt before returning a usable owner. Ambiguous CREATE or persistence errors
// must stop the attempt; never adopt a GET by matching owner text alone.
func AcquireFaultOwner(ctx context.Context, c dynamic.Interface, dir string, b FaultOwnerBinding) (*FaultOwner, error) {
	if err := ownerScope(ctx, c, b); err != nil {
		return nil, err
	}
	if err := writeOwnerRecord(dir, ownerIntentFile, b); err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": faultOwnerName, "namespace": b.Namespace}, "immutable": true, "data": b.data()}}
	created, err := c.Resource(ownerResource).Namespace(b.Namespace).Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	if err := validateOwnerObject(created, b, ""); err != nil {
		return nil, err
	}
	if err := writeOwnerRecord(dir, ownerReceiptFile, created.Object); err != nil {
		return nil, err
	}
	o, err := LoadFaultOwner(c, dir, b)
	if err != nil {
		return nil, err
	}
	if err := o.Check(ctx); err != nil {
		return nil, err
	}
	return o, nil
}

func validateOwnerObject(u *unstructured.Unstructured, b FaultOwnerBinding, uid string) error {
	if u == nil || u.GetAPIVersion() != "v1" || u.GetKind() != "ConfigMap" || u.GetName() != faultOwnerName || u.GetNamespace() != b.Namespace || u.GetUID() == "" || u.GetResourceVersion() == "" || u.GetDeletionTimestamp() != nil || len(u.GetOwnerReferences()) != 0 || u.Object["immutable"] != true || !reflect.DeepEqual(u.Object["data"], b.data()) || u.Object["binaryData"] != nil {
		return errors.New("invalid fault owner claim")
	}
	if uid != "" && string(u.GetUID()) != uid {
		return errors.New("fault owner claim replaced")
	}
	return nil
}

func LoadFaultOwner(c dynamic.Interface, dir string, b FaultOwnerBinding) (*FaultOwner, error) {
	if c == nil || !b.valid() {
		return nil, errors.New("invalid owner binding")
	}
	var saved FaultOwnerBinding
	if err := readOwnerRecord(dir, ownerIntentFile, &saved); err != nil {
		return nil, err
	}
	if saved != b {
		return nil, errors.New("owner intent differs from admission")
	}
	var object map[string]interface{}
	if err := readOwnerRecord(dir, ownerReceiptFile, &object); err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: object}
	if err := validateOwnerObject(u, b, ""); err != nil {
		return nil, err
	}
	return &FaultOwner{client: c, directory: dir, binding: b, uid: string(u.GetUID())}, nil
}

func (o *FaultOwner) Check(ctx context.Context) error {
	if o == nil {
		return errors.New("missing fault owner")
	}
	fresh, err := LoadFaultOwner(o.client, o.directory, o.binding)
	if err != nil {
		return err
	}
	if fresh.uid != o.uid {
		return errors.New("owner receipt replaced")
	}
	if err := ownerScope(ctx, o.client, o.binding); err != nil {
		return err
	}
	u, err := o.client.Resource(ownerResource).Namespace(o.binding.Namespace).Get(ctx, faultOwnerName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := validateOwnerObject(u, o.binding, o.uid); err != nil {
		return err
	}
	return ctx.Err()
}

// Release requires fresh external proof of joined workers and complete recovery.
// No automatic defer-release: a failed recovery must keep this claim. Both server
// preconditions bind the exact object read. Records remain for audit/reconciliation.
func (o *FaultOwner) Release(ctx context.Context, recovered func(context.Context) error) error {
	if recovered == nil {
		return errors.New("recovery proof required before release")
	}
	if err := o.Check(ctx); err != nil {
		return err
	}
	if err := recovered(ctx); err != nil {
		return err
	}
	if err := o.Check(ctx); err != nil {
		return err
	}
	r := o.client.Resource(ownerResource).Namespace(o.binding.Namespace)
	u, err := r.Get(ctx, faultOwnerName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := validateOwnerObject(u, o.binding, o.uid); err != nil {
		return err
	}
	uid, rv := u.GetUID(), u.GetResourceVersion()
	if err := r.Delete(ctx, faultOwnerName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil {
		return err
	}
	_, err = r.Get(ctx, faultOwnerName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	return errors.New("fault owner claim remains after delete")
}

func writeOwnerRecord(dir, name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > networkRecoveryLimit {
		return errors.New("owner record too large")
	}
	r, err := recoveryRoot(dir)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := r.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
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
func readOwnerRecord(dir, name string, value any) error {
	r, err := recoveryRoot(dir)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := openRecoveryRecord(r, name)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Mode().Perm()&0077 != 0 || st.Size() > networkRecoveryLimit {
		return errors.New("unsafe owner record")
	}
	data, err := io.ReadAll(io.LimitReader(f, networkRecoveryLimit+1))
	if err != nil {
		return err
	}
	if len(data) > networkRecoveryLimit {
		return errors.New("owner record too large")
	}
	strict, err := strictjson.UnmarshalStrict(data, value, strictjson.DisallowDuplicateFields, strictjson.DisallowUnknownFields)
	if err != nil {
		return err
	}
	if len(strict) != 0 {
		return errors.New("invalid owner record")
	}
	return nil
}
