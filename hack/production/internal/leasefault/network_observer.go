package leasefault

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// NetworkObserver binds the repository's composite read-only script to lifecycle
// hooks. Admit must check exclusive ownership AND the admitted script/dependency
// bundle, explicit environment and its kubeconfig/cluster binding to Client.
// Retain must durably record every output/status privately. Neither is optional.
// Inputs observer-pod.json and observer-targets.json must already be private files
// in Directory; the former exactly matches independent Network.PodBefore and the
// latter matches the independently admitted TargetsSHA256, not a discovered hash.
type NetworkObserver struct {
	Directory, StatefulSetName, ScriptDirectory, TargetsSHA256 string
	Network                                                    NetworkRecovery
	Client                                                     dynamic.Interface
	Env                                                        []string
	Admit                                                      func(context.Context) error
	Retain                                                     func(string, []byte, error) error
	// Only the concrete runtime may separate source checks from live admission.
	nonceTools    func(context.Context) error
	preparedTools func(context.Context) error
	faultTools    func(context.Context) error
}

// Prepared verifies the owned-label identity and absence of the reserved policy
// from the endpoint plus backend TCP. PrepareFault separately verifies the live
// API policy is the recorded inactive reservation; this method cannot replace it.
func (o NetworkObserver) Prepared(ctx context.Context) (err error) {
	if ctx == nil {
		return errors.New("prepared observation requires context")
	}
	if o.preparedTools != nil {
		if err := o.preparedTools(ctx); err != nil {
			return err
		}
		defer func() { err = errors.Join(err, o.preparedTools(ctx), ctx.Err()) }()
	}
	return o.observe(ctx, "prepared", NetworkLabelOwned)
}

// Active observes only policy realization, never backend TCP reachability.
// It verifies the exact owned active API policy on every pending observation
// and uses the SAME original fault deadline. Packet drops remain a separate gate.
func (o NetworkObserver) Active(ctx context.Context, origin time.Time) error {
	return o.observeFault(ctx, "active", origin, 0)
}

// CheckActive refreshes ownership, Pod identity, the exact active API policy and
// frozen inputs without running a dataplane script. It is suitable for the
// network part of FaultObservation.CheckIsolation before/after each Status read.
// It does not establish Cilium realization; Active and Drops remain mandatory.
func (o NetworkObserver) CheckActive(ctx context.Context, origin time.Time) error {
	return o.observeFault(ctx, "check-active", origin, 0)
}

// Drops captures once (including on exit 75), binds both backend classes and
// retains the result before accepting it. durationSeconds is the remote capture
// interval, not a fresh budget. The full child is supervised under ctx.
// Use with FaultObservation.Active and independent successor checks; packets
// alone neither establish term loss nor the original request's outcome.
func (o NetworkObserver) Drops(ctx context.Context, origin time.Time, durationSeconds int) error {
	if durationSeconds < 1 || durationSeconds > 9 {
		return errors.New("drop capture duration must be 1..9 seconds")
	}
	return o.observeFault(ctx, "drops", origin, durationSeconds)
}

func (o NetworkObserver) observeFault(ctx context.Context, stage string, origin time.Time, durationSeconds int) (err error) {
	if ctx == nil || origin.IsZero() || origin.After(time.Now()) {
		return errors.New("invalid network fault clock")
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(origin.Add(30*time.Second)) {
		return errors.New("network fault observation needs original fault deadline")
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	if o.faultTools != nil {
		if err := o.faultTools(ctx); err != nil {
			return err
		}
		defer func() { err = errors.Join(err, o.faultTools(ctx), ctx.Err()) }()
	}
	err = o.observeCapture(ctx, stage, NetworkLabelOwned, origin, durationSeconds)
	if !time.Now().Before(deadline) {
		return errors.Join(err, context.DeadlineExceeded)
	}
	return err
}

// Restored chooses only between absent and this owner's label using fresh API
// identity checks. A label change during an attempt is fatal, not a mode retry.
func (o NetworkObserver) Restored(ctx context.Context) error {
	return o.observe(ctx, "restored", NetworkLabelRecovery)
}

func (o NetworkObserver) Unlabelled(ctx context.Context) error {
	return o.observe(ctx, "unlabelled", NetworkUnlabelled)
}

func (o NetworkObserver) observe(ctx context.Context, stage string, phase NetworkLabelPhase) error {
	return o.observeCapture(ctx, stage, phase, time.Time{}, 0)
}

func (o NetworkObserver) observeCapture(ctx context.Context, stage string, phase NetworkLabelPhase, origin time.Time, durationSeconds int) error {
	if o.Client == nil || o.Admit == nil || o.Retain == nil || !filepath.IsAbs(o.ScriptDirectory) || filepath.Clean(o.ScriptDirectory) != o.ScriptDirectory {
		return errors.New("incomplete network observer binding")
	}
	// Freeze slice-backed admission for the duration of all pending observations.
	o.Network.PodBefore = bytes.Clone(o.Network.PodBefore)
	o.Network.ApprovedPolicy = bytes.Clone(o.Network.ApprovedPolicy)
	o.Env = append([]string{}, o.Env...)
	wanted, err := hex.DecodeString(o.TargetsSHA256)
	if err != nil || len(wanted) != sha256.Size || hex.EncodeToString(wanted) != o.TargetsSHA256 {
		return errors.New("invalid admitted targets digest")
	}
	if err := CheckNetworkIdentity(ctx, o.Client, o.Network, o.StatefulSetName, phase, o.Admit); err != nil {
		return err
	}
	if phase == NetworkLabelRecovery {
		pod, err := o.Client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(o.Network.Namespace).Get(ctx, o.Network.PodName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pod == nil || string(pod.GetUID()) != o.Network.PodUID {
			return errors.New("observer Pod replaced")
		}
		label, exists := pod.GetLabels()["kubebrain.io/fault-owner"]
		if !exists {
			phase = NetworkUnlabelled
		} else if label == o.Network.Nonce {
			phase = NetworkLabelOwned
		} else {
			return errors.New("observer label is foreign")
		}
	}
	receipt, err := LoadNetworkReservation(o.Directory, o.Network)
	if err != nil {
		return err
	}
	var policy unstructured.Unstructured
	if err := policy.UnmarshalJSON(receipt); err != nil {
		return err
	}
	inputs := func() error {
		r, err := recoveryRoot(o.Directory)
		if err != nil {
			return err
		}
		defer r.Close()
		for _, name := range []string{"observer-pod.json", "observer-targets.json"} {
			f, err := openRecoveryRecord(r, name)
			if err != nil {
				return err
			}
			st, err := f.Stat()
			if err != nil || st.Mode().Perm()&0077 != 0 || st.Size() > networkRecoveryLimit {
				f.Close()
				return errors.New("invalid observer input file")
			}
			data, readErr := io.ReadAll(io.LimitReader(f, networkRecoveryLimit+1))
			closeErr := f.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				return err
			}
			if len(data) > networkRecoveryLimit {
				return errors.New("oversized observer input")
			}
			if name == "observer-pod.json" {
				if !bytes.Equal(data, o.Network.PodBefore) {
					return errors.New("observer Pod input differs from admission")
				}
			} else {
				digest := sha256.Sum256(data)
				if !bytes.Equal(digest[:], wanted) {
					return errors.New("observer targets input differs from admission")
				}
			}
		}
		return nil
	}
	admit := func(ctx context.Context) error {
		if err := CheckNetworkIdentity(ctx, o.Client, o.Network, o.StatefulSetName, phase, o.Admit); err != nil {
			return err
		}
		current, err := LoadNetworkReservation(o.Directory, o.Network)
		if err != nil {
			return err
		}
		if !bytes.Equal(receipt, current) {
			return errors.New("observer reservation changed")
		}
		if stage == "active" || stage == "drops" || stage == "check-active" {
			active, err := o.Client.Resource(schema.GroupVersionResource{Group: "cilium.io", Version: "v2", Resource: "ciliumnetworkpolicies"}).Namespace(o.Network.Namespace).Get(ctx, o.Network.PolicyName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if active == nil || active.GetUID() != policy.GetUID() {
				return errors.New("active policy replaced")
			}
			validated := active.DeepCopy()
			label, found, err := unstructured.NestedString(validated.Object, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner")
			if err != nil || !found || label != o.Network.Nonce {
				return errors.New("policy is not the owned active selector")
			}
			if err := unstructured.SetNestedField(validated.Object, o.Network.ReservedNonce, "spec", "endpointSelector", "matchLabels", "kubebrain.io/fault-owner"); err != nil {
				return err
			}
			raw, err := validated.MarshalJSON()
			if err != nil {
				return err
			}
			if err := validateReservation(o.Network, raw); err != nil {
				return err
			}
		}
		return inputs()
	}
	if stage == "check-active" {
		return admit(ctx)
	}
	mode := "absent"
	if phase == NetworkUnlabelled {
		mode = "absent-unlabelled"
	}
	args := []string{filepath.Join(o.ScriptDirectory, "observe-local-network-restored.sh"), o.Directory, mode, string(policy.GetUID()), o.Network.PolicyName, filepath.Join(o.Directory, "observer-pod.json"), filepath.Join(o.Directory, "observer-targets.json")}
	if stage == "active" {
		args = []string{filepath.Join(o.ScriptDirectory, "observe-local-policy-state.sh"), o.Directory, "present", string(policy.GetUID()), o.Network.PolicyName, filepath.Join(o.Directory, "observer-pod.json")}
	}
	if stage == "drops" {
		if err := admit(ctx); err != nil {
			return err
		}
		args = []string{filepath.Join(o.ScriptDirectory, "observe-local-backend-drops.sh"), o.Directory, filepath.Join(o.Directory, "observer-pod.json"), filepath.Join(o.Directory, "observer-targets.json"), strconv.Itoa(durationSeconds), strconv.FormatInt(origin.UnixNano(), 10)}
		out, observed := RunRecoveryObserver(ctx, "/bin/bash", args, o.Env)
		if observed == nil {
			// Exact output from the admitted child, never accept an arbitrary
			// exit-zero wrapper or a marker embedded in unrelated log output.
			lines := strings.Split(string(out), "\n")
			if len(lines) != 3 || lines[2] != "" || lines[1] != "SAME_SOURCE_PD_AND_TIKV_POLICY_DROPS_NOT_TERM_OR_RPC_PROOF" || !strings.HasPrefix(lines[0], "EVIDENCE=") {
				observed = errors.New("invalid drop observation completion")
			} else {
				path := strings.TrimPrefix(lines[0], "EVIDENCE=")
				if filepath.Dir(path) != o.Directory || !regexp.MustCompile(`^backend-drops\.[A-Za-z0-9]{8}$`).MatchString(filepath.Base(path)) {
					observed = errors.New("drop evidence outside owned directory")
				} else {
					observed = checkDropReceipt(path, origin)
				}
			}
		}
		if err := o.Retain(stage, out, observed); err != nil {
			return errors.Join(err, observed, ctx.Err())
		}
		if err := errors.Join(observed, ctx.Err()); err != nil {
			return err
		}
		return admit(ctx)
	}
	err = WaitRecoveryObserver(ctx, "/bin/bash", args, o.Env, admit, func(out []byte, observed error) error { return o.Retain(stage, out, observed) })
	if err != nil {
		return err
	}
	// A matched script cannot mask changed admission or inputs on its return.
	return admit(ctx)
}

// Read only fixed private receipt names after the actual child has joined.
// This checks consistency of the admitted producer, not imported evidence.
func checkDropReceipt(path string, origin time.Time) error {
	r, err := recoveryRoot(path)
	if err != nil {
		return err
	}
	defer r.Close()
	for name, expected := range map[string]string{"observation.exit": "0\n", "origin": strconv.FormatInt(origin.UnixNano(), 10) + "\n"} {
		f, err := openRecoveryRecord(r, name)
		if err != nil {
			return err
		}
		st, err := f.Stat()
		if err != nil || st.Mode().Perm()&0077 != 0 || st.Size() > 63 {
			f.Close()
			return errors.New("invalid private drop receipt")
		}
		data, readErr := io.ReadAll(io.LimitReader(f, 64))
		closeErr := f.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		if string(data) != expected {
			return errors.New("drop observation receipt mismatch")
		}
	}
	return nil
}
