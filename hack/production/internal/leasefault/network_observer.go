package leasefault

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"

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
}

// Prepared verifies the owned-label identity and absence of the reserved policy
// from the endpoint plus backend TCP. PrepareFault separately verifies the live
// API policy is the recorded inactive reservation; this method cannot replace it.
func (o NetworkObserver) Prepared(ctx context.Context) error {
	return o.observe(ctx, "prepared", NetworkLabelOwned)
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
		return inputs()
	}
	mode := "absent"
	if phase == NetworkUnlabelled {
		mode = "absent-unlabelled"
	}
	args := []string{filepath.Join(o.ScriptDirectory, "observe-local-network-restored.sh"), o.Directory, mode, string(policy.GetUID()), o.Network.PolicyName, filepath.Join(o.Directory, "observer-pod.json"), filepath.Join(o.Directory, "observer-targets.json")}
	err = WaitRecoveryObserver(ctx, "/bin/bash", args, o.Env, admit, func(out []byte, observed error) error { return o.Retain(stage, out, observed) })
	if err != nil {
		return err
	}
	// A matched script cannot mask changed admission or inputs on its return.
	return admit(ctx)
}
