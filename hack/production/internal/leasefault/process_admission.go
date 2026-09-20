package leasefault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	strictjson "sigs.k8s.io/json"
)

// CheckLivePodProcess compares a fresh Pod GET with an independently admitted
// snapshot using the repository's canonical same-pod-process.jq predicate.
// admit must pin jq and the predicate's contents and authenticate the snapshot;
// this function does not establish image provenance, term, TLS or ownership.
// It permits readiness changes, not a new container, restart, spec or Pod IP.
// retain must preserve its supplied JSON (including failures) durably. There is
// one GET and one child, no retry or new deadline; callers repeat admission at
// the required lifecycle boundaries, as a GET is not a process lock.
func CheckLivePodProcess(ctx context.Context, client dynamic.Interface, expected []byte, jq, predicate string, admit func(context.Context) error, retain func([]byte, error) error) error {
	if err := ownerContext(ctx); err != nil {
		return err
	}
	if client == nil || admit == nil || retain == nil || len(expected) == 0 || len(expected) > 256<<10 || !filepath.IsAbs(predicate) || filepath.Clean(predicate) != predicate {
		return errors.New("incomplete live process admission")
	}
	var before unstructured.Unstructured
	strict, err := strictjson.UnmarshalStrict(expected, &before.Object, strictjson.DisallowDuplicateFields)
	if err != nil || len(strict) != 0 || before.GetAPIVersion() != "v1" || before.GetKind() != "Pod" || !recoveryIdentity.MatchString(before.GetNamespace()) || !recoveryIdentity.MatchString(before.GetName()) || before.GetUID() == "" {
		return errors.New("invalid admitted process snapshot")
	}
	if err := processgroup.ValidateExecutable(jq); err != nil {
		return err
	}
	if err := admit(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, getErr := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(before.GetNamespace()).Get(ctx, before.GetName(), metav1.GetOptions{})
	input, err := json.Marshal(map[string]any{"expected": &before, "current": current})
	if err != nil {
		return errors.Join(getErr, err)
	}
	if len(input) > 768<<10 {
		return errors.Join(getErr, errors.New("oversized live process snapshot"))
	}
	var output []byte
	observed := getErr
	// Process bytes can remain unchanged while Kubernetes ownership changes.
	// The independently admitted owner references must survive this live read;
	// the jq predicate deliberately focuses on process identity, not ownership.
	if observed == nil && (current == nil || !reflect.DeepEqual(before.GetOwnerReferences(), current.GetOwnerReferences())) {
		observed = errors.New("live Pod owner references differ from admission")
	}
	if observed == nil {
		cmd := exec.CommandContext(ctx, jq, "-e", "-f", predicate)
		cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin"}
		cmd.Stdin = bytes.NewReader(input)
		processgroup.Configure(cmd)
		cmd.WaitDelay = processgroup.DefaultWaitDelay
		output, observed = processgroup.CombinedOutput(cmd, 64<<10)
		if observed == nil && !bytes.Equal(output, []byte("true\n")) {
			observed = errors.New("invalid process predicate result")
		}
	}
	observed = errors.Join(observed, ctx.Err())
	evidence, err := json.Marshal(struct {
		Input  json.RawMessage
		Output []byte
	}{input, output})
	if err != nil {
		return errors.Join(observed, err)
	}
	if err := errors.Join(observed, retain(evidence, observed), ctx.Err()); err != nil {
		return err
	}
	err = admit(ctx)
	return errors.Join(err, ctx.Err())
}
