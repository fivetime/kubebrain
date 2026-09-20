package leasefault

import (
	"bytes"
	"context"
	"errors"
	"reflect"

	"github.com/kubewharf/kubebrain/hack/production/internal/imageprepull"
)

// AuthenticateCommandRelease downloads and authenticates release/CI evidence,
// then binds it to the exact independently approved command plan. It creates
// release evidence and runs gh, but sends no Kubernetes or fault RPC requests.
// This is a preflight, not continuing runtime admission: SourceHash, transitive
// tools, TLS identities and live process/network state still need verification.
func AuthenticateCommandRelease(ctx context.Context, commandPath, commandDigest, releasePath, releaseDigest string) error {
	return authenticateCommandRelease(ctx, commandPath, commandDigest, releasePath, releaseDigest, imageprepull.FetchReleasePlan)
}

func authenticateCommandRelease(ctx context.Context, commandPath, commandDigest, releasePath, releaseDigest string, fetch func(context.Context, string, string) (imageprepull.ReleaseEvidence, []byte, error)) error {
	if ctx == nil {
		return errors.New("release preflight requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := LoadNativeCommandPlan(commandPath, commandDigest)
	if err != nil {
		return err
	}
	receipt, index, err := fetch(ctx, releasePath, releaseDigest)
	if err != nil {
		return err
	}
	if err := p.matchAuthenticatedRelease(receipt, index); err != nil {
		return err
	}
	// A successful download must not conceal a command/input change while the
	// network requests were in flight. Never adopt a newly discovered digest.
	if _, err := LoadNativeCommandPlan(commandPath, commandDigest); err != nil {
		return err
	}
	return ctx.Err()
}

// Only use receipt/index obtained from the authenticated downloader. Matching
// arbitrary caller-provided JSON would establish consistency, not provenance.
func (p NativeCommandPlan) matchAuthenticatedRelease(receipt imageprepull.ReleaseEvidence, index []byte) error {
	if receipt.Source != p.Bindings.Source || receipt.Image != p.Bindings.Image || !bytes.Equal(index, p.Release.Index) || !reflect.DeepEqual(receipt.Platforms, p.Release.Reviewed) {
		return errors.New("authenticated release differs from command source, image, index or reviewed platforms")
	}
	return nil
}
