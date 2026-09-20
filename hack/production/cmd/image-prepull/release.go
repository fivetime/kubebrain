package main

import (
	"context"
	"encoding/json"
	"io"

	"github.com/kubewharf/kubebrain/hack/production/internal/imageprepull"
)

// Keep the existing command plan wire format and command fixtures unchanged.
type releaseDownloadPlan = imageprepull.ReleaseDownloadPlan

func fetchRelease(ctx context.Context, path, approved string, out io.Writer) error {
	receipt, _, err := imageprepull.FetchReleasePlan(ctx, path, approved)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Release imageprepull.ReleaseEvidence `json:"release"`
		Scope   string                       `json:"scope"`
	}{receipt, "authenticated release and regression evidence only; not deployment or fault acceptance"})
}
