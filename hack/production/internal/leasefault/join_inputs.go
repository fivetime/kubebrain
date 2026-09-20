package leasefault

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
)

const IsolatedJoinScript = "join-isolated-fault-workers.sh"
const IsolatedJoinIdentity = "join-namespace.tsv"

var isolatedJoinRecord = regexp.MustCompile(`\Av1\t[0-9]+:[0-9]+\t[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\t[1-9][0-9]*\t[0-9]+:[0-9]+\n\z`)

// VerifyJoinInputs enforces the packaged isolated Join's additional private
// input. Both execution and recovery call it at every file-admission gate.
// Other independently audited scripts may have different input contracts;
// this does not authenticate script bytes, runtime isolation, or provenance.
// In particular, renaming a script does not make it independently admitted.
func VerifyJoinInputs(ctx context.Context, directory, script string, files map[string]string) error {
	if ctx == nil {
		return errors.New("join input verification requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if filepath.Base(script) != IsolatedJoinScript {
		return nil
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == "/" {
		return errors.New("invalid isolated Join owner directory")
	}
	path := filepath.Join(directory, IsolatedJoinIdentity)
	if !planinput.ValidSHA256(files[path]) {
		return errors.New("isolated Join requires pinned namespace identity")
	}
	data, err := planinput.ReadFile(path, true, 256)
	if err != nil {
		return err
	}
	if planinput.SHA256(data) != files[path] {
		return errors.New("isolated Join identity differs from admission")
	}
	if !isolatedJoinRecord.Match(data) {
		return errors.New("invalid isolated Join namespace record")
	}
	return ctx.Err()
}
