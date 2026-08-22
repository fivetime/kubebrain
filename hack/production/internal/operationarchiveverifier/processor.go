package operationarchiveverifier

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditbuilder"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type CommandRunner func(context.Context, string, []string) error

type Processor struct {
	executor          string
	objectStoreID     string
	bucket            string
	prefix            string
	retentionMode     string
	retentionDuration time.Duration
	now               func() time.Time
	run               CommandRunner
}

func NewProcessor(
	executor, objectStoreID, bucket, prefix, retentionMode string, retentionDuration time.Duration,
) (*Processor, error) {
	prefix = strings.Trim(prefix, "/")
	if executor == "" || objectStoreID == "" || bucket == "" || prefix == "" {
		return nil, errors.New("operation archive verifier configuration is incomplete")
	}
	if retentionMode != "COMPLIANCE" && retentionMode != "GOVERNANCE" {
		return nil, errors.New("operation archive verifier retention mode must be COMPLIANCE or GOVERNANCE")
	}
	if retentionDuration <= 0 || path.Clean(prefix) != prefix || strings.HasPrefix(prefix, "../") {
		return nil, errors.New("operation archive verifier scope or retention is invalid")
	}
	p := &Processor{
		executor: executor, objectStoreID: objectStoreID, bucket: bucket, prefix: prefix,
		retentionMode: retentionMode, retentionDuration: retentionDuration, now: time.Now,
	}
	p.run = p.runCommand
	return p, nil
}

func (p *Processor) Process(ctx context.Context, object *unstructured.Unstructured) error {
	artifact, err := operationauditbuilder.FromOperation(object)
	if err != nil {
		return err
	}
	if contains(object.GetFinalizers(), operationaudit.Finalizer) {
		return errors.New("operation still has the audit finalizer")
	}
	annotations := object.GetAnnotations()
	receiptSHA := annotations[operationaudit.ReceiptSHAAnnotation]
	artifactSHA := annotations[operationaudit.ArtifactSHAAnnotation]
	versionID := annotations[operationaudit.VersionAnnotation]
	if receiptSHA == "" || artifactSHA == "" || versionID == "" {
		return errors.New("released terminal operation is missing complete archive evidence annotations")
	}
	retainUntil := time.Unix(artifact.CompletedAtUnix, 0).Add(p.retentionDuration).Unix()
	if retainUntil <= p.now().Unix() {
		return errors.New("operation archive retention deadline is not in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-operation-archive-verify-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	artifactPath := path.Join(dir, "artifact.json")
	if err := operationaudit.WriteAtomic(artifactPath, artifact); err != nil {
		return err
	}
	objectKey := path.Join(p.prefix, artifact.Namespace, artifact.UID+".json")
	environment := []string{
		"ACTION=audit-version-verify", "INPUT=" + artifactPath,
		"OBJECT_STORE_ID=" + p.objectStoreID, "S3_BUCKET=" + p.bucket,
		"S3_OBJECT_KEY=" + objectKey, "VERSION_ID=" + versionID,
		"EXPECTED_RECEIPT_SHA256=" + receiptSHA, "EXPECTED_ARTIFACT_SHA256=" + artifactSHA,
		"RETENTION_MODE=" + p.retentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
	}
	if err := p.run(ctx, p.executor, environment); err != nil {
		return fmt.Errorf("operation archive evidence verifier failed: %w", err)
	}
	return nil
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (p *Processor) runCommand(ctx context.Context, executable string, environment []string) error {
	command := exec.CommandContext(ctx, executable)
	command.Env = mergeEnvironment(os.Environ(), environment)
	processgroup.Configure(command)
	command.WaitDelay = processgroup.DefaultWaitDelay
	output, err := processgroup.CombinedOutput(command, processgroup.DefaultOutputLimitBytes)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: %s", ctxErr, strings.TrimSpace(string(output)))
		}
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func mergeEnvironment(base, overrides []string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	order := make([]string, 0, len(base)+len(overrides))
	for _, entries := range [][]string{base, overrides} {
		for _, entry := range entries {
			key, _, found := strings.Cut(entry, "=")
			if !found || key == "" {
				continue
			}
			if _, exists := values[key]; !exists {
				order = append(order, key)
			}
			values[key] = entry
		}
	}
	result := make([]string, 0, len(order))
	for _, key := range order {
		result = append(result, values[key])
	}
	return result
}
