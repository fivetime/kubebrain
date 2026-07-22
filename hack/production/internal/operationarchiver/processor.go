package operationarchiver

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
	"github.com/kubewharf/kubebrain/hack/production/internal/operationauditrelease"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

type CommandRunner func(context.Context, string, []string) error

type ArchiveProcessor struct {
	client            dynamic.Interface
	executor          string
	objectStoreID     string
	bucket            string
	prefix            string
	retentionMode     string
	retentionDuration time.Duration
	now               func() time.Time
	run               CommandRunner
}

func NewArchiveProcessor(
	client dynamic.Interface,
	executor, objectStoreID, bucket, prefix, retentionMode string,
	retentionDuration time.Duration,
) (*ArchiveProcessor, error) {
	prefix = strings.Trim(prefix, "/")
	if client == nil || executor == "" || objectStoreID == "" || bucket == "" || prefix == "" {
		return nil, errors.New("operation archive processor configuration is incomplete")
	}
	if retentionMode != "COMPLIANCE" && retentionMode != "GOVERNANCE" {
		return nil, errors.New("operation archive retention mode must be COMPLIANCE or GOVERNANCE")
	}
	if retentionDuration <= 0 {
		return nil, errors.New("operation archive retention duration must be positive")
	}
	processor := &ArchiveProcessor{
		client: client, executor: executor, objectStoreID: objectStoreID, bucket: bucket,
		prefix: prefix, retentionMode: retentionMode, retentionDuration: retentionDuration,
		now: time.Now,
	}
	processor.run = processor.runCommand
	return processor, nil
}

func (p *ArchiveProcessor) Process(ctx context.Context, object *unstructured.Unstructured) error {
	artifact, err := operationauditbuilder.FromOperation(object)
	if err != nil {
		return err
	}
	retainUntil := time.Unix(artifact.CompletedAtUnix, 0).Add(p.retentionDuration).Unix()
	if retainUntil <= p.now().Unix() {
		return errors.New("operation archive retention deadline is not in the future")
	}
	dir, err := os.MkdirTemp("", "kubebrain-operation-audit-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	artifactPath := path.Join(dir, "artifact.json")
	receiptPath := path.Join(dir, "receipt.json")
	if err := operationaudit.WriteAtomic(artifactPath, artifact); err != nil {
		return err
	}
	objectKey := path.Join(p.prefix, artifact.Namespace, artifact.UID+".json")
	environment := []string{
		"ACTION=archive",
		"INPUT=" + artifactPath,
		"OBJECT_STORE_ID=" + p.objectStoreID,
		"S3_BUCKET=" + p.bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"RETENTION_MODE=" + p.retentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + receiptPath,
	}
	if err := p.run(ctx, p.executor, environment); err != nil {
		return err
	}
	_, err = operationauditrelease.ReleaseWithExpectedReceipt(
		ctx, p.client, artifact.Namespace, artifact.Name, artifactPath, receiptPath,
		operationauditrelease.ExpectedArchiveReceipt{
			ObjectStoreID: p.objectStoreID, Bucket: p.bucket, ObjectKey: objectKey,
			RetentionMode: p.retentionMode, RetainUntilUnix: retainUntil,
		},
	)
	return err
}

func (p *ArchiveProcessor) runCommand(ctx context.Context, executable string, environment []string) error {
	command := exec.CommandContext(ctx, executable)
	command.Env = mergeEnvironment(os.Environ(), environment)
	processgroup.Configure(command)
	command.WaitDelay = 5 * time.Second
	output, err := command.CombinedOutput()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf(
				"object archive executor failed: %w: %s",
				ctxErr, strings.TrimSpace(string(output)),
			)
		}
		return fmt.Errorf("object archive executor failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func mergeEnvironment(base, overrides []string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	order := make([]string, 0, len(base)+len(overrides))
	add := func(entry string) {
		key, _, found := strings.Cut(entry, "=")
		if !found || key == "" {
			return
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = entry
	}
	for _, entry := range base {
		add(entry)
	}
	for _, entry := range overrides {
		add(entry)
	}
	result := make([]string, 0, len(order))
	for _, key := range order {
		result = append(result, values[key])
	}
	return result
}
