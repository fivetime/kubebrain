package operationarchiver

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type timeoutProcessor struct {
	processor Processor
	timeout   time.Duration
}

func WithTimeout(processor Processor, timeout time.Duration) (Processor, error) {
	if processor == nil {
		return nil, errors.New("operation archive processor is required")
	}
	if timeout <= 0 {
		return nil, errors.New("operation archive timeout must be positive")
	}
	return &timeoutProcessor{processor: processor, timeout: timeout}, nil
}

func (p *timeoutProcessor) Process(
	parent context.Context, object *unstructured.Unstructured,
) error {
	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()
	return p.processor.Process(ctx, object)
}
