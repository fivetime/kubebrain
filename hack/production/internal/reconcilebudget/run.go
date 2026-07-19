package reconcilebudget

import (
	"context"
	"errors"
	"time"
)

type Func func(context.Context) (int, error)

func Run(parent context.Context, timeout time.Duration, reconcile Func) (int, error) {
	if parent == nil || reconcile == nil {
		return 0, errors.New("reconcile context and function are required")
	}
	if timeout <= 0 {
		return 0, errors.New("reconcile timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return reconcile(ctx)
}
