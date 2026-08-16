package httpserver

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type shutdownServer interface {
	Shutdown(context.Context) error
	Close() error
}

// Shutdown drains an HTTP server within timeout and force-closes it when the
// graceful drain fails. The returned error retains both failures when forced
// close cannot finish either.
func Shutdown(server shutdownServer, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		closeErr := server.Close()
		if closeErr != nil {
			closeErr = fmt.Errorf("force close HTTP server: %w", closeErr)
		}
		return errors.Join(fmt.Errorf("graceful HTTP shutdown failed: %w", err), closeErr)
	}
	return nil
}
