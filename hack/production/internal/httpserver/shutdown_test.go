package httpserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeShutdownServer struct {
	shutdown    func(context.Context) error
	shutdownErr error
	closeErr    error
	shutdowns   int
	closes      int
}

func (s *fakeShutdownServer) Shutdown(ctx context.Context) error {
	s.shutdowns++
	if s.shutdown != nil {
		return s.shutdown(ctx)
	}
	return s.shutdownErr
}

func (s *fakeShutdownServer) Close() error {
	s.closes++
	return s.closeErr
}

func TestShutdownPropagatesGracefulAndForcedCloseFailures(t *testing.T) {
	shutdownErr := errors.New("drain timed out")
	closeErr := errors.New("listener close failed")
	server := &fakeShutdownServer{shutdownErr: shutdownErr, closeErr: closeErr}

	err := Shutdown(server, time.Second)

	require.ErrorIs(t, err, shutdownErr)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, 1, server.shutdowns)
	require.Equal(t, 1, server.closes)
}

func TestShutdownDoesNotForceCloseAfterSuccessfulDrain(t *testing.T) {
	server := &fakeShutdownServer{}

	require.NoError(t, Shutdown(server, time.Second))
	require.Equal(t, 1, server.shutdowns)
	require.Zero(t, server.closes)
}

func TestShutdownForceClosesAfterTimeout(t *testing.T) {
	server := &fakeShutdownServer{shutdown: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}

	err := Shutdown(server, 10*time.Millisecond)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, server.shutdowns)
	require.Equal(t, 1, server.closes)
}
