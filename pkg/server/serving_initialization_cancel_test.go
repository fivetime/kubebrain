package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServingInitializationRejectsCanceledSuccess(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before_call", false: "during_call"}[before], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if before {
				cancel()
			}
			calls := 0
			s := &server{}
			ok := s.runServingInitializationStage(ctx, "test", func(context.Context) error {
				calls++
				cancel()
				return nil
			})
			require.False(t, ok, "completed storage work does not authorize serving a canceled term")
			if before {
				require.Zero(t, calls, "do not begin another stage after cancellation")
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}
