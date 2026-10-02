package leasefault

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNativePreActivationSourceBoundary(t *testing.T) {
	for _, mode := range []string{"success", "source-before", "source-during", "plan-during", "online-during", "cancel", "stage-error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			changed := errors.New("changed admission")
			files, plans, live := 0, 0, 0
			fileChanged, planChanged, liveChanged := mode == "source-before", false, false
			tools, before := nativeSourceAdmissions(func(context.Context) error {
				plans++
				if planChanged {
					return changed
				}
				return nil
			}, func(context.Context) error {
				files++
				if fileChanged {
					return changed
				}
				return nil
			}, func(context.Context) error {
				live++
				if liveChanged {
					return changed
				}
				return nil
			})
			entered := false
			var retained context.Context
			err := before(ctx, func(scoped context.Context) error {
				retained = scoped
				entered = true
				actual, ok := scoped.Deadline()
				require.True(t, ok)
				require.Equal(t, deadline, actual)
				for i := 0; i < 10; i++ {
					require.NoError(t, tools(scoped))
				}
				require.Equal(t, 2, files, "nested reads share only this fresh source boundary")
				switch mode {
				case "source-during":
					fileChanged = true
				case "plan-during":
					planChanged = true
					require.ErrorIs(t, tools(scoped), changed)
				case "online-during":
					liveChanged = true
					require.ErrorIs(t, tools(scoped), changed)
				case "cancel":
					cancel()
				case "stage-error":
					return changed
				}
				return nil
			})
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, 4, files)
				require.Equal(t, 12, live)
				require.Equal(t, 24, plans)
				// The marker cannot escape to activation or a new command.
				require.NoError(t, tools(ctx))
				require.Equal(t, 6, files)
				require.NoError(t, tools(retained))
				require.Equal(t, 8, files, "scope expires even if its context is retained")
				otherTools, _ := nativeSourceAdmissions(func(context.Context) error { return nil }, func(context.Context) error { files++; return nil }, func(context.Context) error { return nil })
				require.NoError(t, otherTools(ctx))
				require.Equal(t, 10, files)
			} else if mode == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, changed)
			}
			if mode == "source-before" {
				require.False(t, entered)
				require.Zero(t, live)
			}
		})
	}
}
