package leasefault

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOriginalObservationOrderFailuresRemainSticky(t *testing.T) {
	for _, mode := range []string{"early-evidence", "finish-before-pending", "changed-clock", "closed-evidence"} {
		t.Run(mode, func(t *testing.T) {
			origin := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			o := &ObservedOriginal{binding: Binding{Origin: origin, InitialTerm: 4}}
			var err error
			switch mode {
			case "early-evidence":
				_, _, err = o.Evidence(ctx)
			case "finish-before-pending":
				err = o.Finish(ctx, origin, 5)
			case "changed-clock":
				o.pending = true
				err = o.Finish(ctx, origin.Add(time.Nanosecond), 5)
			case "closed-evidence":
				o.finished = true
				o.closed = true
				_, _, err = o.Evidence(ctx)
			}
			require.Error(t, err)
			require.Same(t, err, o.Pending(ctx, origin))
			require.Same(t, err, o.Finish(ctx, origin, 5))
			_, _, again := o.Evidence(ctx)
			require.Same(t, err, again)
		})
	}
}
