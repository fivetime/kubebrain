package metricsworker

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const ready = "READY\t/owner/metrics-worker.abcdefgh\t/owner/metrics.abcdefgh\n"
const captured = "CAPTURED\t/owner/metrics.ijklmnop\t/owner/metrics-schedule.abcdefgh\n"

func TestProtocol(t *testing.T) {
	p, err := NewProtocol(strings.NewReader(ready+captured), "/owner")
	require.NoError(t, err)
	r, err := p.ReadReady()
	require.NoError(t, err)
	require.Equal(t, "/owner/metrics.abcdefgh", r.Baseline)
	c, err := p.ReadCaptured()
	require.NoError(t, err)
	require.Equal(t, "/owner/metrics.ijklmnop", c.Capture)
	require.NoError(t, p.Finish())
	require.Error(t, p.Finish())
}

func TestProtocolRejectsMalformedReady(t *testing.T) {
	for _, input := range []string{
		"", strings.TrimSuffix(ready, "\n"), captured, ready[:5] + strings.Repeat("x", maxRecordBytes) + "\n",
		"INVALID\n" + ready + captured,
		strings.ReplaceAll(ready, "/owner/", "/other/"),
		strings.Replace(ready, "/owner/metrics.", "/owner/../owner/metrics.", 1),
		strings.Replace(ready, "abcdefgh", "abc", 1),
		strings.Replace(ready, "abcdefgh", "abcd\x00fgh", 1),
		strings.Replace(ready, "\n", "\r\n", 1),
		strings.Replace(ready, "metrics-worker", "metrics", 1),
		strings.Replace(ready, "\n", "\textra\n", 1),
	} {
		p, err := NewProtocol(strings.NewReader(input), "/owner")
		require.NoError(t, err)
		_, err = p.ReadReady()
		require.Error(t, err)
		_, err = p.ReadReady()
		require.Error(t, err, "invalid stream cannot recover")
	}
}

func TestProtocolRejectsOrderReuseAndTrailingOutput(t *testing.T) {
	for _, input := range []string{ready + ready, ready + strings.Replace(captured, "ijklmnop", "abcdefgh", 1), ready + captured + "extra\n", ready + captured + captured} {
		p, err := NewProtocol(strings.NewReader(input), "/owner")
		require.NoError(t, err)
		_, err = p.ReadReady()
		require.NoError(t, err)
		_, err = p.ReadCaptured()
		if err == nil {
			require.Error(t, p.Finish())
		}
	}
	p, err := NewProtocol(strings.NewReader(ready+captured), "/owner")
	require.NoError(t, err)
	_, err = p.ReadCaptured()
	require.Error(t, err)
	_, err = p.ReadReady()
	require.Error(t, err)
	for _, owner := range []string{"", "/", "relative", "/owner/", "/owner/../owner", "/owner\n"} {
		_, err := NewProtocol(strings.NewReader(""), owner)
		require.Error(t, err)
	}
}
