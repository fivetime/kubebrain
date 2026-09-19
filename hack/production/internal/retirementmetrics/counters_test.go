package retirementmetrics

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCounterValidation(t *testing.T) {
	valid := "# TYPE leader_retirement_local_result counter\nleader_retirement_local_result{outcome=\"deadline\"} 2\n"
	parsed, err := Parse([]byte(valid))
	require.NoError(t, err)
	require.Equal(t, Counters{{"local", "deadline"}: 2}, parsed)
	for name, raw := range map[string]string{
		"gauge":         strings.ReplaceAll(valid, " counter", " gauge"),
		"nan":           strings.ReplaceAll(valid, " 2\n", " NaN\n"),
		"infinity":      strings.ReplaceAll(valid, " 2\n", " +Inf\n"),
		"negative":      strings.ReplaceAll(valid, " 2\n", " -1\n"),
		"fraction":      strings.ReplaceAll(valid, " 2\n", " 1.5\n"),
		"precision":     strings.ReplaceAll(valid, " 2\n", " 9007199254740992\n"),
		"timestamp":     strings.ReplaceAll(valid, " 2\n", " 2 123\n"),
		"wrong-outcome": strings.ReplaceAll(valid, "deadline", "canceled_before_send"),
		"extra-label":   strings.ReplaceAll(valid, "{outcome=", "{holder=\"secret\",outcome="),
		"duplicate":     valid + "leader_retirement_local_result{outcome=\"deadline\"} 3\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(raw))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestDeltaDoesNotInventAbsentBaseline(t *testing.T) {
	empty, err := Parse([]byte("unrelated 1\n"))
	require.NoError(t, err)
	require.Empty(t, empty)
	key := Key{"peer", "confirmed"}
	process := Process{PodUID: "pod", ContainerID: "container", StartedAt: "start"}
	for _, pair := range [][2]Counters{{empty, {key: 1}}, {{key: 1}, empty}, {{key: 2}, {key: 1}}} {
		_, err := Delta(pair[0], pair[1], key, process, process)
		require.Error(t, err)
	}
	delta, err := Delta(Counters{key: 2}, Counters{key: 3}, key, process, process)
	require.NoError(t, err)
	require.Equal(t, float64(1), delta)
	for _, changed := range []Process{
		{}, {PodUID: "other", ContainerID: "container", StartedAt: "start"},
		{PodUID: "pod", ContainerID: "other", StartedAt: "start"},
		{PodUID: "pod", ContainerID: "container", StartedAt: "other"},
		{PodUID: "pod", ContainerID: "container", StartedAt: "start", RestartCount: 1},
	} {
		_, err := Delta(Counters{key: 2}, Counters{key: 3}, key, process, changed)
		require.Error(t, err)
	}
}
