package meteringarchive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCollectorBuildsCanonicalCompleteSample(t *testing.T) {
	var mu sync.Mutex
	queries := make([]string, 0, len(Metrics)+1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		require.Equal(t, "Bearer projected-token", request.Header.Get("Authorization"))
		require.Equal(t, "/prometheus/api/v1/query", request.URL.Path)
		require.Equal(t, "1700003600", request.URL.Query().Get("time"))
		query := request.URL.Query().Get("query")
		mu.Lock()
		queries = append(queries, query)
		mu.Unlock()
		value := "1"
		if !strings.HasPrefix(query, completenessMetric+"{") {
			value = strconv.Itoa(len(queries) * 10)
		}
		writePrometheusVector(t, response, "instance-a", 1_700_003_590, value)
	}))
	defer server.Close()
	collector, err := NewCollector(
		server.URL+"/prometheus/", server.Client(), "projected-token", 5*time.Minute,
	)
	require.NoError(t, err)
	sample, err := collector.Collect(
		context.Background(), "instance-a",
		time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0),
	)
	require.NoError(t, err)
	require.Equal(t, Format, sample.Format)
	require.True(t, sample.Complete)
	require.Len(t, sample.Metrics, len(Metrics))
	for i, metric := range sample.Metrics {
		require.Equal(t, Metrics[i], metric.Name)
		require.Positive(t, metric.Value)
	}
	require.Len(t, queries, len(Metrics)+1)

	output := filepath.Join(t.TempDir(), "sample.json")
	status, err := WriteAtomic(output, sample, 5*time.Minute)
	require.NoError(t, err)
	first, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, append(mustJSON(t, sample), '\n'), first)
	require.Len(t, status.SHA256, 64)
	require.Equal(t, int64(len(first)), status.Bytes)
	retried, err := WriteAtomic(output, sample, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, status, retried)
	read, err := ReadSample(output, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, sample, read)

	changed := sample
	changed.Metrics = append([]MetricValue(nil), sample.Metrics...)
	changed.Metrics[0].Value++
	_, err = WriteAtomic(output, changed, 5*time.Minute)
	require.ErrorContains(t, err, "refusing to overwrite")

	require.NoError(t, os.WriteFile(output, append([]byte(" "), first...), 0o600))
	_, err = ReadSample(output, 5*time.Minute)
	require.ErrorContains(t, err, "not canonical")
}

func TestNewCollectorRejectsUnsafePrometheusURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "control character", raw: "https://prometheus.example\nother", want: "unsupported characters"},
		{name: "DEL", raw: "https://prometheus.example\x7fother", want: "unsupported characters"},
		{name: "quote", raw: `https://prometheus.example"other`, want: "unsupported characters"},
		{name: "backslash", raw: `https://prometheus.example\other`, want: "unsupported characters"},
		{name: "credentials", raw: "https://user:pass@prometheus.example", want: "credentials"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collector, err := NewCollector(tc.raw, http.DefaultClient, "", 5*time.Minute)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, collector)
		})
	}
}

func TestSampleWriterTreatsConcurrentIdenticalLinkAsIdempotent(t *testing.T) {
	sample := validSample()
	data := append(mustJSON(t, sample), '\n')
	originalLink := linkMeteringArchiveFile
	t.Cleanup(func() { linkMeteringArchiveFile = originalLink })
	linkMeteringArchiveFile = func(_, path string) error {
		require.NoError(t, os.WriteFile(path, data, 0o600))
		return os.ErrExist
	}

	status, err := WriteAtomic(filepath.Join(t.TempDir(), "sample.json"), sample, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), status.Bytes)
	require.Equal(t, sample, status.Sample)
}

func TestSampleRejectsOversizedInputs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxMeteringArchiveJSONBytes+1), 0o600))
	_, err := ReadSample(path, 5*time.Minute)
	require.ErrorContains(t, err, "metering sample exceeds")

	output := filepath.Join(dir, "existing.json")
	require.NoError(t, os.WriteFile(output, make([]byte, maxMeteringArchiveJSONBytes+1), 0o600))
	_, err = WriteAtomic(output, validSample(), 5*time.Minute)
	require.ErrorContains(t, err, "existing metering artifact exceeds")
}

func TestCollectorV3RequiresAndArchivesExactObjectRequestCounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query().Get("query")
		value := "1"
		if strings.HasPrefix(query, objectRequestPeriodEndMetric+"{") {
			value = "1700003600"
		}
		if strings.HasPrefix(query, MetricsV3[len(Metrics)]+"{") {
			value = "17"
		}
		writePrometheusVector(t, response, "instance-a", 1_700_003_590, value)
	}))
	defer server.Close()
	collector, err := NewCollectorV3(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	sample, err := collector.Collect(context.Background(), "instance-a",
		time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0))
	require.NoError(t, err)
	require.Equal(t, FormatV3, sample.Format)
	require.Len(t, sample.Metrics, len(MetricsV3))
	require.Equal(t, float64(17), sample.Metrics[len(Metrics)].Value)

	sample.Metrics[len(Metrics)].Value = 1.5
	require.ErrorContains(t, sample.Validate(5*time.Minute), "exact")
	sample.Metrics[len(Metrics)].Value = float64(1<<53) + 2
	require.ErrorContains(t, sample.Validate(5*time.Minute), "exact")
}

func TestCollectorV3FailsClosedWithoutRequestCompleteness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		value := "1"
		if strings.HasPrefix(request.URL.Query().Get("query"), objectRequestCompletenessMetric+"{") {
			value = "0"
		}
		writePrometheusVector(t, response, "instance-a", 1_700_003_590, value)
	}))
	defer server.Close()
	collector, err := NewCollectorV3(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	_, err = collector.Collect(context.Background(), "instance-a",
		time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0))
	require.ErrorContains(t, err, "object request data is incomplete")
}

func TestCollectorV3RejectsWrongFinalizedRequestPeriod(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		value := "1"
		if strings.HasPrefix(request.URL.Query().Get("query"), objectRequestPeriodEndMetric+"{") {
			value = "1700000000"
		}
		writePrometheusVector(t, response, "instance-a", 1_700_003_590, value)
	}))
	defer server.Close()
	collector, err := NewCollectorV3(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	_, err = collector.Collect(context.Background(), "instance-a",
		time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0))
	require.ErrorContains(t, err, "period does not end")
}

func TestCollectorFailsClosedOnIncompleteDuplicateStaleAndInvalidValues(t *testing.T) {
	tests := []struct {
		name      string
		handler   func(*testing.T, http.ResponseWriter, *http.Request)
		wantError string
	}{
		{
			name: "incomplete",
			handler: func(t *testing.T, response http.ResponseWriter, _ *http.Request) {
				writePrometheusVector(t, response, "instance-a", 1_700_003_590, "0")
			},
			wantError: "data is incomplete",
		},
		{
			name: "duplicate",
			handler: func(t *testing.T, response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				fmt.Fprint(response, `{"status":"success","data":{"resultType":"vector","result":[`+
					`{"metric":{"dbaas_instance":"instance-a"},"value":[1700003590,"1"]},`+
					`{"metric":{"dbaas_instance":"instance-a"},"value":[1700003590,"1"]}]}}`)
			},
			wantError: "exactly one series",
		},
		{
			name: "stale",
			handler: func(t *testing.T, response http.ResponseWriter, _ *http.Request) {
				writePrometheusVector(t, response, "instance-a", 1_700_000_000, "1")
			},
			wantError: "stale or in the future",
		},
		{
			name: "nan",
			handler: func(t *testing.T, response http.ResponseWriter, _ *http.Request) {
				writePrometheusVector(t, response, "instance-a", 1_700_003_590, "NaN")
			},
			wantError: "finite and non-negative",
		},
		{
			name: "wrong instance",
			handler: func(t *testing.T, response http.ResponseWriter, _ *http.Request) {
				writePrometheusVector(t, response, "instance-b", 1_700_003_590, "1")
			},
			wantError: "different dbaas_instance",
		},
		{
			name: "trailing json",
			handler: func(t *testing.T, response http.ResponseWriter, _ *http.Request) {
				writePrometheusVector(t, response, "instance-a", 1_700_003_590, "1")
				fmt.Fprint(response, `{"status":"success"}`)
			},
			wantError: "trailing JSON",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				test.handler(t, response, request)
			}))
			defer server.Close()
			collector, err := NewCollector(server.URL, server.Client(), "", 5*time.Minute)
			require.NoError(t, err)
			_, err = collector.Collect(
				context.Background(), "instance-a",
				time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0),
			)
			require.ErrorContains(t, err, test.wantError)
		})
	}
}

func TestCollectorRejectsNonJSONPrometheusSuccessResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/html")
		fmt.Fprint(response, `{"status":"success","data":{"resultType":"vector","result":[`+
			`{"metric":{"dbaas_instance":"instance-a"},"value":[1700003590,"1"]}]}}`)
	}))
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	_, err = collector.Collect(
		context.Background(), "instance-a",
		time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0),
	)
	require.ErrorContains(t, err, "non-JSON response")
}

func TestCollectorRejectsOversizedPrometheusResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write(bytes.Repeat([]byte("x"), maxPrometheusResponseBytes+1))
	}))
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	_, err = collector.Collect(
		context.Background(), "instance-a",
		time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0),
	)
	require.ErrorContains(t, err, "prometheus response exceeds")
}

func TestCollectorAcceptsPrometheusResponseExtensions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		fmt.Fprint(response, `{"status":"success","warnings":["partial metadata"],`+
			`"data":{"resultType":"vector","result":[{"metric":{"__name__":"recording_rule",`+
			`"dbaas_instance":"instance-a"},"value":[1700003590,"1"]}]}}`)
	}))
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	sample, err := collector.Collect(
		context.Background(), "instance-a",
		time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0),
	)
	require.NoError(t, err)
	require.True(t, sample.Complete)
}

func TestSampleValidationPinsMetricOrderAndSlot(t *testing.T) {
	sample := validSample()
	require.NoError(t, sample.Validate(5*time.Minute))

	legacy := sample
	legacy.Format = LegacyFormat
	legacy.Metrics = append([]MetricValue(nil), sample.Metrics...)
	for i := range legacy.Metrics {
		legacy.Metrics[i].Name = LegacyMetrics[i]
	}
	require.NoError(t, legacy.Validate(5*time.Minute))

	reordered := sample
	reordered.Metrics = append([]MetricValue(nil), sample.Metrics...)
	reordered.Metrics[0], reordered.Metrics[1] = reordered.Metrics[1], reordered.Metrics[0]
	require.Error(t, reordered.Validate(5*time.Minute))

	badSlot := sample
	badSlot.QueryUnix++
	require.Error(t, badSlot.Validate(5*time.Minute))

	require.Error(t, sampleWithInstance("tenant/escape").Validate(5*time.Minute))
	_, err := NewCollector("file:///metrics", http.DefaultClient, "", time.Minute)
	require.Error(t, err)
}

func validSample() Sample {
	return sampleWithInstance("instance-a")
}

func sampleWithInstance(instance string) Sample {
	metrics := make([]MetricValue, len(Metrics))
	for i, name := range Metrics {
		metrics[i] = MetricValue{Name: name, Value: float64(i + 1), TimestampUnix: 1_700_003_590}
	}
	return Sample{
		Format: Format, Instance: instance, SlotStartUnix: 1_700_000_000,
		SlotEndUnix: 1_700_003_600, QueryUnix: 1_700_003_600, Complete: true, Metrics: metrics,
	}
}

func writePrometheusVector(
	t *testing.T,
	response http.ResponseWriter,
	instance string,
	timestamp int64,
	value string,
) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(response).Encode(map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result": []any{map[string]any{
				"metric": map[string]string{"dbaas_instance": instance},
				"value":  []any{timestamp, value},
			}},
		},
	}))
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}
