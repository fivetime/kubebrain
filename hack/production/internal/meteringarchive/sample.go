package meteringarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

const FormatV3 = "kubebrain.metering-sample.v3"
const Format = "kubebrain.metering-sample.v2"
const LegacyFormat = "kubebrain.metering-sample.v1"

const completenessMetric = "kubebrain_dbaas:metering_hour_complete"
const objectRequestCompletenessMetric = "kubebrain_dbaas:object_request_hour_complete"
const objectRequestPeriodEndMetric = "kubebrain_dbaas:object_request_period_end:last"

var Metrics = []string{
	"kubebrain_dbaas:cpu_usage_core_seconds:hour",
	"kubebrain_dbaas:memory_working_set_bytes:hour_avg",
	"kubebrain_dbaas:network_receive_bytes:hour",
	"kubebrain_dbaas:network_transmit_bytes:hour",
	"kubebrain_dbaas:storage_provisioned_bytes:hour_avg",
	"kubebrain_dbaas:storage_used_bytes:hour_avg",
	"kubebrain_dbaas:logical_backup_artifact_bytes:last",
	"kubebrain_dbaas:logical_backup_age_seconds:last",
}

var MetricsV3 = append(append([]string(nil), Metrics...),
	"kubebrain_dbaas:object_store_write_requests:hour",
	"kubebrain_dbaas:object_store_list_requests:hour",
	"kubebrain_dbaas:object_store_read_requests:hour",
	"kubebrain_dbaas:object_store_delete_requests:hour",
)

var LegacyMetrics = []string{
	"kubebrain_dbaas:cpu_usage_cores:sum",
	"kubebrain_dbaas:memory_working_set_bytes:sum",
	"kubebrain_dbaas:network_receive_bytes_per_second:sum",
	"kubebrain_dbaas:network_transmit_bytes_per_second:sum",
	"kubebrain_dbaas:storage_provisioned_bytes:sum",
	"kubebrain_dbaas:storage_used_bytes:sum",
	"kubebrain_dbaas:logical_backup_artifact_bytes:last",
	"kubebrain_dbaas:logical_backup_age_seconds:last",
}

var instancePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type MetricValue struct {
	Name          string  `json:"name"`
	Value         float64 `json:"value"`
	TimestampUnix int64   `json:"timestamp_unix"`
}

type Sample struct {
	Format                     string        `json:"format"`
	Instance                   string        `json:"instance"`
	SlotStartUnix              int64         `json:"slot_start_unix"`
	SlotEndUnix                int64         `json:"slot_end_unix"`
	QueryUnix                  int64         `json:"query_unix"`
	Complete                   bool          `json:"complete"`
	ObjectRequestPeriodEndUnix int64         `json:"object_request_period_end_unix,omitempty"`
	Metrics                    []MetricValue `json:"metrics"`
}

type Status struct {
	Sample Sample
	SHA256 string
	Bytes  int64
}

type Collector struct {
	BaseURL      *url.URL
	Client       *http.Client
	BearerToken  string
	MaxStaleness time.Duration
	Format       string
}

func NewCollector(rawURL string, client *http.Client, bearerToken string, maxStaleness time.Duration) (*Collector, error) {
	base, err := url.Parse(rawURL)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, errors.New("prometheus URL must be an absolute http or https URL")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("prometheus URL cannot contain a query or fragment")
	}
	if client == nil || maxStaleness <= 0 {
		return nil, errors.New("metering collector configuration is incomplete")
	}
	return &Collector{
		BaseURL: base, Client: client, BearerToken: bearerToken, MaxStaleness: maxStaleness,
		Format: Format,
	}, nil
}

func NewCollectorV3(rawURL string, client *http.Client, bearerToken string, maxStaleness time.Duration) (*Collector, error) {
	collector, err := NewCollector(rawURL, client, bearerToken, maxStaleness)
	if err != nil {
		return nil, err
	}
	collector.Format = FormatV3
	return collector, nil
}

func (c *Collector) Collect(ctx context.Context, instance string, slotStart, slotEnd time.Time) (Sample, error) {
	if !instancePattern.MatchString(instance) {
		return Sample{}, errors.New("metering instance has an invalid identifier")
	}
	slotStart = slotStart.UTC()
	slotEnd = slotEnd.UTC()
	if !slotEnd.After(slotStart) {
		return Sample{}, errors.New("metering slot end must be after start")
	}
	complete, err := c.queryOne(ctx, completenessMetric, instance, slotEnd)
	if err != nil {
		return Sample{}, fmt.Errorf("query metering completeness: %w", err)
	}
	if complete.Value != 1 {
		return Sample{}, fmt.Errorf("metering data is incomplete at %d", slotEnd.Unix())
	}
	metrics := Metrics
	if c.Format == FormatV3 {
		requestsComplete, err := c.queryOne(ctx, objectRequestCompletenessMetric, instance, slotEnd)
		if err != nil {
			return Sample{}, fmt.Errorf("query object request completeness: %w", err)
		}
		if requestsComplete.Value != 1 {
			return Sample{}, fmt.Errorf("object request data is incomplete at %d", slotEnd.Unix())
		}
		requestPeriodEnd, queryErr := c.queryOne(ctx, objectRequestPeriodEndMetric, instance, slotEnd)
		if queryErr != nil {
			return Sample{}, fmt.Errorf("query object request period end: %w", queryErr)
		}
		if requestPeriodEnd.Value != float64(slotEnd.Unix()) {
			return Sample{}, fmt.Errorf("object request period does not end at %d", slotEnd.Unix())
		}
		metrics = MetricsV3
	} else if c.Format != Format {
		return Sample{}, errors.New("metering collector format is unsupported")
	}
	values := make([]MetricValue, 0, len(metrics))
	for _, name := range metrics {
		value, err := c.queryOne(ctx, name, instance, slotEnd)
		if err != nil {
			return Sample{}, fmt.Errorf("query %s: %w", name, err)
		}
		values = append(values, value)
	}
	sample := Sample{
		Format: c.Format, Instance: instance, SlotStartUnix: slotStart.Unix(),
		SlotEndUnix: slotEnd.Unix(), QueryUnix: slotEnd.Unix(), Complete: true, Metrics: values,
	}
	if c.Format == FormatV3 {
		sample.ObjectRequestPeriodEndUnix = slotEnd.Unix()
	}
	if err := sample.Validate(c.MaxStaleness); err != nil {
		return Sample{}, err
	}
	return sample, nil
}

func (c *Collector) queryOne(
	ctx context.Context,
	metric, instance string,
	at time.Time,
) (MetricValue, error) {
	endpoint := *c.BaseURL
	endpoint.Path = joinURLPath(endpoint.Path, "/api/v1/query")
	query := endpoint.Query()
	query.Set("query", metric+`{dbaas_instance=`+strconv.Quote(instance)+`}`)
	query.Set("time", strconv.FormatInt(at.Unix(), 10))
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return MetricValue{}, err
	}
	request.Header.Set("Accept", "application/json")
	if c.BearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+c.BearerToken)
	}
	response, err := c.Client.Do(request)
	if err != nil {
		return MetricValue{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return MetricValue{}, err
	}
	if response.StatusCode != http.StatusOK {
		return MetricValue{}, fmt.Errorf("prometheus returned HTTP %d: %s", response.StatusCode, string(body))
	}
	var envelope struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
		Error string `json:"error"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&envelope); err != nil {
		return MetricValue{}, fmt.Errorf("decode prometheus response: %w", err)
	}
	if envelope.Status != "success" || envelope.Data.ResultType != "vector" {
		return MetricValue{}, fmt.Errorf("prometheus query failed: %s", envelope.Error)
	}
	if len(envelope.Data.Result) != 1 {
		return MetricValue{}, fmt.Errorf("expected exactly one series, got %d", len(envelope.Data.Result))
	}
	result := envelope.Data.Result[0]
	if result.Metric["dbaas_instance"] != instance {
		return MetricValue{}, errors.New("prometheus returned a different dbaas_instance")
	}
	if len(result.Value) != 2 {
		return MetricValue{}, errors.New("prometheus sample must contain timestamp and value")
	}
	var timestamp float64
	if err := json.Unmarshal(result.Value[0], &timestamp); err != nil {
		return MetricValue{}, errors.New("prometheus sample timestamp is invalid")
	}
	var rawValue string
	if err := json.Unmarshal(result.Value[1], &rawValue); err != nil {
		return MetricValue{}, errors.New("prometheus sample value is invalid")
	}
	value, err := strconv.ParseFloat(rawValue, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return MetricValue{}, errors.New("prometheus sample value must be finite and non-negative")
	}
	timestampUnix := int64(timestamp)
	if timestampUnix > at.Unix() || at.Unix()-timestampUnix > int64(c.MaxStaleness/time.Second) {
		return MetricValue{}, errors.New("prometheus sample timestamp is stale or in the future")
	}
	return MetricValue{Name: metric, Value: value, TimestampUnix: timestampUnix}, nil
}

func (s Sample) Validate(maxStaleness time.Duration) error {
	expectedMetrics := MetricsV3
	if s.Format == LegacyFormat {
		expectedMetrics = LegacyMetrics
	} else if s.Format == Format {
		expectedMetrics = Metrics
	} else if s.Format != FormatV3 {
		return errors.New("metering sample has an unsupported format")
	}
	if !instancePattern.MatchString(s.Instance) ||
		s.SlotStartUnix <= 0 || s.SlotEndUnix <= s.SlotStartUnix ||
		s.QueryUnix != s.SlotEndUnix || !s.Complete || len(s.Metrics) != len(expectedMetrics) ||
		maxStaleness <= 0 {
		return errors.New("metering sample is incomplete")
	}
	if (s.Format == FormatV3 && s.ObjectRequestPeriodEndUnix != s.SlotEndUnix) ||
		(s.Format != FormatV3 && s.ObjectRequestPeriodEndUnix != 0) {
		return errors.New("metering sample object request period is invalid")
	}
	for i, metric := range s.Metrics {
		if metric.Name != expectedMetrics[i] || math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) ||
			metric.Value < 0 || metric.TimestampUnix > s.QueryUnix ||
			s.QueryUnix-metric.TimestampUnix > int64(maxStaleness/time.Second) {
			return errors.New("metering sample contains an invalid metric")
		}
		if s.Format == FormatV3 && i >= len(Metrics) &&
			(metric.Value != math.Trunc(metric.Value) || metric.Value > 1<<53) {
			return errors.New("object request metric must be an exact non-negative integer")
		}
	}
	return nil
}

func WriteAtomic(path string, sample Sample, maxStaleness time.Duration) (Status, error) {
	if path == "" {
		return Status{}, errors.New("metering artifact output is empty")
	}
	if err := sample.Validate(maxStaleness); err != nil {
		return Status{}, err
	}
	data, err := json.Marshal(sample)
	if err != nil {
		return Status{}, err
	}
	data = append(data, '\n')
	sum := sha256.Sum256(data)
	status := Status{Sample: sample, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data))}
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, data) {
			return status, nil
		}
		return Status{}, fmt.Errorf("refusing to overwrite existing metering artifact %q", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Status{}, err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return Status{}, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return Status{}, err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return Status{}, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return Status{}, err
	}
	if err := temp.Close(); err != nil {
		return Status{}, err
	}
	if err := os.Link(tempName, path); err != nil {
		return Status{}, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return Status{}, err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return Status{}, err
	}
	return status, nil
}

func ReadSample(path string, maxStaleness time.Duration) (Sample, error) {
	var sample Sample
	data, err := os.ReadFile(path)
	if err != nil {
		return sample, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&sample); err != nil {
		return sample, fmt.Errorf("decode metering sample: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return sample, errors.New("metering sample contains trailing JSON")
	}
	if err := sample.Validate(maxStaleness); err != nil {
		return sample, err
	}
	canonical, err := json.Marshal(sample)
	if err != nil {
		return sample, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return sample, errors.New("metering sample is not canonical")
	}
	return sample, nil
}

func joinURLPath(base, suffix string) string {
	return string(bytes.TrimRight([]byte(base), "/")) + suffix
}
