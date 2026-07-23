package meteringarchive

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

const RollupFormatV3 = "kubebrain.metering-rollup.v3"
const RollupFormat = "kubebrain.metering-rollup.v2"

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type SampleSource struct {
	SlotStartUnix   int64  `json:"slot_start_unix"`
	SlotEndUnix     int64  `json:"slot_end_unix"`
	ArtifactFormat  string `json:"artifact_format"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
}

type VerifiedSample struct {
	Sample Sample
	Source SampleSource
}

type Quantity struct {
	Name  string  `json:"name"`
	Unit  string  `json:"unit"`
	Value float64 `json:"value"`
}

type Observation struct {
	Name string  `json:"name"`
	Unit string  `json:"unit"`
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Last float64 `json:"last"`
}

type Rollup struct {
	Format          string         `json:"format"`
	Instance        string         `json:"instance"`
	PeriodStartUnix int64          `json:"period_start_unix"`
	PeriodEndUnix   int64          `json:"period_end_unix"`
	SlotSeconds     int64          `json:"slot_seconds"`
	Complete        bool           `json:"complete"`
	Sources         []SampleSource `json:"sources"`
	Quantities      []Quantity     `json:"quantities"`
	Observations    []Observation  `json:"observations"`
}

type RollupStatus struct {
	Rollup Rollup
	SHA256 string
	Bytes  int64
}

var quantityDefinitions = []struct {
	name   string
	unit   string
	metric int
}{
	{"cpu_core_seconds", "core_seconds", 0},
	{"memory_byte_seconds", "byte_seconds", 1},
	{"network_receive_bytes", "bytes", 2},
	{"network_transmit_bytes", "bytes", 3},
	{"storage_provisioned_byte_seconds", "byte_seconds", 4},
	{"storage_used_byte_seconds", "byte_seconds", 5},
}

var requestQuantityDefinitions = []struct {
	name   string
	unit   string
	metric int
}{
	{"object_storage_write_requests", "requests", 8},
	{"object_storage_list_requests", "requests", 9},
	{"object_storage_read_requests", "requests", 10},
	{"object_storage_delete_requests", "requests", 11},
}

var observationDefinitions = []struct {
	name   string
	unit   string
	metric int
}{
	{"logical_backup_artifact_bytes", "bytes", 6},
	{"logical_backup_age_seconds", "seconds", 7},
}

func BuildRollup(
	instance string,
	periodStart, periodEnd time.Time,
	slotDuration, maxStaleness time.Duration,
	inputs []VerifiedSample,
) (Rollup, error) {
	periodStart, periodEnd = periodStart.UTC(), periodEnd.UTC()
	if !instancePattern.MatchString(instance) || !periodEnd.After(periodStart) ||
		slotDuration < time.Minute || periodStart.Unix()%int64(slotDuration/time.Second) != 0 ||
		periodEnd.Unix()%int64(slotDuration/time.Second) != 0 ||
		periodEnd.Sub(periodStart)%slotDuration != 0 ||
		periodEnd.Sub(periodStart) > 31*24*time.Hour {
		return Rollup{}, errors.New("metering rollup period is invalid")
	}
	expectedSlots := int(periodEnd.Sub(periodStart) / slotDuration)
	if expectedSlots == 0 || len(inputs) != expectedSlots {
		return Rollup{}, fmt.Errorf("metering rollup requires %d samples, got %d", expectedSlots, len(inputs))
	}
	slotSeconds := int64(slotDuration / time.Second)
	sources := make([]SampleSource, len(inputs))
	includeRequests := true
	for _, input := range inputs {
		if input.Sample.Format != FormatV3 {
			includeRequests = false
			break
		}
	}
	definitions := append([]struct {
		name   string
		unit   string
		metric int
	}{}, quantityDefinitions...)
	rollupFormat := RollupFormat
	if includeRequests {
		definitions = append(definitions, requestQuantityDefinitions...)
		rollupFormat = RollupFormatV3
	}
	quantities := make([]Quantity, len(definitions))
	observations := make([]Observation, len(observationDefinitions))
	for i, definition := range definitions {
		quantities[i] = Quantity{Name: definition.name, Unit: definition.unit}
	}
	for i, definition := range observationDefinitions {
		observations[i] = Observation{
			Name: definition.name, Unit: definition.unit,
			Min: math.Inf(1), Max: math.Inf(-1),
		}
	}
	for i, input := range inputs {
		expectedStart := periodStart.Add(time.Duration(i) * slotDuration).Unix()
		expectedEnd := expectedStart + slotSeconds
		if err := input.Sample.Validate(maxStaleness); err != nil {
			return Rollup{}, fmt.Errorf("validate metering sample %d: %w", i, err)
		}
		source := input.Source
		if input.Sample.Instance != instance ||
			input.Sample.SlotStartUnix != expectedStart || input.Sample.SlotEndUnix != expectedEnd ||
			source.SlotStartUnix != expectedStart || source.SlotEndUnix != expectedEnd ||
			source.ArtifactFormat != input.Sample.Format ||
			(source.ArtifactFormat != FormatV3 && source.ArtifactFormat != Format &&
				source.ArtifactFormat != LegacyFormat) ||
			source.ObjectKey == "" || source.VersionID == "" ||
			!digestPattern.MatchString(source.ArtifactSHA256) || source.ObjectBytes <= 0 ||
			source.RetainUntilUnix < periodEnd.Unix() {
			return Rollup{}, fmt.Errorf("metering sample %d source does not match the period", i)
		}
		sources[i] = source
		for j, definition := range definitions {
			value := input.Sample.Metrics[definition.metric].Value
			if input.Sample.Format == LegacyFormat || definition.metric == 1 ||
				definition.metric == 4 || definition.metric == 5 {
				value *= float64(slotSeconds)
			}
			quantities[j].Value += value
			if math.IsInf(quantities[j].Value, 0) || math.IsNaN(quantities[j].Value) {
				return Rollup{}, errors.New("metering rollup quantity overflowed")
			}
			if definition.metric >= len(Metrics) &&
				(quantities[j].Value != math.Trunc(quantities[j].Value) ||
					quantities[j].Value > 1<<53) {
				return Rollup{}, errors.New("object request rollup is not an exact integer")
			}
		}
		for j, definition := range observationDefinitions {
			value := input.Sample.Metrics[definition.metric].Value
			observations[j].Min = math.Min(observations[j].Min, value)
			observations[j].Max = math.Max(observations[j].Max, value)
			observations[j].Last = value
		}
	}
	rollup := Rollup{
		Format: rollupFormat, Instance: instance,
		PeriodStartUnix: periodStart.Unix(), PeriodEndUnix: periodEnd.Unix(),
		SlotSeconds: slotSeconds, Complete: true, Sources: sources,
		Quantities: quantities, Observations: observations,
	}
	if err := rollup.Validate(); err != nil {
		return Rollup{}, err
	}
	return rollup, nil
}

func (r Rollup) Validate() error {
	definitions := quantityDefinitions
	if r.Format == RollupFormatV3 {
		definitions = append(append([]struct {
			name   string
			unit   string
			metric int
		}{}, quantityDefinitions...), requestQuantityDefinitions...)
	} else if r.Format != RollupFormat {
		return errors.New("metering rollup has an unsupported format")
	}
	if !instancePattern.MatchString(r.Instance) ||
		r.PeriodStartUnix <= 0 || r.PeriodEndUnix <= r.PeriodStartUnix ||
		r.SlotSeconds < 60 || (r.PeriodEndUnix-r.PeriodStartUnix)%r.SlotSeconds != 0 ||
		!r.Complete || len(r.Sources) != int((r.PeriodEndUnix-r.PeriodStartUnix)/r.SlotSeconds) ||
		len(r.Quantities) != len(definitions) ||
		len(r.Observations) != len(observationDefinitions) {
		return errors.New("metering rollup is incomplete")
	}
	for i, source := range r.Sources {
		expectedStart := r.PeriodStartUnix + int64(i)*r.SlotSeconds
		if source.SlotStartUnix != expectedStart ||
			source.SlotEndUnix != expectedStart+r.SlotSeconds ||
			(source.ArtifactFormat != FormatV3 && source.ArtifactFormat != Format &&
				source.ArtifactFormat != LegacyFormat) ||
			(r.Format == RollupFormatV3 && source.ArtifactFormat != FormatV3) ||
			source.ObjectKey == "" || source.VersionID == "" ||
			!digestPattern.MatchString(source.ArtifactSHA256) || source.ObjectBytes <= 0 ||
			source.RetainUntilUnix < r.PeriodEndUnix {
			return errors.New("metering rollup contains an invalid source")
		}
	}
	for i, quantity := range r.Quantities {
		definition := definitions[i]
		if quantity.Name != definition.name || quantity.Unit != definition.unit ||
			quantity.Value < 0 || math.IsNaN(quantity.Value) || math.IsInf(quantity.Value, 0) {
			return errors.New("metering rollup contains an invalid quantity")
		}
		if definition.metric >= len(Metrics) &&
			(quantity.Value != math.Trunc(quantity.Value) || quantity.Value > 1<<53) {
			return errors.New("metering rollup contains an inexact object request quantity")
		}
	}
	for i, observation := range r.Observations {
		definition := observationDefinitions[i]
		if observation.Name != definition.name || observation.Unit != definition.unit ||
			observation.Min < 0 || observation.Max < observation.Min ||
			observation.Last < observation.Min || observation.Last > observation.Max ||
			math.IsNaN(observation.Min) || math.IsNaN(observation.Max) ||
			math.IsNaN(observation.Last) || math.IsInf(observation.Min, 0) ||
			math.IsInf(observation.Max, 0) || math.IsInf(observation.Last, 0) {
			return errors.New("metering rollup contains an invalid observation")
		}
	}
	return nil
}

func WriteRollupAtomic(path string, rollup Rollup) (RollupStatus, error) {
	if path == "" {
		return RollupStatus{}, errors.New("metering rollup output is empty")
	}
	if err := rollup.Validate(); err != nil {
		return RollupStatus{}, err
	}
	data, err := json.Marshal(rollup)
	if err != nil {
		return RollupStatus{}, err
	}
	data = append(data, '\n')
	if err := ensureMeteringArchiveJSONWithinLimit(data, "metering rollup"); err != nil {
		return RollupStatus{}, err
	}
	sum := sha256.Sum256(data)
	status := RollupStatus{
		Rollup: rollup, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}
	if existing, err := readBoundedFile(path, "existing metering rollup", int64(len(data))); err == nil {
		if bytes.Equal(existing, data) {
			return status, nil
		}
		return RollupStatus{}, fmt.Errorf("refusing to overwrite existing metering rollup %q", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return RollupStatus{}, err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return RollupStatus{}, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return RollupStatus{}, err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return RollupStatus{}, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return RollupStatus{}, err
	}
	if err := temp.Close(); err != nil {
		return RollupStatus{}, err
	}
	if err := os.Link(tempName, path); err != nil {
		return RollupStatus{}, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return RollupStatus{}, err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return RollupStatus{}, err
	}
	return status, nil
}

func ReadRollup(path string) (Rollup, error) {
	status, err := ReadRollupStatus(path)
	if err != nil {
		return Rollup{}, err
	}
	return status.Rollup, nil
}

func ReadRollupStatus(path string) (RollupStatus, error) {
	var rollup Rollup
	data, err := readBoundedFile(path, "metering rollup", maxMeteringArchiveJSONBytes)
	if err != nil {
		return RollupStatus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rollup); err != nil {
		return RollupStatus{}, fmt.Errorf("decode metering rollup: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return RollupStatus{}, errors.New("metering rollup contains trailing JSON")
	}
	if err := rollup.Validate(); err != nil {
		return RollupStatus{}, err
	}
	canonical, err := json.Marshal(rollup)
	if err != nil {
		return RollupStatus{}, err
	}
	if !bytes.Equal(data, append(canonical, '\n')) {
		return RollupStatus{}, errors.New("metering rollup is not canonical")
	}
	sum := sha256.Sum256(data)
	return RollupStatus{
		Rollup: rollup, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}
