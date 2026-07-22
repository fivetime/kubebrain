package meteringbilling

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
)

const CatalogFormat = "kubebrain.metering-price-catalog.v1"
const CatalogFormatV2 = "kubebrain.metering-price-catalog.v2"
const CatalogFormatV3 = "kubebrain.metering-price-catalog.v3"
const MeasurementPolicy = "kubebrain.metering-rollup.v2"
const MeasurementPolicyV2 = "kubebrain.metering-rollup.v2+object-storage-rollup.v1"
const MeasurementPolicyV3 = "kubebrain.metering-rollup.v3+object-storage-rollup.v1"
const maxPriceCatalogBytes = 1 << 20

var versionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
var decimalPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{0,17}[1-9])?$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Rate struct {
	Name      string `json:"name"`
	Unit      string `json:"unit"`
	UnitPrice string `json:"unit_price"`
}

type Catalog struct {
	Format             string `json:"format"`
	Version            string `json:"version"`
	Currency           string `json:"currency"`
	EffectiveStartUnix int64  `json:"effective_start_unix"`
	EffectiveEndUnix   int64  `json:"effective_end_unix"`
	MeasurementPolicy  string `json:"measurement_policy"`
	Rates              []Rate `json:"rates"`
}

type CatalogStatus struct {
	Catalog Catalog
	SHA256  string
	Bytes   int64
}

var pricedQuantities = []struct {
	name string
	unit string
}{
	{"cpu_core_seconds", "core_seconds"},
	{"memory_byte_seconds", "byte_seconds"},
	{"network_receive_bytes", "bytes"},
	{"network_transmit_bytes", "bytes"},
	{"storage_provisioned_byte_seconds", "byte_seconds"},
	{"storage_used_byte_seconds", "byte_seconds"},
}

var pricedQuantitiesV2 = append(append([]struct {
	name string
	unit string
}{}, pricedQuantities...), struct {
	name string
	unit string
}{"object_storage_byte_seconds", "byte_seconds"})

var pricedQuantitiesV3 = append(append([]struct {
	name string
	unit string
}{}, pricedQuantities...), []struct {
	name string
	unit string
}{
	{"object_storage_write_requests", "requests"},
	{"object_storage_list_requests", "requests"},
	{"object_storage_read_requests", "requests"},
	{"object_storage_delete_requests", "requests"},
	{"object_storage_byte_seconds", "byte_seconds"},
}...)

func (c Catalog) Validate() error {
	definitions, policy, ok := catalogDefinitions(c.Format)
	if !ok || !versionPattern.MatchString(c.Version) ||
		!currencyPattern.MatchString(c.Currency) || c.EffectiveStartUnix <= 0 ||
		c.EffectiveEndUnix <= c.EffectiveStartUnix ||
		c.MeasurementPolicy != policy || len(c.Rates) != len(definitions) {
		return errors.New("metering price catalog is incomplete")
	}
	for i, rate := range c.Rates {
		expected := definitions[i]
		if rate.Name != expected.name || rate.Unit != expected.unit ||
			!decimalPattern.MatchString(rate.UnitPrice) {
			return errors.New("metering price catalog contains an invalid rate")
		}
		value, ok := new(big.Rat).SetString(rate.UnitPrice)
		if !ok || value.Sign() < 0 {
			return errors.New("metering price catalog contains an invalid decimal")
		}
	}
	return nil
}

func catalogDefinitions(format string) ([]struct {
	name string
	unit string
}, string, bool) {
	switch format {
	case CatalogFormat:
		return pricedQuantities, MeasurementPolicy, true
	case CatalogFormatV2:
		return pricedQuantitiesV2, MeasurementPolicyV2, true
	case CatalogFormatV3:
		return pricedQuantitiesV3, MeasurementPolicyV3, true
	default:
		return nil, "", false
	}
}

func ReadCatalog(path string) (CatalogStatus, error) {
	var catalog Catalog
	data, err := readBoundedFile(path, "metering price catalog", maxPriceCatalogBytes)
	if err != nil {
		return CatalogStatus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return CatalogStatus{}, fmt.Errorf("decode metering price catalog: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return CatalogStatus{}, errors.New("metering price catalog contains trailing JSON")
	}
	if err := catalog.Validate(); err != nil {
		return CatalogStatus{}, err
	}
	canonical, err := json.Marshal(catalog)
	if err != nil {
		return CatalogStatus{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return CatalogStatus{}, errors.New("metering price catalog is not canonical")
	}
	sum := sha256.Sum256(data)
	return CatalogStatus{
		Catalog: catalog, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}

func WriteCatalogAtomic(path string, catalog Catalog) (CatalogStatus, error) {
	if path == "" {
		return CatalogStatus{}, errors.New("metering price catalog output is empty")
	}
	if err := catalog.Validate(); err != nil {
		return CatalogStatus{}, err
	}
	data, err := json.Marshal(catalog)
	if err != nil {
		return CatalogStatus{}, err
	}
	data = append(data, '\n')
	if err := writeCanonicalAtomic(path, data, "metering price catalog"); err != nil {
		return CatalogStatus{}, err
	}
	return ReadCatalog(path)
}

func writeCanonicalAtomic(path string, data []byte, description string) error {
	if existing, err := readBoundedFile(path, "existing "+description, int64(len(data))); err == nil {
		if bytes.Equal(existing, data) {
			return nil
		}
		return fmt.Errorf("refusing to overwrite existing %s %q", description, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Link(tempName, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func readBoundedFile(path, description string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", description, limit)
	}
	return data, nil
}
