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
	"strconv"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
)

const ChargeFormat = "kubebrain.metering-charge.v1"
const RoundingPolicy = "half_even_to_currency_micro.v1"

type Source struct {
	ArtifactFormat  string `json:"artifact_format"`
	ArtifactID      string `json:"artifact_id"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
}

type Line struct {
	Name            string `json:"name"`
	Unit            string `json:"unit"`
	QuantityDecimal string `json:"quantity_decimal"`
	UnitPrice       string `json:"unit_price"`
	AmountMicros    int64  `json:"amount_micros"`
}

type Charge struct {
	Format          string `json:"format"`
	Instance        string `json:"instance"`
	PeriodStartUnix int64  `json:"period_start_unix"`
	PeriodEndUnix   int64  `json:"period_end_unix"`
	Currency        string `json:"currency"`
	PriceVersion    string `json:"price_version"`
	RoundingPolicy  string `json:"rounding_policy"`
	RollupSource    Source `json:"rollup_source"`
	CatalogSource   Source `json:"catalog_source"`
	Lines           []Line `json:"lines"`
	TotalMicros     int64  `json:"total_micros"`
}

type ChargeStatus struct {
	Charge Charge
	SHA256 string
	Bytes  int64
}

func BuildCharge(
	rollup meteringarchive.Rollup,
	rollupSource Source,
	catalog Catalog,
	catalogSource Source,
) (Charge, error) {
	if err := rollup.Validate(); err != nil {
		return Charge{}, err
	}
	if err := catalog.Validate(); err != nil {
		return Charge{}, err
	}
	if catalog.EffectiveStartUnix > rollup.PeriodStartUnix ||
		catalog.EffectiveEndUnix < rollup.PeriodEndUnix {
		return Charge{}, errors.New("price catalog does not cover the complete metering period")
	}
	if err := validateSource(
		rollupSource, meteringarchive.RollupFormat,
		rollup.Instance+":"+strconv.FormatInt(rollup.PeriodStartUnix, 10)+":"+
			strconv.FormatInt(rollup.PeriodEndUnix, 10),
		rollup.PeriodEndUnix,
	); err != nil {
		return Charge{}, fmt.Errorf("rollup source: %w", err)
	}
	if err := validateSource(catalogSource, CatalogFormat, catalog.Version, rollup.PeriodEndUnix); err != nil {
		return Charge{}, fmt.Errorf("catalog source: %w", err)
	}
	lines := make([]Line, len(pricedQuantities))
	var total int64
	for i, expected := range pricedQuantities {
		quantity := rollup.Quantities[i]
		rate := catalog.Rates[i]
		if quantity.Name != expected.name || quantity.Unit != expected.unit ||
			rate.Name != expected.name || rate.Unit != expected.unit {
			return Charge{}, errors.New("rollup quantity and price rate do not align")
		}
		quantityDecimal := strconv.FormatFloat(quantity.Value, 'f', -1, 64)
		amount, err := amountMicros(quantityDecimal, rate.UnitPrice)
		if err != nil {
			return Charge{}, fmt.Errorf("price %s: %w", expected.name, err)
		}
		if amount > 0 && total > int64(^uint64(0)>>1)-amount {
			return Charge{}, errors.New("metering charge total overflows int64")
		}
		total += amount
		lines[i] = Line{
			Name: expected.name, Unit: expected.unit, QuantityDecimal: quantityDecimal,
			UnitPrice: rate.UnitPrice, AmountMicros: amount,
		}
	}
	charge := Charge{
		Format: ChargeFormat, Instance: rollup.Instance,
		PeriodStartUnix: rollup.PeriodStartUnix, PeriodEndUnix: rollup.PeriodEndUnix,
		Currency: catalog.Currency, PriceVersion: catalog.Version,
		RoundingPolicy: RoundingPolicy, RollupSource: rollupSource, CatalogSource: catalogSource,
		Lines: lines, TotalMicros: total,
	}
	if err := charge.Validate(); err != nil {
		return Charge{}, err
	}
	return charge, nil
}

func (c Charge) Validate() error {
	if c.Format != ChargeFormat || c.Instance == "" ||
		c.PeriodStartUnix <= 0 || c.PeriodEndUnix <= c.PeriodStartUnix ||
		!currencyPattern.MatchString(c.Currency) || !versionPattern.MatchString(c.PriceVersion) ||
		c.RoundingPolicy != RoundingPolicy || len(c.Lines) != len(pricedQuantities) ||
		c.TotalMicros < 0 {
		return errors.New("metering charge is incomplete")
	}
	if err := validateSource(c.RollupSource, meteringarchive.RollupFormat,
		c.Instance+":"+strconv.FormatInt(c.PeriodStartUnix, 10)+":"+
			strconv.FormatInt(c.PeriodEndUnix, 10), c.PeriodEndUnix); err != nil {
		return err
	}
	if err := validateSource(c.CatalogSource, CatalogFormat, c.PriceVersion, c.PeriodEndUnix); err != nil {
		return err
	}
	var total int64
	for i, line := range c.Lines {
		expected := pricedQuantities[i]
		if line.Name != expected.name || line.Unit != expected.unit ||
			!decimalPattern.MatchString(line.QuantityDecimal) ||
			!decimalPattern.MatchString(line.UnitPrice) || line.AmountMicros < 0 {
			return errors.New("metering charge contains an invalid line")
		}
		amount, err := amountMicros(line.QuantityDecimal, line.UnitPrice)
		if err != nil || amount != line.AmountMicros {
			return errors.New("metering charge line amount is not reproducible")
		}
		if amount > 0 && total > int64(^uint64(0)>>1)-amount {
			return errors.New("metering charge total overflows int64")
		}
		total += amount
	}
	if total != c.TotalMicros {
		return errors.New("metering charge total does not match line amounts")
	}
	return nil
}

func validateSource(source Source, format, artifactID string, minRetainUntil int64) error {
	if source.ArtifactFormat != format || source.ArtifactID != artifactID ||
		source.ObjectKey == "" || source.VersionID == "" ||
		!digestPattern.MatchString(source.ArtifactSHA256) || source.ObjectBytes <= 0 ||
		source.RetainUntilUnix < minRetainUntil {
		return errors.New("immutable source is incomplete")
	}
	return nil
}

func amountMicros(quantity, unitPrice string) (int64, error) {
	q, ok := new(big.Rat).SetString(quantity)
	if !ok || q.Sign() < 0 {
		return 0, errors.New("quantity decimal is invalid")
	}
	price, ok := new(big.Rat).SetString(unitPrice)
	if !ok || price.Sign() < 0 {
		return 0, errors.New("unit price decimal is invalid")
	}
	scaled := new(big.Rat).Mul(q, price)
	scaled.Mul(scaled, big.NewRat(1_000_000, 1))
	rounded := roundHalfEven(scaled)
	if !rounded.IsInt64() {
		return 0, errors.New("line amount overflows int64")
	}
	return rounded.Int64(), nil
}

func roundHalfEven(value *big.Rat) *big.Int {
	quotient := new(big.Int)
	remainder := new(big.Int)
	quotient.QuoRem(value.Num(), value.Denom(), remainder)
	twiceRemainder := new(big.Int).Lsh(remainder, 1)
	comparison := twiceRemainder.Cmp(value.Denom())
	if comparison > 0 || (comparison == 0 && quotient.Bit(0) == 1) {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

func WriteChargeAtomic(path string, charge Charge) (ChargeStatus, error) {
	if path == "" {
		return ChargeStatus{}, errors.New("metering charge output is empty")
	}
	if err := charge.Validate(); err != nil {
		return ChargeStatus{}, err
	}
	data, err := json.Marshal(charge)
	if err != nil {
		return ChargeStatus{}, err
	}
	data = append(data, '\n')
	if err := writeCanonicalAtomic(path, data, "metering charge"); err != nil {
		return ChargeStatus{}, err
	}
	sum := sha256.Sum256(data)
	return ChargeStatus{
		Charge: charge, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}

func ReadCharge(path string) (ChargeStatus, error) {
	var charge Charge
	data, err := os.ReadFile(path)
	if err != nil {
		return ChargeStatus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&charge); err != nil {
		return ChargeStatus{}, fmt.Errorf("decode metering charge: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ChargeStatus{}, errors.New("metering charge contains trailing JSON")
	}
	if err := charge.Validate(); err != nil {
		return ChargeStatus{}, err
	}
	canonical, err := json.Marshal(charge)
	if err != nil {
		return ChargeStatus{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return ChargeStatus{}, errors.New("metering charge is not canonical")
	}
	sum := sha256.Sum256(data)
	return ChargeStatus{
		Charge: charge, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}
