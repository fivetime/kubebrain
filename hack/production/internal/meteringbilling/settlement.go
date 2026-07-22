package meteringbilling

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

const AdjustmentFormat = "kubebrain.metering-adjustment.v1"
const InvoicePlanFormat = "kubebrain.metering-invoice-plan.v1"
const InvoiceFormat = "kubebrain.metering-invoice.v1"
const BillingApprover = "system:serviceaccount:kubebrain-operations:kubebrain-billing-approver"
const maxSettlementArtifactBytes = 4 << 20

var approvalIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$`)

type Approval struct {
	ApprovedBy     string `json:"approved_by"`
	ApprovalID     string `json:"approval_id"`
	ApprovedAtUnix int64  `json:"approved_at_unix"`
}

type Adjustment struct {
	Format          string   `json:"format"`
	ID              string   `json:"id"`
	InvoiceID       string   `json:"invoice_id"`
	Instance        string   `json:"instance"`
	PeriodStartUnix int64    `json:"period_start_unix"`
	PeriodEndUnix   int64    `json:"period_end_unix"`
	Currency        string   `json:"currency"`
	AmountMicros    int64    `json:"amount_micros"`
	ReasonCode      string   `json:"reason_code"`
	ChargeSource    Source   `json:"charge_source"`
	Approval        Approval `json:"approval"`
}

type PlannedCharge struct {
	PeriodStartUnix int64  `json:"period_start_unix"`
	PeriodEndUnix   int64  `json:"period_end_unix"`
	ArtifactFormat  string `json:"artifact_format"`
}

type InvoicePlan struct {
	Format          string          `json:"format"`
	ID              string          `json:"id"`
	Instance        string          `json:"instance"`
	PeriodStartUnix int64           `json:"period_start_unix"`
	PeriodEndUnix   int64           `json:"period_end_unix"`
	Currency        string          `json:"currency"`
	Charges         []PlannedCharge `json:"charges"`
	AdjustmentIDs   []string        `json:"adjustment_ids"`
	Approval        Approval        `json:"approval"`
}

type InvoiceCharge struct {
	Source      Source `json:"source"`
	TotalMicros int64  `json:"total_micros"`
}

type InvoiceAdjustment struct {
	Source       Source `json:"source"`
	ID           string `json:"id"`
	AmountMicros int64  `json:"amount_micros"`
}

type Invoice struct {
	Format                string              `json:"format"`
	ID                    string              `json:"id"`
	Instance              string              `json:"instance"`
	PeriodStartUnix       int64               `json:"period_start_unix"`
	PeriodEndUnix         int64               `json:"period_end_unix"`
	Currency              string              `json:"currency"`
	PlanSource            Source              `json:"plan_source"`
	Charges               []InvoiceCharge     `json:"charges"`
	Adjustments           []InvoiceAdjustment `json:"adjustments"`
	SubtotalMicros        int64               `json:"subtotal_micros"`
	AdjustmentTotalMicros int64               `json:"adjustment_total_micros"`
	TotalMicros           int64               `json:"total_micros"`
	FinalizedAtUnix       int64               `json:"finalized_at_unix"`
}

type SettlementStatus[T any] struct {
	Value  T
	SHA256 string
	Bytes  int64
}

var adjustmentReasons = map[string]struct{}{
	"billing_error": {}, "service_credit": {}, "sla_credit": {}, "tax_correction": {},
}

func (a Approval) validate(notBefore int64) error {
	if a.ApprovedBy != BillingApprover || !approvalIDPattern.MatchString(a.ApprovalID) ||
		a.ApprovedAtUnix < notBefore {
		return errors.New("billing approval evidence is invalid")
	}
	return nil
}

func (a Adjustment) Validate() error {
	if a.Format != AdjustmentFormat || !versionPattern.MatchString(a.ID) ||
		!versionPattern.MatchString(a.InvoiceID) ||
		!versionPattern.MatchString(a.Instance) || a.PeriodStartUnix <= 0 ||
		a.PeriodEndUnix-a.PeriodStartUnix != 86400 || a.PeriodStartUnix%86400 != 0 ||
		!currencyPattern.MatchString(a.Currency) || a.AmountMicros == 0 {
		return errors.New("metering adjustment is incomplete")
	}
	if _, ok := adjustmentReasons[a.ReasonCode]; !ok {
		return errors.New("metering adjustment reason is unsupported")
	}
	artifactID := periodArtifactIDUnix(a.Instance, a.PeriodStartUnix, a.PeriodEndUnix)
	if (a.ChargeSource.ArtifactFormat != ChargeFormat &&
		a.ChargeSource.ArtifactFormat != ChargeFormatV2 &&
		a.ChargeSource.ArtifactFormat != ChargeFormatV3) ||
		validateSource(a.ChargeSource, a.ChargeSource.ArtifactFormat, artifactID,
			a.PeriodEndUnix) != nil {
		return errors.New("metering adjustment charge source is invalid")
	}
	return a.Approval.validate(a.PeriodEndUnix)
}

func (p InvoicePlan) Validate() error {
	if p.Format != InvoicePlanFormat || !versionPattern.MatchString(p.ID) ||
		!versionPattern.MatchString(p.Instance) || p.PeriodStartUnix <= 0 ||
		p.PeriodEndUnix <= p.PeriodStartUnix || p.PeriodStartUnix%86400 != 0 ||
		p.PeriodEndUnix%86400 != 0 || !currencyPattern.MatchString(p.Currency) ||
		len(p.Charges) == 0 ||
		int64(len(p.Charges)) != (p.PeriodEndUnix-p.PeriodStartUnix)/86400 {
		return errors.New("metering invoice plan is incomplete")
	}
	for i, charge := range p.Charges {
		start := p.PeriodStartUnix + int64(i)*86400
		if charge.PeriodStartUnix != start || charge.PeriodEndUnix != start+86400 ||
			(charge.ArtifactFormat != ChargeFormat && charge.ArtifactFormat != ChargeFormatV2 &&
				charge.ArtifactFormat != ChargeFormatV3) {
			return errors.New("metering invoice plan charges are not complete and ordered")
		}
	}
	for i, id := range p.AdjustmentIDs {
		if !versionPattern.MatchString(id) || (i > 0 && id <= p.AdjustmentIDs[i-1]) {
			return errors.New("metering invoice plan adjustment IDs are not unique and sorted")
		}
	}
	return p.Approval.validate(p.PeriodEndUnix)
}

func BuildInvoice(
	plan InvoicePlan,
	planSource Source,
	charges []Charge,
	chargeSources []Source,
	adjustments []Adjustment,
	adjustmentSources []Source,
	finalizedAtUnix int64,
) (Invoice, error) {
	if err := plan.Validate(); err != nil {
		return Invoice{}, err
	}
	if validateSource(planSource, InvoicePlanFormat, plan.ID, plan.PeriodEndUnix) != nil {
		return Invoice{}, errors.New("metering invoice plan source is invalid")
	}
	if len(charges) != len(plan.Charges) || len(chargeSources) != len(charges) ||
		len(adjustments) != len(plan.AdjustmentIDs) ||
		len(adjustmentSources) != len(adjustments) || finalizedAtUnix < plan.Approval.ApprovedAtUnix {
		return Invoice{}, errors.New("metering invoice inputs are incomplete")
	}
	invoiceCharges := make([]InvoiceCharge, len(charges))
	var subtotal int64
	for i, charge := range charges {
		planned := plan.Charges[i]
		source := chargeSources[i]
		if err := charge.Validate(); err != nil {
			return Invoice{}, err
		}
		artifactID := periodArtifactIDUnix(plan.Instance, planned.PeriodStartUnix, planned.PeriodEndUnix)
		if charge.Format != planned.ArtifactFormat || charge.Instance != plan.Instance ||
			charge.PeriodStartUnix != planned.PeriodStartUnix ||
			charge.PeriodEndUnix != planned.PeriodEndUnix || charge.Currency != plan.Currency ||
			validateSource(source, planned.ArtifactFormat, artifactID, plan.PeriodEndUnix) != nil {
			return Invoice{}, errors.New("metering invoice charge does not match plan")
		}
		next, ok := addInt64(subtotal, charge.TotalMicros)
		if !ok {
			return Invoice{}, errors.New("metering invoice subtotal overflows int64")
		}
		subtotal = next
		invoiceCharges[i] = InvoiceCharge{Source: source, TotalMicros: charge.TotalMicros}
	}
	invoiceAdjustments := make([]InvoiceAdjustment, len(adjustments))
	var adjustmentTotal int64
	for i, adjustment := range adjustments {
		source := adjustmentSources[i]
		if err := adjustment.Validate(); err != nil {
			return Invoice{}, err
		}
		if adjustment.ID != plan.AdjustmentIDs[i] || adjustment.InvoiceID != plan.ID ||
			adjustment.Instance != plan.Instance ||
			adjustment.Currency != plan.Currency ||
			adjustment.PeriodStartUnix < plan.PeriodStartUnix ||
			adjustment.PeriodEndUnix > plan.PeriodEndUnix ||
			validateSource(source, AdjustmentFormat, adjustment.ID, plan.PeriodEndUnix) != nil {
			return Invoice{}, errors.New("metering invoice adjustment does not match plan")
		}
		chargeIndex := int((adjustment.PeriodStartUnix - plan.PeriodStartUnix) / 86400)
		if chargeIndex < 0 || chargeIndex >= len(chargeSources) ||
			adjustment.ChargeSource != chargeSources[chargeIndex] {
			return Invoice{}, errors.New("metering invoice adjustment references a different charge")
		}
		next, ok := addInt64(adjustmentTotal, adjustment.AmountMicros)
		if !ok {
			return Invoice{}, errors.New("metering invoice adjustments overflow int64")
		}
		adjustmentTotal = next
		invoiceAdjustments[i] = InvoiceAdjustment{
			Source: source, ID: adjustment.ID, AmountMicros: adjustment.AmountMicros,
		}
	}
	total, ok := addInt64(subtotal, adjustmentTotal)
	if !ok || total < 0 {
		return Invoice{}, errors.New("metering invoice total is invalid")
	}
	invoice := Invoice{
		Format: InvoiceFormat, ID: plan.ID, Instance: plan.Instance,
		PeriodStartUnix: plan.PeriodStartUnix, PeriodEndUnix: plan.PeriodEndUnix,
		Currency: plan.Currency, PlanSource: planSource, Charges: invoiceCharges,
		Adjustments: invoiceAdjustments, SubtotalMicros: subtotal,
		AdjustmentTotalMicros: adjustmentTotal, TotalMicros: total,
		FinalizedAtUnix: finalizedAtUnix,
	}
	if err := invoice.Validate(); err != nil {
		return Invoice{}, err
	}
	return invoice, nil
}

func BuildInvoiceWithStatuses(
	planStatus SettlementStatus[InvoicePlan],
	planSource Source,
	chargeStatuses []ChargeStatus,
	chargeSources []Source,
	adjustmentStatuses []SettlementStatus[Adjustment],
	adjustmentSources []Source,
	finalizedAtUnix int64,
) (Invoice, error) {
	if planSource.ArtifactSHA256 != planStatus.SHA256 ||
		planSource.ObjectBytes != planStatus.Bytes {
		return Invoice{}, errors.New("metering invoice plan source does not match plan bytes")
	}
	if len(chargeStatuses) != len(chargeSources) ||
		len(adjustmentStatuses) != len(adjustmentSources) {
		return Invoice{}, errors.New("metering invoice status inputs are incomplete")
	}
	charges := make([]Charge, len(chargeStatuses))
	for index, status := range chargeStatuses {
		if chargeSources[index].ArtifactSHA256 != status.SHA256 ||
			chargeSources[index].ObjectBytes != status.Bytes {
			return Invoice{}, errors.New("metering invoice charge source does not match charge bytes")
		}
		charges[index] = status.Charge
	}
	adjustments := make([]Adjustment, len(adjustmentStatuses))
	for index, status := range adjustmentStatuses {
		if adjustmentSources[index].ArtifactSHA256 != status.SHA256 ||
			adjustmentSources[index].ObjectBytes != status.Bytes {
			return Invoice{}, errors.New("metering invoice adjustment source does not match adjustment bytes")
		}
		adjustments[index] = status.Value
	}
	return BuildInvoice(
		planStatus.Value, planSource, charges, chargeSources,
		adjustments, adjustmentSources, finalizedAtUnix,
	)
}

func (i Invoice) Validate() error {
	if i.Format != InvoiceFormat || !versionPattern.MatchString(i.ID) ||
		!versionPattern.MatchString(i.Instance) || i.PeriodStartUnix <= 0 ||
		i.PeriodEndUnix <= i.PeriodStartUnix || !currencyPattern.MatchString(i.Currency) ||
		len(i.Charges) == 0 || i.SubtotalMicros < 0 || i.TotalMicros < 0 ||
		i.FinalizedAtUnix < i.PeriodEndUnix ||
		validateSource(i.PlanSource, InvoicePlanFormat, i.ID, i.PeriodEndUnix) != nil {
		return errors.New("metering invoice is incomplete")
	}
	var subtotal int64
	for index, charge := range i.Charges {
		start := i.PeriodStartUnix + int64(index)*86400
		artifactID := periodArtifactIDUnix(i.Instance, start, start+86400)
		if charge.TotalMicros < 0 ||
			(charge.Source.ArtifactFormat != ChargeFormat &&
				charge.Source.ArtifactFormat != ChargeFormatV2 &&
				charge.Source.ArtifactFormat != ChargeFormatV3) ||
			validateSource(charge.Source, charge.Source.ArtifactFormat, artifactID,
				i.PeriodEndUnix) != nil {
			return errors.New("metering invoice contains an invalid charge")
		}
		var ok bool
		subtotal, ok = addInt64(subtotal, charge.TotalMicros)
		if !ok {
			return errors.New("metering invoice subtotal overflows int64")
		}
	}
	if int64(len(i.Charges)) != (i.PeriodEndUnix-i.PeriodStartUnix)/86400 ||
		subtotal != i.SubtotalMicros {
		return errors.New("metering invoice subtotal does not match charges")
	}
	var adjustmentTotal int64
	previous := ""
	for _, adjustment := range i.Adjustments {
		if !versionPattern.MatchString(adjustment.ID) ||
			(previous != "" && adjustment.ID <= previous) ||
			adjustment.AmountMicros == 0 ||
			validateSource(adjustment.Source, AdjustmentFormat, adjustment.ID,
				i.PeriodEndUnix) != nil {
			return errors.New("metering invoice contains an invalid adjustment")
		}
		var ok bool
		adjustmentTotal, ok = addInt64(adjustmentTotal, adjustment.AmountMicros)
		if !ok {
			return errors.New("metering invoice adjustments overflow int64")
		}
		previous = adjustment.ID
	}
	total, ok := addInt64(subtotal, adjustmentTotal)
	if !ok || adjustmentTotal != i.AdjustmentTotalMicros || total != i.TotalMicros {
		return errors.New("metering invoice total does not match sources")
	}
	return nil
}

func addInt64(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right ||
		right < 0 && left < math.MinInt64-right {
		return 0, false
	}
	return left + right, true
}

func periodArtifactIDUnix(instance string, start, end int64) string {
	return instance + ":" + strconv.FormatInt(start, 10) + ":" + strconv.FormatInt(end, 10)
}

func ReadAdjustment(path string) (SettlementStatus[Adjustment], error) {
	return readSettlement(path, "metering adjustment", func(value Adjustment) error {
		return value.Validate()
	})
}

func WriteAdjustmentAtomic(path string, value Adjustment) (SettlementStatus[Adjustment], error) {
	return writeSettlement(path, "metering adjustment", value, value.Validate, ReadAdjustment)
}

func ReadInvoicePlan(path string) (SettlementStatus[InvoicePlan], error) {
	return readSettlement(path, "metering invoice plan", func(value InvoicePlan) error {
		return value.Validate()
	})
}

func WriteInvoicePlanAtomic(path string, value InvoicePlan) (SettlementStatus[InvoicePlan], error) {
	return writeSettlement(path, "metering invoice plan", value, value.Validate, ReadInvoicePlan)
}

func ReadInvoice(path string) (SettlementStatus[Invoice], error) {
	return readSettlement(path, "metering invoice", func(value Invoice) error {
		return value.Validate()
	})
}

func WriteInvoiceAtomic(path string, value Invoice) (SettlementStatus[Invoice], error) {
	return writeSettlement(path, "metering invoice", value, value.Validate, ReadInvoice)
}

func readSettlement[T any](
	path, description string,
	validate func(T) error,
) (SettlementStatus[T], error) {
	var value T
	data, err := readBoundedFile(path, description, maxSettlementArtifactBytes)
	if err != nil {
		return SettlementStatus[T]{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return SettlementStatus[T]{}, fmt.Errorf("decode %s: %w", description, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return SettlementStatus[T]{}, fmt.Errorf("%s contains trailing JSON", description)
	}
	if err := validate(value); err != nil {
		return SettlementStatus[T]{}, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return SettlementStatus[T]{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return SettlementStatus[T]{}, fmt.Errorf("%s is not canonical", description)
	}
	sum := sha256.Sum256(data)
	return SettlementStatus[T]{
		Value: value, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}

func writeSettlement[T any](
	path, description string,
	value T,
	validate func() error,
	read func(string) (SettlementStatus[T], error),
) (SettlementStatus[T], error) {
	if path == "" {
		return SettlementStatus[T]{}, fmt.Errorf("%s output is empty", description)
	}
	if err := validate(); err != nil {
		return SettlementStatus[T]{}, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return SettlementStatus[T]{}, err
	}
	data = append(data, '\n')
	if err := writeCanonicalAtomic(path, data, description); err != nil {
		return SettlementStatus[T]{}, err
	}
	return read(path)
}

func SortedAdjustmentIDs(values []Adjustment) []string {
	ids := make([]string, len(values))
	for index, value := range values {
		ids[index] = value.ID
	}
	sort.Strings(ids)
	return ids
}

func settlementObjectKey(prefix, instance, id string) string {
	return filepath.ToSlash(filepath.Join(prefix, instance, id+".json"))
}
