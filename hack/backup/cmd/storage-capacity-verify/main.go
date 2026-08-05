package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/api/resource"
)

const maxInventoryBytes = 4 << 20

type inventory struct {
	Claims []claim `json:"claims"`
	PVs    []pv    `json:"pvs"`
}

type claim struct {
	Name             string `json:"name"`
	PV               string `json:"pv"`
	RequestedStorage string `json:"requested_storage"`
}

type pv struct {
	Name     string `json:"name"`
	Capacity string `json:"capacity"`
}

func main() {
	if err := verify(os.Stdin); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func verify(r io.Reader) error {
	data, err := io.ReadAll(io.LimitReader(r, maxInventoryBytes+1))
	if err != nil {
		return fmt.Errorf("read storage inventory: %w", err)
	}
	if len(data) > maxInventoryBytes {
		return errors.New("storage inventory exceeds 4 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var value inventory
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode storage inventory: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return err
	}
	if len(value.Claims) == 0 || len(value.Claims) != len(value.PVs) {
		return errors.New("storage inventory cardinality mismatch")
	}

	capacities := make(map[string]resource.Quantity, len(value.PVs))
	for _, volume := range value.PVs {
		if volume.Name == "" || volume.Capacity == "" {
			return errors.New("PV name and capacity are required")
		}
		if _, exists := capacities[volume.Name]; exists {
			return fmt.Errorf("duplicate PV %q", volume.Name)
		}
		capacity, err := resource.ParseQuantity(volume.Capacity)
		if err != nil || capacity.Sign() < 0 {
			return fmt.Errorf("invalid capacity for PV %q", volume.Name)
		}
		capacities[volume.Name] = capacity
	}

	seenClaims := make(map[string]struct{}, len(value.Claims))
	for _, claim := range value.Claims {
		if claim.Name == "" || claim.PV == "" || claim.RequestedStorage == "" {
			return errors.New("PVC name, PV, and requested storage are required")
		}
		if _, exists := seenClaims[claim.Name]; exists {
			return fmt.Errorf("duplicate PVC %q", claim.Name)
		}
		seenClaims[claim.Name] = struct{}{}
		requested, err := resource.ParseQuantity(claim.RequestedStorage)
		if err != nil || requested.Sign() < 0 {
			return fmt.Errorf("invalid storage request for PVC %q", claim.Name)
		}
		capacity, exists := capacities[claim.PV]
		if !exists {
			return fmt.Errorf("PVC %q references missing PV %q", claim.Name, claim.PV)
		}
		if capacity.Cmp(requested) < 0 {
			return fmt.Errorf("PV %q capacity %s is smaller than PVC %q request %s", claim.PV, capacity.String(), claim.Name, requested.String())
		}
	}
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("storage inventory contains trailing JSON")
		}
		return fmt.Errorf("decode storage inventory: %w", err)
	}
	return nil
}
