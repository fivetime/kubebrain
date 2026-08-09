package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

const maxInputBytes = 4 << 20

type witness struct {
	Format        string `json:"format"`
	Prefix        string `json:"prefix"`
	Revision      int64  `json:"revision"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	Records       int    `json:"records"`
	Leases        int    `json:"leases"`
	SHA256        string `json:"sha256"`
	FileSHA256    string `json:"file_sha256"`
}

type receipt struct {
	Format          string          `json:"format"`
	OperationID     string          `json:"operation_id"`
	CreatedAt       string          `json:"created_at"`
	Inventory       json.RawMessage `json:"inventory"`
	Snapshots       json.RawMessage `json:"snapshots"`
	SemanticWitness witness         `json:"semantic_witness"`
}

func main() {
	inventoryPath := flag.String("inventory", "", "normalized frozen preflight inventory JSON")
	snapshotsPath := flag.String("snapshots", "", "validated snapshot inventory JSON")
	witnessPath := flag.String("witness-status", "", "logical-status JSON")
	operationID := flag.String("operation-id", "", "cold snapshot operation ID")
	createdAt := flag.String("created-at", "", "snapshot capture completion time")
	witnessFileSHA := flag.String("witness-file-sha256", "", "SHA-256 of the frozen logical witness file")
	flag.Parse()

	value, err := build(*inventoryPath, *snapshotsPath, *witnessPath, *operationID, *createdAt, *witnessFileSHA)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func build(inventoryPath, snapshotsPath, witnessPath, operationID, createdAt, witnessFileSHA string) (receipt, error) {
	if inventoryPath == "" || snapshotsPath == "" || witnessPath == "" || operationID == "" {
		return receipt{}, errors.New("receipt input paths and operation ID must be non-empty")
	}
	if _, err := time.Parse(time.RFC3339, createdAt); err != nil {
		return receipt{}, errors.New("snapshot created-at is invalid")
	}
	if !validDigest(witnessFileSHA) {
		return receipt{}, errors.New("witness file SHA-256 is invalid")
	}
	inventory, err := readJSON(inventoryPath, "inventory")
	if err != nil {
		return receipt{}, err
	}
	snapshots, err := readJSON(snapshotsPath, "snapshots")
	if err != nil {
		return receipt{}, err
	}
	var inventoryObject map[string]any
	if err := decodeStrict(inventory, &inventoryObject); err != nil || inventoryObject == nil {
		return receipt{}, errors.New("inventory must be one JSON object")
	}
	var snapshotArray []json.RawMessage
	if err := decodeStrict(snapshots, &snapshotArray); err != nil || snapshotArray == nil {
		return receipt{}, errors.New("snapshots must be one JSON array")
	}
	witnessData, err := readJSON(witnessPath, "witness status")
	if err != nil {
		return receipt{}, err
	}
	var semantic witness
	if err := decodeStrict(witnessData, &semantic); err != nil {
		return receipt{}, fmt.Errorf("decode witness status: %w", err)
	}
	if semantic.Format != "kubebrain.logical.v2" || semantic.Prefix == "" || semantic.Revision <= 0 ||
		semantic.CreatedAtUnix <= 0 || semantic.Records < 0 || semantic.Leases < 0 || !validDigest(semantic.SHA256) {
		return receipt{}, errors.New("witness status is incomplete")
	}
	semantic.FileSHA256 = witnessFileSHA
	return receipt{
		Format: "kubebrain.cold-physical-snapshot.v2", OperationID: operationID, CreatedAt: createdAt,
		Inventory: inventory, Snapshots: snapshots, SemanticWitness: semantic,
	}, nil
}

func readJSON(path, description string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxInputBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxInputBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", description, maxInputBytes)
	}
	return data, nil
}

func decodeStrict(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("input contains trailing JSON")
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
