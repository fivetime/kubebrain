package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadSourceRejectsOversizedInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.json")
	if err := os.WriteFile(path, make([]byte, maxInvoiceSourceBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := readSource(path)
	if err == nil || err.Error() != "invoice source exceeds 1048576 bytes" {
		t.Fatalf("readSource error = %v, want invoice source size error", err)
	}
}
