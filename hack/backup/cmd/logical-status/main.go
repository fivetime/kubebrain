package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
)

func main() {
	status, err := backupfile.Inspect(os.Getenv("INPUT"))
	if err != nil {
		log.Fatalf("backup integrity validation failed: %v", err)
	}
	if err := validateCompletion(status, time.Now()); err != nil {
		log.Fatalf("backup completion validation failed: %v", err)
	}
	switch os.Getenv("FIELD") {
	case "format":
		fmt.Println(status.Format)
		return
	case "prefix":
		fmt.Println(status.Prefix)
		return
	case "revision":
		fmt.Println(status.Revision)
		return
	case "created_at_unix":
		fmt.Println(status.CreatedAtUnix)
		return
	case "records":
		fmt.Println(status.Records)
		return
	case "leases":
		fmt.Println(status.Leases)
		return
	}
	if err := json.NewEncoder(os.Stdout).Encode(status); err != nil {
		log.Fatal(err)
	}
}

func validateCompletion(status backupfile.Status, now time.Time) error {
	if expected := os.Getenv("EXPECTED_PREFIX"); expected != "" && status.Prefix != expected {
		return fmt.Errorf("expected prefix %q, got %q", expected, status.Prefix)
	}
	if raw := os.Getenv("MIN_RECORDS"); raw != "" {
		minimum, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || minimum < 0 {
			return fmt.Errorf("MIN_RECORDS must be a non-negative integer, got %q", raw)
		}
		if int64(status.Records) < minimum {
			return fmt.Errorf("expected at least %d records, got %d", minimum, status.Records)
		}
	}
	if raw := os.Getenv("MAX_AGE_SECONDS"); raw != "" {
		maximum, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || maximum <= 0 {
			return fmt.Errorf("MAX_AGE_SECONDS must be a positive integer, got %q", raw)
		}
		if status.CreatedAtUnix == 0 {
			return fmt.Errorf("backup does not contain a creation timestamp")
		}
		age := now.Unix() - status.CreatedAtUnix
		if age < -300 {
			return fmt.Errorf("backup creation timestamp is %d seconds in the future", -age)
		}
		if age > maximum {
			return fmt.Errorf("backup is %d seconds old, maximum is %d", age, maximum)
		}
	}
	return nil
}
