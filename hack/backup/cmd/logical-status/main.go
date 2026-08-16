package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
)

func main() {
	if err := run(os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(out io.Writer) error {
	status, err := backupfile.Inspect(os.Getenv("INPUT"))
	if err != nil {
		return fmt.Errorf("backup integrity validation failed: %w", err)
	}
	if err := validateCompletion(status, time.Now()); err != nil {
		return fmt.Errorf("backup completion validation failed: %w", err)
	}
	switch os.Getenv("REQUIRE_GRANTED_TTL") {
	case "", "false":
	case "true":
		if err := requireGrantedTTL(os.Getenv("INPUT")); err != nil {
			return fmt.Errorf("backup physical lease validation failed: %w", err)
		}
	default:
		return errors.New("REQUIRE_GRANTED_TTL must be true or false")
	}
	switch os.Getenv("FIELD") {
	case "format":
		_, err = fmt.Fprintln(out, status.Format)
	case "prefix":
		_, err = fmt.Fprintln(out, status.Prefix)
	case "revision":
		_, err = fmt.Fprintln(out, status.Revision)
	case "created_at_unix":
		_, err = fmt.Fprintln(out, status.CreatedAtUnix)
	case "records":
		_, err = fmt.Fprintln(out, status.Records)
	case "leases":
		_, err = fmt.Fprintln(out, status.Leases)
	case "sha256":
		_, err = fmt.Fprintln(out, status.SHA256)
	default:
		err = json.NewEncoder(out).Encode(status)
	}
	return err
}

func requireGrantedTTL(path string) (retErr error) {
	verified, err := backupfile.OpenVerified(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, verified.Close()) }()
	return verified.Leases(func(lease record.Lease) error {
		if lease.GrantedTTL <= 0 {
			return fmt.Errorf("lease %d lacks a valid granted_ttl; re-export with the current logical exporter", lease.ID)
		}
		return nil
	})
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
