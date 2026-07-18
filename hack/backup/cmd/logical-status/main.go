package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
)

func main() {
	status, err := backupfile.Inspect(os.Getenv("INPUT"))
	if err != nil {
		log.Fatalf("backup integrity validation failed: %v", err)
	}
	if os.Getenv("FIELD") == "records" {
		fmt.Println(status.Records)
		return
	}
	if err := json.NewEncoder(os.Stdout).Encode(status); err != nil {
		log.Fatal(err)
	}
}
