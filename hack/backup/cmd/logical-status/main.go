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
	switch os.Getenv("FIELD") {
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
