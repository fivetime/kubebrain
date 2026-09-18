// peer-retirement-scope computes operator policy from already verified inputs.
// It performs no network access, storage reads/writes, or cluster discovery.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	f := flag.NewFlagSet("peer-retirement-scope", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	var cluster uint64
	var keyspace, prefix string
	f.Uint64Var(&cluster, "storage-cluster-id", 0, "independently verified PD/TiKV cluster ID")
	f.StringVar(&keyspace, "keyspace", "", "exact keyspace; explicitly pass empty for the default namespace")
	f.StringVar(&prefix, "election-prefix", "", "effective backend prefix, including any CLI keyspace suffix")
	invalid := errors.New("requires --storage-cluster-id, --keyspace and --election-prefix; no positional arguments")
	if f.Parse(args) != nil || f.NArg() != 0 {
		return invalid
	}
	count := 0
	f.Visit(func(*flag.Flag) { count++ })
	if count != 3 {
		return invalid
	}
	scope, err := election.ComputeRetirementScope(cluster, keyspace, prefix)
	if err != nil {
		return invalid
	}
	return json.NewEncoder(out).Encode(struct {
		Scope          string `json:"scope"`
		Cluster        string `json:"storage_cluster_id"`
		Keyspace       string `json:"keyspace"`
		Prefix         string `json:"election_prefix"`
		InputsVerified bool   `json:"inputs_verified"`
	}{scope, strconv.FormatUint(cluster, 10), keyspace, prefix, false})
}
