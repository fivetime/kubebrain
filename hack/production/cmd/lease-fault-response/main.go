// lease-fault-response validates the original probe JSONL on stdin. The caller
// must bound execution by the original fault deadline and independently verify
// isolation, probe exit, all remaining gates, and restoration.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/leasefault"
)

func run(args []string, input io.Reader, output io.Writer) error {
	f := flag.NewFlagSet("lease-fault-response", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	names := []string{"lease-id", "cluster-id", "initial-member-id", "initial-term", "successor-term", "fault-origin-ns"}
	values := make([]string, len(names))
	for i, name := range names {
		f.StringVar(&values[i], name, "", "independently recorded canonical decimal binding")
	}
	bad := errors.New("invalid response binding arguments")
	if f.Parse(args) != nil || f.NArg() != 0 {
		return bad
	}
	// Reject repeated options rather than silently taking the last identity.
	if len(args) != 2*len(names) {
		return bad
	}
	seen := make(map[string]bool)
	for i := 0; i < len(args); i += 2 {
		if seen[args[i]] || len(args[i]) < 3 || args[i][:2] != "--" || f.Lookup(args[i][2:]) == nil {
			return bad
		}
		seen[args[i]] = true
	}
	lease, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || strconv.FormatInt(lease, 10) != values[0] {
		return bad
	}
	var ids [4]uint64
	for i := range ids {
		ids[i], err = strconv.ParseUint(values[i+1], 10, 64)
		if err != nil || strconv.FormatUint(ids[i], 10) != values[i+1] {
			return bad
		}
	}
	origin, err := strconv.ParseInt(values[5], 10, 64)
	if err != nil || strconv.FormatInt(origin, 10) != values[5] {
		return bad
	}
	data, err := io.ReadAll(io.LimitReader(input, (12<<10)+1))
	if err != nil {
		return errors.New("cannot read original response evidence")
	}
	r, err := leasefault.ValidateOriginalResponse(data, leasefault.Binding{
		LeaseID: lease, ClusterID: ids[0], InitialMemberID: ids[1], InitialTerm: ids[2], SuccessorTerm: ids[3], Origin: time.Unix(0, origin),
	})
	if err != nil {
		return err
	}
	// Strings preserve exact integers for downstream JSON tools as well.
	return json.NewEncoder(output).Encode(struct {
		ResponseAt            string `json:"response_at"`
		ElapsedNS             string `json:"fault_to_response_ns"`
		TTL                   string `json:"ttl"`
		MemberID              string `json:"member_id"`
		Term                  string `json:"raft_term"`
		FaultAcceptanceProven bool   `json:"fault_acceptance_proven"`
	}{r.At.Format(time.RFC3339Nano), strconv.FormatInt(int64(r.FaultToResponse), 10), strconv.FormatInt(r.TTL, 10), strconv.FormatUint(r.MemberID, 10), strconv.FormatUint(r.Term, 10), false})
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
