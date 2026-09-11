package tikv

import (
	"fmt"
	"os"
	"testing"

	clienttikv "github.com/tikv/client-go/v2/tikv"
)

// The SDK switch is not atomic. Initialize once, before any test can create
// clients or background workers; never toggle it between cases or -count runs.
var protocolFailpointsEnabled bool

func TestMain(m *testing.M) {
	switch os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_ENABLE_TEST_FAILPOINTS") {
	case "":
	case "1":
		clienttikv.EnableFailpoints()
		protocolFailpointsEnabled = true
	default:
		fmt.Fprintln(os.Stderr, "invalid explicit protocol test failpoint consent")
		os.Exit(2)
	}
	os.Exit(m.Run())
}
