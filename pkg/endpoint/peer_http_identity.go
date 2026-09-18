package endpoint

import (
	"net/http"
	"time"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"google.golang.org/grpc"
)

func (e *Endpoint) newPeerHTTPTransport(rpc *grpc.Server, handler http.Handler) *grpcMuxedHTTPServer {
	transport := newGRPCMuxedHTTPServer(rpc, peerHTTPTransportIdentity(handler), e.tlsIdentities)
	if e.config != nil && e.config.ExperimentalPeerRetirement != nil {
		// Bound admission before the control handler's body/storage budgets begin.
		// Leave whole-request deadlines unset: this port also carries gRPC streams.
		transport.httpServer.svr.ReadHeaderTimeout = 5 * time.Second
		transport.httpServer.svr.MaxHeaderBytes = 16 << 10
		transport.classificationTimeout = 5 * time.Second
	}
	return transport
}

// TLS terminates outside net/http in the peer cmux listener, so Request.TLS is
// normally nil even for verified mTLS. Restore only connection-owned state from
// the registry, never a header or caller-supplied serialized certificate. Keep
// an existing native TLS state authoritative. Authorization remains the
// handler's responsibility (including verified chain, scope and member pin).
func peerHTTPTransportIdentity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			if state, ok := transportidentity.TLSStateFromContext(r.Context()); ok {
				r = r.Clone(r.Context())
				r.TLS = &state
			}
		}
		next.ServeHTTP(w, r)
	})
}
