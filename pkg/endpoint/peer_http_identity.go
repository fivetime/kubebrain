package endpoint

import (
	"net/http"

	"github.com/kubewharf/kubebrain/pkg/transportidentity"
	"google.golang.org/grpc"
)

func (e *Endpoint) newPeerHTTPTransport(rpc *grpc.Server, handler http.Handler) *grpcMuxedHTTPServer {
	return newGRPCMuxedHTTPServer(rpc, peerHTTPTransportIdentity(handler), e.tlsIdentities)
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
