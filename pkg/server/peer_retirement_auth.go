package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"time"
)

var errPeerRetirementUnauthorized = errors.New("peer retirement unauthorized")

// peerRetirementAuthorizer is internal to the opt-in peer transport. TLS CA
// membership alone does not authorize a holder's retirement.
// Each configured holder must have distinct SHA-256 SPKI pins; multiple pins
// for ONE holder permit key rotation without granting another holder authority.
// Configuration must come from the instance operator, never the request.
type peerRetirementAuthorizer struct {
	instance string
	holders  map[string]map[[sha256.Size]byte]struct{}
}

func newPeerRetirementAuthorizer(instance string, holderPins map[string][]string) (*peerRetirementAuthorizer, error) {
	if instance == "" || len(holderPins) == 0 {
		return nil, errors.New("peer retirement requires explicit instance and holder identities")
	}
	a := &peerRetirementAuthorizer{instance: instance, holders: make(map[string]map[[sha256.Size]byte]struct{}, len(holderPins))}
	owners := make(map[[sha256.Size]byte]string)
	for holder, pins := range holderPins {
		if holder == "" || len(pins) == 0 {
			return nil, errors.New("peer retirement requires a key pin for every holder")
		}
		keys := make(map[[sha256.Size]byte]struct{}, len(pins))
		for _, pin := range pins {
			decoded, err := hex.DecodeString(pin)
			if err != nil || len(decoded) != sha256.Size {
				return nil, errors.New("invalid peer retirement key pin")
			}
			key := [sha256.Size]byte(decoded)
			if previous, exists := owners[key]; exists && previous != holder {
				return nil, errors.New("peer retirement keys must not be shared across holders")
			}
			owners[key] = holder
			keys[key] = struct{}{}
		}
		a.holders[holder] = keys
	}
	return a, nil
}

// authorize consumes transport-owned TLS state, never request-provided CNs,
// headers, source IPs or serialized certificate objects. A successful result
// proves only configured sender authority, NOT retirement or CAS freshness.
func (a *peerRetirementAuthorizer) authorize(state *tls.ConnectionState, instance, holder string, now time.Time) error {
	if state == nil || !state.HandshakeComplete {
		return errPeerRetirementUnauthorized
	}
	return a.authorizeVerifiedCertificate(state, instance, holder, now)
}

// authorizeVerifiedCertificate also supports tls.Config.VerifyConnection, which
// runs after certificate verification but before HandshakeComplete is published.
// Only that callback may use this entry before handshake completion. HTTP
// request authorization continues to require the completed-handshake wrapper.
func (a *peerRetirementAuthorizer) authorizeVerifiedCertificate(state *tls.ConnectionState, instance, holder string, now time.Time) error {
	if a == nil || instance != a.instance || state == nil || state.Version < tls.VersionTLS12 || len(state.PeerCertificates) == 0 {
		return errPeerRetirementUnauthorized
	}
	keys, known := a.holders[holder]
	if !known {
		return errPeerRetirementUnauthorized
	}
	leaf := state.PeerCertificates[0]
	if leaf == nil || len(leaf.Raw) == 0 || len(leaf.RawSubjectPublicKeyInfo) == 0 {
		return errPeerRetirementUnauthorized
	}
	// VerifiedChains must refer to this exact presented leaf. Check validity at
	// request time as well: an established connection can outlive its certificate.
	verified := false
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || chain[0] == nil || !bytes.Equal(chain[0].Raw, leaf.Raw) {
			continue
		}
		valid := true
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
				valid = false
				break
			}
		}
		verified = verified || valid
	}
	if !verified {
		return errPeerRetirementUnauthorized
	}
	if _, allowed := keys[sha256.Sum256(leaf.RawSubjectPublicKeyInfo)]; !allowed {
		return errPeerRetirementUnauthorized
	}
	return nil
}
