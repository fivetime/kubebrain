package election

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
)

// MaxOwnershipConditionBytes bounds the encoded condition, including base64.
// A transport must enforce this limit while reading, not only after buffering.
const MaxOwnershipConditionBytes = 96 << 10

var errInvalidOwnershipCondition = errors.New("invalid ownership condition")

type ownershipConditionWire struct {
	Version int    `json:"version"`
	Holder  string `json:"holder"`
	Record  []byte `json:"record"`
	Token   []byte `json:"token"`
}

// MarshalBinary preserves the exact record bytes used by CAS. The output
// contains a replayable transaction condition and must not be logged. It proves
// neither sender identity nor retirement; only the joined lifecycle may send it.
func (c OwnershipCondition) MarshalBinary() ([]byte, error) {
	if _, err := validateRetiredOwnership(c.claim); err != nil {
		return nil, errInvalidOwnershipCondition
	}
	payload, err := json.Marshal(ownershipConditionWire{1, c.claim.holder, c.claim.record, c.claim.token})
	if err != nil || len(payload) > MaxOwnershipConditionBytes {
		return nil, errInvalidOwnershipCondition
	}
	return payload, nil
}

// ParseOwnershipCondition binds the message to a holder already authorized by
// the transport for this instance. Passing an unauthenticated request's holder
// here does NOT authenticate it. Decoding never performs storage I/O or release.
func ParseOwnershipCondition(payload []byte, authorizedHolder string) (OwnershipCondition, error) {
	invalid := func() (OwnershipCondition, error) { return OwnershipCondition{}, errInvalidOwnershipCondition }
	if authorizedHolder == "" || len(payload) == 0 || len(payload) > MaxOwnershipConditionBytes {
		return invalid()
	}
	d := json.NewDecoder(bytes.NewReader(payload))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return invalid()
	}
	var wire ownershipConditionWire
	var recordEncoded, tokenEncoded string
	seen := make(map[string]bool, 4)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return invalid()
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return invalid()
		}
		seen[name] = true
		var target any
		switch name {
		case "version":
			target = &wire.Version
		case "holder":
			target = &wire.Holder
		case "record":
			target = &recordEncoded
		case "token":
			target = &tokenEncoded
		default:
			return invalid()
		}
		if err := d.Decode(target); err != nil {
			return invalid()
		}
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') || len(seen) != 4 || wire.Version != 1 || wire.Holder != authorizedHolder {
		return invalid()
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return invalid()
	}
	// Decoding directly into []byte also accepts JSON numeric arrays. Restrict
	// the wire schema to base64 strings without alternative byte representations.
	wire.Record, err = base64.StdEncoding.Strict().DecodeString(recordEncoded)
	if err != nil {
		return invalid()
	}
	wire.Token, err = base64.StdEncoding.Strict().DecodeString(tokenEncoded)
	if err != nil {
		return invalid()
	}
	claim := retiredOwnership{holder: wire.Holder, record: wire.Record, token: wire.Token}
	if _, err := validateRetiredOwnership(claim); err != nil {
		return invalid()
	}
	return OwnershipCondition{claim: claim}, nil
}
