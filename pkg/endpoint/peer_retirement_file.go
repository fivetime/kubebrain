package endpoint

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

var errPeerRetirementFile = errors.New("invalid experimental peer retirement configuration file")

// LoadPeerRetirementOptions reads a startup-only, bounded operator policy. TLS
// files are still supplied by the peer TLS flags and reloaded independently.
// Do not echo file contents or parser errors into startup logs.
func LoadPeerRetirementOptions(path string) (*PeerRetirementOptions, error) {
	data, err := readPeerCredentialFile(path, 64<<10)
	if err != nil || !utf8.Valid(data) {
		return nil, errPeerRetirementFile
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueJSONValue(d, 0); err != nil {
		return nil, errPeerRetirementFile
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errPeerRetirementFile
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || len(fields) != 8 {
		return nil, errPeerRetirementFile
	}
	var scope, read, operation, send string
	o := &PeerRetirementOptions{}
	for name, target := range map[string]any{
		"scope": &scope, "holder_pins": &o.HolderPins, "endpoint_holders": &o.EndpointHolders,
		"read_budget": &read, "operation_budget": &operation, "send_budget": &send,
		"concurrency": &o.Concurrency, "requests_per_second": &o.RequestsPerSecond,
	} {
		raw, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, target) != nil {
			return nil, errPeerRetirementFile
		}
	}
	o.Scope = scope
	for _, budget := range []struct {
		text   string
		target *time.Duration
	}{{read, &o.ReadBudget}, {operation, &o.OperationBudget}, {send, &o.SendBudget}} {
		value, err := time.ParseDuration(budget.text)
		if err != nil || value <= 0 || value > time.Minute {
			return nil, errPeerRetirementFile
		}
		*budget.target = value
	}
	validText := func(s string) bool {
		return s != "" && len(s) <= 4096 && strings.TrimSpace(s) == s && strings.IndexFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) < 0
	}
	if !validText(scope) || len(o.HolderPins) < 2 || len(o.HolderPins) > 17 || len(o.EndpointHolders) == 0 || len(o.EndpointHolders) > 16 || o.Concurrency < 1 || o.Concurrency > 64 || !(o.RequestsPerSecond > 0 && o.RequestsPerSecond <= 1000) {
		return nil, errPeerRetirementFile
	}
	seen := make(map[string]bool)
	for holder, pins := range o.HolderPins {
		if !validText(holder) || len(pins) == 0 || len(pins) > 8 {
			return nil, errPeerRetirementFile
		}
		for _, pin := range pins {
			decoded, err := hex.DecodeString(pin)
			if err != nil || len(decoded) != 32 || seen[string(decoded)] {
				return nil, errPeerRetirementFile
			}
			seen[string(decoded)] = true
		}
	}
	for address, holder := range o.EndpointHolders {
		u, err := url.Parse(address)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.String() != address || len(o.HolderPins[holder]) == 0 {
			return nil, errPeerRetirementFile
		}
	}
	return o, nil
}

// encoding/json otherwise silently accepts duplicate object names. Limit nesting
// before typed decoding, and compare decoded names (including escape aliases).
func uniqueJSONValue(d *json.Decoder, depth int) error {
	if depth > 8 {
		return errPeerRetirementFile
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return errPeerRetirementFile
	}
	seen := make(map[string]bool)
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok || seen[name] {
				return errPeerRetirementFile
			}
			seen[name] = true
		}
		if err := uniqueJSONValue(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}
