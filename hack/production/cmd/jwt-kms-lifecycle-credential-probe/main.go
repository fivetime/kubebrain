// Command jwt-kms-lifecycle-credential-probe obtains and verifies a short-lived,
// provider-signed readiness receipt before a lifecycle credential is activated.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const maxFileBytes = 1 << 20

type probeRequest struct {
	CredentialID               string `json:"credential_id"`
	CredentialSecretDataSHA256 string `json:"credential_secret_data_sha256"`
	CredentialSecretUID        string `json:"credential_secret_uid"`
	Format                     string `json:"format"`
}
type probeResponse struct {
	Format  string          `json:"format"`
	Receipt json.RawMessage `json:"receipt"`
}
type envelope struct {
	Format    string `json:"format"`
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}
type readinessReceipt struct {
	CredentialID               string   `json:"credential_id"`
	CredentialSecretDataSHA256 string   `json:"credential_secret_data_sha256"`
	CredentialSecretUID        string   `json:"credential_secret_uid"`
	Endpoint                   string   `json:"endpoint"`
	ExpiresAtUnix              int64    `json:"expires_at_unix"`
	Format                     string   `json:"format"`
	ObservedAtUnix             int64    `json:"observed_at_unix"`
	Principal                  string   `json:"principal"`
	Scopes                     []string `json:"scopes"`
}

func main() {
	var endpoint, tokenPath, caPath, keyPath, credentialID, secretUID, secretSHA, input, output string
	flag.StringVar(&endpoint, "endpoint", "", "external KMS HTTPS origin")
	flag.StringVar(&tokenPath, "token-file", "", "dedicated lifecycle token")
	flag.StringVar(&caPath, "ca-file", "", "provider CA bundle")
	flag.StringVar(&keyPath, "public-key", "", "provider Ed25519 receipt key")
	flag.StringVar(&credentialID, "credential-id", "", "versioned credential identifier")
	flag.StringVar(&secretUID, "credential-secret-uid", "", "immutable Secret UID")
	flag.StringVar(&secretSHA, "credential-secret-data-sha256", "", "canonical Secret data SHA-256")
	flag.StringVar(&input, "receipt-input", "", "verify an existing receipt without network access")
	flag.StringVar(&output, "receipt-output", "", "exclusive verified receipt output")
	flag.Parse()
	if flag.NArg() != 0 || output == "" {
		fatal(errors.New("receipt-output is required and positional arguments are unsupported"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	receipt, err := run(ctx, endpoint, tokenPath, caPath, keyPath, credentialID, secretUID, secretSHA, input, time.Now())
	if err != nil {
		fatal(err)
	}
	if err := writeNoClobber(output, receipt); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "JWT KMS lifecycle credential probe failed:", err)
	os.Exit(1)
}

func run(ctx context.Context, endpoint, tokenPath, caPath, keyPath, credentialID, secretUID, secretSHA, input string, now time.Time) ([]byte, error) {
	origin, err := parseOrigin(endpoint)
	if err != nil || !validID(credentialID, 40) || !validID(secretUID, 128) || !validSHA(secretSHA) {
		return nil, errors.New("probe identity or endpoint is invalid")
	}
	keyData, err := readSecure(keyPath, maxFileBytes, 0o022)
	if err != nil {
		return nil, errors.New("receipt public key is invalid")
	}
	key, err := parseKey(keyData)
	if err != nil {
		return nil, err
	}
	var receipt []byte
	if input != "" {
		receipt, err = readSecure(input, 64<<10, 0o022)
		if err != nil {
			return nil, errors.New("readiness receipt input is invalid")
		}
	} else {
		receipt, err = request(ctx, origin, tokenPath, caPath, credentialID, secretUID, secretSHA)
		if err != nil {
			return nil, err
		}
	}
	if err := verify(receipt, key, origin.String(), credentialID, secretUID, secretSHA, now); err != nil {
		return nil, err
	}
	return receipt, nil
}

func request(ctx context.Context, origin *url.URL, tokenPath, caPath, credentialID, secretUID, secretSHA string) ([]byte, error) {
	token, err := readSecure(tokenPath, 16<<10, 0o077)
	if err != nil {
		return nil, errors.New("lifecycle token file is invalid")
	}
	tokenText := strings.TrimSuffix(string(token), "\n")
	if tokenText == "" || strings.ContainsAny(tokenText, " \t\r\n") {
		return nil, errors.New("lifecycle token is invalid")
	}
	ca, err := readSecure(caPath, maxFileBytes, 0o022)
	if err != nil {
		return nil, errors.New("provider CA file is invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("provider CA contains no certificates")
	}
	body, _ := json.Marshal(probeRequest{Format: "kubebrain.jwt-kms-lifecycle-credential-probe-request.v1", CredentialID: credentialID, CredentialSecretUID: secretUID, CredentialSecretDataSHA256: secretSHA})
	target := *origin
	target.Path = "/v1/jwt-key-versions:lifecycle-probe"
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+tokenText)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(httpRequest)
	if err != nil {
		return nil, errors.New("provider readiness request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provider returned HTTP %d", response.StatusCode)
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return nil, errors.New("provider returned an unsupported content type")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxFileBytes+1))
	if err != nil || len(data) == 0 || len(data) > maxFileBytes {
		return nil, errors.New("provider response is invalid")
	}
	var result probeResponse
	if err := decodeExact(data, &result); err != nil || result.Format != "kubebrain.jwt-kms-lifecycle-credential-probe-response.v1" || len(result.Receipt) == 0 || len(result.Receipt) > 64<<10 {
		return nil, errors.New("provider response schema is invalid")
	}
	return append([]byte(nil), result.Receipt...), nil
}

func verify(data []byte, key ed25519.PublicKey, endpoint, credentialID, secretUID, secretSHA string, now time.Time) error {
	var outer envelope
	if err := decodeExact(data, &outer); err != nil || outer.Format != "kubebrain.jwt-kms-lifecycle-credential-readiness-envelope.v1" {
		return errors.New("readiness envelope is invalid")
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(outer.Payload)
	if err != nil || len(payload) == 0 || len(payload) > 64<<10 {
		return errors.New("readiness payload is invalid")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(outer.Signature)
	if err != nil || !ed25519.Verify(key, payload, signature) {
		return errors.New("readiness signature is invalid")
	}
	var receipt readinessReceipt
	if err := decodeExact(payload, &receipt); err != nil {
		return errors.New("readiness payload schema is invalid")
	}
	canonical, _ := json.Marshal(receipt)
	if !bytes.Equal(canonical, payload) {
		return errors.New("readiness payload is not canonical")
	}
	if receipt.Format != "kubebrain.jwt-kms-lifecycle-credential-readiness.v1" || receipt.CredentialID != credentialID || receipt.CredentialSecretUID != secretUID || receipt.CredentialSecretDataSHA256 != secretSHA || receipt.Endpoint != endpoint || !validPrincipal(receipt.Principal) || receipt.Scopes == nil || len(receipt.Scopes) != 2 || receipt.Scopes[0] != "promote" || receipt.Scopes[1] != "revoke" {
		return errors.New("readiness identity or scope binding is invalid")
	}
	if receipt.ObservedAtUnix <= 0 || receipt.ExpiresAtUnix <= receipt.ObservedAtUnix || receipt.ExpiresAtUnix-receipt.ObservedAtUnix > 900 || now.Unix() < receipt.ObservedAtUnix-60 || now.Unix() > receipt.ExpiresAtUnix {
		return errors.New("readiness validity window is invalid")
	}
	return nil
}

func parseOrigin(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("endpoint must be an HTTPS origin")
	}
	parsed.Path = ""
	return parsed, nil
}
func parseKey(data []byte) (ed25519.PublicKey, error) {
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "PUBLIC KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("receipt public key PEM is invalid")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	key, ok := parsed.(ed25519.PublicKey)
	if err != nil || !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("receipt public key must be Ed25519")
	}
	return key, nil
}
func decodeExact(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
func readSecure(path string, maximum int64, forbidden os.FileMode) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("invalid path")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open failed")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum || info.Mode().Perm()&forbidden != 0 {
		return nil, errors.New("file boundary is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, maximum+1))
	final, statErr := file.Stat()
	if err != nil || statErr != nil || int64(len(data)) != info.Size() || final.Size() != info.Size() {
		return nil, errors.New("file changed while reading")
	}
	return data, nil
}
func writeNoClobber(path string, data []byte) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("receipt output path is invalid")
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".jwt-kms-readiness-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
func validID(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for i, c := range []byte(value) {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '-' || c == '.' || c == '_' || c == ':')) {
			return false
		}
	}
	return true
}
func validSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func validPrincipal(value string) bool {
	if len(value) == 0 || len(value) > 512 {
		return false
	}
	for _, char := range value {
		if char <= ' ' || char == 0x7f {
			return false
		}
	}
	return true
}
