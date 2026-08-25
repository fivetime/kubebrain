// Command jwt-kms-lifecycle-client performs one tightly bound external KMS
// promotion or revocation and persists only the provider-signed receipt.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
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

const maxLifecycleInputBytes = 1 << 20
const maxLifecycleReceiptBytes = 64 << 10

type operationReceipt struct {
	Attempt          int64  `json:"attempt"`
	CompletedAtUnix  int64  `json:"completed_at_unix"`
	Format           string `json:"format"`
	Instance         string `json:"instance"`
	NewKeyVersionID  string `json:"new_key_version_id"`
	OldKeyVersionID  string `json:"old_key_version_id"`
	OperationID      string `json:"operation_id"`
	OperationUID     string `json:"operation_uid"`
	ParametersSHA256 string `json:"parameters_sha256"`
	PhaseAGateSHA    string `json:"phase_a_gate_receipt_sha256"`
	PhaseAPublishSHA string `json:"phase_a_publish_receipt_sha256"`
	PhaseBGateSHA    string `json:"phase_b_gate_receipt_sha256"`
	PhaseBPublishSHA string `json:"phase_b_publish_receipt_sha256"`
	PhaseCGateSHA    string `json:"phase_c_gate_receipt_sha256"`
	PhaseCPublishSHA string `json:"phase_c_publish_receipt_sha256"`
	RequestID        string `json:"request_id"`
}
type lifecycleRequest struct {
	Format                 string `json:"format"`
	Instance               string `json:"instance"`
	NewVersionID           string `json:"new_version_id"`
	OldVersionID           string `json:"old_version_id"`
	OperationID            string `json:"operation_id"`
	OperationReceiptSHA256 string `json:"operation_receipt_sha256"`
	PreviousReceiptSHA256  string `json:"previous_receipt_sha256"`
	RequestID              string `json:"request_id"`
}
type lifecycleResponse struct {
	Format  string          `json:"format"`
	Receipt json.RawMessage `json:"receipt"`
}

func main() {
	var action, endpoint, tokenFile, caFile, operationPath, previousPath, output string
	flag.StringVar(&action, "action", "", "promote or revoke")
	flag.StringVar(&endpoint, "endpoint", "", "external KMS lifecycle HTTPS origin")
	flag.StringVar(&tokenFile, "token-file", "", "dedicated short-lived lifecycle Bearer token")
	flag.StringVar(&caFile, "ca-file", "", "provider CA bundle")
	flag.StringVar(&operationPath, "operation-receipt", "", "canonical JWT rotation v3 receipt")
	flag.StringVar(&previousPath, "previous-receipt", "", "promotion receipt required for revoke")
	flag.StringVar(&output, "receipt-output", "", "signed lifecycle receipt output")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("positional arguments are not supported"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	receipt, err := mutate(ctx, action, endpoint, tokenFile, caFile, operationPath, previousPath)
	if err != nil {
		fatal(err)
	}
	if err := writeNoClobber(output, receipt); err != nil {
		fatal(err)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "JWT KMS lifecycle mutation failed:", err); os.Exit(1) }

func mutate(ctx context.Context, action, endpoint, tokenFile, caFile, operationPath, previousPath string) ([]byte, error) {
	if action != "promote" && action != "revoke" {
		return nil, errors.New("action must be promote or revoke")
	}
	operationData, err := readSecure(operationPath, maxLifecycleInputBytes, 0o022)
	if err != nil {
		return nil, errors.New("operation receipt file is invalid")
	}
	var operation operationReceipt
	if err := decodeCanonical(operationData, &operation); err != nil || operation.Format != "kubebrain.jwt-key-rotation.operation.receipt.v3" {
		return nil, errors.New("operation receipt is not canonical v3")
	}
	if operation.Attempt <= 0 || operation.Attempt > 5 || operation.CompletedAtUnix <= 0 || !validRequestID(operation.RequestID) || !validOperationID(operation.OperationID) || !validInstance(operation.Instance) || !validVersion(operation.OldKeyVersionID) || !validVersion(operation.NewKeyVersionID) || operation.OldKeyVersionID == operation.NewKeyVersionID || operation.OperationUID == "" || len(operation.OperationUID) > 253 {
		return nil, errors.New("operation receipt identity is invalid")
	}
	for _, digest := range []string{operation.ParametersSHA256, operation.PhaseAGateSHA, operation.PhaseAPublishSHA, operation.PhaseBGateSHA, operation.PhaseBPublishSHA, operation.PhaseCGateSHA, operation.PhaseCPublishSHA} {
		if !validSHA(digest) {
			return nil, errors.New("operation receipt digest is invalid")
		}
	}
	operationSum := sha256.Sum256(operationData)
	previousSHA := ""
	if action == "promote" {
		if previousPath != "" {
			return nil, errors.New("promotion must not have a previous receipt")
		}
	} else {
		if previousPath == "" {
			return nil, errors.New("revocation requires the promotion receipt")
		}
		previous, readErr := readSecure(previousPath, maxLifecycleReceiptBytes, 0o022)
		if readErr != nil {
			return nil, errors.New("promotion receipt file is invalid")
		}
		sum := sha256.Sum256(previous)
		previousSHA = hex.EncodeToString(sum[:])
	}
	base, err := url.Parse(endpoint)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || (base.Path != "" && base.Path != "/") || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("endpoint must be an HTTPS origin")
	}
	base.Path = "/v1/jwt-key-versions:" + action
	token, err := readSecure(tokenFile, 16<<10, 0o077)
	if err != nil {
		return nil, errors.New("lifecycle token file is invalid")
	}
	tokenText := strings.TrimSuffix(string(token), "\n")
	if tokenText == "" || strings.ContainsAny(tokenText, " \t\r\n") {
		return nil, errors.New("lifecycle token is invalid")
	}
	ca, err := readSecure(caFile, 1<<20, 0o022)
	if err != nil {
		return nil, errors.New("provider CA file is invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("provider CA contains no certificates")
	}
	payload, err := json.Marshal(lifecycleRequest{Format: "kubebrain.jwt-kms-lifecycle-request.v1", Instance: operation.Instance, NewVersionID: operation.NewKeyVersionID, OldVersionID: operation.OldKeyVersionID, OperationID: operation.OperationID, OperationReceiptSHA256: hex.EncodeToString(operationSum[:]), PreviousReceiptSHA256: previousSHA, RequestID: operation.RequestID})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+tokenText)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("provider lifecycle request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provider returned HTTP %d", response.StatusCode)
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return nil, errors.New("provider returned an unsupported content type")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxLifecycleReceiptBytes+4097))
	if err != nil || len(body) == 0 || len(body) > maxLifecycleReceiptBytes+4096 {
		return nil, errors.New("provider response is invalid")
	}
	var result lifecycleResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, errors.New("provider response schema is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("provider response has trailing JSON")
	}
	if result.Format != "kubebrain.jwt-kms-lifecycle-response.v1" || len(result.Receipt) == 0 || len(result.Receipt) > maxLifecycleReceiptBytes {
		return nil, errors.New("provider lifecycle receipt is invalid")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(result.Receipt, &envelope); err != nil || len(envelope) != 3 || envelope["format"] == nil || envelope["payload"] == nil || envelope["signature"] == nil {
		return nil, errors.New("provider lifecycle envelope is invalid")
	}
	return append([]byte(nil), result.Receipt...), nil
}

func decodeCanonical(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	canonical, err := json.Marshal(output)
	if err != nil || !bytes.Equal(canonical, data) {
		return errors.New("noncanonical JSON")
	}
	return nil
}
func readSecure(path string, maximum int64, forbidden os.FileMode) ([]byte, error) {
	if !canonicalPath(path) {
		return nil, errors.New("path is invalid")
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
	if !canonicalPath(path) {
		return errors.New("receipt output path is invalid")
	}
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".jwt-kms-lifecycle-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(temporary, path); err != nil {
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
func canonicalPath(path string) bool {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return false
	}
	directory := filepath.Dir(path)
	resolved, err := filepath.EvalSymlinks(directory)
	return err == nil && resolved == directory
}
func validSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == string(bytes.ToLower([]byte(value)))
}
func validRequestID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	isAlnum := func(c byte) bool { return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' }
	if !isAlnum(value[0]) || !isAlnum(value[len(value)-1]) {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if isAlnum(c) || c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}
func validOperationID(value string) bool {
	const prefix = "jwt-key-rotate-"
	if len(value) != len(prefix)+20 || !strings.HasPrefix(value, prefix) {
		return false
	}
	for _, c := range []byte(value[len(prefix):]) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func validInstance(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (i > 0 && (c == '.' || c == '_' || c == '-')) {
			continue
		}
		return false
	}
	return true
}
func validVersion(value string) bool {
	if len(value) == 0 || len(value) > 512 {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (i > 0 && strings.ContainsRune("._:/@+=-", rune(c))) {
			continue
		}
		return false
	}
	return true
}
