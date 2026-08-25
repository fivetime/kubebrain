// Command jwt-kms-export-client obtains one versioned JWT signing key and its
// signed export receipt from the fixed external KMS provider API.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
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

const (
	maxTokenBytes    = 16 << 10
	maxCABytes       = 1 << 20
	maxResponseBytes = 1 << 20
	maxMaterialBytes = 512 << 10
	maxReceiptBytes  = 64 << 10
)

type exportRequest struct {
	Format    string `json:"format"`
	RequestID string `json:"request_id"`
	Instance  string `json:"instance"`
	VersionID string `json:"version_id"`
}

type exportResponse struct {
	Format   string          `json:"format"`
	Material string          `json:"material"`
	Receipt  json.RawMessage `json:"receipt"`
}

func main() {
	var endpoint, tokenFile, caFile, requestID, instance, versionID, materialOutput, receiptOutput string
	flag.StringVar(&endpoint, "endpoint", "", "external KMS provider HTTPS origin")
	flag.StringVar(&tokenFile, "token-file", "", "short-lived provider Bearer token file")
	flag.StringVar(&caFile, "ca-file", "", "provider CA bundle")
	flag.StringVar(&requestID, "request-id", "", "external rotation request ID")
	flag.StringVar(&instance, "instance", "", "DBaaS instance")
	flag.StringVar(&versionID, "version-id", "", "immutable external KMS version")
	flag.StringVar(&materialOutput, "material-output", "", "private material output path")
	flag.StringVar(&receiptOutput, "receipt-output", "", "signed receipt output path")
	flag.Parse()
	if flag.NArg() != 0 {
		fatal(errors.New("positional arguments are not supported"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	material, receipt, err := export(ctx, endpoint, tokenFile, caFile, requestID, instance, versionID)
	if err != nil {
		fatal(err)
	}
	if err := publishPair(materialOutput, receiptOutput, material, receipt); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, "JWT KMS export failed:", err); os.Exit(1) }

func export(ctx context.Context, endpoint, tokenFile, caFile, requestID, instance, versionID string) ([]byte, []byte, error) {
	if !validIdentity(requestID, 128) || !validIdentity(instance, 128) || !validVersion(versionID) {
		return nil, nil, errors.New("request identity is invalid")
	}
	base, err := url.Parse(endpoint)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || (base.Path != "" && base.Path != "/") || base.RawQuery != "" || base.Fragment != "" {
		return nil, nil, errors.New("endpoint must be an HTTPS origin")
	}
	token, err := readSecureFile(tokenFile, maxTokenBytes, 0o077)
	if err != nil {
		return nil, nil, errors.New("provider token file is invalid")
	}
	tokenText := strings.TrimSuffix(string(token), "\n")
	if tokenText == "" || strings.ContainsAny(tokenText, " \t\r\n") {
		return nil, nil, errors.New("provider token is invalid")
	}
	ca, err := readSecureFile(caFile, maxCABytes, 0o022)
	if err != nil {
		return nil, nil, errors.New("provider CA file is invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("provider CA contains no certificates")
	}
	payload, err := json.Marshal(exportRequest{Format: "kubebrain.jwt-kms-export-request.v1", RequestID: requestID, Instance: instance, VersionID: versionID})
	if err != nil {
		return nil, nil, err
	}
	base.Path = "/v1/jwt-key-versions:export"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+tokenText)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, errors.New("provider request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("provider returned HTTP %d", response.StatusCode)
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return nil, nil, errors.New("provider returned an unsupported content type")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) == 0 || len(body) > maxResponseBytes {
		return nil, nil, errors.New("provider response is invalid")
	}
	var result exportResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, nil, errors.New("provider response schema is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("provider response has trailing JSON")
	}
	if result.Format != "kubebrain.jwt-kms-export-response.v1" {
		return nil, nil, errors.New("provider response format is invalid")
	}
	material, err := base64.StdEncoding.Strict().DecodeString(result.Material)
	if err != nil || len(material) == 0 || len(material) > maxMaterialBytes || len(result.Receipt) == 0 || len(result.Receipt) > maxReceiptBytes {
		return nil, nil, errors.New("provider response payload is invalid")
	}
	var receipt map[string]json.RawMessage
	if err := json.Unmarshal(result.Receipt, &receipt); err != nil || len(receipt) != 3 || receipt["format"] == nil || receipt["payload"] == nil || receipt["signature"] == nil {
		return nil, nil, errors.New("provider receipt envelope is invalid")
	}
	return material, append([]byte(nil), result.Receipt...), nil
}

func publishPair(materialPath, receiptPath string, material, receipt []byte) error {
	if materialPath == receiptPath || !validOutputPath(materialPath) || !validOutputPath(receiptPath) {
		return errors.New("output paths must be distinct canonical absolute paths")
	}
	if err := writeNoClobber(materialPath, material); err != nil {
		return fmt.Errorf("publish material: %w", err)
	}
	if err := writeNoClobber(receiptPath, receipt); err != nil {
		_ = os.Remove(materialPath)
		return fmt.Errorf("publish receipt: %w", err)
	}
	return nil
}

func writeNoClobber(path string, data []byte) error {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".jwt-kms-export-*")
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

func readSecureFile(path string, maximum int64, forbidden os.FileMode) ([]byte, error) {
	if !validOutputPath(path) {
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
	finalInfo, statErr := file.Stat()
	if err != nil || statErr != nil || int64(len(data)) != info.Size() || finalInfo.Size() != info.Size() || len(data) == 0 || int64(len(data)) > maximum {
		return nil, errors.New("file read is invalid")
	}
	return data, nil
}

func validOutputPath(path string) bool {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Base(path) == "." {
		return false
	}
	directory := filepath.Dir(path)
	resolved, err := filepath.EvalSymlinks(directory)
	return err == nil && resolved == directory
}
func validIdentity(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, c := range value {
		if c <= ' ' || c == 0x7f {
			return false
		}
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
