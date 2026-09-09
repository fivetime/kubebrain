package imageprepull

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var ErrResponseTooLarge = errors.New("pre-pull API response exceeds its byte limit")

// NewBoundedClient uses certificate-verified HTTPS and the native context-aware
// HTTP transport. timeout bounds each HTTP attempt, including body reads; SDK
// retries/backoff must additionally be bounded by the caller's operation/phase
// context (Executor supplies phase deadlines). Bodies are fully size-checked
// before Kubernetes decoding, so a valid prefix cannot hide oversized trailing
// bytes. This is a finite-request client, not a watch/log/exec streaming client.
// External credential plugins and custom transports are not accepted: arbitrary
// extension code is not guaranteed to honor cancellation. Static client certs,
// bearer tokens/token files, and in-cluster configurations remain supported.
func NewBoundedClient(config *rest.Config, timeout time.Duration, maxResponseBytes int64) (kubernetes.Interface, error) {
	if config == nil || timeout <= 0 || timeout > 30*time.Second || maxResponseBytes < 1 || maxResponseBytes > 32<<20 {
		return nil, errors.New("pre-pull API client requires a config, timeout <=30s and response limit 1..32MiB")
	}
	endpoint, err := url.Parse(config.Host)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || config.Insecure {
		return nil, errors.New("pre-pull API client requires a certificate-verified HTTPS endpoint without URL credentials/query/fragment")
	}
	if config.ExecProvider != nil || config.AuthProvider != nil || config.Transport != nil || config.WrapTransport != nil || config.Dial != nil || config.Proxy != nil {
		return nil, errors.New("pre-pull bounded API client does not support external auth plugins or custom transport/dial/proxy callbacks")
	}
	copy := rest.CopyConfig(config)
	copy.Timeout = timeout
	// Avoid decompression outside the bounded reader. The native transport will
	// not request compressed responses; unsolicited encodings fail closed.
	copy.DisableCompression = true
	client, err := rest.HTTPClientFor(copy)
	if err != nil {
		return nil, err
	}
	client.Transport = boundedResponses{base: client.Transport, limit: maxResponseBytes}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("pre-pull API redirects are forbidden")
	}
	return kubernetes.NewForConfigAndClient(copy, client)
}

type boundedResponses struct {
	base  http.RoundTripper
	limit int64
}

func (b boundedResponses) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	response, err := b.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, errors.New("pre-pull API returned an incomplete HTTP response")
	}
	defer response.Body.Close()
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return nil, errors.New("pre-pull API returned an unrequested content encoding")
	}
	if response.ContentLength > b.limit {
		return nil, ErrResponseTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, b.limit+1))
	if err != nil {
		return nil, fmt.Errorf("read bounded pre-pull API response: %w", err)
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	if int64(len(body)) > b.limit {
		return nil, ErrResponseTooLarge
	}
	if response.ContentLength >= 0 && int64(len(body)) != response.ContentLength {
		return nil, errors.New("pre-pull API response content length is inconsistent")
	}
	result := *response
	result.Body = io.NopCloser(bytes.NewReader(body))
	return &result, nil
}
