// Package metricsworker validates the private protected-metrics-worker protocol.
// Records alone do not prove artifact validity or successful process cleanup.
package metricsworker

import (
	"bufio"
	"errors"
	"io"
	"path/filepath"
	"strings"
)

const maxRecordBytes = 8192

type Ready struct{ Worker, Baseline string }
type Captured struct{ Capture, Schedule string }

// Protocol is single-consumer and fail-closed: any malformed or out-of-order
// record permanently invalidates the stream. The caller must bound blocking
// reads with process cancellation and join the worker before restoration.
type Protocol struct {
	r        *bufio.Reader
	owner    string
	state    int
	baseline string
}

func NewProtocol(r io.Reader, owner string) (*Protocol, error) {
	if r == nil || !filepath.IsAbs(owner) || filepath.Clean(owner) != owner || owner == "/" || strings.ContainsAny(owner, "\x00\r\n\t") {
		return nil, errors.New("invalid worker protocol owner")
	}
	return &Protocol{r: bufio.NewReaderSize(r, maxRecordBytes), owner: owner}, nil
}

func (p *Protocol) record(state int, kind string) ([]string, error) {
	if p.state != state {
		p.state = -1
		return nil, errors.New("worker protocol out of order")
	}
	p.state = -1
	line, err := p.r.ReadSlice('\n')
	if err != nil {
		return nil, errors.New("incomplete or oversized worker record")
	}
	fields := strings.Split(strings.TrimSuffix(string(line), "\n"), "\t")
	if len(fields) != 3 || fields[0] != kind {
		return nil, errors.New("invalid worker record")
	}
	return fields, nil
}

func (p *Protocol) path(path, prefix string) bool {
	if filepath.Clean(path) != path || filepath.Dir(path) != p.owner || strings.ContainsAny(path, "\x00\r\n\t") {
		return false
	}
	suffix, ok := strings.CutPrefix(filepath.Base(path), prefix+".")
	if !ok || len(suffix) != 8 {
		return false
	}
	for _, c := range suffix {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// ReadReady must be followed by independent LoadCapture validation of Baseline
// before the outer supervisor permits fault injection.
func (p *Protocol) ReadReady() (Ready, error) {
	f, err := p.record(0, "READY")
	if err != nil {
		return Ready{}, err
	}
	if !p.path(f[1], "metrics-worker") || !p.path(f[2], "metrics") {
		return Ready{}, errors.New("invalid ready paths")
	}
	p.state, p.baseline = 1, f[2]
	return Ready{Worker: f[1], Baseline: f[2]}, nil
}

func (p *Protocol) ReadCaptured() (Captured, error) {
	f, err := p.record(1, "CAPTURED")
	if err != nil {
		return Captured{}, err
	}
	if !p.path(f[1], "metrics") || !p.path(f[2], "metrics-schedule") || f[1] == p.baseline {
		return Captured{}, errors.New("invalid captured paths")
	}
	p.state = 2
	return Captured{Capture: f[1], Schedule: f[2]}, nil
}

// Finish rejects extra output. EOF is not a successful exit or a cleanup proof;
// the supervisor must additionally wait and validate receipts and artifacts.
func (p *Protocol) Finish() error {
	if p.state != 2 {
		p.state = -1
		return errors.New("incomplete worker protocol")
	}
	p.state = -1
	_, err := p.r.ReadByte()
	if err != io.EOF {
		return errors.New("trailing worker output or read error")
	}
	p.state = 3
	return nil
}
