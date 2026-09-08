// Command jwt-token-issuer obtains one etcd JWT without exposing credentials or
// the token through argv, environment variables, stdout, or stderr.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	maxCredentialBytes = 16 << 10
	maxPublicFileBytes = 1 << 20
	maxTokenBytes      = 1 << 20
)

type options struct {
	endpoint, credentialRoot, usernameFile, passwordFile string
	output, caFile, certFile, keyFile, serverName        string
	timeout                                              time.Duration
}

type tokenIssuer func(context.Context, options, string, string, *tls.Config) (string, error)

func main() {
	var o options
	flag.StringVar(&o.endpoint, "endpoint", "", "one exact approved etcd endpoint")
	flag.StringVar(&o.credentialRoot, "credential-root", "", "dedicated Kubernetes Secret mount root")
	flag.StringVar(&o.usernameFile, "username-file", "", "username file below credential root")
	flag.StringVar(&o.passwordFile, "password-file", "", "password file below credential root")
	flag.StringVar(&o.output, "output", "", "new 0600 no-clobber JWT output file")
	flag.StringVar(&o.caFile, "cacert", "", "trusted PEM CA for a TLS endpoint")
	flag.StringVar(&o.certFile, "cert", "", "optional PEM client certificate")
	flag.StringVar(&o.keyFile, "key", "", "optional PEM client private key")
	flag.StringVar(&o.serverName, "server-name", "", "optional TLS server-name override")
	flag.DurationVar(&o.timeout, "timeout", 10*time.Second, "dial and Authenticate timeout")
	flag.Parse()
	if err := run(context.Background(), o, authenticate); err != nil {
		fmt.Fprintln(os.Stderr, "JWT token issuer:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, o options, issue tokenIssuer) error {
	if strings.TrimSpace(o.endpoint) == "" || strings.ContainsAny(o.endpoint, "\r\n\t ,") {
		return errors.New("--endpoint must be one non-empty endpoint without whitespace or commas")
	}
	if o.credentialRoot == "" || o.usernameFile == "" || o.passwordFile == "" || o.output == "" || o.timeout <= 0 || o.timeout > time.Minute {
		return errors.New("credential files, --output, and 0 < --timeout <= 1m are required")
	}
	if exists, err := validateExistingToken(o.output); err != nil {
		return err
	} else if exists {
		return nil
	}
	usernameBytes, err := readCredentialFile(o.credentialRoot, o.usernameFile, maxCredentialBytes)
	if err != nil {
		return fmt.Errorf("read username: %w", err)
	}
	username := string(usernameBytes)
	if username == "" || strings.TrimSpace(username) != username || strings.ContainsAny(username, "\r\n\t") {
		return errors.New("username must be non-empty and contain no surrounding or control whitespace")
	}
	passwordBytes, err := readCredentialFile(o.credentialRoot, o.passwordFile, maxCredentialBytes)
	if err != nil {
		return fmt.Errorf("read password: %w", err)
	}
	if len(passwordBytes) == 0 {
		return errors.New("password must be non-empty")
	}
	tlsConfig, err := loadTLS(o)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, o.timeout)
	defer cancel()
	token, err := issue(ctx, o, username, string(passwordBytes), tlsConfig)
	if err != nil {
		return errors.New("etcd Authenticate failed")
	}
	if token == "" || len(token) > maxTokenBytes || strings.ContainsAny(token, "\r\n\t ") {
		return errors.New("etcd Authenticate returned an invalid token")
	}
	return publishNoClobber(o.output, []byte(token))
}

func authenticate(ctx context.Context, o options, username, password string, tlsConfig *tls.Config) (string, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{o.endpoint}, DialTimeout: o.timeout, TLS: tlsConfig})
	if err != nil {
		return "", err
	}
	defer cli.Close()
	response, err := cli.Authenticate(ctx, username, password)
	if err != nil {
		return "", err
	}
	return response.Token, nil
}

func readCredentialFile(root, path string, limit int64) ([]byte, error) {
	rootPath, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, errors.New("credential root cannot be resolved")
	}
	rootPath, err = filepath.Abs(rootPath)
	if err != nil {
		return nil, errors.New("credential root cannot be made absolute")
	}
	rootInfo, err := os.Stat(rootPath)
	if err != nil || !rootInfo.IsDir() {
		return nil, errors.New("credential root must be a directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, errors.New("credential path cannot be resolved")
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return nil, errors.New("credential path cannot be made absolute")
	}
	relative, err := filepath.Rel(rootPath, resolved)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return nil, errors.New("credential path must resolve below credential root")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("credential target must be a regular file")
	}
	if info.Mode().Perm()&0o222 != 0 || info.Mode().Perm()&0o007 != 0 {
		return nil, errors.New("credential target must be read-only and inaccessible to other")
	}
	if info.Size() <= 0 || info.Size() > limit {
		return nil, fmt.Errorf("credential must contain 1..%d bytes", limit)
	}
	return os.ReadFile(resolved)
}

func validateExistingToken(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.New("inspect existing output")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > maxTokenBytes || !ok || stat.Nlink != 1 {
		return false, errors.New("existing output must be a 0600 regular token file")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 || strings.ContainsAny(string(data), "\r\n\t ") {
		return false, errors.New("existing output contains an invalid token")
	}
	return true, nil
}

func publishNoClobber(output string, token []byte) error {
	directory := filepath.Dir(output)
	file, err := os.CreateTemp(directory, ".jwt-token-issuer-*")
	if err != nil {
		return errors.New("create token staging file")
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return errors.New("secure token staging file")
	}
	if _, err := file.Write(token); err != nil {
		file.Close()
		return errors.New("write token staging file")
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return errors.New("sync token staging file")
	}
	if err := file.Close(); err != nil {
		return errors.New("close token staging file")
	}
	if err := os.Link(temporary, output); err != nil {
		return errors.New("publish token without overwrite")
	}
	if err := os.Remove(temporary); err != nil {
		return errors.New("unlink token staging file")
	}
	dir, err := os.Open(directory)
	if err != nil {
		return errors.New("open token output directory")
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.New("sync token output directory")
	}
	return nil
}

func loadTLS(o options) (*tls.Config, error) {
	if o.caFile == "" && o.certFile == "" && o.keyFile == "" && o.serverName == "" {
		return nil, nil
	}
	if o.caFile == "" || (o.certFile == "") != (o.keyFile == "") {
		return nil, errors.New("TLS requires --cacert and paired --cert/--key")
	}
	caPEM, err := readPublicFile(o.caFile, maxPublicFileBytes)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("CA file contains no valid certificate")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: o.serverName}
	if o.certFile != "" {
		certPEM, err := readPublicFile(o.certFile, maxPublicFileBytes)
		if err != nil {
			return nil, fmt.Errorf("read client certificate: %w", err)
		}
		keyPEM, err := readPrivateFile(o.keyFile, maxPublicFileBytes)
		if err != nil {
			return nil, fmt.Errorf("read client key: %w", err)
		}
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, errors.New("load client key pair")
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func readPublicFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
		return nil, errors.New("file must be a bounded regular non-symlink file")
	}
	return os.ReadFile(path)
}

func readPrivateFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() <= 0 || info.Size() > limit {
		return nil, errors.New("file must be a bounded private regular non-symlink file")
	}
	return os.ReadFile(path)
}
