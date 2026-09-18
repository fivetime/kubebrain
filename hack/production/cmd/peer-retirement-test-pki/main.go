// peer-retirement-test-pki creates disposable, distinct peer identities offline.
// It never contacts Kubernetes and must not be used as a production CA service.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"k8s.io/apimachinery/pkg/util/validation"
)

type options struct {
	dir, service, members, keyspace, prefix string
	cluster                                 uint64
	port                                    int
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	f := flag.NewFlagSet("peer-retirement-test-pki", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	var o options
	f.StringVar(&o.dir, "output-dir", "", "new absolute directory in a trusted private parent")
	f.StringVar(&o.service, "peer-service", "", "peer service DNS name")
	f.StringVar(&o.members, "members", "", "comma-separated member names, used as directory names and DNS prefixes")
	f.Uint64Var(&o.cluster, "storage-cluster-id", 0, "independently verified storage cluster ID")
	f.StringVar(&o.keyspace, "keyspace", "", "exact keyspace; explicitly pass empty if applicable")
	f.StringVar(&o.prefix, "election-prefix", "", "effective backend election prefix")
	f.IntVar(&o.port, "peer-port", 0, "peer TLS port")
	if f.Parse(args) != nil || f.NArg() != 0 {
		return errors.New("invalid test PKI arguments")
	}
	count := 0
	f.Visit(func(*flag.Flag) { count++ })
	if count != 7 {
		return errors.New("all seven test PKI flags must be explicit")
	}
	return generate(o, time.Now())
}

func generate(o options, now time.Time) error {
	invalid := errors.New("invalid test PKI scope, members, service, port or output directory")
	scope, err := election.ComputeRetirementScope(o.cluster, o.keyspace, o.prefix)
	members := strings.Split(o.members, ",")
	if err != nil || !filepath.IsAbs(o.dir) || filepath.Clean(o.dir) != o.dir || o.dir == "/" || len(validation.IsDNS1123Subdomain(o.service)) != 0 || o.port < 1 || o.port > 65535 || len(members) < 2 || len(members) > 17 {
		return invalid
	}
	seen := make(map[string]bool)
	for _, member := range members {
		if len(validation.IsDNS1123Label(member)) != 0 || seen[member] || len(validation.IsDNS1123Subdomain(member+"."+o.service)) != 0 {
			return invalid
		}
		seen[member] = true
	}
	// Exclusive mkdir refuses existing directories and symlinks. Never overwrite
	// an earlier CA or recursively remove partial evidence after a failure.
	if err := os.Mkdir(o.dir, 0700); err != nil {
		return err
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := serialNumber()
	if err != nil {
		return err
	}
	ca := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "kubebrain-disposable-peer-ca"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(72 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, MaxPathLenZero: true}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		return err
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := writeNew(filepath.Join(o.dir, "ca.crt"), caPEM); err != nil {
		return err
	}
	if err := writeKey(filepath.Join(o.dir, "ca.key"), caKey); err != nil {
		return err
	}
	pins := make(map[string][]string)
	holders := make(map[string]string)
	for _, member := range members {
		dir := filepath.Join(o.dir, member)
		if err := os.Mkdir(dir, 0700); err != nil {
			return err
		}
		dns := member + "." + o.service
		holder := net.JoinHostPort(dns, strconv.Itoa(o.port))
		holders[member] = holder
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		serial, err := serialNumber()
		if err != nil {
			return err
		}
		leaf := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: member},
			DNSNames: []string{dns, o.service}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour),
			BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		if err != nil {
			return err
		}
		parsed, err := x509.ParseCertificate(der)
		if err != nil {
			return err
		}
		pin := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)
		pins[holder] = []string{hex.EncodeToString(pin[:])}
		if err := writeNew(filepath.Join(dir, "tls.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})); err != nil {
			return err
		}
		if err := writeKey(filepath.Join(dir, "tls.key"), key); err != nil {
			return err
		}
		if err := writeNew(filepath.Join(dir, "ca.crt"), caPEM); err != nil {
			return err
		}
	}
	for _, member := range members {
		targets := make(map[string]string)
		for other, holder := range holders {
			if other != member {
				targets["https://"+holder] = holder
			}
		}
		policy, err := json.MarshalIndent(map[string]any{"scope": scope, "holder_pins": pins, "endpoint_holders": targets,
			"read_budget": "1s", "operation_budget": "1s", "send_budget": "1s", "concurrency": 2, "requests_per_second": 4}, "", "  ")
		if err != nil {
			return err
		}
		if err := writeNew(filepath.Join(o.dir, member, "policy.json"), append(policy, '\n')); err != nil {
			return err
		}
	}
	// A final marker distinguishes complete bundles from partial failure. It is
	// not a deployment approval or verification of caller-provided scope inputs.
	return writeNew(filepath.Join(o.dir, "COMPLETE"), []byte("disposable offline test PKI; no deployment performed\n"))
}

func serialNumber() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	return n.Add(n, big.NewInt(1)), nil
}

func writeKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	return writeNew(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func writeNew(path string, data []byte) (retErr error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	_, err = f.Write(data)
	return err
}
