package leasefault

import (
	"errors"
	"net"

	"google.golang.org/grpc"
	"k8s.io/client-go/dynamic"
)

// CommandConnections owns the two lazy RPC transports for one command attempt.
// Opening transports is not live endpoint/Pod admission. RunVerified must still
// check actual member identities and independently admitted endpoint mappings.
type CommandConnections struct {
	Client              dynamic.Interface
	Original, Successor *grpc.ClientConn
}

func (c *CommandConnections) Close() error {
	if c == nil {
		return nil
	}
	var err error
	if c.Original != nil {
		err = errors.Join(err, c.Original.Close())
	}
	if c.Successor != nil {
		err = errors.Join(err, c.Successor.Close())
	}
	return err
}

// OpenConnections derives the original RPC transport from the very endpoint and
// TLS files used to build the original probe argv. Observer credentials use the
// same admitted client identity, with a separately approved endpoint/server name.
// Literal, distinct IPs prevent a Service/DNS alias from silently selecting the
// original Pod as the supposedly independent observer. Files must pin all TLS
// inputs and the embedded-mTLS kubeconfig; no connection is returned on failure.
func (p ObservationCommandPlan) OpenConnections(kubeconfig, kubeContext, apiServer, observerEndpoint, observerServerName string, files map[string]string) (*CommandConnections, error) {
	originalHost, _, originalErr := net.SplitHostPort(p.Endpoint)
	observerHost, _, observerErr := net.SplitHostPort(observerEndpoint)
	originalIP, observerIP := net.ParseIP(originalHost), net.ParseIP(observerHost)
	if originalErr != nil || observerErr != nil || originalIP == nil || observerIP == nil || originalIP.Equal(observerIP) {
		return nil, errors.New("command requires distinct original and observer Pod IPs")
	}
	plan := ConnectionPlan{Kubeconfig: kubeconfig, Context: kubeContext, APIServer: apiServer, Endpoint: p.Endpoint, ServerName: p.ServerName, CA: p.CA, Certificate: p.Certificate, Key: p.Key, Files: files}
	client, original, err := NewFaultConnections(plan)
	if err != nil {
		return nil, err
	}
	plan.Endpoint, plan.ServerName = observerEndpoint, observerServerName
	_, successor, err := NewFaultConnections(plan)
	if err != nil {
		return nil, errors.Join(err, original.Close())
	}
	return &CommandConnections{Client: client, Original: original, Successor: successor}, nil
}

// Bind refuses preinstalled clients, so callers cannot validate one transport
// and accidentally run preparation or observation with a different one.
// RunVerified installs recovery using this same successor connection.
func (c *CommandConnections) Bind(r MeasuredNetworkFaultRuntime) (MeasuredNetworkFaultRuntime, error) {
	if c == nil || c.Client == nil || c.Original == nil || c.Successor == nil || c.Original == c.Successor || r.Network.Lifecycle.Preparation.Client != nil || r.Network.Lifecycle.Preparation.Connection != nil || r.Network.SuccessorConnection != nil || r.Network.Lifecycle.RecoveryConnection != nil {
		return MeasuredNetworkFaultRuntime{}, errors.New("invalid or conflicting command connections")
	}
	r.Network.Lifecycle.Preparation.Client = c.Client
	r.Network.Lifecycle.Preparation.Connection = c.Original
	r.Network.SuccessorConnection = c.Successor
	return r, nil
}
