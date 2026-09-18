package option

import (
	"errors"

	"github.com/kubewharf/kubebrain/pkg/endpoint"
)

func (o *KubeBrainOption) loadPeerRetirementConfig() error {
	// Clear before every load: a failed validation cannot leave a previous
	// opt-in policy installed. Run re-reads before creating any storage client.
	o.epsConf.ExperimentalPeerRetirement = nil
	if o.peerRetirementConfigFile == "" {
		return nil
	}
	invalid := errors.New("invalid --experimental-peer-retirement-config: requires valid policy and strict peer mTLS")
	s := o.epsConf.PeerSecurityConfig
	if s == nil || s.CertFile == "" || s.KeyFile == "" || s.CA == "" || !s.ClientAuth || s.AllowInsecure {
		return invalid
	}
	policy, err := endpoint.LoadPeerRetirementOptions(o.peerRetirementConfigFile)
	if err != nil {
		return invalid
	}
	identity, err := o.buildIdentity()
	if err != nil || len(policy.HolderPins[identity]) == 0 {
		return invalid
	}
	for _, holder := range policy.EndpointHolders {
		if holder == identity {
			return invalid
		}
	}
	o.epsConf.ExperimentalPeerRetirement = policy
	return nil
}
