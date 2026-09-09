package imageprepull

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// A preparation snapshot contains fingerprints, not source workload payloads.
// It cannot authorize a rollout without RestoreVerified's fresh API checks.
type preparationSnapshot struct {
	Image               string            `json:"image"`
	ClientService       string            `json:"clientService"`
	SourceSpecSHA256    string            `json:"sourceSpecSHA256"`
	SourceRevision      string            `json:"sourceRevision"`
	RuntimeClassSHA256  string            `json:"runtimeClassSHA256"`
	PriorityClassSHA256 string            `json:"priorityClassSHA256"`
	ServicesSHA256      string            `json:"servicesSHA256"`
	Nodes               []preparationNode `json:"nodes"`
}

type preparationNode struct {
	Name         string    `json:"name"`
	UID          types.UID `json:"uid"`
	Architecture string    `json:"architecture"`
	Runtime      string    `json:"runtime"`
	JobName      string    `json:"jobName"`
	PodUID       types.UID `json:"podUID"`
	JobSHA256    string    `json:"jobSHA256"`
	HoldSeconds  int64     `json:"holdSeconds"`
	TTLSeconds   int32     `json:"ttlSeconds"`
	Digests      []string  `json:"digests"`
}

func fingerprint(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), nil
}

func runtimeFingerprint(class *nodev1.RuntimeClass) (string, error) {
	if class == nil {
		return fingerprint(nil)
	}
	// ResourceVersion and unrelated metadata may change without changing the
	// runtime contract. Replacement UID, scheduling, handler and overhead may not.
	return fingerprint(struct {
		Name       string
		UID        types.UID
		Handler    string
		Scheduling *nodev1.Scheduling
		Overhead   *nodev1.Overhead
	}{class.Name, class.UID, class.Handler, class.Scheduling, class.Overhead})
}

func servicesFingerprint(services []corev1.Service) (string, error) {
	type serviceIdentity struct {
		Namespace string
		Name      string
		UID       types.UID
		Deleting  bool
		Spec      corev1.ServiceSpec
	}
	identities := make([]serviceIdentity, 0, len(services))
	for _, service := range services {
		identities = append(identities, serviceIdentity{service.Namespace, service.Name, service.UID, service.DeletionTimestamp != nil, service.Spec})
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].Name < identities[j].Name })
	return fingerprint(identities)
}

func (j *RecoveryJournal) savePreparation(session *Session) error {
	if err := j.checkSession(session); err != nil {
		return err
	}
	if j.record.Preparation != nil {
		return errors.New("pre-pull preparation snapshot already exists")
	}
	first := session.entries[0].request
	proof := &preparationSnapshot{Image: first.Image, ClientService: first.ClientServiceName, SourceRevision: first.Source.Status.CurrentRevision}
	var err error
	proof.SourceSpecSHA256, err = fingerprint(first.Source.Spec)
	if err != nil {
		return err
	}
	proof.RuntimeClassSHA256, err = runtimeFingerprint(first.RuntimeClass)
	if err != nil {
		return err
	}
	proof.PriorityClassSHA256, err = priorityFingerprint(first.PriorityClass)
	if err != nil {
		return err
	}
	proof.ServicesSHA256, err = servicesFingerprint(first.Services)
	if err != nil {
		return err
	}
	for _, entry := range session.entries {
		jobHash, err := fingerprint(entry.wanted)
		if err != nil {
			return err
		}
		proof.Nodes = append(proof.Nodes, preparationNode{
			Name: entry.request.Node.Name, UID: entry.request.Node.UID,
			Architecture: entry.request.Node.Status.NodeInfo.Architecture, Runtime: entry.request.Node.Status.NodeInfo.ContainerRuntimeVersion,
			JobName: entry.wanted.Name, PodUID: entry.podUID, JobSHA256: jobHash,
			HoldSeconds: entry.request.HoldSeconds, TTLSeconds: entry.request.TTLSeconds, Digests: append([]string(nil), entry.digests...),
		})
	}
	next := j.record
	next.Preparation = proof
	return j.persist(next)
}

func validatePreparation(proof *preparationSnapshot, targets []recoveryTarget) error {
	if proof == nil {
		return nil
	}
	if !pinnedImage.MatchString(proof.Image) || len(proof.Image) > 2048 ||
		len(validation.IsDNS1123Label(proof.ClientService)) != 0 || !validIdentity(proof.SourceRevision) ||
		!validDigest(proof.SourceSpecSHA256) || !validDigest(proof.RuntimeClassSHA256) || !validDigest(proof.PriorityClassSHA256) || !validDigest(proof.ServicesSHA256) ||
		len(proof.Nodes) == 0 || len(proof.Nodes) != len(targets) {
		return errors.New("invalid pre-pull preparation snapshot identity or fingerprints")
	}
	names, uids, pods := map[string]bool{}, map[types.UID]bool{}, map[types.UID]bool{}
	for i, node := range proof.Nodes {
		if len(validation.IsDNS1123Subdomain(node.Name)) != 0 || !validIdentity(string(node.UID)) || names[node.Name] || uids[node.UID] ||
			!validIdentity(string(node.PodUID)) || pods[node.PodUID] || !validDigest(node.JobSHA256) ||
			(node.Architecture != "amd64" && node.Architecture != "arm64") || !validIdentity(node.Runtime) ||
			node.HoldSeconds < 60 || node.HoldSeconds > 86400 || node.TTLSeconds < 1 || node.TTLSeconds > 3600 ||
			node.JobName != targets[i].Name || !targets[i].Attempted || targets[i].UID == "" || targets[i].Removed || len(node.Digests) < 1 || len(node.Digests) > 4 {
			return errors.New("pre-pull preparation lacks a live, unique, fully identified target")
		}
		names[node.Name], uids[node.UID], pods[node.PodUID] = true, true, true
		digests := map[string]bool{}
		for _, digest := range node.Digests {
			if !validDigest(digest) || digests[digest] {
				return errors.New("pre-pull preparation contains invalid or duplicate platform digests")
			}
			digests[digest] = true
		}
	}
	return nil
}

func (j *RecoveryJournal) invalidatePreparation() error {
	if j.closed {
		return errors.New("pre-pull recovery journal is closed")
	}
	if j.record.Preparation == nil {
		return nil
	}
	next := j.record
	next.Preparation = nil
	return j.persist(next)
}

// RestoreVerified reconstructs a Session only from a prepared snapshot plus
// fresh, matching source/runtime/Service/node evidence and still-live original
// holder Job/Pod UIDs. It performs GET/LIST only; failure never creates replacement
// holders or retries preparation. Supply freshly reviewed image/digest evidence,
// not values blindly copied from the receipt. Still fence the subsequent
// StatefulSet image mutation by UID/resourceVersion/spec in the rollout runner.
func (e Executor) RestoreVerified(ctx context.Context, expectedImage string, approved map[string][]string) (*Session, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	journal := e.Journal
	if journal == nil || journal.closed || journal.record.Preparation == nil {
		return nil, errors.New("no active prepared snapshot; cleanup receipts alone cannot authorize verification")
	}
	if err := validateRecoveryRecord(journal.record); err != nil {
		return nil, err
	}
	proof := journal.record.Preparation
	if proof.Image != expectedImage {
		return nil, errors.New("prepared image differs from the currently approved image")
	}
	for _, node := range proof.Nodes {
		if !reflect.DeepEqual(node.Digests, approved["linux/"+node.Architecture]) {
			return nil, errors.New("prepared platform digests differ from current reviewed evidence")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, e.PrepareTimeout)
	defer cancel()
	if err := journal.checkNamespace(ctx, e); err != nil {
		return nil, err
	}
	scope := journal.record.Scope
	source, err := e.Client.AppsV1().StatefulSets(scope.Namespace).Get(ctx, scope.SourceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	sourceHash, err := fingerprint(source.Spec)
	if err != nil {
		return nil, err
	}
	if source.Name != scope.SourceName || source.Namespace != scope.Namespace || source.UID != scope.SourceUID ||
		sourceHash != proof.SourceSpecSHA256 || source.Status.CurrentRevision != proof.SourceRevision {
		return nil, errors.New("prepared source identity, spec fingerprint or revision changed")
	}
	placement, err := DiscoverPlacement(ctx, e.Client, source)
	if err != nil {
		return nil, err
	}
	runtimeHash, err := runtimeFingerprint(placement.RuntimeClass)
	if err != nil {
		return nil, err
	}
	if runtimeHash != proof.RuntimeClassSHA256 || len(placement.Nodes) != len(proof.Nodes) {
		return nil, errors.New("prepared runtime or hard-placement pool changed")
	}
	priorityHash, err := priorityFingerprint(placement.PriorityClass)
	if err != nil {
		return nil, err
	}
	if priorityHash != proof.PriorityClassSHA256 {
		return nil, errors.New("prepared PriorityClass identity or policy changed")
	}
	services, err := e.Client.CoreV1().Services(scope.Namespace).List(ctx, metav1.ListOptions{Limit: 257})
	if err != nil {
		return nil, err
	}
	if services.Continue != "" || len(services.Items) > 256 {
		return nil, errors.New("prepared Service inventory is incomplete or exceeds its bound")
	}
	serviceHash, err := servicesFingerprint(services.Items)
	if err != nil {
		return nil, err
	}
	if serviceHash != proof.ServicesSHA256 {
		return nil, errors.New("prepared Service identities or specs changed")
	}
	session := &Session{}
	for i, saved := range proof.Nodes {
		var actual *corev1.Node
		for n := range placement.Nodes {
			if placement.Nodes[n].Name == saved.Name {
				actual = &placement.Nodes[n]
				break
			}
		}
		if actual == nil || actual.UID != saved.UID || actual.Status.NodeInfo.Architecture != saved.Architecture || actual.Status.NodeInfo.ContainerRuntimeVersion != saved.Runtime {
			return nil, errors.New("prepared Node identity or runtime changed")
		}
		request := JobRequest{Source: source, Node: actual, RuntimeClass: placement.RuntimeClass, PriorityClass: placement.PriorityClass, Services: services.Items,
			ClientServiceName: proof.ClientService, Name: saved.JobName, Image: expectedImage, HoldSeconds: saved.HoldSeconds, TTLSeconds: saved.TTLSeconds}
		wanted, err := BuildJob(request)
		if err != nil {
			return nil, err
		}
		wanted.Annotations[attemptAnnotation] = journal.record.Attempt
		jobHash, err := fingerprint(wanted)
		if err != nil {
			return nil, err
		}
		if jobHash != saved.JobSHA256 {
			return nil, errors.New("prepared Job policy cannot be reproduced exactly")
		}
		session.entries = append(session.entries, preparedEntry{request: request, wanted: wanted, uid: journal.record.Targets[i].UID, podUID: saved.PodUID, digests: append([]string(nil), saved.Digests...)})
	}
	if err := e.Verify(ctx, session); err != nil {
		return nil, err
	}
	return session, nil
}
