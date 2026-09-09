package imageprepull

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

const attemptAnnotation = "kubebrain.io/prepull-attempt"

// Executor owns only Jobs in a caller-reviewed plan and checks it against the
// entire hard-placement pool. The caller must obtain the per-platform digest
// allowlist from verified release evidence and fence rollout mutations. Client
// transports must enforce response-size and request-time bounds.
type Executor struct {
	Client           kubernetes.Interface
	Journal          *RecoveryJournal // Required by deployment callers; nil is non-durable library/test mode.
	PrepareTimeout   time.Duration
	CleanupTimeout   time.Duration
	PollInterval     time.Duration
	MinRemainingHold time.Duration
}

// Session is an in-memory ownership record, not a deployment-success or durable
// recovery receipt. Use it serially, hold it through rollout, call Verify just
// before mutation, and always call Cleanup. Configure Journal before Prepare
// for cleanup recovery; the journal alone cannot resume or authorize a rollout.
type Session struct {
	entries []preparedEntry
}

type preparedEntry struct {
	request JobRequest
	wanted  *batchv1.Job
	uid     types.UID
	podUID  types.UID
	digests []string
}

func (e Executor) validate() error {
	if e.Client == nil || e.PrepareTimeout <= 0 || e.CleanupTimeout <= 0 ||
		e.PollInterval <= 0 || e.PollInterval > time.Second || e.MinRemainingHold <= 0 {
		return errors.New("pre-pull executor requires a client and positive bounded timing controls")
	}
	if e.PrepareTimeout > time.Hour || e.CleanupTimeout > 5*time.Minute || e.MinRemainingHold > 24*time.Hour {
		return errors.New("pre-pull executor timing controls exceed their limits")
	}
	return nil
}

func (e Executor) Prepare(ctx context.Context, requests []JobRequest, approved map[string][]string) (_ *Session, retErr error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(requests) == 0 || len(requests) > 32 {
		return nil, errors.New("pre-pull plan must contain 1..32 distinct nodes")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	session := &Session{}
	seenNodes, seenNames := map[string]bool{}, map[string]bool{}
	seenUIDs := map[types.UID]bool{}
	for _, request := range requests {
		wanted, err := BuildJob(request)
		if err != nil {
			return nil, err
		}
		if seenNodes[request.Node.Name] || seenNames[request.Name] || seenUIDs[request.Node.UID] {
			return nil, errors.New("pre-pull plan duplicates a node or Job name")
		}
		seenNodes[request.Node.Name], seenNames[request.Name] = true, true
		seenUIDs[request.Node.UID] = true
		if len(session.entries) > 0 {
			first := session.entries[0].request
			if request.Source.Namespace != first.Source.Namespace || request.Source.Name != first.Source.Name ||
				request.Source.UID != first.Source.UID || !reflect.DeepEqual(request.Source.Spec, first.Source.Spec) || request.Image != first.Image ||
				request.ClientServiceName != first.ClientServiceName || !reflect.DeepEqual(request.Services, first.Services) {
				return nil, errors.New("pre-pull plan mixes source identities, specs, or candidate images")
			}
			if !sameRuntimeClass(first.RuntimeClass, request.RuntimeClass) {
				return nil, errors.New("pre-pull plan mixes RuntimeClass snapshots")
			}
			if !samePriorityClass(first.PriorityClass, request.PriorityClass) {
				return nil, errors.New("pre-pull plan mixes PriorityClass snapshots")
			}
		}
		digests := approved["linux/"+request.Node.Status.NodeInfo.Architecture]
		if len(digests) == 0 || len(digests) > 4 {
			return nil, errors.New("pre-pull plan lacks a bounded reviewed platform digest set")
		}
		seen := map[string]bool{}
		for _, digest := range digests {
			if !pinnedImage.MatchString("image@"+digest) || seen[digest] {
				return nil, errors.New("pre-pull plan contains an invalid or duplicate runtime digest")
			}
			seen[digest] = true
		}
		if time.Duration(request.HoldSeconds)*time.Second <= e.MinRemainingHold {
			return nil, errors.New("pre-pull hold does not cover the required remaining rollout window")
		}
		wanted.Annotations[attemptAnnotation] = hex.EncodeToString(nonce[:])
		// Own defensive snapshots, not references the caller can later change.
		request.Source, request.Node = request.Source.DeepCopy(), request.Node.DeepCopy()
		request.RuntimeClass = request.RuntimeClass.DeepCopy()
		request.PriorityClass = request.PriorityClass.DeepCopy()
		services := make([]corev1.Service, len(request.Services))
		for i := range request.Services {
			services[i] = *request.Services[i].DeepCopy()
		}
		request.Services = services
		session.entries = append(session.entries, preparedEntry{request: request, wanted: wanted, digests: append([]string(nil), digests...)})
	}
	ctx, cancel := context.WithTimeout(ctx, e.PrepareTimeout)
	defer cancel()
	if e.Journal != nil {
		// Binding an existing attempt must fail without cleaning that attempt.
		if err := e.Journal.bind(session); err != nil {
			return nil, err
		}
	}
	// Compensation gets its own bounded context even if preparation was canceled.
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, e.Cleanup(context.WithoutCancel(ctx), session))
		}
	}()
	if e.Journal != nil {
		if err := e.Journal.checkNamespace(ctx, e); err != nil {
			return nil, err
		}
	}
	if err := e.checkPlacement(ctx, session); err != nil {
		return nil, err
	}
	// Check every target name before creating any Job; a conflict is not ours.
	for i := range session.entries {
		entry := &session.entries[i]
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := e.checkSource(ctx, entry.request); err != nil {
			return nil, err
		}
		_, err := e.Client.BatchV1().Jobs(entry.wanted.Namespace).Get(ctx, entry.wanted.Name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("pre-pull target Job is present or unreadable: %s: %w", entry.wanted.Name, errOrConflict(err))
		}
	}
	for i := range session.entries {
		entry := &session.entries[i]
		if err := e.checkPlacement(ctx, session); err != nil {
			return nil, err
		}
		if err := e.checkSource(ctx, entry.request); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.Journal != nil {
			if err := e.Journal.update(entry.wanted.Name, true, "", false); err != nil {
				return nil, fmt.Errorf("persist pre-pull CREATE intent: %w", err)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		created, err := e.Client.BatchV1().Jobs(entry.wanted.Namespace).Create(ctx, entry.wanted.DeepCopy(), metav1.CreateOptions{})
		if err != nil {
			// A response can be lost after creation. Only this unpredictable
			// attempt marker plus the exact source owner can establish ownership.
			observed, readErr := e.Client.BatchV1().Jobs(entry.wanted.Namespace).Get(ctx, entry.wanted.Name, metav1.GetOptions{})
			if readErr == nil && ownsAttempt(entry, observed) {
				entry.uid = observed.UID
				if e.Journal != nil {
					err = errors.Join(err, e.Journal.update(entry.wanted.Name, true, entry.uid, false))
				}
			} else if readErr == nil {
				err = errors.Join(err, errors.New("unowned Job at creation target left untouched"))
			} else if !apierrors.IsNotFound(readErr) {
				err = errors.Join(err, fmt.Errorf("creation ownership could not be reconciled: %w", readErr))
			}
			return nil, fmt.Errorf("create pre-pull Job %s: %w", entry.wanted.Name, err)
		}
		// A successful CREATE response establishes the concrete object UID even
		// if admission changed policy/owner fields that must now be rejected.
		if created != nil && created.Name == entry.wanted.Name && created.Namespace == entry.wanted.Namespace && validIdentity(string(created.UID)) {
			entry.uid = created.UID
			if e.Journal != nil {
				if err := e.Journal.update(entry.wanted.Name, true, entry.uid, false); err != nil {
					return nil, fmt.Errorf("persist pre-pull created UID: %w", err)
				}
			}
		}
		if !ownsAttempt(entry, created) {
			return nil, fmt.Errorf("pre-pull Job creation identity mismatch: %s", entry.wanted.Name)
		}
		entry.uid = created.UID
		if err := checkJob(entry, created, e.MinRemainingHold); err != nil {
			return nil, err
		}
	}
	for {
		ready, err := e.observe(ctx, session)
		if err != nil {
			return nil, err
		}
		if ready {
			if e.Journal != nil {
				if err := e.Journal.savePreparation(session); err != nil {
					return nil, fmt.Errorf("persist prepared snapshot: %w", err)
				}
				// Disk persistence can consume time too. Do not publish a snapshot
				// as ready without rechecking cancellation and the live hold window.
				if err := e.Verify(ctx, session); err != nil {
					return nil, err
				}
			}
			return session, nil
		}
		if err := pause(ctx, e.PollInterval); err != nil {
			return nil, fmt.Errorf("pre-pull did not become ready: %w", err)
		}
	}
}

func errOrConflict(err error) error {
	if err == nil {
		return errors.New("already exists")
	}
	return err
}

func ownsAttempt(entry *preparedEntry, job *batchv1.Job) bool {
	return job != nil && validIdentity(string(job.UID)) && job.Name == entry.wanted.Name &&
		job.Namespace == entry.wanted.Namespace && job.Annotations[attemptAnnotation] == entry.wanted.Annotations[attemptAnnotation] &&
		reflect.DeepEqual(job.OwnerReferences, entry.wanted.OwnerReferences)
}

func (e Executor) checkSource(ctx context.Context, request JobRequest) error {
	source, err := e.Client.AppsV1().StatefulSets(request.Source.Namespace).Get(ctx, request.Source.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if source.UID != request.Source.UID || source.DeletionTimestamp != nil || !reflect.DeepEqual(source.Spec, request.Source.Spec) {
		return errors.New("source StatefulSet identity or spec changed during pre-pull")
	}
	if source.Spec.Replicas == nil || *source.Spec.Replicas < 1 || source.Status.ReadyReplicas != *source.Spec.Replicas ||
		source.Status.CurrentRevision == "" || source.Status.CurrentRevision != source.Status.UpdateRevision ||
		source.Generation < 1 || source.Status.ObservedGeneration != source.Generation {
		return errors.New("source StatefulSet is not Ready at one stable revision")
	}
	node, err := e.Client.CoreV1().Nodes().Get(ctx, request.Node.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.UID != request.Node.UID || node.Status.NodeInfo.Architecture != request.Node.Status.NodeInfo.Architecture ||
		node.Status.NodeInfo.ContainerRuntimeVersion != request.Node.Status.NodeInfo.ContainerRuntimeVersion {
		return errors.New("pre-pull Node identity or platform changed")
	}
	services, err := e.Client.CoreV1().Services(request.Source.Namespace).List(ctx, metav1.ListOptions{Limit: 257})
	if err != nil {
		return err
	}
	if services.Continue != "" || len(services.Items) > 256 || len(services.Items) != len(request.Services) {
		return errors.New("pre-pull Service inventory changed or exceeds its bound")
	}
	for _, expected := range request.Services {
		found := false
		for _, actual := range services.Items {
			if actual.Name == expected.Name && actual.UID == expected.UID && reflect.DeepEqual(actual.Spec, expected.Spec) {
				found = true
			}
		}
		if !found {
			return errors.New("pre-pull Service identity or spec changed")
		}
	}
	request.Node, request.Services = node, services.Items
	_, err = BuildJob(request)
	return err
}

func (e Executor) Verify(ctx context.Context, session *Session) error {
	if err := e.validate(); err != nil {
		return err
	}
	if e.Journal != nil {
		if err := e.Journal.checkSession(session); err != nil {
			return err
		}
		if e.Journal.record.Preparation == nil {
			return errors.New("no active prepared snapshot; this attempt has not completed preparation or has begun cleanup")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, e.PrepareTimeout)
	defer cancel()
	if e.Journal != nil {
		if err := e.Journal.checkNamespace(ctx, e); err != nil {
			return err
		}
	}
	ready, err := e.observe(ctx, session)
	if err != nil {
		return err
	}
	if !ready {
		return errors.New("pre-pull holders are no longer ready")
	}
	return nil
}

func (e Executor) observe(ctx context.Context, session *Session) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if session == nil || len(session.entries) == 0 {
		return false, errors.New("pre-pull session is empty")
	}
	if err := e.checkPlacement(ctx, session); err != nil {
		return false, err
	}
	allReady := true
	var earliestExpiry time.Time
	for i := range session.entries {
		entry := &session.entries[i]
		if err := e.checkSource(ctx, entry.request); err != nil {
			return false, err
		}
		job, err := e.Client.BatchV1().Jobs(entry.wanted.Namespace).Get(ctx, entry.wanted.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if job.UID != entry.uid || !ownsAttempt(entry, job) {
			return false, errors.New("pre-pull Job identity changed")
		}
		if err := checkJob(entry, job, e.MinRemainingHold); err != nil {
			return false, err
		}
		if job.Status.StartTime != nil {
			expiry := job.Status.StartTime.Add(time.Duration(*job.Spec.ActiveDeadlineSeconds)*time.Second - 2*time.Second)
			if earliestExpiry.IsZero() || expiry.Before(earliestExpiry) {
				earliestExpiry = expiry
			}
		}
		pods, err := e.ownedPods(ctx, entry)
		if err != nil {
			return false, err
		}
		if len(pods) == 0 {
			allReady = false
			continue
		}
		if len(pods) != 1 {
			return false, errors.New("pre-pull Job has multiple dependent Pods")
		}
		if !validIdentity(string(pods[0].UID)) || (entry.podUID != "" && pods[0].UID != entry.podUID) {
			return false, errors.New("pre-pull Pod identity changed")
		}
		entry.podUID = pods[0].UID
		ready, err := checkPod(entry, &pods[0])
		if err != nil {
			return false, err
		}
		allReady = allReady && ready && job.Status.StartTime != nil && job.Status.Active == 1
	}
	// Earlier targets may age while later API requests are in flight. Their
	// remaining hold budget must still be sufficient when observation returns.
	if err := e.checkPlacement(ctx, session); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if allReady && earliestExpiry.Sub(time.Now()) < e.MinRemainingHold {
		return false, errors.New("pre-pull hold window expired during observation")
	}
	return allReady, nil
}

func (e Executor) ownedPods(ctx context.Context, entry *preparedEntry) ([]corev1.Pod, error) {
	list, err := e.Client.CoreV1().Pods(entry.wanted.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "batch.kubernetes.io/controller-uid=" + string(entry.uid), Limit: 3,
	})
	if err != nil {
		return nil, err
	}
	if list.Continue != "" || len(list.Items) > 2 {
		return nil, errors.New("pre-pull dependent Pod inventory exceeds its bound")
	}
	var owned []corev1.Pod
	for _, pod := range list.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.APIVersion == "batch/v1" && owner.Kind == "Job" && owner.Name == entry.wanted.Name &&
				owner.UID == entry.uid && ptr.Deref(owner.Controller, false) {
				owned = append(owned, pod)
			}
		}
	}
	return owned, nil
}

func (e Executor) Cleanup(ctx context.Context, session *Session) error {
	if err := e.validate(); err != nil {
		return err
	}
	if session == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, e.CleanupTimeout)
	defer cancel()
	if e.Journal != nil {
		if err := e.Journal.checkSession(session); err != nil {
			return err
		}
		if err := e.Journal.checkNamespace(ctx, e); err != nil {
			return err
		}
		if err := e.Journal.invalidatePreparation(); err != nil {
			return err
		}
	}
	var failures []error
	for i := len(session.entries) - 1; i >= 0; i-- {
		entry := &session.entries[i]
		if entry.uid == "" {
			continue
		}
		if err := e.cleanupOne(ctx, entry); err != nil {
			failures = append(failures, fmt.Errorf("cleanup pre-pull Job %s: %w", entry.wanted.Name, err))
		} else if e.Journal != nil {
			if err := e.Journal.update(entry.wanted.Name, true, entry.uid, true); err != nil {
				failures = append(failures, fmt.Errorf("persist cleanup pre-pull Job %s: %w", entry.wanted.Name, err))
			}
		}
	}
	if e.Journal != nil {
		failures = append(failures, e.RecoverCleanup(ctx, e.Journal))
	}
	return errors.Join(failures...)
}

func (e Executor) cleanupOne(ctx context.Context, entry *preparedEntry) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		job, err := e.Client.BatchV1().Jobs(entry.wanted.Namespace).Get(ctx, entry.wanted.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			pods, err := e.ownedPods(ctx, entry)
			if err != nil {
				return err
			}
			if len(pods) == 0 {
				return nil
			}
		} else if err != nil {
			return err
		} else {
			if job.UID != entry.uid || job.Name != entry.wanted.Name || job.Namespace != entry.wanted.Namespace || !validIdentity(job.ResourceVersion) {
				return errors.New("refusing to delete replaced or unreadable pre-pull Job")
			}
			if job.DeletionTimestamp == nil {
				uid, rv := entry.uid, job.ResourceVersion
				err := e.Client.BatchV1().Jobs(job.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{
					Preconditions:     &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
					PropagationPolicy: ptr.To(metav1.DeletePropagationForeground),
				})
				if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
					return err
				}
			}
		}
		if err := pause(ctx, e.PollInterval); err != nil {
			return fmt.Errorf("pre-pull Job or dependents remain: %w", err)
		}
	}
}

func pause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func imageIDMatches(image, imageID string, digests []string) bool {
	repository, _, _ := strings.Cut(image, "@")
	for _, digest := range digests {
		if imageID == digest || imageID == repository+"@"+digest ||
			imageID == "containerd://"+digest || imageID == "cri-o://"+digest || imageID == "docker://"+digest {
			return true
		}
	}
	return false
}
