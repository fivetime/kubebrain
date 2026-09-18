package server

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
)

const (
	retirementInstanceHeader = "X-Kubebrain-Retirement-Instance"
	retirementHolderHeader   = "X-Kubebrain-Retirement-Holder"
)

var errPeerRetirementRequest = errors.New("invalid peer retirement request")

// readPeerRetirementCondition is internal and not registered on any endpoint.
// It authenticates before consuming the body and bounds bytes even for chunked
// requests. The eventual handler MUST additionally set transport read deadlines,
// concurrency/rate limits and a backend operation budget. A context deadline
// alone cannot interrupt every possible Body reader. The caller owns Body.Close.
func readPeerRetirementCondition(r *http.Request, auth *peerRetirementAuthorizer) (election.OwnershipCondition, error) {
	invalid := func() (election.OwnershipCondition, error) {
		return election.OwnershipCondition{}, errPeerRetirementRequest
	}
	holder, err := authorizePeerRetirementRequest(r, auth)
	if err != nil {
		return election.OwnershipCondition{}, err
	}
	types := r.Header.Values("Content-Type")
	if len(types) != 1 || types[0] != "application/json" || len(r.Header.Values("Content-Encoding")) != 0 || r.ContentLength > election.MaxOwnershipConditionBytes {
		return invalid()
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, election.MaxOwnershipConditionBytes+1))
	if err != nil || len(payload) > election.MaxOwnershipConditionBytes || r.Context().Err() != nil {
		return invalid()
	}
	condition, err := election.ParseOwnershipCondition(payload, holder)
	if err != nil {
		return invalid()
	}
	return condition, nil
}

// Split out authentication so an invalid peer cannot consume the handler's
// authenticated request rate budget. No body reads occur here.
func authorizePeerRetirementRequest(r *http.Request, auth *peerRetirementAuthorizer) (string, error) {
	if r == nil || r.Method != http.MethodPost || r.Body == nil || r.Context().Err() != nil {
		return "", errPeerRetirementRequest
	}
	identity := func(name string) (string, bool) {
		values := r.Header.Values(name)
		if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 4096 || strings.TrimSpace(values[0]) != values[0] {
			return "", false
		}
		return values[0], true
	}
	instance, okInstance := identity(retirementInstanceHeader)
	holder, okHolder := identity(retirementHolderHeader)
	if !okInstance || !okHolder {
		return "", errPeerRetirementRequest
	}
	if err := auth.authorize(r.TLS, instance, holder, time.Now()); err != nil {
		return "", errPeerRetirementUnauthorized
	}
	return holder, nil
}
