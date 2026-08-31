package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/Masterminds/semver/v3"
	"github.com/kubewharf/kubebrain/hack/internal/etcdutil"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

var statusVersionPattern = regexp.MustCompile(`^([0-9]+)\.([0-9]+)\.[0-9]+(?:[-+][0-9A-Za-z][0-9A-Za-z.-]*)?$`)

type statusProbeHeader struct {
	ClusterID uint64 `json:"cluster_id"`
	MemberID  uint64 `json:"member_id"`
	Revision  int64  `json:"revision"`
	RaftTerm  uint64 `json:"raft_term"`
}

type statusProbeDowngradeInfo struct {
	Enabled       bool   `json:"enabled"`
	TargetVersion string `json:"target_version"`
}

type statusProbeResult struct {
	Header           statusProbeHeader        `json:"header"`
	Version          string                   `json:"version"`
	DBSize           int64                    `json:"db_size"`
	Leader           uint64                   `json:"leader"`
	RaftIndex        uint64                   `json:"raft_index"`
	RaftTerm         uint64                   `json:"raft_term"`
	RaftAppliedIndex uint64                   `json:"raft_applied_index"`
	Errors           []string                 `json:"errors"`
	DBSizeInUse      int64                    `json:"db_size_in_use"`
	IsLearner        bool                     `json:"is_learner"`
	StorageVersion   string                   `json:"storage_version"`
	DBSizeQuota      int64                    `json:"db_size_quota"`
	DowngradeInfo    statusProbeDowngradeInfo `json:"downgrade_info"`
}

func main() {
	if err := run(os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(stdout io.Writer) (retErr error) {
	if os.Getenv("ENDPOINT") == "" {
		return errors.New("ENDPOINT is required")
	}
	timeout, err := etcdutil.TimeoutFromEnv()
	if err != nil {
		return err
	}
	client, err := etcdutil.NewClientFromEnv()
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, client.Close())
		}
	}()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(rootCtx, timeout)
	defer cancel()
	response, err := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Status(ctx, &etcdserverpb.StatusRequest{})
	if err != nil {
		return err
	}
	result, err := validateStatusResponse(response)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if err := client.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	return nil
}

func validateStatusResponse(response *etcdserverpb.StatusResponse) (statusProbeResult, error) {
	if response == nil {
		return statusProbeResult{}, errors.New("maintenance Status returned a nil response")
	}
	header := response.GetHeader()
	if header == nil {
		return statusProbeResult{}, errors.New("maintenance Status response header is missing")
	}
	if header.GetClusterId() == 0 {
		return statusProbeResult{}, errors.New("maintenance Status cluster ID must be positive")
	}
	if header.GetMemberId() == 0 {
		return statusProbeResult{}, errors.New("maintenance Status member ID must be positive")
	}
	if header.GetRevision() < 0 {
		return statusProbeResult{}, fmt.Errorf("maintenance Status revision must be non-negative, got %d", header.GetRevision())
	}
	if header.GetRaftTerm() == 0 {
		return statusProbeResult{}, errors.New("maintenance Status header raft term must be positive")
	}
	if response.GetDbSize() < 0 {
		return statusProbeResult{}, fmt.Errorf("maintenance Status db size must be non-negative, got %d", response.GetDbSize())
	}
	if response.GetDbSizeInUse() < 0 {
		return statusProbeResult{}, fmt.Errorf("maintenance Status db size in use must be non-negative, got %d", response.GetDbSizeInUse())
	}
	requiresVersionedFields, err := statusVersionCoreAtLeast3Minor(response.GetVersion(), "6")
	if err != nil {
		return statusProbeResult{}, err
	}
	downgradeInfo := response.GetDowngradeInfo()
	if !requiresVersionedFields &&
		(response.GetStorageVersion() != "" || response.GetDbSizeQuota() != 0 || downgradeInfo != nil) {
		return statusProbeResult{}, errors.New("maintenance Status versioned fields are unavailable before etcd 3.6")
	}
	if storageVersion := response.GetStorageVersion(); storageVersion != "" {
		parsedStorageVersion, parseErr := semver.StrictNewVersion(storageVersion)
		if parseErr != nil || parsedStorageVersion.Patch() != 0 || parsedStorageVersion.Prerelease() != "" ||
			parsedStorageVersion.Metadata() != "" || parsedStorageVersion.String() != storageVersion {
			return statusProbeResult{}, errors.New("maintenance Status storage version must be a canonical major.minor.0 release")
		}
	}
	if requiresVersionedFields && downgradeInfo == nil {
		return statusProbeResult{}, errors.New("maintenance Status downgrade information is missing for etcd 3.6 or later")
	}
	if downgradeInfo != nil {
		if !downgradeInfo.GetEnabled() {
			if downgradeInfo.GetTargetVersion() != "" {
				return statusProbeResult{}, errors.New("maintenance Status downgrade information is inconsistent")
			}
		} else {
			targetVersion, parseErr := semver.StrictNewVersion(downgradeInfo.GetTargetVersion())
			if parseErr != nil || targetVersion.Prerelease() != "" || targetVersion.Metadata() != "" ||
				!statusDowngradeTargetMatchesVersion(response.GetVersion(), targetVersion) {
				return statusProbeResult{}, errors.New("maintenance Status downgrade information is inconsistent")
			}
		}
	}
	if requiresVersionedFields && response.GetDbSizeQuota() == 0 {
		return statusProbeResult{}, errors.New("maintenance Status database quota must be non-zero for etcd 3.6 or later")
	}
	result := statusProbeResult{
		Header: statusProbeHeader{
			ClusterID: header.GetClusterId(),
			MemberID:  header.GetMemberId(),
			Revision:  header.GetRevision(),
			RaftTerm:  header.GetRaftTerm(),
		},
		Version:          response.GetVersion(),
		DBSize:           response.GetDbSize(),
		Leader:           response.GetLeader(),
		RaftIndex:        response.GetRaftIndex(),
		RaftTerm:         response.GetRaftTerm(),
		RaftAppliedIndex: response.GetRaftAppliedIndex(),
		Errors:           append([]string{}, response.GetErrors()...),
		DBSizeInUse:      response.GetDbSizeInUse(),
		IsLearner:        response.GetIsLearner(),
		StorageVersion:   response.GetStorageVersion(),
		DBSizeQuota:      response.GetDbSizeQuota(),
	}
	if downgradeInfo != nil {
		result.DowngradeInfo = statusProbeDowngradeInfo{
			Enabled:       downgradeInfo.GetEnabled(),
			TargetVersion: downgradeInfo.GetTargetVersion(),
		}
	}
	return result, nil
}

func statusVersionCoreAtLeast3Minor(value string, minor string) (bool, error) {
	parts := statusVersionPattern.FindStringSubmatch(value)
	if parts == nil {
		return false, fmt.Errorf("maintenance Status version must be a semver string, got %q", value)
	}
	majorComparison := compareUnsignedDecimals(parts[1], "3")
	if majorComparison != 0 {
		return majorComparison > 0, nil
	}
	return compareUnsignedDecimals(parts[2], minor) >= 0, nil
}

func statusDowngradeTargetMatchesVersion(value string, target *semver.Version) bool {
	parts := statusVersionPattern.FindStringSubmatch(value)
	if parts == nil || target.Patch() != 0 ||
		compareUnsignedDecimals(parts[1], strconv.FormatUint(target.Major(), 10)) != 0 {
		return false
	}
	targetMinor := strconv.FormatUint(target.Minor(), 10)
	return compareUnsignedDecimals(parts[2], targetMinor) == 0 ||
		compareUnsignedDecimals(parts[2], incrementUnsignedDecimal(targetMinor)) == 0
}

func incrementUnsignedDecimal(value string) string {
	digits := []byte(value)
	for index := len(digits) - 1; index >= 0; index-- {
		if digits[index] != '9' {
			digits[index]++
			return string(digits)
		}
		digits[index] = '0'
	}
	return "1" + string(digits)
}

func compareUnsignedDecimals(left, right string) int {
	normalize := func(value string) string {
		value = strings.TrimLeft(value, "0")
		if value == "" {
			return "0"
		}
		return value
	}
	left, right = normalize(left), normalize(right)
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return strings.Compare(left, right)
}
