package restorationfence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

const TokenFormat = "kubebrain.restoration-fence-token.v1"

var (
	idRE     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sha256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Token binds exclusive target-writer ownership to one immutable restore plan.
// Its canonical JSON bytes are the durable value stored in every fence key.
type Token struct {
	Format          string `json:"format"`
	OperationID     string `json:"operation_id"`
	PlanSHA256      string `json:"plan_sha256"`
	TargetClusterID uint64 `json:"target_cluster_id"`
	Keyspace        string `json:"keyspace"`
}

func NewToken(operationID, planSHA256 string, targetClusterID uint64, keyspace string) (Token, error) {
	t := Token{Format: TokenFormat, OperationID: operationID, PlanSHA256: planSHA256, TargetClusterID: targetClusterID, Keyspace: keyspace}
	return t, t.Validate()
}

func (t Token) Validate() error {
	if t.Format != TokenFormat || !idRE.MatchString(t.OperationID) || !sha256RE.MatchString(t.PlanSHA256) || t.TargetClusterID == 0 || !idRE.MatchString(t.Keyspace) {
		return errors.New("invalid restoration fence token")
	}
	return nil
}

func (t Token) Bytes() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(t)
}

func (t Token) SHA256() (string, error) {
	b, err := t.Bytes()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Acquire atomically changes every absent/open fence key to token. Reacquiring
// the exact same token is idempotent; any foreign or partially-held state fails
// closed. The caller must independently verify the live target cluster ID.
func Acquire(ctx context.Context, store storage.KvStorage, prefix string, token Token) (resumed bool, err error) {
	tokenBytes, err := token.Bytes()
	if err != nil {
		return false, err
	}
	keys := AllKeys(prefix)
	type action struct {
		key     []byte
		missing bool
	}
	actions := make([]action, 0, len(keys))
	allHeld := true
	held := 0
	for _, key := range keys {
		value, getErr := store.Get(ctx, key)
		switch {
		case getErr == nil && bytes.Equal(value, tokenBytes):
			held++
			continue
		case getErr == nil && bytes.Equal(value, []byte(Open)):
			allHeld = false
			actions = append(actions, action{key: key})
		case errors.Is(getErr, storage.ErrKeyNotFound):
			allHeld = false
			actions = append(actions, action{key: key, missing: true})
		case getErr != nil:
			return false, fmt.Errorf("read restoration fence %q: %w", key, getErr)
		default:
			return false, fmt.Errorf("restoration fence %q is held by another operation", key)
		}
	}
	if allHeld {
		return true, nil
	}
	if held != 0 {
		return false, errors.New("restoration fence is only partially held by this operation")
	}
	batch := store.BeginBatchWrite()
	for _, action := range actions {
		if action.missing {
			batch.PutIfNotExist(action.key, tokenBytes, 0)
		} else {
			batch.CAS(action.key, tokenBytes, []byte(Open), 0)
		}
	}
	if err := batch.Commit(ctx); err != nil {
		return false, fmt.Errorf("atomically acquire restoration fence: %w", err)
	}
	if err := Verify(ctx, store, prefix, token); err != nil {
		return false, err
	}
	return false, nil
}

// Verify proves that the exact token still owns every control and writer key.
func Verify(ctx context.Context, store storage.KvStorage, prefix string, token Token) error {
	tokenBytes, err := token.Bytes()
	if err != nil {
		return err
	}
	for _, key := range AllKeys(prefix) {
		value, getErr := store.Get(ctx, key)
		if getErr != nil || !bytes.Equal(value, tokenBytes) {
			return fmt.Errorf("restoration fence ownership lost at %q", key)
		}
	}
	return nil
}

// VerifyOpen proves that every control and writer key is initialized and open.
func VerifyOpen(ctx context.Context, store storage.KvStorage, prefix string) error {
	for _, key := range AllKeys(prefix) {
		value, err := store.Get(ctx, key)
		if err != nil || !bytes.Equal(value, []byte(Open)) {
			return fmt.Errorf("restoration fence is not open at %q", key)
		}
	}
	return nil
}

// Release atomically reopens every key only for its exact current owner.
func Release(ctx context.Context, store storage.KvStorage, prefix string, token Token) error {
	tokenBytes, err := token.Bytes()
	if err != nil {
		return err
	}
	batch := store.BeginBatchWrite()
	for _, key := range AllKeys(prefix) {
		batch.CAS(key, []byte(Open), tokenBytes, 0)
	}
	if err := batch.Commit(ctx); err != nil {
		return fmt.Errorf("atomically release restoration fence: %w", err)
	}
	return VerifyOpen(ctx, store, prefix)
}
