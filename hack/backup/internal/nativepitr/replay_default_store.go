package nativepitr

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	bolt "go.etcd.io/bbolt"
)

const replayDefaultStoreBatchBytes = 8 << 20

var replayDefaultBucket = []byte("default-cf")

type replayDefaultStore struct {
	db           *bolt.DB
	path         string
	tx           *bolt.Tx
	bucket       *bolt.Bucket
	pendingBytes uint64
}

func newReplayDefaultStore(scratchDir string) (*replayDefaultStore, error) {
	file, err := os.CreateTemp(scratchDir, ".kubebrain-native-pitr-defaults-*.db")
	if err != nil {
		return nil, err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return nil, errors.Join(err, os.Remove(path))
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{NoSync: true, NoFreelistSync: true})
	if err != nil {
		return nil, errors.Join(err, os.Remove(path))
	}
	store := &replayDefaultStore{db: db, path: path}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(replayDefaultBucket)
		return err
	}); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func (s *replayDefaultStore) begin() error {
	if s.tx != nil {
		return nil
	}
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket(replayDefaultBucket)
	if bucket == nil {
		_ = tx.Rollback()
		return errors.New("native PITR replay default-CF bucket is missing")
	}
	s.tx, s.bucket = tx, bucket
	return nil
}

func (s *replayDefaultStore) Put(joinKey string, value []byte) error {
	if uint64(len(joinKey)) > math.MaxUint32 {
		return errors.New("native PITR replay default-CF key is too large")
	}
	if err := s.begin(); err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(joinKey))
	if existing := s.bucket.Get(digest[:]); existing != nil {
		existingKey, existingValue, err := decodeReplayDefaultRecord(existing)
		if err != nil {
			return err
		}
		if !bytes.Equal(existingKey, []byte(joinKey)) {
			return errors.New("native PITR replay default-CF join-key digest collision")
		}
		if !bytes.Equal(existingValue, value) {
			return errors.New("stream log contains conflicting default-CF entries")
		}
		return nil
	}
	maxInt := int(^uint(0) >> 1)
	if len(value) > maxInt-4 || len(joinKey) > maxInt-4-len(value) {
		return errors.New("native PITR replay default-CF record is too large")
	}
	record := make([]byte, 4+len(joinKey)+len(value))
	binary.BigEndian.PutUint32(record, uint32(len(joinKey)))
	copy(record[4:], joinKey)
	copy(record[4+len(joinKey):], value)
	if err := s.bucket.Put(digest[:], record); err != nil {
		return err
	}
	s.pendingBytes += uint64(len(digest) + len(record))
	if s.pendingBytes >= replayDefaultStoreBatchBytes {
		return s.Flush()
	}
	return nil
}

func (s *replayDefaultStore) Get(joinKey string) ([]byte, bool, error) {
	if err := s.Flush(); err != nil {
		return nil, false, err
	}
	digest := sha256.Sum256([]byte(joinKey))
	var value []byte
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		record := tx.Bucket(replayDefaultBucket).Get(digest[:])
		if record == nil {
			return nil
		}
		storedKey, storedValue, err := decodeReplayDefaultRecord(record)
		if err != nil {
			return err
		}
		if !bytes.Equal(storedKey, []byte(joinKey)) {
			return errors.New("native PITR replay default-CF join-key digest collision")
		}
		found = true
		value = append([]byte(nil), storedValue...)
		return nil
	})
	return value, found, err
}

func decodeReplayDefaultRecord(record []byte) ([]byte, []byte, error) {
	if len(record) < 4 {
		return nil, nil, errors.New("native PITR replay default-CF index is corrupt")
	}
	keyLength := uint64(binary.BigEndian.Uint32(record))
	if keyLength > uint64(len(record)-4) {
		return nil, nil, errors.New("native PITR replay default-CF index is corrupt")
	}
	return record[4 : 4+keyLength], record[4+keyLength:], nil
}

func (s *replayDefaultStore) Flush() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.Commit()
	s.tx, s.bucket, s.pendingBytes = nil, nil, 0
	if err != nil {
		return fmt.Errorf("commit native PITR replay default-CF index: %w", err)
	}
	return nil
}

func (s *replayDefaultStore) Close() error {
	var rollbackErr error
	if s.tx != nil {
		rollbackErr = s.tx.Rollback()
		s.tx, s.bucket = nil, nil
	}
	return errors.Join(rollbackErr, s.db.Close(), os.Remove(s.path))
}
