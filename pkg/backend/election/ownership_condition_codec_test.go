package election

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOwnershipConditionWireRoundTripAndReplay(t *testing.T) {
	_, helper, _, claim := retiredReleaseFixture(t)
	payload, err := (OwnershipCondition{claim: claim}).MarshalBinary()
	require.NoError(t, err)
	condition, err := ParseOwnershipCondition(payload, claim.holder)
	require.NoError(t, err)
	require.Equal(t, claim, condition.claim)
	for i := range payload {
		payload[i] = 0
	}
	require.Equal(t, claim, condition.claim, "decoded condition must not alias the request buffer")
	require.NoError(t, helper.releaseRetiredOwnership(context.Background(), condition.claim))
	require.ErrorIs(t, helper.releaseRetiredOwnership(context.Background(), condition.claim), storage.ErrCASFailed)
	// Encoding must not normalize the exact record bytes used by CAS.
	claim.record = append([]byte(" \n"), claim.record...)
	payload, err = (OwnershipCondition{claim: claim}).MarshalBinary()
	require.NoError(t, err)
	condition, err = ParseOwnershipCondition(payload, claim.holder)
	require.NoError(t, err)
	require.Equal(t, claim.record, condition.claim.record)
}

func TestOwnershipConditionWireRejectsAmbiguityAndWrongHolder(t *testing.T) {
	_, _, _, claim := retiredReleaseFixture(t)
	valid, err := (OwnershipCondition{claim: claim}).MarshalBinary()
	require.NoError(t, err)
	for name, payload := range map[string][]byte{
		"empty": nil, "null": []byte("null"), "array": []byte("[]"),
		"trailing object":    append(append([]byte(nil), valid...), []byte("{}")...),
		"trailing null":      append(append([]byte(nil), valid...), []byte("null")...),
		"unknown":            bytes.Replace(valid, []byte(`"version":1`), []byte(`"unknown":true,"version":1`), 1),
		"duplicate":          bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"escaped duplicate":  bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"\u0076ersion":1`), 1),
		"unknown version":    bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":2`), 1),
		"null version":       bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":null`), 1),
		"missing":            bytes.Replace(valid, []byte(`"version":1,`), nil, 1),
		"case alias":         bytes.Replace(valid, []byte(`"version":1`), []byte(`"Version":1`), 1),
		"wrong inner holder": bytes.Replace(valid, []byte(`"holder":"old"`), []byte(`"holder":"other"`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			condition, err := ParseOwnershipCondition(payload, "old")
			require.ErrorIs(t, err, errInvalidOwnershipCondition)
			require.Empty(t, condition.claim.record)
			require.Empty(t, condition.claim.token)
		})
	}
	for _, holder := range []string{"", "other"} {
		_, err := ParseOwnershipCondition(valid, holder)
		require.ErrorIs(t, err, errInvalidOwnershipCondition)
	}
	_, err = (OwnershipCondition{}).MarshalBinary()
	require.ErrorIs(t, err, errInvalidOwnershipCondition)
	// Envelope and embedded record holder must agree even if outer auth passed.
	altered := bytes.Replace(valid, []byte(`"holder":"old"`), []byte(`"holder":"other"`), 1)
	_, err = ParseOwnershipCondition(altered, "other")
	require.ErrorIs(t, err, errInvalidOwnershipCondition)
}

func TestOwnershipConditionWireSizeBoundary(t *testing.T) {
	_, _, _, claim := retiredReleaseFixture(t)
	payload, err := (OwnershipCondition{claim: claim}).MarshalBinary()
	require.NoError(t, err)
	padded := append(payload, bytes.Repeat([]byte(" "), MaxOwnershipConditionBytes-len(payload))...)
	_, err = ParseOwnershipCondition(padded, claim.holder)
	require.NoError(t, err)
	_, err = ParseOwnershipCondition(append(padded, ' '), claim.holder)
	require.ErrorIs(t, err, errInvalidOwnershipCondition)
	claim.record = bytes.Repeat([]byte(" "), (64<<10)+1)
	_, err = (OwnershipCondition{claim: claim}).MarshalBinary()
	require.ErrorIs(t, err, errInvalidOwnershipCondition)
}

func FuzzOwnershipCondition(f *testing.F) {
	valid, err := json.Marshal(ownershipConditionWire{1, "old",
		[]byte(`{"holderIdentity":"old","leaseDurationSeconds":30,"leaderTransitions":7}`),
		[]byte("10000000-0000-4000-8000-000000000001")})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(`{"version":1,"version":2}`))
	f.Add([]byte("null"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		condition, err := ParseOwnershipCondition(payload, "old")
		if err != nil {
			return
		}
		encoded, err := condition.MarshalBinary()
		require.NoError(t, err)
		again, err := ParseOwnershipCondition(encoded, "old")
		require.NoError(t, err)
		require.Equal(t, condition, again)
	})
}

func TestOwnershipConditionWireRejectsNumericByteArrays(t *testing.T) {
	_, _, _, claim := retiredReleaseFixture(t)
	for _, field := range []string{"record", "token"} {
		t.Run(field, func(t *testing.T) {
			wire := map[string]any{"version": 1, "holder": claim.holder, "record": claim.record, "token": claim.token}
			original := claim.record
			if field == "token" {
				original = claim.token
			}
			array := make([]int, len(original))
			for i, b := range original {
				array[i] = int(b)
			}
			wire[field] = array
			payload, err := json.Marshal(wire)
			require.NoError(t, err)
			_, err = ParseOwnershipCondition(payload, claim.holder)
			require.ErrorIs(t, err, errInvalidOwnershipCondition, "wire byte fields must be base64 strings, not an alternative array representation")
		})
	}
}
