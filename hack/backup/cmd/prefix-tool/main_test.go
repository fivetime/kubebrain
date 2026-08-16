package main

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunRejectsInvalidActionBeforeConnecting(t *testing.T) {
	t.Setenv("PREFIX", "/registry/test")
	t.Setenv("ACTION", "destroy-everything")
	require.ErrorContains(t, run(io.Discard), `unknown ACTION "destroy-everything"`)
}

func TestRunValidatesLeasePutBeforeGrant(t *testing.T) {
	t.Setenv("PREFIX", "/registry/test")
	t.Setenv("ACTION", "lease-put")
	t.Setenv("LEASE_TTL", "30")
	t.Setenv("KEY_SUFFIXES", "/a,")
	require.ErrorContains(t, run(io.Discard), "KEY_SUFFIXES contains an empty suffix")
}

func TestRunRejectsEmptyPrefixBeforeDestructiveAction(t *testing.T) {
	t.Setenv("ACTION", "delete")
	t.Setenv("PREFIX", "")
	require.ErrorContains(t, run(io.Discard), "PREFIX is required")
}
