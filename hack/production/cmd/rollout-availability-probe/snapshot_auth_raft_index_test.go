package main

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// A discard-only test logger: never encode fields, call String(), or retain
// request pointers. Only the entry index and last Authenticate index survive.
// The pinned etcd apply loop logs these two messages sequentially per member.
type restoredAuthRaftIndexCore struct {
	mu                  sync.Mutex
	entry, authenticate uint64
}

func (c *restoredAuthRaftIndexCore) Enabled(zapcore.Level) bool        { return true }
func (c *restoredAuthRaftIndexCore) With([]zapcore.Field) zapcore.Core { return c }
func (c *restoredAuthRaftIndexCore) Sync() error                       { return nil }
func (c *restoredAuthRaftIndexCore) Check(e zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if e.Message == "Applying entry" || e.Message == "Apply" {
		return checked.AddCore(e, c)
	}
	return checked
}
func (c *restoredAuthRaftIndexCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.Message == "Applying entry" {
		c.entry = 0 // Missing/changed upstream fields must not reuse a stale index.
		for _, field := range fields {
			if field.Key == "entry-index" && field.Type == zapcore.Uint64Type {
				c.entry = uint64(field.Integer)
			}
		}
	} else if e.Message == "Apply" {
		for _, field := range fields {
			if field.Key == "raftReq" {
				if request, ok := field.Interface.(*etcdserverpb.InternalRaftRequest); ok && request != nil && request.Authenticate != nil {
					c.authenticate = c.entry
				}
			}
		}
	}
	return nil
}
func (c *restoredAuthRaftIndexCore) authIndex() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authenticate
}

type unformattableAuthIndexField struct{}

func (unformattableAuthIndexField) String() string { panic("diagnostic must never format log fields") }

func TestRestoredAuthRaftIndexCoreDiscardsPayloads(t *testing.T) {
	core := &restoredAuthRaftIndexCore{}
	logger := zap.New(core).With(zap.Stringer("ignored", unformattableAuthIndexField{}))
	logger.Debug("Applying entry", zap.Uint64("entry-index", 42), zap.Stringer("ignored", unformattableAuthIndexField{}))
	logger.Debug("Apply", zap.Stringer("raftReq", &etcdserverpb.InternalRaftRequest{
		Authenticate: &etcdserverpb.InternalAuthenticateRequest{Name: "fixture", Password: "must-not-be-retained"},
	}))
	require.Equal(t, uint64(42), core.authIndex())
	logger.Debug("unrelated", zap.Stringer("ignored", unformattableAuthIndexField{}))
	logger.Debug("Applying entry", zap.String("entry-index", "wrong-type"))
	logger.Debug("Apply", zap.Stringer("raftReq", &etcdserverpb.InternalRaftRequest{Authenticate: &etcdserverpb.InternalAuthenticateRequest{}}))
	require.Zero(t, core.authIndex())
}
