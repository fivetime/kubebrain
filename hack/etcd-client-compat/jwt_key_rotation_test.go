package compat

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestJWTSigningKeyRotation requires an auth-enabled disposable JWT cluster.
// Stage A (old signer plus next verify-key) must already be fully rolled out.
// The partial Stage B command promotes exactly one replica to the new signer
// plus old verify-key, allowing the test to prove both token directions while
// old and new signers coexist. The complete command promotes the remaining
// replicas. Retirement must remove the old verify-key only after tokenTTL.
func TestJWTSigningKeyRotation(t *testing.T) {
	rawEndpoints := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_ENDPOINTS"))
	issuerEndpoint := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_OLD_ISSUER_ENDPOINT"))
	newIssuerEndpoint := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_NEW_ISSUER_ENDPOINT"))
	partialCommand := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_PARTIAL_PROMOTE_COMMAND"))
	completeCommand := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_COMPLETE_PROMOTE_COMMAND"))
	retireCommand := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_RETIRE_COMMAND"))
	ttlText := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_TOKEN_TTL"))
	if rawEndpoints == "" || issuerEndpoint == "" || newIssuerEndpoint == "" ||
		partialCommand == "" || completeCommand == "" || retireCommand == "" || ttlText == "" {
		t.Skip("set JWT rotation endpoints, issuers, stage commands, and token TTL")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.Len(t, endpoints, 3)
	for index := range endpoints {
		endpoints[index] = strings.TrimSpace(endpoints[index])
		require.NotEmpty(t, endpoints[index])
	}
	tokenTTL, err := time.ParseDuration(ttlText)
	require.NoError(t, err)
	require.Positive(t, tokenTTL)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	username := strings.TrimSpace(os.Getenv("KUBEBRAIN_JWT_ROTATION_USERNAME"))
	password := os.Getenv("KUBEBRAIN_JWT_ROTATION_PASSWORD")
	require.NotEmpty(t, username)
	require.NotEmpty(t, password)

	oldIssuer := authClient(t, issuerEndpoint, "", "")
	oldIssuedAt := time.Now()
	oldAuth, err := oldIssuer.Authenticate(ctx, username, password)
	require.NoError(t, err)
	require.NotEmpty(t, oldAuth.Token)
	requireJWTTokenAcrossEndpoints(t, ctx, endpoints, oldAuth.Token, true, "stage-a old token")

	output, err := runCompatShellCommandContext(t, ctx, partialCommand)
	require.NoErrorf(t, err, "partially promote JWT signer: %s", strings.TrimSpace(string(output)))
	newIssuer := authClient(t, newIssuerEndpoint, "", "")
	newAuth, err := newIssuer.Authenticate(ctx, username, password)
	require.NoError(t, err)
	require.NotEmpty(t, newAuth.Token)
	requireJWTTokenAcrossEndpoints(t, ctx, endpoints, oldAuth.Token, true, "mixed stage old token")
	requireJWTTokenAcrossEndpoints(t, ctx, endpoints, newAuth.Token, true, "mixed stage new token")

	output, err = runCompatShellCommandContext(t, ctx, completeCommand)
	require.NoErrorf(t, err, "complete JWT signer promotion: %s", strings.TrimSpace(string(output)))
	requireJWTTokenAcrossEndpoints(t, ctx, endpoints, oldAuth.Token, true, "promoted old token")
	requireJWTTokenAcrossEndpoints(t, ctx, endpoints, newAuth.Token, true, "promoted new token")

	retireAt := oldIssuedAt.Add(tokenTTL + 2*time.Second)
	if wait := time.Until(retireAt); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
	freshNewAuth, err := newIssuer.Authenticate(ctx, username, password)
	require.NoError(t, err)
	output, err = runCompatShellCommandContext(t, ctx, retireCommand)
	require.NoErrorf(t, err, "retire old JWT verification key: %s", strings.TrimSpace(string(output)))
	requireJWTTokenAcrossEndpoints(t, ctx, endpoints, freshNewAuth.Token, true, "retired fresh new token")
	requireJWTTokenAcrossEndpoints(t, ctx, endpoints, oldAuth.Token, false, "retired old token")
}

func requireJWTTokenAcrossEndpoints(t *testing.T, ctx context.Context, endpoints []string, token string, accepted bool, stage string) {
	t.Helper()
	for _, endpoint := range endpoints {
		client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Token: token})
		require.NoError(t, err)
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, callErr := client.Get(callCtx, "/jwt-rotation/key")
		cancel()
		require.NoError(t, client.Close())
		if accepted {
			require.NoErrorf(t, callErr, "%s must be accepted by %s", stage, endpoint)
			continue
		}
		require.Error(t, callErr, "%s must be rejected by %s", stage, endpoint)
		require.Truef(t,
			errors.Is(rpctypes.Error(callErr), rpctypes.ErrInvalidAuthToken),
			"%s on %s returned %v, want invalid auth token", stage, endpoint, callErr,
		)
	}
}
