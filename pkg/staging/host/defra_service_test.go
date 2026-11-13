package host

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/staging/hostkey"
)

// testKeys generates a real set of node keys in a temp dir.
func testKeys(t *testing.T) hostkey.NodeKeys {
	t.Helper()
	keys, _, err := hostkey.Ensure(t.TempDir(), "")
	require.NoError(t, err)
	return keys
}

// newTestDefra returns a Defra with real keys and its own temp data dir,
// stopped when the test ends.
func newTestDefra(t *testing.T) *Defra {
	t.Helper()
	d := NewDefra(hostconfig.Config{DataDir: t.TempDir()}, zap.NewNop(), testKeys(t))
	t.Cleanup(func() { _ = d.Stop(context.Background()) })
	return d
}

func TestDefraStartAndStop(t *testing.T) {
	ctx := context.Background()
	d := newTestDefra(t)

	require.NoError(t, d.Start(ctx))
	require.NotNil(t, d.node)

	require.NoError(t, d.Stop(ctx))
	require.Nil(t, d.node)
}

func TestDefraNodeUsesHostIdentity(t *testing.T) {
	ctx := context.Background()
	d := newTestDefra(t)
	require.NoError(t, d.Start(ctx))

	ident, err := d.node.DB.GetNodeIdentity(ctx)
	require.NoError(t, err)
	require.True(t, ident.HasValue())
	require.Equal(t, d.keys.DID(), ident.Value().DID)
}

func TestDefraStartTwiceFails(t *testing.T) {
	ctx := context.Background()
	d := newTestDefra(t)

	require.NoError(t, d.Start(ctx))
	require.ErrorIs(t, d.Start(ctx), ErrDefraAlreadyStarted)
}

func TestDefraStopIsNoOpWhenNotRunning(t *testing.T) {
	ctx := context.Background()
	d := newTestDefra(t)

	require.NoError(t, d.Stop(ctx), "before Start")
	require.NoError(t, d.Start(ctx))
	require.NoError(t, d.Stop(ctx))
	require.NoError(t, d.Stop(ctx), "after Stop")
}

// A failed start must return an error, not panic, and leave nothing running.
func TestDefraStartFailureLeavesNothingRunning(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADir, nil, 0o600))
	d := NewDefra(hostconfig.Config{DataDir: notADir}, zap.NewNop(), testKeys(t))

	require.Error(t, d.Start(context.Background()))
	require.Nil(t, d.node)
}

// The node outlives Start's ctx: only Stop ends it.
func TestDefraNodeIgnoresStartContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := newTestDefra(t)

	require.NoError(t, d.Start(ctx))
	require.NotNil(t, d.node)
}

// Stop must release the store, or a restarted host couldn't reopen it.
func TestDefraRestartsOnSameDataDir(t *testing.T) {
	ctx := context.Background()
	d := newTestDefra(t)

	require.NoError(t, d.Start(ctx))
	require.NoError(t, d.Stop(ctx))
	require.NoError(t, d.Start(ctx))
}

func TestDefraViewsIsNilBeforeStart(t *testing.T) {
	require.Nil(t, newTestDefra(t).Views())
}

func TestDefraHandlerIsNilBeforeStart(t *testing.T) {
	require.Nil(t, newTestDefra(t).Handler())
}
