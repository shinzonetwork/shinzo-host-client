package defradb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// errorApplier fails every schema apply with err.
type errorApplier struct{ err error }

func (a errorApplier) ApplySchema(context.Context, *node.Node) error { return a.err }

// Any schema apply error stops the start, including DefraDB's error for a collection that already
// exists.
func TestStartDefraInstance_SchemaApplyError(t *testing.T) {
	applyErr := errors.New("collection already exists")
	cfg := *DefaultConfig

	_, err := StartDefraInstanceWithTestConfig(t, &cfg, errorApplier{err: applyErr})
	require.ErrorIs(t, err, applyErr)
}

// thingApplier adds the Thing collection unless the store holds it, and records each apply in events.
type thingApplier struct{ events *[]string }

func (a thingApplier) ApplySchema(ctx context.Context, n *node.Node) error {
	*a.events = append(*a.events, "apply")
	if _, err := n.DB.GetCollectionByName(ctx, "Thing"); err == nil {
		return nil
	}
	_, err := n.DB.AddCollection(ctx, "type Thing { a: Int }")
	return err
}

// Collections are resolved after the schema is applied, and on a restart also before it.
func TestStartDefraInstance_ResolvesCollections(t *testing.T) {
	ctx := context.Background()
	ipAddress, err := getLANIP()
	require.NoError(t, err)
	cfg := *DefaultConfig
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.URL = ipAddress + ":0"
	cfg.DefraDB.P2P.ListenAddr = "/ip4/" + ipAddress + "/tcp/0"
	cfg.DefraDB.P2P.Enabled = false
	cfg.DefraDB.KeyringSecret = "testSecret"

	var events []string
	resolve := func(cols []client.Collection) {
		var names []string
		for _, col := range cols {
			names = append(names, col.Name())
		}
		events = append(events, "resolve "+strings.Join(names, ","))
	}
	for _, want := range [][]string{
		{"apply", "resolve Thing"},
		{"resolve Thing", "apply", "resolve Thing"},
	} {
		events = nil
		n, _, err := StartDefraInstance(&cfg, thingApplier{events: &events}, nil, nil, nil, resolve)
		require.NoError(t, err)
		require.NoError(t, n.Close(ctx))
		require.Equal(t, want, events)
	}
}
