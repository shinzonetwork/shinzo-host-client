package host

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"strings"
	"time"

	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

// chainsApplier creates the tables of every chain in chains and subscribes to the enabled chains. It
// implements defradb.SchemaApplier.
type chainsApplier struct {
	schemaCfg config.SchemaConfig
	chains    []chain.Config
	// enabled is set by ApplySchema to the chains it subscribed to, in config order.
	enabled []chain.Config
}

// ApplySchema creates each chain's tables with schema.ChainApplier, in config order, and subscribes
// to them. A chain after the first whose tables drift (schema.ErrSchemaDrift) is disabled: it is
// unsubscribed and left out of enabled. Any other failure, and drift in the first chain, is
// returned. Before creating any table, it returns errUnlistedChain when the store holds a chain
// that chains does not list.
func (a *chainsApplier) ApplySchema(ctx context.Context, n *node.Node) error {
	cols, err := n.DB.GetCollections(ctx)
	if err != nil {
		return fmt.Errorf("read stored collections: %w", err)
	}
	configured := make(map[string]bool, len(a.chains))
	for _, c := range a.chains {
		configured[c.Prefix] = true
	}
	// Every chain has a "<prefix>__BlockSignature" table, so one under a prefix the config does not
	// list means the store holds another chain. A view's name is whatever its SDL declares, so views
	// are skipped.
	signatureSuffix := chain.EVM("").BlockSignature.Name
	for _, col := range cols {
		prefix, ok := strings.CutSuffix(col.Name(), signatureSuffix)
		if ok && !col.Version().Query.HasValue() && !configured[prefix] {
			return fmt.Errorf("%s: %w", prefix, errUnlistedChain)
		}
	}

	var enabled []chain.Config
	for i, c := range a.chains {
		collections := chain.EVM(c.Prefix)
		applier := schema.ChainApplier{
			Tables: func(ctx context.Context) (string, error) {
				return resolveSchema(ctx, a.schemaCfg, c, collections)
			},
			Served: func(ctx context.Context) iter.Seq2[string, schema.Response] {
				return servedSchemas(ctx, a.schemaCfg, c)
			},
			Collections: collections,
		}
		err = applier.ApplySchema(ctx, n)
		// The readiness probe, event filter and snapshots read only the first chain, so drift there is
		// returned instead of disabling it.
		if i > 0 && errors.Is(err, schema.ErrSchemaDrift) {
			// DefraDB keeps subscriptions across restarts and reloads them when the node starts, so one
			// made by an earlier start is removed.
			if err := n.DB.DeleteP2PCollections(ctx, collections.Subscribed()); err != nil {
				return fmt.Errorf("unsubscribe %s: %w", c.Prefix, err)
			}
			logger.Sugar.Errorf("Chain %s disabled: %v", c.Prefix, err)
			continue
		}
		if err != nil {
			return err
		}
		if err := n.DB.AddP2PCollections(ctx, collections.Subscribed()); err != nil {
			return fmt.Errorf("subscribe %s: %w", c.Prefix, err)
		}
		enabled = append(enabled, c)
	}
	a.enabled = enabled
	return nil
}

// resolveSchema returns the schema of one of the host's chains. With generators configured, it is
// the first usable schema they serve, tried in order, or errNoGeneratorSchema when none serves one.
// With none configured, Ethereum mainnet uses the built-in schema and any other chain gets
// errNoChainSchema. The built-in schema is never a fallback: generators may serve tables that
// differ from it, and a host whose tables differ receives none of the chain's data. Each request
// gets the timeout from schemaCfg.
func resolveSchema(ctx context.Context, schemaCfg config.SchemaConfig, served chain.Config, collections chain.Collections) (string, error) {
	if len(served.Generators) == 0 {
		if served.Prefix != chain.EthereumMainnet {
			return "", fmt.Errorf("no generators configured: %w", errNoChainSchema)
		}
		logger.Sugar.Infof("No generators configured for %s, using the built-in schema", served.Prefix)
		return schema.GetSchema(), nil
	}

	client := schema.NewSchemaHTTPClient(schemaCfg)
	for _, g := range served.Generators {
		var sdl string
		schemaURL, err := url.JoinPath(g.URL, schemaCfg.IndexerSchemaEndpoint)
		if err == nil {
			sdl, err = schema.FetchSchema(ctx, client, schemaURL, collections)
		}
		if err != nil {
			logger.Sugar.Warnf("Schema from generator %s not used: %v", g.URL, err)
			continue
		}
		logger.Sugar.Infof("Using the schema from generator %s", g.URL)
		return sdl, nil
	}

	return "", errNoGeneratorSchema
}

// servedSchemas yields the URL of each of the chain's generators and its schema response, in order,
// skipping those that do not answer. A generator answers when it returns a schema for the chain and
// the schema parses.
// Generators are asked one at a time as the caller ranges, so none is asked after the caller stops.
// The schemas are yielded without FetchSchema's checks, so a schema those checks refuse is compared
// with the stored tables instead of being skipped. The requests share one deadline, the timeout
// schemaCfg gives a single request, counted from when the caller starts ranging, which bounds this
// chain's check, so a restart can wait that timeout once per chain: the chain's generators that do
// not answer delay its check by that timeout once, not once each, and a generator that hangs leaves
// less of it, or none, for the ones after it.
func servedSchemas(ctx context.Context, schemaCfg config.SchemaConfig, served chain.Config) iter.Seq2[string, schema.Response] {
	return func(yield func(string, schema.Response) bool) {
		// Each range gets its own deadline. A zero timeout leaves the requests unbounded, as it does
		// in the HTTP client.
		ctx := ctx
		if schemaCfg.HTTPClientTimeoutSecs > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(schemaCfg.HTTPClientTimeoutSecs)*time.Second)
			defer cancel()
		}
		client := schema.NewSchemaHTTPClient(schemaCfg)
		answered := false
		for _, g := range served.Generators {
			var resp schema.Response
			schemaURL, err := url.JoinPath(g.URL, schemaCfg.IndexerSchemaEndpoint)
			if err == nil {
				resp, err = schema.FetchResponse(ctx, client, schemaURL)
			}
			if err == nil && resp.Network != served.Prefix {
				err = fmt.Errorf("network %q: %w", resp.Network, schema.ErrSchemaWrongNetwork)
			}
			if err != nil {
				logger.Sugar.Warnf("Generator %s did not answer for %s: %v", g.URL, served.Prefix, err)
				continue
			}
			answered = true
			if !yield(g.URL, resp) {
				return
			}
		}
		if !answered {
			logger.Sugar.Warnf("No generator answered for %s, so the stored tables are not compared with a served schema", served.Prefix)
		}
	}
}
