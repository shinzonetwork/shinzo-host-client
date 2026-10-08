package host

import (
	"context"
	"fmt"
	"iter"
	"net/url"
	"strings"

	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

// chainsApplier creates the tables of every chain in chains. It implements defradb.SchemaApplier.
type chainsApplier struct {
	schemaCfg config.SchemaConfig
	chains    []chain.Config
}

// ApplySchema creates each chain's tables with schema.ChainApplier, in config order. Before creating
// any, it returns errUnlistedChain when the store holds a chain that chains does not list.
func (a chainsApplier) ApplySchema(ctx context.Context, n *node.Node) error {
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

	for _, c := range a.chains {
		collections := chain.EVM(c.Prefix)
		applier := schema.ChainApplier{
			Tables: func(ctx context.Context) (string, error) {
				return resolveSchema(ctx, a.schemaCfg, c, collections)
			},
			Served: func(ctx context.Context) iter.Seq2[string, string] {
				return servedSchemas(ctx, a.schemaCfg, c)
			},
			Collections: collections,
		}
		if err := applier.ApplySchema(ctx, n); err != nil {
			return err
		}
	}
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

// servedSchemas yields the URL of each of the chain's generators and the schema it serves, in order,
// skipping those that do not answer. A generator answers when it returns a schema for the chain.
// Generators are asked one at a time as the caller ranges, so none is asked after the caller stops.
// The schemas are yielded without FetchSchema's checks, so a schema those checks refuse is compared
// with the stored tables instead of being skipped.
func servedSchemas(ctx context.Context, schemaCfg config.SchemaConfig, served chain.Config) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
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
			if !yield(g.URL, resp.Schema) {
				return
			}
		}
		if !answered {
			logger.Sugar.Warnf("No generator answered for %s, so the stored tables are not compared with a served schema", served.Prefix)
		}
	}
}
