package host

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

// chainsApplier creates the chains' tables and subscribes to the enabled chains; ApplySchema says which
// chains it disables. It implements defradb.SchemaApplier.
type chainsApplier struct {
	schemaCfg config.SchemaConfig
	chains    []chain.Config
	// enabled is set by ApplySchema to the chains it subscribed to, in config order.
	enabled []chain.Config
	// reasons is set by ApplySchema to the reason each chain it disabled is not served, keyed by prefix.
	// A chain disabled for server.ReasonSchemaDrift has all its tables stored, one disabled for
	// server.ReasonTablesPartiallyStored some, and one disabled for any other reason none.
	reasons map[string]string
	// removed is set by ApplySchema to the prefixes of the chains the store holds but chains does not
	// list.
	removed []string
}

// ApplySchema creates each chain's tables with schema.ChainApplier, in config order, and subscribes
// to them. A chain whose tables cannot be set up (see disabledReason) is disabled: its stored tables
// are unsubscribed and it is left out of enabled. When every chain is disabled, it returns
// errNoChainEnabled with each chain's error. Any other failure is returned. A chain the store holds
// but chains does not list is removed: it is unsubscribed before any table is created, and its
// tables and data stay until the store is wiped.
func (a *chainsApplier) ApplySchema(ctx context.Context, n *node.Node) error {
	removed, err := a.removeUnlistedChains(ctx, n)
	if err != nil {
		return err
	}

	var enabled []chain.Config
	reasons := make(map[string]string)
	var disabled []error
	for _, c := range a.chains {
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
		if reason := disabledReason(err); reason != "" {
			// DefraDB keeps subscriptions across restarts, so any from an earlier start are removed.
			stored, listErr := n.DB.GetCollections(ctx)
			if listErr != nil {
				return fmt.Errorf("read stored collections: %w", listErr)
			}
			if err := n.DB.DeleteP2PCollections(ctx, storedNames(stored, collections.Subscribed())); err != nil {
				return fmt.Errorf("unsubscribe %s: %w", c.Prefix, err)
			}
			logger.Sugar.Errorf("Chain %s disabled: %v", c.Prefix, err)
			disabled = append(disabled, fmt.Errorf("chain %s: %w", c.Prefix, err))
			reasons[c.Prefix] = reason
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
	if len(enabled) == 0 {
		return fmt.Errorf("%w: %w", errNoChainEnabled, errors.Join(disabled...))
	}
	a.enabled = enabled
	a.reasons = reasons
	a.removed = removed
	return nil
}

// disabledReason returns the reason a chain is disabled when setting up its tables fails with err, or
// "" when err does not disable a chain.
func disabledReason(err error) string {
	switch {
	case errors.Is(err, schema.ErrSchemaDrift):
		return server.ReasonSchemaDrift
	case errors.Is(err, errNoGeneratorAnswered):
		return server.ReasonNoGeneratorAnswered
	case errors.Is(err, errNoGeneratorSchema):
		return server.ReasonSchemaRefused
	case errors.Is(err, errNoChainSchema):
		return server.ReasonNoGeneratorConfigured
	case errors.Is(err, schema.ErrSchemaPartiallyStored):
		return server.ReasonTablesPartiallyStored
	}
	return ""
}

// removeUnlistedChains unsubscribes each chain the store holds but chains does not list, and returns
// their prefixes.
func (a *chainsApplier) removeUnlistedChains(ctx context.Context, n *node.Node) ([]string, error) {
	cols, err := n.DB.GetCollections(ctx)
	if err != nil {
		return nil, fmt.Errorf("read stored collections: %w", err)
	}
	configured := make(map[string]bool, len(a.chains))
	for _, c := range a.chains {
		configured[c.Prefix] = true
	}
	// Every chain has a "<prefix>__BlockSignature" table, so one under a prefix the config does not
	// list belongs to a removed chain. A view's name is whatever its SDL declares, so views are
	// skipped.
	signatureSuffix := chain.EVM("").BlockSignature.Name
	var removed []string
	for _, col := range cols {
		prefix, ok := strings.CutSuffix(col.Name(), signatureSuffix)
		if !ok || col.Version().Query.HasValue() || configured[prefix] {
			continue
		}
		if err := n.DB.DeleteP2PCollections(ctx, storedNames(cols, chain.EVM(prefix).Subscribed())); err != nil {
			return nil, fmt.Errorf("unsubscribe removed chain %s: %w", prefix, err)
		}
		logger.Sugar.Warnf("Chain %s removed: the config does not list it, so it is unsubscribed; its tables and data stay until the store is wiped", prefix)
		removed = append(removed, prefix)
	}
	return removed, nil
}

// storedNames returns the names in names that a collection in cols is stored under. DefraDB's
// DeleteP2PCollections errors on a name the store does not hold, so it is given only these.
func storedNames(cols []client.Collection, names []string) []string {
	var stored []string
	for _, col := range cols {
		if slices.Contains(names, col.Name()) {
			stored = append(stored, col.Name())
		}
	}
	return stored
}

// resolveSchema returns the schema of one of the host's chains. With generators configured, it is
// the first schema they serve that passes FetchSchema's checks, tried in order. A generator answers
// when FetchResponse accepts its response; a response whose network names another chain is an
// answer whose schema the checks refuse. When no schema passes, it returns errNoGeneratorSchema if a
// generator answered, and errNoGeneratorAnswered if none did (each failed with
// schema.ErrSchemaNoResponse).
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
	refused := false
	for _, g := range served.Generators {
		var sdl string
		schemaURL, err := url.JoinPath(g.URL, schemaCfg.IndexerSchemaEndpoint)
		if err == nil {
			sdl, err = schema.FetchSchema(ctx, client, schemaURL, collections)
			if err != nil && !errors.Is(err, schema.ErrSchemaNoResponse) {
				refused = true
			}
		}
		if err != nil {
			logger.Sugar.Warnf("Schema from generator %s not used: %v", g.URL, err)
			continue
		}
		logger.Sugar.Infof("Using the schema from generator %s", g.URL)
		return sdl, nil
	}

	if refused {
		return "", errNoGeneratorSchema
	}
	return "", errNoGeneratorAnswered
}

// servedSchemas yields the URL of each of the chain's generators and its schema response, in order,
// skipping those that do not answer. A generator answers when FetchResponse accepts its response; a
// response whose network names another chain is an answer whose schema is refused.
// Generators are asked one at a time as the caller ranges, so none is asked after the caller stops.
// The responses are yielded without FetchSchema's checks, which schema.ChainApplier runs before it
// compares a schema with the stored tables. The requests share one deadline, the timeout schemaCfg
// gives a single request, counted from when the caller starts ranging, which bounds this chain's
// check, so a restart can wait that timeout once per chain: the chain's generators that do not
// answer delay its check by that timeout once, not once each, and a generator that hangs leaves
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
