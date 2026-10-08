package host

import (
	"context"
	"fmt"
	"net/url"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

// resolveSchema returns the schema of the chain the host serves. With generators configured, it is
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

// servedSchema returns the schema served by the first of the chain's generators to answer, tried in
// order, or false when none answers. A generator answers when it returns a schema for the served
// chain. The schema is returned unchecked, so that every difference from the stored tables counts.
func servedSchema(ctx context.Context, schemaCfg config.SchemaConfig, served chain.Config) (string, bool) {
	client := schema.NewSchemaHTTPClient(schemaCfg)
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
		return resp.Schema, true
	}
	logger.Sugar.Warnf("No generator answered for %s, so the stored tables are not compared with a served schema", served.Prefix)
	return "", false
}
