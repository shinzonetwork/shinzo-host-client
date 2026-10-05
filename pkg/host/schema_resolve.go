package host

import (
	"context"
	"net/url"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

// resolveSchema returns the schema of the chain the host serves: the first usable schema from the
// chain's generators, tried in order, or the built-in schema when no generator is configured or
// none serves a usable one. Each request gets the timeout from schemaCfg.
func resolveSchema(ctx context.Context, schemaCfg config.SchemaConfig, served chain.Config, collections chain.Collections) string {
	if len(served.Generators) == 0 {
		logger.Sugar.Infof("No generators configured for %s, using the built-in schema", served.Prefix)
		return schema.GetSchema()
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
		return sdl
	}

	logger.Sugar.Warnf("No generator served a usable schema for %s, using the built-in schema", served.Prefix)
	return schema.GetSchema()
}
