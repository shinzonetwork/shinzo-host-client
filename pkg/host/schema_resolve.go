package host

import (
	"context"
	"net/url"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
)

// resolveSchema parses the indexer base URL from the config, joins the schema
// endpoint path, and either fetches the schema dynamically or falls back to the
// embedded schema. On fetch failure the embedded schema is returned and the
// error is logged.
func resolveSchema(ctx context.Context, cfg *config.Config) string {
	parsedURL, parseErr := url.Parse(cfg.HostConfig.Snapshot.IndexerURL)

	var resolvedSchema string
	switch {
	case parseErr != nil || parsedURL.Scheme == "" || parsedURL.Host == "":
		logger.Sugar.Warnf("Invalid indexer URL, using embedded schema: %s", cfg.HostConfig.Snapshot.IndexerURL)
		resolvedSchema = schema.GetSchema()

	default:
		parsedURL = parsedURL.JoinPath(cfg.Schema.IndexerSchemaEndpoint)

		var schemaErr error
		schemaHTTPClient := schema.NewSchemaHTTPClient(cfg.Schema)
		resolvedSchema, schemaErr = schema.GetSchemaDynamic(ctx, schemaHTTPClient, parsedURL.String())
		if schemaErr != nil {
			switch {
			case schema.IsDataLevelError(schemaErr):
				logger.Sugar.Warnf("Schema data error, using embedded schema: %v", schemaErr)
			case schema.IsNetworkLevelError(schemaErr):
				logger.Sugar.Warnf("Schema fetch failed, using embedded schema: %v", schemaErr)
			default:
				logger.Sugar.Warnf("Unexpected schema error, using embedded schema: %v", schemaErr)
			}
		}
	}

	return resolvedSchema
}
