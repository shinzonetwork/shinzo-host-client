package host

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/sourcenetwork/corelog"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-host-client/pkg/defradb"
	"github.com/shinzonetwork/shinzo-host-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

const bytesPerMB = 1024 * 1024

const (
	schemaReadyPollInterval = time.Second
	schemaReadyMaxAttempts  = 30
)

type DefraService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	DB() node.DB
	Options() *options.NodeOptions
	Metrics() *server.HostMetrics
}

func NewDefraService(
	cfg *hostconfig.Config,
	log *zap.Logger,
	identityKey identity.FullIdentity,
	peerKeySeed []byte,
) DefraService {
	return &defraService{
		cfg:         cfg,
		log:         log,
		identityKey: identityKey,
		peerKeySeed: peerKeySeed,
		metrics:     server.NewHostMetrics(),
	}
}

type defraService struct {
	cfg         *hostconfig.Config
	log         *zap.Logger
	identityKey identity.FullIdentity
	peerKeySeed []byte
	metrics     *server.HostMetrics

	node *node.Node
}

func (s *defraService) Start(ctx context.Context) error {
	configureCorelog(s.cfg)

	nodeOpts, err := buildNodeOptions(s.cfg, s.identityKey, s.peerKeySeed)
	if err != nil {
		return err
	}

	defraNode, err := node.New(ctx, nodeOpts)
	if err != nil {
		return fmt.Errorf("configuring defra node: %w", err)
	}

	if err := defraNode.Start(ctx); err != nil {
		return fmt.Errorf("starting defra node: %w", err)
	}

	schemaStr := resolveDefraSchema(ctx, s.cfg, s.log.Sugar())
	if err := applyDefraSchema(ctx, defraNode, schemaStr); err != nil {
		_ = defraNode.Close(ctx)
		return fmt.Errorf("applying schema: %w", err)
	}

	if err := waitSchemaQueryable(ctx, defraNode); err != nil {
		_ = defraNode.Close(ctx)
		return err
	}

	if s.cfg.P2P.Enabled {
		if err := defraNode.DB.AddP2PCollections(ctx, constants.AllCollections); err != nil {
			_ = defraNode.Close(ctx)
			return fmt.Errorf("registering p2p collections: %w", err)
		}
	}

	s.node = defraNode
	return nil
}

func (s *defraService) Stop(ctx context.Context) error {
	if s.node == nil {
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- s.node.Close(ctx) }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		s.log.Sugar().Warn("defra did not close within the shutdown budget, abandoning it so the process can exit")
		return ctx.Err()
	}
}

func (s *defraService) DB() node.DB {
	if s.node == nil {
		return nil
	}
	return s.node.DB
}

func (s *defraService) Options() *options.NodeOptions {
	if s.node == nil {
		return nil
	}
	return s.node.Options()
}

func (s *defraService) Metrics() *server.HostMetrics {
	return s.metrics
}

func configureCorelog(cfg *hostconfig.Config) {
	format := corelog.FormatJSON
	if cfg.Logger.Development {
		format = ""
	}

	general := corelog.Config{
		Level:  corelogLevel(cfg.Logger.Level),
		Format: format,
		Output: corelog.OutputStdout,
	}
	corelog.SetConfig(general)

	corelog.SetConfigOverride("http", corelog.Config{
		Level:  corelog.LevelError,
		Format: format,
		Output: corelog.OutputStdout,
	})
}

func corelogLevel(zapLevel string) string {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(zapLevel)); err == nil && lvl >= zapcore.ErrorLevel {
		return corelog.LevelError
	}
	return corelog.LevelInfo
}

func buildNodeOptions(
	cfg *hostconfig.Config,
	identityKey identity.FullIdentity,
	peerKeySeed []byte,
) (*options.NodeOptionsBuilder, error) {
	nodeOpts := options.Node()
	nodeOpts.DB().SetLensRuntime("wazero").SetNodeIdentity(identityKey)
	nodeOpts.Store().SetPath(cfg.Store.Path)
	if cfg.Store.ValueLogFileSizeMB > 0 {
		nodeOpts.Store().SetBadgerFileSize(cfg.Store.ValueLogFileSizeMB * bytesPerMB)
	}

	nodeOpts.SetDisableAPI(true)
	nodeOpts.SetDisableP2P(!cfg.P2P.Enabled)

	if !cfg.P2P.Enabled {
		return nodeOpts, nil
	}

	p2pKey, err := p2pPrivateKeyBytes(peerKeySeed)
	if err != nil {
		return nil, fmt.Errorf("p2p key: %w", err)
	}

	nodeOpts.P2P().
		SetEnablePubSub(true).
		SetListenAddresses(cfg.P2P.ListenAddr).
		SetPrivateKey(p2pKey)

	return nodeOpts, nil
}

func p2pPrivateKeyBytes(seed []byte) ([]byte, error) {
	priv, _, err := libp2pcrypto.GenerateEd25519Key(bytes.NewReader(seed))
	if err != nil {
		return nil, fmt.Errorf("generating p2p key: %w", err)
	}
	raw, err := priv.Raw()
	if err != nil {
		return nil, fmt.Errorf("marshaling p2p key: %w", err)
	}
	return raw, nil
}

func waitSchemaQueryable(ctx context.Context, defraNode *node.Node) error {
	query := `{ ` + constants.CollectionBlock + ` { __typename } }`

	var lastErr error
	for attempt := 1; attempt <= schemaReadyMaxAttempts; attempt++ {
		_, err := defradb.QuerySingle[map[string]any](ctx, defraNode, query)
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for schema to become queryable: %w", ctx.Err())
		case <-time.After(schemaReadyPollInterval):
		}
	}

	return fmt.Errorf("after %d attempts: %w: %w", schemaReadyMaxAttempts, ErrDefraDBNotReady, lastErr)
}

func resolveDefraSchema(ctx context.Context, cfg *hostconfig.Config, log *zap.SugaredLogger) string {
	parsedURL, err := url.Parse(cfg.Snapshot.IndexerURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		log.Warnf("no usable indexer URL (%q), using the embedded schema", cfg.Snapshot.IndexerURL)
		return schema.GetSchema()
	}
	endpoint := parsedURL.JoinPath(cfg.Schema.IndexerSchemaEndpoint).String()

	fetched, err := schema.GetSchemaDynamic(ctx, schemaHTTPClient(cfg.Schema), endpoint)
	if err != nil {
		switch {
		case schema.IsDataLevelError(err):
			log.Warnw("indexer returned an invalid schema, using the embedded schema", "endpoint", endpoint, "error", err)
		case schema.IsNetworkLevelError(err):
			log.Warnw("could not reach the indexer for schema, using the embedded schema", "endpoint", endpoint, "error", err)
		default:
			log.Warnw("schema fetch failed, using the embedded schema", "endpoint", endpoint, "error", err)
		}
		return schema.GetSchema()
	}
	return fetched
}

func schemaHTTPClient(cfg hostconfig.SchemaConfig) *http.Client {
	client := &http.Client{Timeout: time.Duration(cfg.HTTPClientTimeoutSecs) * time.Second}
	if cfg.AuthToken != "" {
		client.Transport = schemaAuthTransport{token: cfg.AuthToken}
	}
	return client
}

type schemaAuthTransport struct {
	token string
}

func (t schemaAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(req)
}

func applyDefraSchema(ctx context.Context, defraNode *node.Node, schemaStr string) error {
	_, err := defraNode.DB.AddCollection(ctx, schemaStr)
	if err != nil && strings.Contains(err.Error(), "collection already exists") {
		return nil
	}
	return err
}
