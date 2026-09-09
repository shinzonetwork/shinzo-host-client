// config.toml for host.Start. Separate from the old YAML config package
// on purpose, that one's untouched.
package hostconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

type Config struct {
	Node        NodeConfig        `toml:"node"`
	HTTP        HTTPConfig        `toml:"http"`
	P2P         P2PConfig         `toml:"p2p"`
	Store       StoreConfig       `toml:"store"`
	Shinzo      ShinzoConfig      `toml:"shinzo"`
	Schema      SchemaConfig      `toml:"schema"`
	Logger      LoggerConfig      `toml:"logger"`
	Playground  PlaygroundConfig  `toml:"playground"`
	Snapshot    SnapshotConfig    `toml:"snapshot"`
	EventFilter EventFilterConfig `toml:"event_filter"`
	ACP         ACPConfig         `toml:"acp"`
}

// mirrors pkg/acp.Config, just TOML-sourced instead of env-sourced. no
// secrets in here, build acp.Config from this and call its own Validate()
// where it's actually used, not duplicated here. ChainID isn't here,
// it's shinzo.chain_id, one value for the whole node, not acp-specific.
type ACPConfig struct {
	Enabled         bool   `toml:"enabled"`
	MinQueryBalance string `toml:"min_query_balance"`
	EpochLength     uint64 `toml:"epoch_length"`
	ASBaseURL       string `toml:"as_base_url"`

	// Go duration string, e.g. "5m", parsed where it's consumed
	AttesterWindow string `toml:"attester_window"`
}

type NodeConfig struct {
	Name    string `toml:"name"`
	DataDir string `toml:"data_dir"`
	KeyDir  string `toml:"key_dir"`

	KeyringPassword string `toml:"keyring_password"`
}

// the one port health/graphql/playground all end up on
type HTTPConfig struct {
	Addr string `toml:"addr"`
}

type P2PConfig struct {
	Enabled                bool     `toml:"enabled"`
	ListenAddr             string   `toml:"listen_addr"`
	BootstrapPeers         []string `toml:"bootstrap_peers"`
	MaxRetries             int      `toml:"max_retries"`
	RetryBaseDelayMs       int      `toml:"retry_base_delay_ms"`
	ReconnectIntervalMs    int      `toml:"reconnect_interval_ms"`
	EnableAutoReconnect    bool     `toml:"enable_auto_reconnect"`
	PeerDiscoveryTimeoutMs int      `toml:"peer_discovery_timeout_ms"`
}

type StoreConfig struct {
	Path                    string `toml:"path"`
	BlockCacheMB            int64  `toml:"block_cache_mb"`
	MemTableMB              int64  `toml:"memtable_mb"`
	IndexCacheMB            int64  `toml:"index_cache_mb"`
	NumCompactors           int    `toml:"num_compactors"`
	NumLevelZeroTables      int    `toml:"num_level_zero_tables"`
	NumLevelZeroTablesStall int    `toml:"num_level_zero_tables_stall"`
	ValueLogFileSizeMB      int64  `toml:"value_log_file_size_mb"`
}

type ShinzoConfig struct {
	HubBaseURL string `toml:"hub_base_url"`

	// EVM chain id, used wherever the node needs one, not just acp's
	// EIP-712 verification, one value for the whole node
	ChainID uint64 `toml:"chain_id"`

	MinimumAttestations int    `toml:"minimum_attestations"`
	StartHeight         uint64 `toml:"start_height"`

	// defaults to the operator key's address when empty
	PayoutAddress string `toml:"payout_address"`

	ViewInactivityTimeout string `toml:"view_inactivity_timeout"`
	ViewCleanupInterval   string `toml:"view_cleanup_interval"`
	ViewWorkerCount       int    `toml:"view_worker_count"`
	ViewQueueSize         int    `toml:"view_queue_size"`

	CacheQueueSize int `toml:"cache_queue_size"`

	BatchWriterCount           int  `toml:"batch_writer_count"`
	BatchSize                  int  `toml:"batch_size"`
	BatchFlushInterval         int  `toml:"batch_flush_interval"`
	MaxConcurrentVerifications int  `toml:"max_concurrent_verifications"`
	UseBlockSignatures         bool `toml:"use_block_signatures"`
	DocWorkerCount             int  `toml:"doc_worker_count"`
	DocQueueSize               int  `toml:"doc_queue_size"`
}

type SchemaConfig struct {
	IndexerSchemaEndpoint string `toml:"indexer_schema_endpoint"`
	HTTPClientTimeoutSecs int    `toml:"http_client_timeout_secs"`

	// env only, SHINZO_HOST_SCHEMA_AUTH_TOKEN
	AuthToken string `toml:"-"`
}

type LoggerConfig struct {
	Development bool   `toml:"development"`
	Level       string `toml:"level"`
}

type PlaygroundConfig struct {
	Enabled bool `toml:"enabled"`
}

type SnapshotConfig struct {
	Enabled          bool         `toml:"enabled"`
	IndexerURL       string       `toml:"indexer_url"`
	HistoricalRanges []BlockRange `toml:"historical_ranges"`
}

type BlockRange struct {
	Start int64 `toml:"start"`
	End   int64 `toml:"end"`
}

const (
	DefaultIndexerSchemaEndpoint   = "/api/v1/schema"
	DefaultSchemaHTTPClientTimeout = 30
	MaxSchemaHTTPClientTimeout     = 300
)

var (
	ErrNegativeSchemaTimeout  = errors.New("schema.http_client_timeout_secs must be non-negative")
	ErrExcessiveSchemaTimeout = fmt.Errorf("schema.http_client_timeout_secs must not exceed %d", MaxSchemaHTTPClientTimeout)
	ErrMissingHTTPAddr        = errors.New("http.addr must be set")
	ErrInvalidEventFilterMode = errors.New(`event_filter.mode must be "allowlist" or "blocklist"`)
)

func Default() Config {
	return Config{
		Node: NodeConfig{Name: "default", KeyringPassword: "shinzo-host"},
		HTTP: HTTPConfig{Addr: ":8080"},
		P2P: P2PConfig{
			Enabled:                true,
			ListenAddr:             "/ip4/0.0.0.0/tcp/9171",
			MaxRetries:             5,
			RetryBaseDelayMs:       500,
			ReconnectIntervalMs:    5000,
			EnableAutoReconnect:    true,
			PeerDiscoveryTimeoutMs: 10000,
		},
		Shinzo: ShinzoConfig{
			HubBaseURL:            "testnet.shinzo.network",
			MinimumAttestations:   1,
			ViewInactivityTimeout: "24h",
			ViewCleanupInterval:   "1h",
			ViewWorkerCount:       2,
			ViewQueueSize:         1000,
			CacheQueueSize:        1000,
		},
		Schema: SchemaConfig{
			IndexerSchemaEndpoint: DefaultIndexerSchemaEndpoint,
			HTTPClientTimeoutSecs: DefaultSchemaHTTPClientTimeout,
		},
		Playground: PlaygroundConfig{Enabled: true},
		EventFilter: EventFilterConfig{
			Mode: "allowlist",
			File: "filters.json",
		},
	}
}

func Load(path string) (*Config, error) {
	usingDefaultPath := path == ""
	if usingDefaultPath {
		path = DefaultConfigPath(Default().Node.Name)
	}

	cfg := Default()

	data, err := os.ReadFile(path) //nolint:gosec // operator-controlled config path
	if err != nil {
		if !usingDefaultPath || !os.IsNotExist(err) {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
		if err := writeDefaultConfig(path); err != nil {
			return nil, fmt.Errorf("creating default config at %s: %w", path, err)
		}
		data, err = os.ReadFile(path) //nolint:gosec // just wrote it ourselves
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
	}

	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	if cfg.Node.DataDir == "" {
		cfg.Node.DataDir = DefaultInstanceDir(cfg.Node.Name)
	}
	if cfg.Node.KeyDir == "" {
		cfg.Node.KeyDir = filepath.Join(cfg.Node.DataDir, "keys")
	}
	if cfg.Store.Path == "" {
		cfg.Store.Path = filepath.Join(cfg.Node.DataDir, "data")
	}

	applyEnvOverrides(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating %s: %w", path, err)
	}

	return &cfg, nil
}

func writeDefaultConfig(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil { //nolint:mnd
		return fmt.Errorf("creating config directory: %w", err)
	}

	data, err := toml.Marshal(Default())
	if err != nil {
		return fmt.Errorf("marshaling default config: %w", err)
	}

	if err := os.WriteFile(path, data, 0o600); err != nil { //nolint:mnd
		return fmt.Errorf("writing %s: %w", path, err)
	}

	return nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("SHINZO_HOST_SCHEMA_AUTH_TOKEN"); v != "" {
		cfg.Schema.AuthToken = v
	}
}

func (c *Config) Validate() error {
	var errs []error

	if c.HTTP.Addr == "" {
		errs = append(errs, ErrMissingHTTPAddr)
	}
	if c.Schema.HTTPClientTimeoutSecs < 0 {
		errs = append(errs, ErrNegativeSchemaTimeout)
	}
	if c.Schema.HTTPClientTimeoutSecs > MaxSchemaHTTPClientTimeout {
		errs = append(errs, ErrExcessiveSchemaTimeout)
	}
	if c.EventFilter.Enabled {
		switch c.EventFilter.Mode {
		case "allowlist", "blocklist":
		default:
			errs = append(errs, ErrInvalidEventFilterMode)
		}
	}

	return errors.Join(errs...)
}
