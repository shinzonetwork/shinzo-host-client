package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	appConfig "github.com/shinzonetwork/shinzo-app-sdk/pkg/config"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/pruner"
	"gopkg.in/yaml.v3"
)

const CollectionName = "shinzo"

// DefraDBP2PConfig represents P2P configuration for DefraDB
type DefraDBP2PConfig struct {
	Enabled             bool     `yaml:"enabled"`
	BootstrapPeers      []string `yaml:"bootstrap_peers"`
	ListenAddr          string   `yaml:"listen_addr"`
	MaxRetries          int      `yaml:"max_retries"`
	RetryBaseDelayMs    int      `yaml:"retry_base_delay_ms"`
	ReconnectIntervalMs int      `yaml:"reconnect_interval_ms"`
	EnableAutoReconnect bool     `yaml:"enable_auto_reconnect"`
}

// DefraDBStoreConfig represents store configuration for DefraDB
type DefraDBStoreConfig struct {
	Path string `yaml:"path"`
	// Badger memory configuration
	BlockCacheMB int64 `yaml:"block_cache_mb"`
	MemTableMB   int64 `yaml:"memtable_mb"`
	IndexCacheMB int64 `yaml:"index_cache_mb"`
	// Badger compaction configuration
	NumCompactors           int `yaml:"num_compactors"`
	NumLevelZeroTables      int `yaml:"num_level_zero_tables"`
	NumLevelZeroTablesStall int `yaml:"num_level_zero_tables_stall"`
	// Badger value log configuration
	ValueLogFileSizeMB int64 `yaml:"value_log_file_size_mb"`
}

// DefraDBConfig represents DefraDB configuration
type DefraDBConfig struct {
	Url           string             `yaml:"url"`
	KeyringSecret string             `yaml:"keyring_secret"`
	P2P           DefraDBP2PConfig   `yaml:"p2p"`
	Store         DefraDBStoreConfig `yaml:"store"`
}

// LoggerConfig represents logger configuration
type LoggerConfig struct {
	Development bool `yaml:"development"`
}

type Config struct {
	DefraDB    DefraDBConfig `yaml:"defradb"`
	Shinzo     ShinzoConfig  `yaml:"shinzo"`
	Logger     LoggerConfig  `yaml:"logger"`
	HostConfig HostConfig    `yaml:"host"`
	Pruner     pruner.Config `yaml:"pruner"`
}

type ShinzoConfig struct {
	MinimumAttestations int    `yaml:"minimum_attestations"`
	HubBaseURL          string `yaml:"hub_base_url"`
	StartHeight         uint64 `yaml:"start_height"`

	// P2P Control Settings
	P2PEnabled bool `yaml:"p2p_enabled"`

	// View Management Settings
	ViewInactivityTimeout string `yaml:"view_inactivity_timeout"` // Stop updating after inactivity (default: 24h)
	ViewCleanupInterval   string `yaml:"view_cleanup_interval"`   // Check for inactive views (default: 1h)
	ViewWorkerCount       int    `yaml:"view_worker_count"`       // Workers for lens transformations (default: 2)
	ViewQueueSize         int    `yaml:"view_queue_size"`         // Queue size for view processing jobs (default: 1000)

	// Queue Settings
	CacheQueueSize int `yaml:"cache_queue_size"` // Size of job queue for document processing

	// Batch Attestation Processing Settings
	BatchWriterCount           int  `yaml:"batch_writer_count"`           // Number of batch writers
	BatchSize                  int  `yaml:"batch_size"`                   // Max attestations per batch
	BatchFlushInterval         int  `yaml:"batch_flush_interval"`         // Flush interval in milliseconds
	MaxConcurrentVerifications int  `yaml:"max_concurrent_verifications"` // Max concurrent signature verifications
	UseBatchSignatures         bool `yaml:"use_batch_signatures"`         // Use batch signatures for attestations
	DocWorkerCount             int  `yaml:"doc_worker_count"`             // Number of document processing workers
	DocQueueSize               int  `yaml:"doc_queue_size"`               // Queue size for document event notifications

	// Event Filtering
	EventFilter EventFilterConfig `yaml:"event_filter"` // Configure filtering of P2P events
}

// EventFilterConfig configures content-based filtering of P2P events.
type EventFilterConfig struct {
	Enabled        bool              `yaml:"enabled"`         // Master switch for filtering
	Mode           string            `yaml:"mode"`            // "allowlist" (default) or "blocklist"
	CascadeFilters bool              `yaml:"cascade_filters"` // If true, filtering a tx also filters its logs/ALEs
	BlockRange     *BlockRangeFilter `yaml:"block_range"`     // Optional block number range filter
	Groups         []FilterGroup     `yaml:"groups"`          // Named filter groups combined with OR logic
}

// FilterGroup is a named set of contract and topic filters that can be toggled independently.
type FilterGroup struct {
	Name      string           `yaml:"name"`      // Human-readable name (e.g., "uniswap-v3")
	Enabled   bool             `yaml:"enabled"`   // Toggle this group on/off
	Contracts []ContractFilter `yaml:"contracts"` // Contract address filters
	Topics    []TopicFilter    `yaml:"topics"`    // Event topic filters
}

// ContractFilter matches events by contract address.
type ContractFilter struct {
	Address string   `yaml:"address"` // Contract address (0x...)
	Name    string   `yaml:"name"`    // Human-readable name for logging
	Types   []string `yaml:"types"`   // Collection types to apply to: "transaction", "log", "accessListEntry"
}

// TopicFilter matches log events by topic values.
type TopicFilter struct {
	Topic0 string `yaml:"topic0"` // Event signature hash (required)
	Topic1 string `yaml:"topic1"` // Optional indexed parameter 1
	Topic2 string `yaml:"topic2"` // Optional indexed parameter 2
	Topic3 string `yaml:"topic3"` // Optional indexed parameter 3
	Name   string `yaml:"name"`   // Human-readable name (e.g., "Swap", "Transfer")
}

// BlockRangeFilter restricts processing to a range of block numbers.
type BlockRangeFilter struct {
	MinBlock uint64 `yaml:"min_block"` // Minimum block number (inclusive)
	MaxBlock uint64 `yaml:"max_block"` // Maximum block number (inclusive), 0 = no upper limit
}

type HostConfig struct {
	LensRegistryPath   string         `yaml:"lens_registry_path"`    // At this path, we will store the lens' wasm files
	HealthServerPort   int            `yaml:"health_server_port"`    // Port for the health server (default: 8080)
	OpenBrowserOnStart bool           `yaml:"open_browser_on_start"` // Auto-open metrics page in browser on startup (default: false)
	Snapshot           SnapshotConfig `yaml:"snapshot"`              // Snapshot bootstrap configuration
}

// SnapshotConfig configures historical snapshot download and import on startup.
type SnapshotConfig struct {
	// IndexerURL is the HTTP base URL of the indexer serving snapshots.
	IndexerURL string `yaml:"indexer_url"`
	// HistoricalRanges specifies block ranges the host needs for bootstrap.
	HistoricalRanges []BlockRange `yaml:"historical_ranges"`
}

// BlockRange represents an inclusive block number range.
type BlockRange struct {
	Start int64 `yaml:"start"`
	End   int64 `yaml:"end"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	// Load YAML config
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Apply environment variable overrides
	if v := os.Getenv("START_HEIGHT"); v != "" {
		height, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid START_HEIGHT value %q: %w", v, err)
		}
		cfg.Shinzo.StartHeight = height
	}

	if v := os.Getenv("BOOTSTRAP_PEERS"); v != "" {
		cfg.DefraDB.P2P.BootstrapPeers = strings.Split(v, ",")
	}

	return &cfg, nil
}

// ToAppConfig converts the host config to an app-sdk config
// Note: BootstrapPeers is intentionally empty - peers are added after ViewManager initializes
func (c *Config) ToAppConfig() *appConfig.Config {
	if c == nil {
		return nil
	}

	return &appConfig.Config{
		DefraDB: appConfig.DefraDBConfig{
			Url:           c.DefraDB.Url,
			KeyringSecret: c.DefraDB.KeyringSecret,
			P2P: appConfig.DefraP2PConfig{
				Enabled:             c.DefraDB.P2P.Enabled,
				BootstrapPeers:      []string{}, // Empty - peers added after ViewManager init
				ListenAddr:          c.DefraDB.P2P.ListenAddr,
				MaxRetries:          c.DefraDB.P2P.MaxRetries,
				RetryBaseDelayMs:    c.DefraDB.P2P.RetryBaseDelayMs,
				ReconnectIntervalMs: c.DefraDB.P2P.ReconnectIntervalMs,
				EnableAutoReconnect: c.DefraDB.P2P.EnableAutoReconnect,
			},
			Store: appConfig.DefraStoreConfig{
				Path:                    c.DefraDB.Store.Path,
				BlockCacheMB:            c.DefraDB.Store.BlockCacheMB,
				MemTableMB:              c.DefraDB.Store.MemTableMB,
				IndexCacheMB:            c.DefraDB.Store.IndexCacheMB,
				NumCompactors:           c.DefraDB.Store.NumCompactors,
				NumLevelZeroTables:      c.DefraDB.Store.NumLevelZeroTables,
				NumLevelZeroTablesStall: c.DefraDB.Store.NumLevelZeroTablesStall,
				ValueLogFileSizeMB:      c.DefraDB.Store.ValueLogFileSizeMB,
			},
		},
		Logger: appConfig.LoggerConfig{
			Development: c.Logger.Development,
		},
	}
}
