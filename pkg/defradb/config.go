package defradb

// Config holds the runtime configuration consumed by StartDefraInstance, the
// signer, and other defradb-level helpers. It mirrors the SDK's config shape
// so existing call sites pass through unchanged; host code populates it via
// (*hostconfig.Config).ToInternalConfig().
type Config struct {
	DefraDB DefraDBConfig `yaml:"defradb"`
	Logger  LoggerConfig  `yaml:"logger"`
}

// DefraDBConfig is a structure for passing the client config to app.
type DefraDBConfig struct { //nolint:revive // TODO: remove the double reference to defra
	URL           string           `yaml:"url"`
	KeyringSecret string           `yaml:"keyring_secret"`
	P2P           DefraP2PConfig   `yaml:"p2p"`
	Store         DefraStoreConfig `yaml:"store"`
}

// DefraP2PConfig is a structure for the default behaviour.
type DefraP2PConfig struct {
	Enabled             bool     `yaml:"enabled"`
	BootstrapPeers      []string `yaml:"bootstrap_peers"`
	ListenAddr          string   `yaml:"listen_addr"`
	MaxRetries          int      `yaml:"max_retries"`
	RetryBaseDelayMs    int      `yaml:"retry_base_delay_ms"`
	ReconnectIntervalMs int      `yaml:"reconnect_interval_ms"`
	EnableAutoReconnect bool     `yaml:"enable_auto_reconnect"`
}

// DefraStoreConfig holds configuration for the DefraDB storage engine.
type DefraStoreConfig struct {
	Path string `yaml:"path"`
	// Badger memory configuration.
	BlockCacheMB int64 `yaml:"block_cache_mb"`
	MemTableMB   int64 `yaml:"memtable_mb"`
	IndexCacheMB int64 `yaml:"index_cache_mb"`
	// Badger compaction configuration.
	NumCompactors           int `yaml:"num_compactors"`
	NumLevelZeroTables      int `yaml:"num_level_zero_tables"`
	NumLevelZeroTablesStall int `yaml:"num_level_zero_tables_stall"`
	// Badger value-log file size in MB. Defaults to 64 inside StartDefraInstance.
	ValueLogFileSizeMB int64 `yaml:"value_log_file_size_mb"`
}

// LoggerConfig holds configuration for the application logger.
type LoggerConfig struct {
	Development bool   `yaml:"development"`
	LogsDir     string `yaml:"logs_dir"`
}
