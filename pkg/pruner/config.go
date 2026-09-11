package pruner

const defaultMaxDocsPerCycle = 50000

// Config represents pruner configuration for removing old documents.
type Config struct {
	Enabled         bool  `yaml:"enabled"`
	MaxBlocks       int64 `yaml:"max_blocks"`      // Number of blocks to retain
	PruneThreshold  int64 `yaml:"prune_threshold"` // Deprecated: kept for backward compatibility, unused by pruner
	IntervalSeconds int   `yaml:"interval_seconds"`
	PruneHistory    bool  `yaml:"prune_history"`
	// MaxDocsPerCycle bounds what one cycle removes. Set it above the arrival rate over one
	// interval, or the store grows.
	MaxDocsPerCycle int64 `yaml:"max_docs_per_cycle"`
}

// Height field names the default collections use.
const (
	blockHeightField     = "number"
	dependentHeightField = "blockNumber"
	// A snapshot covers a range, so retention follows the newest block in it.
	snapshotHeightField = "endBlock"
)

// CollectionHeight names a collection and the field holding the block height its documents
// belong to. The height sweep orders on that field, so it has to be indexed.
type CollectionHeight struct {
	Name        string
	HeightField string
}

// CollectionConfig defines which collections to prune. Dependents are pruned in the order given
// and before the block collection, so a document is never removed while another that references
// it remains.
type CollectionConfig struct {
	Block      CollectionHeight
	Dependents []CollectionHeight
}

// DefaultCollectionConfig returns the default Ethereum mainnet collection config.
func DefaultCollectionConfig() CollectionConfig {
	return CollectionConfig{
		Block: CollectionHeight{Name: "Ethereum__Mainnet__Block", HeightField: blockHeightField},
		Dependents: []CollectionHeight{
			{Name: "Ethereum__Mainnet__AccessListEntry", HeightField: dependentHeightField},
			{Name: "Ethereum__Mainnet__Log", HeightField: dependentHeightField},
			{Name: "Ethereum__Mainnet__Transaction", HeightField: dependentHeightField},
			{Name: "Ethereum__Mainnet__BlockSignature", HeightField: dependentHeightField},
			{Name: "Ethereum__Mainnet__AttestationRecord", HeightField: dependentHeightField},
			{Name: "Ethereum__Mainnet__SnapshotSignature", HeightField: snapshotHeightField},
		},
	}
}

// SetDefaults fills in zero-value fields with sensible defaults.
func (c *Config) SetDefaults() {
	if c.MaxBlocks <= 0 {
		c.MaxBlocks = 10000
	}
	if c.IntervalSeconds <= 0 {
		c.IntervalSeconds = 60
	}
	if c.MaxDocsPerCycle <= 0 {
		c.MaxDocsPerCycle = defaultMaxDocsPerCycle
	}
}
