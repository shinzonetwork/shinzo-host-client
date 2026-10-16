package pruner

import "github.com/shinzonetwork/shinzo-host-client/pkg/chain"

const defaultMaxDocsPerCycle = 50000

// Config represents pruner configuration for removing old documents.
type Config struct {
	Enabled         bool  `yaml:"enabled"`
	MaxBlocks       int64 `yaml:"max_blocks"`      // Newest block heights kept; documents at older heights are deleted
	PruneThreshold  int64 `yaml:"prune_threshold"` // Deprecated: kept for backward compatibility, unused by pruner
	IntervalSeconds int   `yaml:"interval_seconds"`
	PruneHistory    bool  `yaml:"prune_history"`
	// MaxDocsPerCycle bounds what one cycle removes. Set it above the arrival rate over one
	// interval, or the store grows.
	MaxDocsPerCycle int64 `yaml:"max_docs_per_cycle"`
}

// CollectionConfig defines which collections to prune. The sweep deletes the lowest heights first
// across all of them, and at one height it deletes the dependents before the block. It orders on
// each collection's height field, so that field has to be indexed.
type CollectionConfig struct {
	Block      chain.Collection
	Dependents []chain.Collection
}

// CollectionConfigFor returns the collections to prune for a chain: its blocks, and every other
// collection of the chain as a dependent.
func CollectionConfigFor(c chain.Collections) CollectionConfig {
	return CollectionConfig{
		Block: c.Block,
		Dependents: []chain.Collection{
			c.AccessListEntry,
			c.Log,
			c.Transaction,
			c.BlockSignature,
			c.AttestationRecord,
			c.SnapshotSignature,
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
