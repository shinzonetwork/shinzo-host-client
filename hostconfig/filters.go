package hostconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type EventFilterConfig struct {
	Enabled        bool   `toml:"enabled"`
	Mode           string `toml:"mode"`
	CascadeFilters bool   `toml:"cascade_filters"`

	// relative to the instance dir unless absolute, defaults to filters.json
	File string `toml:"file"`
}

type FilterSet struct {
	BlockRange *BlockRangeFilter `json:"block_range,omitempty"`
	Groups     []FilterGroup     `json:"groups"`
}

type BlockRangeFilter struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type FilterGroup struct {
	Name      string           `json:"name"`
	Enabled   bool             `json:"enabled"`
	Contracts []ContractFilter `json:"contracts"`
	Topics    []TopicFilter    `json:"topics"`
}

type ContractFilter struct {
	Address string   `json:"address"`
	Name    string   `json:"name"`
	Types   []string `json:"types"`
}

type TopicFilter struct {
	Topic0 string `json:"topic0"`
	Topic1 string `json:"topic1,omitempty"`
	Topic2 string `json:"topic2,omitempty"`
	Topic3 string `json:"topic3,omitempty"`
	Name   string `json:"name"`
}

// no disk touch when disabled, so "off" and "file's missing" aren't the same case
func LoadFilters(instanceDir string, ref EventFilterConfig) (*FilterSet, error) {
	if !ref.Enabled {
		return &FilterSet{}, nil
	}

	path := ref.File
	if path == "" {
		path = "filters.json"
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(instanceDir, path)
	}

	data, err := os.ReadFile(path) //nolint:gosec // operator-controlled config path
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	var set FilterSet
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	return &set, nil
}
