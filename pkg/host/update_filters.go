package host

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/shinzonetwork/shinzo-host-client/config"
)

func UpdateEventFilter(cfg *config.Config, newRules *config.FilterSet, filter *EventFilter) error {
	if err := writeFiltersAtomic(cfg, newRules); err != nil {
		return fmt.Errorf("writing filters.json: %w", err)
	}

	filter.UpdateRules(newRules)
	return nil
}

func writeFiltersAtomic(cfg *config.Config, newRules *config.FilterSet) error {
	path := config.FiltersPath(cfg.Node.DataDir, cfg.EventFilter)

	data, err := json.MarshalIndent(newRules, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding filters: %w", err)
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".filters-*.json.tmp")
	if err != nil {
		return fmt.Errorf("creating temp file in %s: %w", dir, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) //nolint:errcheck // best-effort cleanup, no-op once renamed

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", tmpPath, path, err)
	}

	return nil
}
