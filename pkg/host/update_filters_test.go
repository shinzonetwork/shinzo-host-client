package host

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

func testFilterCfg(t *testing.T) *hostconfig.Config {
	t.Helper()
	cfg := testHostConfig()
	cfg.Node.DataDir = t.TempDir()
	cfg.EventFilter = hostconfig.EventFilterConfig{Enabled: true, Mode: "allowlist"}
	return cfg
}

func TestUpdateEventFilter_WritesFileAndAppliesLive(t *testing.T) {
	cfg := testFilterCfg(t)

	initial := &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:      "a",
			Enabled:   true,
			Contracts: []hostconfig.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
		}},
	}
	filter := NewEventFilter(cfg.EventFilter, initial)

	updated := &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:      "b",
			Enabled:   true,
			Contracts: []hostconfig.ContractFilter{{Address: "0xB", Types: []string{"log"}}},
		}},
	}

	if err := UpdateEventFilter(cfg, updated, filter); err != nil {
		t.Fatalf("UpdateEventFilter: %v", err)
	}

	if !filter.AllowReplication(context.Background(), constants.CollectionLog, "doc1", map[string]any{"address": "0xB"}) {
		t.Fatal("expected 0xB to be allowed after the update")
	}
	if filter.AllowReplication(context.Background(), constants.CollectionLog, "doc2", map[string]any{"address": "0xA"}) {
		t.Fatal("expected 0xA to no longer match after the update")
	}

	path := hostconfig.FiltersPath(cfg.Node.DataDir, cfg.EventFilter)
	data, err := os.ReadFile(path) //nolint:gosec // test-controlled path
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var onDisk hostconfig.FilterSet
	if err := json.Unmarshal(data, &onDisk); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if len(onDisk.Groups) != 1 || onDisk.Groups[0].Contracts[0].Address != "0xB" {
		t.Fatalf("expected the persisted file to reflect the update, got %+v", onDisk)
	}

	reloaded, err := hostconfig.LoadFilters(cfg.Node.DataDir, cfg.EventFilter)
	if err != nil {
		t.Fatalf("LoadFilters: %v", err)
	}
	if len(reloaded.Groups) != 1 || reloaded.Groups[0].Contracts[0].Address != "0xB" {
		t.Fatalf("expected LoadFilters to see the update after restart, got %+v", reloaded)
	}
}

func TestUpdateEventFilter_LeavesRunningFilterUntouchedOnWriteFailure(t *testing.T) {
	cfg := testFilterCfg(t)

	initial := &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:      "a",
			Enabled:   true,
			Contracts: []hostconfig.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
		}},
	}
	filter := NewEventFilter(cfg.EventFilter, initial)

	broken := *cfg
	broken.Node.DataDir = filepath.Join(cfg.Node.DataDir, "does", "not", "exist")

	failedUpdate := &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:      "b",
			Enabled:   true,
			Contracts: []hostconfig.ContractFilter{{Address: "0xB", Types: []string{"log"}}},
		}},
	}

	if err := UpdateEventFilter(&broken, failedUpdate, filter); err == nil {
		t.Fatal("expected an error when the target directory doesn't exist, got nil")
	}

	if filter.AllowReplication(context.Background(), constants.CollectionLog, "doc1", map[string]any{"address": "0xB"}) {
		t.Fatal("expected 0xB to still be rejected, the failed update must not have been applied live")
	}
	if !filter.AllowReplication(context.Background(), constants.CollectionLog, "doc2", map[string]any{"address": "0xA"}) {
		t.Fatal("expected 0xA to still be allowed, the original rules must still be in effect")
	}
}
