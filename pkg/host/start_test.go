package host

import (
	"context"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
)

func testHostConfig() *hostconfig.Config {
	cfg := hostconfig.Default()
	cfg.HTTP.Addr = ":0"
	return &cfg
}

func TestStart_FailsFastOnInvalidACPConfig(t *testing.T) {
	cfg := testHostConfig()
	cfg.ACP.Enabled = true // missing chain_id/epoch_length/min_query_balance

	_, err := Start(context.Background(), cfg, zap.NewNop(), NodeKeys{})
	if err == nil {
		t.Fatal("expected Start to fail on an invalid acp config, got nil")
	}
	if !strings.Contains(err.Error(), "acp config") {
		t.Fatalf(`expected the error to be wrapped as "acp config: ...", got: %v`, err)
	}
}

func TestBuildACPConfig_DisabledIsValid(t *testing.T) {
	acpCfg, err := buildACPConfig(testHostConfig())
	if err != nil {
		t.Fatalf("expected a disabled acp config to be valid, got: %v", err)
	}
	if acpCfg.Enabled {
		t.Fatal("expected Enabled to default to false")
	}
}

func TestBuildACPConfig_ChainIDComesFromShinzo(t *testing.T) {
	cfg := testHostConfig()
	cfg.Shinzo.ChainID = 12345
	cfg.ACP.Enabled = true
	cfg.ACP.MinQueryBalance = "1000"
	cfg.ACP.EpochLength = 100

	acpCfg, err := buildACPConfig(cfg)
	if err != nil {
		t.Fatalf("buildACPConfig: %v", err)
	}
	if acpCfg.ChainID != 12345 {
		t.Fatalf("expected ChainID 12345 pulled from shinzo.chain_id, got %d", acpCfg.ChainID)
	}
}

func TestBuildACPConfig_InvalidAttesterWindowErrors(t *testing.T) {
	cfg := testHostConfig()
	cfg.ACP.AttesterWindow = "not-a-duration"

	if _, err := buildACPConfig(cfg); err == nil {
		t.Fatal("expected an error for a malformed attester_window, got nil")
	}
}
