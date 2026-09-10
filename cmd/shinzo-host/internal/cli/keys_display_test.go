package cli

import (
	"bytes"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/host"
)

func testNodeKeys(t *testing.T) host.NodeKeys {
	t.Helper()
	cfg := hostconfig.Default()
	cfg.Node.DataDir = t.TempDir()
	cfg.Node.KeyDir = cfg.Node.DataDir + "/keys"

	keys, err := host.EnsureKeys(&cfg, "", zap.NewNop().Sugar())
	if err != nil {
		t.Fatalf("EnsureKeys: %v", err)
	}
	return keys
}

func TestPrintKeyBanner_ContainsAllIdentifiers(t *testing.T) {
	keys := testNodeKeys(t)

	var buf bytes.Buffer
	if err := printKeyBanner(&buf, keys); err != nil {
		t.Fatalf("printKeyBanner: %v", err)
	}
	out := buf.String()

	peerID, err := keys.PeerID()
	if err != nil {
		t.Fatalf("PeerID: %v", err)
	}
	shinzoAddr, err := keys.ShinzoAddress()
	if err != nil {
		t.Fatalf("ShinzoAddress: %v", err)
	}

	for _, want := range []string{
		bannerTitle,
		peerID.String(),
		keys.OperatorAddress().Hex(),
		shinzoAddr,
		keys.DID(),
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected banner to contain %q, got:\n%s", want, out)
		}
	}
}

func TestPrintKeyBanner_IsABox(t *testing.T) {
	keys := testNodeKeys(t)

	var buf bytes.Buffer
	if err := printKeyBanner(&buf, keys); err != nil {
		t.Fatalf("printKeyBanner: %v", err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")

	if len(lines) < 2 {
		t.Fatalf("expected at least a top and bottom border, got %d lines", len(lines))
	}
	if !strings.HasPrefix(lines[0], "╔") || !strings.HasSuffix(lines[0], "╗") {
		t.Fatalf("expected the first line to be the top border, got %q", lines[0])
	}
	last := lines[len(lines)-1]
	if !strings.HasPrefix(last, "╚") || !strings.HasSuffix(last, "╝") {
		t.Fatalf("expected the last line to be the bottom border, got %q", last)
	}
}
