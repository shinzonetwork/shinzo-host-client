package chain

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/shinzonetwork/shinzo-evidence-codec/rule"
)

// prefixPattern is "<Name>__<Network>", each part an upper-case letter followed by letters or
// digits, so the prefix and every collection name built from it are valid GraphQL type names. The
// upper-case first letter refuses a typo like Ethereum__mainnet, which would name a separate set of
// collections.
var prefixPattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*__[A-Z][A-Za-z0-9]*$`)

// Chain modes. Full mode serves every block, transaction, log and access-list entry. Tokens mode
// serves block headers and the signed evidence for the chain's assets only.
const (
	ModeFull   = "full"
	ModeTokens = "tokens"
)

// Config is a chain the host serves.
type Config struct {
	Prefix string `yaml:"prefix"`
	// Mode is ModeFull, the default, or ModeTokens. The keys from CAIP2 to Start are tokens mode's,
	// and a full-mode chain refuses them.
	Mode string `yaml:"mode"`
	// CAIP2 is the chain's CAIP-2 ID, such as eip155:56. Its namespace picks the family that checks
	// the chain's blocks.
	CAIP2 string `yaml:"caip2"`
	// Signers are the compressed secp256k1 public keys, in lower-case hex, of the generators whose
	// block attestations count.
	Signers []string `yaml:"signers"`
	// Required is how many of Signers must agree on a block.
	Required int `yaml:"required"`
	// Assets are the tokens the chain's attestations cover.
	Assets []Asset `yaml:"assets"`
	// Start is the first block the host verifies. The first height has no verified parent, so the
	// agreed block there must have Start.Hash.
	Start Start `yaml:"start"`
	// MaxBlocks is the number of newest block heights the pruner keeps for this chain. Zero keeps
	// pruner.max_blocks.
	MaxBlocks int64 `yaml:"max_blocks"`
	// Generators serve the chain's schema. They are tried in order.
	Generators []Generator `yaml:"generators"`
}

// Asset is a token a tokens-mode chain tracks.
type Asset struct {
	// ID is the token in its family's form: on an evm chain, the contract address as 40 lower-case
	// hex digits.
	ID string `yaml:"id"`
	// SupplySlot is, on an evm chain, the storage slot of the token's total supply in 0x-prefixed
	// hex.
	SupplySlot string `yaml:"supply_slot"`
}

// Start is the block a tokens-mode chain's verification starts at.
type Start struct {
	Block int64  `yaml:"block"`
	Hash  string `yaml:"hash"`
}

// RuleConfig returns the evidence codec's rule configuration of a tokens-mode chain: its CAIP-2 ID,
// signers, required count and assets.
func (c Config) RuleConfig() rule.Config {
	assets := make([]rule.Asset, len(c.Assets))
	for i, a := range c.Assets {
		assets[i] = rule.Asset{ID: a.ID, SupplySlot: a.SupplySlot}
	}
	return rule.Config{Chain: c.CAIP2, Signers: c.Signers, Required: c.Required, Assets: assets}
}

// Generator is a generator node that indexes the chain and serves its schema.
type Generator struct {
	// URL is the base URL of the generator's HTTP API, for example "http://10.0.0.5:8080".
	URL string `yaml:"url"`
	// Peer is the generator's P2P address, which the host dials with its bootstrap peers. Like a
	// bootstrap peer, it is a multiaddr or an IP address with or without a port. It is optional.
	Peer string `yaml:"peer"`
}

// Validate checks every chain's prefix, max_blocks and mode, and each generator's URL and peer. Two
// prefixes that differ only in case are rejected: DefraDB keeps them as separate collections, so
// such a pair is one chain mistyped. Two chains with one caip2 are rejected too. A peer is parsed as
// the host parses it before dialling, so one the host would skip is refused here instead.
func Validate(chains []Config) error {
	seen := make(map[string]string, len(chains))
	caip2s := make(map[string]string, len(chains))
	for _, c := range chains {
		if !prefixPattern.MatchString(c.Prefix) {
			return fmt.Errorf("chain %q: %w", c.Prefix, ErrInvalidPrefix)
		}
		folded := strings.ToLower(c.Prefix)
		if other, ok := seen[folded]; ok {
			return fmt.Errorf("chains %q and %q: %w", other, c.Prefix, ErrDuplicatePrefix)
		}
		seen[folded] = c.Prefix

		if c.MaxBlocks < 0 {
			return fmt.Errorf("chain %q max_blocks %d: %w", c.Prefix, c.MaxBlocks, ErrInvalidMaxBlocks)
		}
		if err := validateMode(c); err != nil {
			return fmt.Errorf("chain %q: %w", c.Prefix, err)
		}
		// Only a tokens-mode chain has a caip2; validateMode refuses one in full mode.
		if c.CAIP2 != "" {
			if other, ok := caip2s[c.CAIP2]; ok {
				return fmt.Errorf("chains %q and %q caip2 %q: %w", other, c.Prefix, c.CAIP2, ErrDuplicateCAIP2)
			}
			caip2s[c.CAIP2] = c.Prefix
		}

		for _, g := range c.Generators {
			u, err := url.Parse(g.URL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("chain %q generator %q: %w", c.Prefix, g.URL, ErrInvalidGeneratorURL)
			}
			if peer := strings.TrimSpace(g.Peer); peer != "" {
				if _, err := ParsePeer(peer); err != nil {
					return fmt.Errorf("chain %q generator %q peer %q: %w: %w", c.Prefix, g.URL, g.Peer, ErrInvalidGeneratorPeer, err)
				}
			}
		}
	}
	return nil
}

// validateMode checks the chain's mode, and that a full-mode chain sets none of the tokens-mode
// keys, which would have no effect there. In tokens mode it checks only what the evidence codec's
// rule.New does not: that assets and start.hash are set and start.block is not negative.
func validateMode(c Config) error {
	switch c.Mode {
	case "", ModeFull:
		tokenKeys := []struct {
			name string
			set  bool
		}{
			{"caip2", c.CAIP2 != ""},
			{"signers", len(c.Signers) > 0},
			{"required", c.Required != 0},
			{"assets", len(c.Assets) > 0},
			{"start", c.Start != (Start{})},
		}
		for _, key := range tokenKeys {
			if key.set {
				return fmt.Errorf("%s: %w", key.name, ErrTokenKeyInFullMode)
			}
		}
	case ModeTokens:
		if len(c.Assets) == 0 {
			return fmt.Errorf("assets: %w", ErrMissingTokenKey)
		}
		if c.Start.Hash == "" {
			return fmt.Errorf("start.hash: %w", ErrMissingTokenKey)
		}
		if c.Start.Block < 0 {
			return fmt.Errorf("start.block %d: %w", c.Start.Block, ErrInvalidStartBlock)
		}
	default:
		return fmt.Errorf("%w: got %q", ErrInvalidMode, c.Mode)
	}
	return nil
}
