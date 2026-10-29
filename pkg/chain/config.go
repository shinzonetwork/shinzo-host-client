package chain

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// prefixPattern is "<Name>__<Network>", each part an upper-case letter followed by letters or
// digits, so the prefix and every collection name built from it are valid GraphQL type names. The
// upper-case first letter refuses a typo like Ethereum__mainnet, which would name a separate set of
// collections.
var prefixPattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*__[A-Z][A-Za-z0-9]*$`)

// Chain modes. Full mode serves every block, transaction, log and access-list entry. Tokens mode
// serves block headers and the logs of the chain's contracts only.
const (
	ModeFull   = "full"
	ModeTokens = "tokens"
)

// Config is a chain the host serves.
type Config struct {
	Prefix string `yaml:"prefix"`
	// ChainID is the chain's ID, which its generators bind every block signature to.
	ChainID uint64 `yaml:"chain_id"`
	// Mode is ModeFull, the default, or ModeTokens.
	Mode string `yaml:"mode"`
	// Contracts are the contracts whose logs a tokens-mode chain serves, as 20-byte hex addresses.
	Contracts []string `yaml:"contracts"`
	// MaxBlocks is the number of newest block heights the pruner keeps for this chain. Zero keeps
	// pruner.max_blocks.
	MaxBlocks int64 `yaml:"max_blocks"`
	// Generators serve the chain's schema. They are tried in order.
	Generators []Generator `yaml:"generators"`
	// Signers are the generators whose block signatures count for this chain: each one's public key
	// in hex, with or without 0x, as its /registration page shows it.
	Signers []string `yaml:"signers"`
}

// AllowsSigner reports whether identity, the public key a BlockSignature document carries, is one
// of the chain's signers. It matches only the form generators write, lower-case hex without 0x: a
// re-cased key still verifies, and would be counted as a second signer of the same block.
func (c Config) AllowsSigner(identity string) bool {
	return slices.ContainsFunc(c.Signers, func(s string) bool { return signerKey(s) == identity })
}

// signerKey is a configured signer's public key in lower-case hex without a 0x prefix.
func signerKey(s string) string {
	return strings.TrimPrefix(strings.ToLower(s), "0x")
}

// Generator is a generator node that indexes the chain and serves its schema.
type Generator struct {
	// URL is the base URL of the generator's HTTP API, for example "http://10.0.0.5:8080".
	URL string `yaml:"url"`
	// Peer is the generator's P2P address, which the host dials with its bootstrap peers. Like a
	// bootstrap peer, it is a multiaddr or an IP address with or without a port. It is optional.
	Peer string `yaml:"peer"`
}

// Validate checks every chain's prefix, chain_id, max_blocks, signers, mode and contracts, and each
// generator's URL and peer. Two prefixes that differ only in case are rejected: DefraDB keeps them
// as separate collections, so such a pair is one chain mistyped. A peer is parsed as the host
// parses it before dialling, so one the host would skip is refused here instead.
func Validate(chains []Config) error {
	seen := make(map[string]string, len(chains))
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
		if c.ChainID == 0 {
			return fmt.Errorf("chain %q: %w", c.Prefix, ErrMissingChainID)
		}
		if len(c.Signers) == 0 {
			return fmt.Errorf("chain %q: %w", c.Prefix, ErrNoSigners)
		}
		for _, signer := range c.Signers {
			key := signerKey(signer)
			if _, err := hex.DecodeString(key); key == "" || err != nil {
				return fmt.Errorf("chain %q signer %q: %w", c.Prefix, signer, ErrInvalidSigner)
			}
		}
		if err := validateMode(c); err != nil {
			return fmt.Errorf("chain %q: %w", c.Prefix, err)
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

// validateMode checks the chain's mode, and that its contracts are distinct addresses in tokens
// mode and absent in full mode, where they would have no effect.
func validateMode(c Config) error {
	switch c.Mode {
	case "", ModeFull:
		if len(c.Contracts) > 0 {
			return ErrInvalidContracts
		}
	case ModeTokens:
		if len(c.Contracts) == 0 {
			return ErrInvalidContracts
		}
		seen := make(map[string]bool, len(c.Contracts))
		for _, raw := range c.Contracts {
			addr, err := contractAddress(raw)
			if err != nil {
				return err
			}
			if seen[addr] {
				return fmt.Errorf("%w: %q is listed twice", ErrInvalidContract, raw)
			}
			seen[addr] = true
		}
	default:
		return fmt.Errorf("%w: got %q", ErrInvalidMode, c.Mode)
	}
	return nil
}

// contractAddress returns a contract address as 40 lower-case hex digits without 0x, the form
// generators name a contract's table with. It accepts either letter case, with or without 0x, and
// returns ErrInvalidContract for anything that is not 20 bytes of hex.
func contractAddress(raw string) (string, error) {
	addr := strings.TrimPrefix(strings.ToLower(raw), "0x")
	if len(addr) != 40 || strings.Trim(addr, "0123456789abcdef") != "" {
		return "", fmt.Errorf("%w: got %q", ErrInvalidContract, raw)
	}
	return addr, nil
}
