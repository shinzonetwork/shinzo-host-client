package chain

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// prefixPattern is "<Name>__<Network>", each part an upper-case letter followed by letters or
// digits, so the prefix and every collection name built from it are valid GraphQL type names. The
// upper-case first letter refuses a typo like Ethereum__mainnet, which would name a separate set of
// collections.
var prefixPattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*__[A-Z][A-Za-z0-9]*$`)

// Config is a chain the host serves.
type Config struct {
	Prefix string `yaml:"prefix"`
	// MaxBlocks is the number of newest block heights the pruner keeps for this chain. Zero keeps
	// pruner.max_blocks.
	MaxBlocks int64 `yaml:"max_blocks"`
	// Generators serve the chain's schema. They are tried in order.
	Generators []Generator `yaml:"generators"`
}

// Generator is a generator node that indexes the chain and serves its schema.
type Generator struct {
	// URL is the base URL of the generator's HTTP API, for example "http://10.0.0.5:8080".
	URL string `yaml:"url"`
	// Peer is the generator's P2P address, which the host dials with its bootstrap peers. Like a
	// bootstrap peer, it is a multiaddr or an IP address with or without a port. It is optional.
	Peer string `yaml:"peer"`
}

// Validate checks every chain's prefix and max_blocks, and each generator's URL and peer. Two
// prefixes that differ only in case are rejected: DefraDB keeps them as separate collections, so
// such a pair is one chain mistyped. A peer is parsed as the host parses it before dialling, so one
// the host would skip is refused here instead.
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
