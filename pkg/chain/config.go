package chain

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// prefixPattern is "<Name>__<Network>", each part a letter followed by letters or digits, so the
// prefix and every collection name built from it are valid GraphQL type names.
var prefixPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*__[A-Za-z][A-Za-z0-9]*$`)

// Config is a chain the host serves.
type Config struct {
	Prefix string `yaml:"prefix"`
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

// Validate checks every chain's prefix and generator URLs. Two prefixes that differ only in case
// are rejected: DefraDB keeps them as separate collections, so such a pair is one chain mistyped.
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

		for _, g := range c.Generators {
			u, err := url.Parse(g.URL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("chain %q generator %q: %w", c.Prefix, g.URL, ErrInvalidGeneratorURL)
			}
		}
	}
	return nil
}
