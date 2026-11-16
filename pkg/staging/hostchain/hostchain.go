package hostchain

import (
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// idPattern is <Name>__<Network>, each part an upper-case letter then
// letters or digits, so collection names built from it are valid
// GraphQL type names.
var idPattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*__[A-Z][A-Za-z0-9]*$`)

// Config holds the settings for one chain the host serves.
type Config struct {
	// MaxBlocks is how many of the newest block heights the pruner keeps.
	// 0 uses the pruner's default.
	MaxBlocks int64 `comment:"newest block heights the pruner keeps for this chain; 0 uses the pruner default" toml:"max_blocks"`

	// Generators serve the chain's schema. They're tried in order.
	Generators []Generator `comment:"generators serving this chain's schema, tried in order" toml:"generators"`
}

// Generator is a generator node that indexes the chain and serves its schema.
type Generator struct {
	// URL is the base URL of the generator's HTTP API, e.g. http://10.0.0.5:8080.
	URL string `toml:"url"`
}

// Validate checks each network ID's shape, that no two differ only in
// case (DefraDB would keep those as separate collections), and each
// chain's settings. Sorted so errors name chains the same way every run.
func Validate(chains map[string]Config) error {
	seen := make(map[string]string, len(chains))
	for _, id := range slices.Sorted(maps.Keys(chains)) {
		if !idPattern.MatchString(id) {
			return fmt.Errorf("chain %q: %w", id, ErrInvalidID)
		}
		folded := strings.ToLower(id)
		if other, ok := seen[folded]; ok {
			return fmt.Errorf("chains %q and %q: %w", other, id, ErrDuplicateID)
		}
		seen[folded] = id

		if err := validateConfig(chains[id]); err != nil {
			return fmt.Errorf("chain %q %w", id, err)
		}
	}
	return nil
}

func validateConfig(c Config) error {
	if c.MaxBlocks < 0 {
		return fmt.Errorf("max_blocks %d: %w", c.MaxBlocks, ErrInvalidMaxBlocks)
	}
	for _, g := range c.Generators {
		u, err := url.Parse(g.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
			return fmt.Errorf("generator %q: %w", g.URL, ErrInvalidGeneratorURL)
		}
	}
	return nil
}

// Require reports whether at least one chain is configured. Separate
// from Validate since only start needs one. init may save none.
func Require(chains map[string]Config) error {
	if len(chains) == 0 {
		return ErrNoChains
	}
	return nil
}
