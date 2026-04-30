package graphql

import (
	"fmt"
	"regexp"
	"strings"
)

// WithBlockNumberFilter adds a block number filter to a GraphQL query.
func WithBlockNumberFilter(query string, startingBlockNumber uint64, endingBlockNumber uint64) (string, error) {
	if query == "" {
		return "", ErrQueryEmpty
	}

	trimmed := strings.TrimSpace(query)
	re := regexp.MustCompile(`(\w+)\s*(\{|\()`)
	matches := re.FindStringSubmatch(trimmed)
	if len(matches) < minRegexMatchGroups {
		return "", fmt.Errorf("query %q: %w", query, ErrParseCollectionName)
	}

	cfg := getCollectionFilterConfig(matches[1], startingBlockNumber, endingBlockNumber)

	if matches[2] == "(" {
		return spliceIntoExistingFilter(trimmed, cfg)
	}
	return insertNewFilter(trimmed, cfg)
}
