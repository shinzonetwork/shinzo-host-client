package graphql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
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

	cfg := collectionFilterConfig{
		rangeFilter: withCollectionRangeFilter(matches[1], startingBlockNumber, endingBlockNumber),
		orderClause: "", // WithBlockNumberFilter does not add ordering
	}

	if matches[2] == "(" {
		return spliceIntoExistingFilter(trimmed, cfg)
	}
	return insertNewFilter(trimmed, cfg)
}

// withCollectionRangeFilter returns only the range filter string for WithBlockNumberFilter (no order clause).
func withCollectionRangeFilter(collectionName string, start, end uint64) string {
	switch collectionName {
	case constants.CollectionBlock:
		return fmt.Sprintf("_and: [ { number: { _ge: %d } }, { number: { _le: %d } } ]", start, end)
	case constants.CollectionAccessListEntry:
		return fmt.Sprintf("_and: [ { transaction: { blockNumber: { _ge: %d } } }, { transaction: { blockNumber: { _le: %d } } } ]", start, end)
	default:
		return fmt.Sprintf("_and: [ { blockNumber: { _ge: %d } }, { blockNumber: { _le: %d } } ]", start, end)
	}
}
