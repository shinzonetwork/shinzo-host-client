package graphql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

type collectionFilterConfig struct {
	rangeFilter string
	orderClause string
}

func getCollectionFilterConfig(collectionName string, start, end uint64) collectionFilterConfig {
	switch collectionName {
	case constants.CollectionBlock:
		return collectionFilterConfig{
			rangeFilter: fmt.Sprintf("_and: [ { number: { _ge: %d } }, { number: { _le: %d } } ]", start, end),
			orderClause: "order: { number: DESC }",
		}
	case constants.CollectionAccessListEntry:
		return collectionFilterConfig{
			rangeFilter: fmt.Sprintf("_and: [ { transaction: { blockNumber: { _ge: %d } } }, { transaction: { blockNumber: { _le: %d } } } ]", start, end),
			orderClause: "order: { transaction: { blockNumber: DESC } }",
		}
	default:
		return collectionFilterConfig{
			rangeFilter: fmt.Sprintf("_and: [ { blockNumber: { _ge: %d } }, { blockNumber: { _le: %d } } ]", start, end),
			orderClause: "order: { blockNumber: DESC }",
		}
	}
}

func spliceIntoExistingFilter(trimmed string, cfg collectionFilterConfig) (string, error) {
	filterStart := strings.Index(trimmed, "(")
	parenCount, filterEnd := 0, 0
	for i := filterStart; i < len(trimmed); i++ {
		if trimmed[i] == '(' {
			parenCount++
		} else if trimmed[i] == ')' {
			parenCount--
			if parenCount == 0 {
				filterEnd = i
				break
			}
		}
	}
	if filterEnd == 0 {
		return "", ErrNoClosingParen
	}

	existing := strings.TrimSpace(trimmed[filterStart+1 : filterEnd])
	newFilter := cfg.rangeFilter + ", " + cfg.orderClause
	if existing != "" {
		newFilter = existing + ", " + newFilter
	}
	return trimmed[:filterStart+1] + newFilter + trimmed[filterEnd:], nil
}

func insertNewFilter(trimmed string, cfg collectionFilterConfig) (string, error) {
	braceStart := strings.Index(trimmed, "{")
	if braceStart == -1 {
		return "", ErrNoSelectionSet
	}
	filterAndOrder := fmt.Sprintf("(filter: { %s }, %s)", cfg.rangeFilter, cfg.orderClause)
	return trimmed[:braceStart] + filterAndOrder + trimmed[braceStart:], nil
}

// AddBlockNumberFilter adds a block number filter to a GraphQL query.
func AddBlockNumberFilter(query string, startingBlockNumber uint64, endingBlockNumber uint64) (string, error) {
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
