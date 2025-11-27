package graphql

import (
	"fmt"
	"regexp"
	"strings"
)

// WithBlockNumberFilter adds a block number filter to a GraphQL query
// It ensures that only results from block numbers greater than startingBlockNumber are returned
func WithBlockNumberFilter(query string, startingBlockNumber uint64, endingBlockNumber uint64) (string, error) {
	if query == "" {
		return "", fmt.Errorf("query cannot be empty")
	}

	// Parse the query to find the main collection type and add the block number filter
	trimmed := strings.TrimSpace(query)

	// Look for patterns like "Log { ... }" or "Log(filter: {...}) { ... }"
	re := regexp.MustCompile(`(\w+)\s*(\{|\()`)
	matches := re.FindStringSubmatch(trimmed)
	if len(matches) < 3 {
		return "", fmt.Errorf("unable to parse collection name from query: %s", query)
	}

	collectionName := matches[1]
	openingBrace := matches[2]

	// Check if there's already a filter
	if openingBrace == "(" {
		// Query already has a filter, we need to add to it
		// Find the end of the existing filter
		parenCount := 0
		filterStart := strings.Index(trimmed, "(")
		filterEnd := 0
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
			return "", fmt.Errorf("unable to find matching closing parenthesis in query")
		}

		// Extract existing filter content
		existingFilter := trimmed[filterStart+1 : filterEnd]
		existingFilter = strings.TrimSpace(existingFilter)

		// Add block number filter to existing filter
		var newFilter string
		if collectionName == "Block" {
			// For Block queries, number is the correct field
			if existingFilter == "" {
				newFilter = fmt.Sprintf("_and: [ { number: { _ge: %d } }, { number: { _le: %d } } ]", startingBlockNumber, endingBlockNumber)
			} else {
				newFilter = fmt.Sprintf("%s, _and: [ { number: { _ge: %d } }, { number: { _le: %d } } ]", existingFilter, startingBlockNumber, endingBlockNumber)
			}
		} else if collectionName == "AccessListEntry" {
			// For AccessListEntry queries, block number is within transaction
			if existingFilter == "" {
				newFilter = fmt.Sprintf("_and: [ { transaction: { blockNumber: { _ge: %d } } }, { transaction: { blockNumber: { _le: %d } } } ]", startingBlockNumber, endingBlockNumber)
			} else {
				newFilter = fmt.Sprintf("%s, _and: [ { transaction: { blockNumber: { _ge: %d } } }, { transaction: { blockNumber: { _le: %d } } } ]", existingFilter, startingBlockNumber, endingBlockNumber)
			}
		} else {
			// For other collections (Log, Transaction) filter on blockNumber field
			if existingFilter == "" {
				newFilter = fmt.Sprintf("_and: [ { blockNumber: { _ge: %d } }, { blockNumber: { _le: %d } } ]", startingBlockNumber, endingBlockNumber)
			} else {
				newFilter = fmt.Sprintf("%s, _and: [ { blockNumber: { _ge: %d } }, { blockNumber: { _le: %d } } ]", existingFilter, startingBlockNumber, endingBlockNumber)
			}
		}

		// Reconstruct the query
		beforeFilter := trimmed[:filterStart+1]
		afterFilter := trimmed[filterEnd:]
		return beforeFilter + newFilter + afterFilter, nil
	} else {
		// No existing filter, add one
		// Find the opening brace of the selection set
		braceStart := strings.Index(trimmed, "{")
		if braceStart == -1 {
			return "", fmt.Errorf("unable to find selection set in query")
		}

		// Insert filter before the selection set
		beforeBrace := trimmed[:braceStart]
		afterBrace := trimmed[braceStart:]

		// Create filter based on collection type
		var filter string
		if collectionName == "Block" {
			// For Block queries, number is a direct field
			filter = fmt.Sprintf("(filter: { _and: [ { number: { _ge: %d } }, { number: { _le: %d } } ] })", startingBlockNumber, endingBlockNumber)
		} else if collectionName == "AccessListEntry" {
			// For AccessListEntry queries, block number is within transaction
			filter = fmt.Sprintf("(filter: { _and: [ { transaction: { blockNumber: { _ge: %d } } }, { transaction: { blockNumber: { _le: %d } } } ] })", startingBlockNumber, endingBlockNumber)
		} else {
			// For other collections (Log, Transaction), filter directly on blockNumber field
			filter = fmt.Sprintf("(filter: { _and: [ { blockNumber: { _ge: %d } }, { blockNumber: { _le: %d } } ] })", startingBlockNumber, endingBlockNumber)
		}
		return beforeBrace + filter + afterBrace, nil
	}
}
