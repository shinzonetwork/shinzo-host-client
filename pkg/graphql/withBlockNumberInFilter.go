package graphql

import (
	"fmt"
	"regexp"
	"strings"
)

// WithBlockNumberInFilter adds a block number _in filter to a GraphQL query.
// This is used to query for specific block numbers that may have arrived out of order.
func WithBlockNumberInFilter(query string, blockNumbers []uint64) (string, error) {
	if query == "" {
		return "", fmt.Errorf("query cannot be empty")
	}

	if len(blockNumbers) == 0 {
		return "", fmt.Errorf("blockNumbers cannot be empty")
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

	// Build the _in filter value
	inValues := make([]string, len(blockNumbers))
	for i, bn := range blockNumbers {
		inValues[i] = fmt.Sprintf("%d", bn)
	}
	inFilterValue := "[" + strings.Join(inValues, ", ") + "]"

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

		// Add block number _in filter to existing filter
		var newFilter string
		if collectionName == "Block" {
			// For Block queries, number is the correct field
			if existingFilter == "" {
				newFilter = fmt.Sprintf("number: { _in: %s }", inFilterValue)
			} else {
				newFilter = fmt.Sprintf("%s, number: { _in: %s }", existingFilter, inFilterValue)
			}
		} else if collectionName == "AccessListEntry" {
			// For AccessListEntry queries, block number is within transaction
			if existingFilter == "" {
				newFilter = fmt.Sprintf("transaction: { blockNumber: { _in: %s } }", inFilterValue)
			} else {
				newFilter = fmt.Sprintf("%s, transaction: { blockNumber: { _in: %s } }", existingFilter, inFilterValue)
			}
		} else {
			// For other collections (Log, Transaction) filter on blockNumber field
			if existingFilter == "" {
				newFilter = fmt.Sprintf("blockNumber: { _in: %s }", inFilterValue)
			} else {
				newFilter = fmt.Sprintf("%s, blockNumber: { _in: %s }", existingFilter, inFilterValue)
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
			filter = fmt.Sprintf("(filter: { number: { _in: %s } })", inFilterValue)
		} else if collectionName == "AccessListEntry" {
			// For AccessListEntry queries, block number is within transaction
			filter = fmt.Sprintf("(filter: { transaction: { blockNumber: { _in: %s } } })", inFilterValue)
		} else {
			// For other collections (Log, Transaction), filter directly on blockNumber field
			filter = fmt.Sprintf("(filter: { blockNumber: { _in: %s } })", inFilterValue)
		}
		return beforeBrace + filter + afterBrace, nil
	}
}

