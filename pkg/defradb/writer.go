package defradb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sourcenetwork/defradb/node"
)

func PostMutation[T any](ctx context.Context, defraNode *node.Node, query string) (*T, error) {
	if !strings.Contains(query, "mutation") {
		return nil, fmt.Errorf("Query must be a mutation, given: %s", query)
	}

	result := defraNode.DB.ExecRequest(ctx, query)
	gqlResult := result.GQL
	if gqlResult.Data == nil {
		return nil, fmt.Errorf("Encountered errors posting mutation: %v", gqlResult.Errors)
	}

	if len(gqlResult.Errors) > 0 {
		err := fmt.Errorf("Error posting mutation %s", query)
		for _, gqlError := range gqlResult.Errors {
			err = fmt.Errorf("%w: %w", err, gqlError)
		}
		return nil, err
	}

	// The GraphQL response data is a map[string]interface{} containing the mutation result
	// We need to find the first array in the data and extract the first element
	data, ok := gqlResult.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data format: %T", gqlResult.Data)
	}

	// Find the first array in the data (mutation results are typically arrays)
	for _, value := range data {

		// Try different array types
		if array, ok := value.([]interface{}); ok && len(array) > 0 {
			// Convert the first element to JSON and unmarshal into result
			firstElementBytes, err := json.Marshal(array[0])
			if err != nil {
				return nil, fmt.Errorf("failed to marshal first element: %w", err)
			}

			var result T
			err = json.Unmarshal(firstElementBytes, &result)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal result: %w", err)
			}

			return &result, nil
		}

		// Try []map[string]interface{} type
		if array, ok := value.([]map[string]interface{}); ok && len(array) > 0 {
			// Convert the first element to JSON and unmarshal into result
			firstElementBytes, err := json.Marshal(array[0])
			if err != nil {
				return nil, fmt.Errorf("failed to marshal first element: %w", err)
			}

			var result T
			err = json.Unmarshal(firstElementBytes, &result)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal result: %w", err)
			}

			return &result, nil
		}
	}

	return nil, fmt.Errorf("no array data found in mutation result")
}
