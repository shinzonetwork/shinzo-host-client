package defradb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/sourcenetwork/defradb/node"
)

// queryClient provides a clean interface for executing GraphQL queries against DefraDB using the direct client.
type queryClient struct {
	defraNode *node.Node
}

// newQueryClient creates a new GraphQL query client using the Defra node directly.
func newQueryClient(defraNode *node.Node) (*queryClient, error) {
	if defraNode == nil {
		return nil, ErrDefraNodeNil
	}

	return &queryClient{
		defraNode: defraNode,
	}, nil
}

// query executes a GraphQL query using the Defra client directly and returns the raw result.
func (c *queryClient) query(ctx context.Context, query string) (any, error) {
	if query == "" {
		return nil, ErrQueryEmpty
	}

	result := c.defraNode.DB.ExecRequest(ctx, query)
	gqlResult := result.GQL

	if len(gqlResult.Errors) > 0 {
		return nil, ErrGraphQLErrors
	}

	return gqlResult.Data, nil
}

// queryAndUnmarshal executes a GraphQL query and unmarshals the result into the provided interface.
func (c *queryClient) queryAndUnmarshal(ctx context.Context, query string, result any) error { //nolint:unused
	data, err := c.query(ctx, query)
	if err != nil {
		return err
	}

	// Convert the data to JSON and then unmarshal into the result
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return ErrUnexpectedDataFormat
	}

	return json.Unmarshal(dataBytes, result)
}

// getDataField extracts the data from a GraphQL response
// For the Defra client, the data is returned directly, not wrapped in a "data" field.
func (c *queryClient) getDataField(ctx context.Context, query string) (map[string]any, error) { //nolint:unused
	data, err := c.query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	dataMap, ok := data.(map[string]any)
	if !ok {
		return nil, ErrUnexpectedDataFormat
	}

	return dataMap, nil
}

// queryInto executes a GraphQL query and unmarshals the result into a struct of the specified type.
func (c *queryClient) queryInto(ctx context.Context, query string, result any) error { //nolint:unused
	data, err := c.query(ctx, query)
	if err != nil {
		return err
	}

	// Convert the data to JSON and then unmarshal into the result
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return json.Unmarshal(dataBytes, result)
}

// queryDataInto executes a GraphQL query and unmarshals only the "data" field into a struct
// This function handles both single objects and arrays in the response.
func (c *queryClient) queryDataInto(ctx context.Context, query string, result any) error { //nolint:unused
	data, err := c.query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	// Check if result is expecting a slice (array) or single object
	resultValue := reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Pointer {
		return ErrResultNotPointer
	}

	resultElem := resultValue.Elem()

	// If result is a slice, find the first array in data and unmarshal it
	if resultElem.Kind() == reflect.Slice {
		if dataMap, ok := data.(map[string]any); ok {
			for _, value := range dataMap {
				// Try different array types
				if array, ok := value.([]any); ok {
					// Convert the array to JSON and unmarshal into result
					arrayBytes, err := json.Marshal(array)
					if err != nil {
						return fmt.Errorf("failed to marshal array: %w", err)
					}
					return json.Unmarshal(arrayBytes, result)
				}

				// Try []map[string]interface{} type
				if array, ok := value.([]map[string]any); ok {
					// Convert the array to JSON and unmarshal into result
					arrayBytes, err := json.Marshal(array)
					if err != nil {
						return fmt.Errorf("failed to marshal array: %w", err)
					}
					return json.Unmarshal(arrayBytes, result)
				}
			}
		}
		// Fallback: try to unmarshal the entire data object
		dataBytes, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal data: %w", err)
		}
		return json.Unmarshal(dataBytes, result)
	}

	// If result is a single struct, find the first array in data and get its first element
	if dataMap, ok := data.(map[string]any); ok {
		for _, value := range dataMap {
			// Try different array types
			if array, ok := value.([]any); ok && len(array) > 0 {
				// Convert the first element to JSON and unmarshal into result
				firstElementBytes, err := json.Marshal(array[0])
				if err != nil {
					return fmt.Errorf("failed to marshal first element: %w", err)
				}
				return json.Unmarshal(firstElementBytes, result)
			}

			// Try []map[string]interface{} type
			if array, ok := value.([]map[string]any); ok && len(array) > 0 {
				// Convert the first element to JSON and unmarshal into result
				firstElementBytes, err := json.Marshal(array[0])
				if err != nil {
					return fmt.Errorf("failed to marshal first element: %w", err)
				}
				return json.Unmarshal(firstElementBytes, result)
			}
		}
	}

	// Fallback: try to unmarshal the entire data object
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	return json.Unmarshal(dataBytes, result)
}

// wrapQueryIfNeeded automatically wraps a query with "query { }" if it doesn't already start with "query", "mutation", or "subscription".
func wrapQueryIfNeeded(query string) string {
	// Trim whitespace to check the actual start
	trimmed := strings.TrimSpace(query)

	// Check if query already starts with GraphQL operation keywords (case insensitive)
	lowerTrimmed := strings.ToLower(trimmed)

	if strings.HasPrefix(lowerTrimmed, "query ") || lowerTrimmed == "query" {
		return query // Return original query as-is
	}

	if strings.HasPrefix(lowerTrimmed, "mutation ") || lowerTrimmed == "mutation" {
		return query // Return original query as-is
	}

	if strings.HasPrefix(lowerTrimmed, "subscription ") || lowerTrimmed == "subscription" {
		return query // Return original query as-is
	}

	// Check if query is already wrapped in curly braces but doesn't start with a keyword
	// This handles cases like "{ Block { __typename } }" which should be wrapped as "query { Block { __typename } }"
	if len(trimmed) >= 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}' {
		// Extract the content inside the braces
		innerContent := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		// Wrap with "query" keyword
		return fmt.Sprintf("query { %s }", innerContent)
	}

	// Wrap the query with "query { }"
	return fmt.Sprintf("query { %s }", strings.TrimSpace(query))
}

// QuerySingle executes a GraphQL query and returns a single item of the specified type
// This is useful when you expect a single object back (not an array).
func QuerySingle[T any](ctx context.Context, defraNode *node.Node, query string) (T, error) {
	var result T
	client, err := newQueryClient(defraNode)
	if err != nil {
		return result, err
	}

	// Auto-wrap query if it doesn't start with "query"
	wrappedQuery := wrapQueryIfNeeded(query)
	err = client.queryDataInto(ctx, wrappedQuery, &result)
	return result, err
}

// QueryArray executes a GraphQL query and returns an array of the specified type
// This is useful when you expect an array of objects back.
func QueryArray[T any](ctx context.Context, defraNode *node.Node, query string) ([]T, error) {
	var result []T
	client, err := newQueryClient(defraNode)
	if err != nil {
		return result, err
	}

	// Auto-wrap query if it doesn't start with "query"
	wrappedQuery := wrapQueryIfNeeded(query)
	err = client.queryDataInto(ctx, wrappedQuery, &result)
	return result, err
}
