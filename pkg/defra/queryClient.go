package defra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
)

// QueryClient provides a clean interface for executing GraphQL queries against DefraDB
type QueryClient struct {
	defraURL string
	client   *http.Client
}

// NewQueryClient creates a new GraphQL query client for the given DefraDB URL
func NewQueryClient(defraURL string) (*QueryClient, error) {
	if defraURL == "" {
		return nil, fmt.Errorf("defraURL parameter is empty")
	}

	// Format URL to include GraphQL endpoint and normalize localhost
	graphqlURL := strings.Replace(fmt.Sprintf("%s/api/v0/graphql", defraURL), "127.0.0.1", "localhost", 1)

	return &QueryClient{
		defraURL: graphqlURL,
		client:   &http.Client{},
	}, nil
}

// NewQueryClientFromPort creates a new GraphQL query client using localhost and port
func NewQueryClientFromPort(port int) (*QueryClient, error) {
	return NewQueryClient(fmt.Sprintf("http://localhost:%d", port))
}

// Query executes a GraphQL query and returns the raw JSON response
func (c *QueryClient) Query(ctx context.Context, query string) (string, error) {
	if query == "" {
		return "", fmt.Errorf("query parameter is empty")
	}

	// Create request body
	requestBody := struct {
		Query string `json:"query"`
	}{
		Query: query,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.defraURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}
	err = c.CheckForErrors(string(respBody))
	if err != nil {
		return "", err
	}

	return string(respBody), nil
}

// QueryAndUnmarshal executes a GraphQL query and unmarshals the result into the provided interface
func (c *QueryClient) QueryAndUnmarshal(ctx context.Context, query string, result interface{}) error {
	response, err := c.Query(ctx, query)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(response), result)
}

// GetDataField extracts the "data" field from a GraphQL response
func (c *QueryClient) GetDataField(ctx context.Context, query string) (map[string]interface{}, error) {
	var response map[string]interface{}
	err := c.QueryAndUnmarshal(ctx, query, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	data, ok := response["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data field not found in response: %v", response)
	}

	return data, nil
}

// CheckForErrors checks if the GraphQL response contains any errors
func (c *QueryClient) CheckForErrors(response string) error {
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if errors, exists := result["errors"]; exists {
		return fmt.Errorf("graphql errors: %v", errors)
	}

	return nil
}

// QueryInto executes a GraphQL query and unmarshals the result into a struct of the specified type
// The result parameter should be a pointer to a struct that matches the expected GraphQL response structure
func (c *QueryClient) QueryInto(ctx context.Context, query string, result interface{}) error {
	response, err := c.Query(ctx, query)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(response), result)
}

// QueryDataInto executes a GraphQL query and unmarshals only the "data" field into a struct
// This function handles both single objects and arrays in the response
func (c *QueryClient) QueryDataInto(ctx context.Context, query string, result interface{}) error {
	var response map[string]interface{}
	err := c.QueryAndUnmarshal(ctx, query, &response)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	data, ok := response["data"]
	if !ok {
		return fmt.Errorf("data field not found in response: %v", response)
	}

	// Check if result is expecting a slice (array) or single object
	resultValue := reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Ptr {
		return fmt.Errorf("result must be a pointer")
	}

	resultElem := resultValue.Elem()

	// If result is a slice, find the first array in data and unmarshal it
	if resultElem.Kind() == reflect.Slice {
		if dataMap, ok := data.(map[string]interface{}); ok {
			for _, value := range dataMap {
				if array, ok := value.([]interface{}); ok {
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
	if dataMap, ok := data.(map[string]interface{}); ok {
		for _, value := range dataMap {
			if array, ok := value.([]interface{}); ok && len(array) > 0 {
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

// QuerySingle executes a GraphQL query and returns a single item of the specified type
// This is useful when you expect a single object back (not an array)
func QuerySingle[T any](c *QueryClient, ctx context.Context, query string) (T, error) {
	var result T
	err := c.QueryDataInto(ctx, query, &result)
	return result, err
}

// QueryArray executes a GraphQL query and returns an array of the specified type
// This is useful when you expect an array of objects back
func QueryArray[T any](c *QueryClient, ctx context.Context, query string) ([]T, error) {
	var result []T
	err := c.QueryDataInto(ctx, query, &result)
	return result, err
}
