package view

import (
	"testing"

	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
)

func TestParseSDLFields(t *testing.T) {
	tests := []struct {
		name           string
		sdl            string
		expectedFields []string
		expectError    bool
	}{
		{
			name:           "simple type",
			sdl:            "type FilteredLog { address: String, topics: [String] }",
			expectedFields: []string{"address", "topics"},
			expectError:    false,
		},
		{
			name: "multiline type",
			sdl: `type TestView {
				name: String
				age: Int
				status: Boolean
			}`,
			expectedFields: []string{"name", "age", "status"},
			expectError:    false,
		},
		{
			name:           "empty SDL",
			sdl:            "",
			expectedFields: nil,
			expectError:    true,
		},
		{
			name:           "no type definition",
			sdl:            "just some random text",
			expectedFields: nil,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, err := parseSDLFields(tt.sdl)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			for _, expectedField := range tt.expectedFields {
				require.Contains(t, fields, expectedField)
			}
		})
	}
}

func TestTransformQueryCollectionNames(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "transform Log",
			input:    "Log { address topics }",
			expected: "Ethereum__Mainnet__Log { address topics }",
		},
		{
			name:     "transform Block",
			input:    "Block { hash number }",
			expected: "Ethereum__Mainnet__Block { hash number }",
		},
		{
			name:     "transform Transaction",
			input:    "Transaction { hash from to }",
			expected: "Ethereum__Mainnet__Transaction { hash from to }",
		},
		{
			name:     "already transformed",
			input:    "Ethereum__Mainnet__Log { address }",
			expected: "Ethereum__Mainnet__Log { address }",
		},
		{
			name:     "unknown collection",
			input:    "CustomCollection { field }",
			expected: "CustomCollection { field }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := transformQueryCollectionNames(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestView_HasLenses(t *testing.T) {
	tests := []struct {
		name     string
		view     View
		expected bool
	}{
		{
			name: "has lenses",
			view: View{
				Transform: models.Transform{
					Lenses: []models.Lens{{Path: "test.wasm"}},
				},
			},
			expected: true,
		},
		{
			name:     "empty transform",
			view:     View{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.view.HasLenses()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestView_NeedsWasmConversion(t *testing.T) {
	tests := []struct {
		name     string
		view     View
		expected bool
	}{
		{
			name: "base64 data needs conversion",
			view: View{
				Transform: models.Transform{
					Lenses: []models.Lens{{Path: "SGVsbG8gV29ybGQ="}},
				},
			},
			expected: true,
		},
		{
			name: "file URL no conversion needed",
			view: View{
				Transform: models.Transform{
					Lenses: []models.Lens{{Path: "file:///path/to/lens.wasm"}},
				},
			},
			expected: false,
		},
		{
			name: "http URL no conversion needed",
			view: View{
				Transform: models.Transform{
					Lenses: []models.Lens{{Path: "http://example.com/lens.wasm"}},
				},
			},
			expected: false,
		},
		{
			name: "https URL no conversion needed",
			view: View{
				Transform: models.Transform{
					Lenses: []models.Lens{{Path: "https://example.com/lens.wasm"}},
				},
			},
			expected: false,
		},
		{
			name: "mixed - one needs conversion",
			view: View{
				Transform: models.Transform{
					Lenses: []models.Lens{
						{Path: "file:///path/to/lens.wasm"},
						{Path: "base64data"},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.view.needsWasmConversion()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestView_GetUniqueViewName(t *testing.T) {
	v := View{Name: "TestView"}
	result := v.getUniqueViewName()
	require.Equal(t, "TestView", result)
}

func TestView_FilterDocumentFields(t *testing.T) {
	sdl := "type TestView { name: String, age: Int }"
	v := View{
		Name: "TestView",
		Sdl:  &sdl,
	}

	document := map[string]any{
		"name":       "Alice",
		"age":        25,
		"email":      "alice@example.com", // Should be filtered out
		"extraField": "value",             // Should be filtered out
	}

	filtered, err := v.filterDocumentFields(document)

	require.NoError(t, err)
	require.Contains(t, filtered, "name")
	require.Contains(t, filtered, "age")
	require.NotContains(t, filtered, "email")
	require.NotContains(t, filtered, "extraField")
	require.Equal(t, "Alice", filtered["name"])
	require.Equal(t, 25, filtered["age"])
}

func TestView_FilterDocumentFields_NoSDL(t *testing.T) {
	v := View{Name: "TestView"}

	document := map[string]any{
		"name":  "Alice",
		"age":   25,
		"email": "alice@example.com",
	}

	filtered, err := v.filterDocumentFields(document)

	require.NoError(t, err)
	// Should return document as-is when no SDL
	require.Equal(t, document, filtered)
}

func TestView_FilterDocumentFields_CaseInsensitive(t *testing.T) {
	sdl := "type TestView { Name: String }"
	v := View{
		Name: "TestView",
		Sdl:  &sdl,
	}

	document := map[string]any{
		"name": "Alice", // lowercase, but schema has uppercase
	}

	filtered, err := v.filterDocumentFields(document)

	require.NoError(t, err)
	// Should match case-insensitively and use the schema's case
	require.Contains(t, filtered, "Name")
	require.Equal(t, "Alice", filtered["Name"])
}

func TestView_BuildLensModules(t *testing.T) {
	v := View{
		Transform: models.Transform{
			Lenses: []models.Lens{
				{
					Label: "filter1",
					Path:  "file:///path1.wasm",
					Arguments: map[string]any{
						"key1": "value1",
					},
				},
				{
					Label: "filter2",
					Path:  "file:///path2.wasm",
					Arguments: map[string]any{
						"key2": "value2",
					},
				},
			},
		},
	}

	lens := v.buildLensModules()

	require.Len(t, lens.Lenses, 2)
	require.Equal(t, "file:///path1.wasm", lens.Lenses[0].Path)
	require.Equal(t, "file:///path2.wasm", lens.Lenses[1].Path)
	require.Equal(t, "value1", lens.Lenses[0].Arguments["key1"])
	require.Equal(t, "value2", lens.Lenses[1].Arguments["key2"])
	require.False(t, lens.Lenses[0].Inverse)
	require.False(t, lens.Lenses[1].Inverse)
}
