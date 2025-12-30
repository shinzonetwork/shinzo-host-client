package shinzohub

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/shinzonetwork/view-creator/core/models"
	"github.com/stretchr/testify/require"
)

func TestExtractNameFromSDL(t *testing.T) {
	tests := []struct {
		name     string
		sdl      string
		expected string
	}{
		{
			name:     "SDL with directive",
			sdl:      "type FilteredAndDecodedLogs @materialized(if: false) {transactionHash: String}",
			expected: "FilteredAndDecodedLogs",
		},
		{
			name:     "SDL without directive",
			sdl:      "type MyView { field: String }",
			expected: "MyView",
		},
		{
			name:     "SDL with multiple spaces",
			sdl:      "type   UserProfile   @index(unique: true) { name: String }",
			expected: "UserProfile",
		},
		{
			name:     "SDL with underscore in name",
			sdl:      "type user_profile { id: String }",
			expected: "user_profile",
		},
		{
			name:     "SDL with numbers in name",
			sdl:      "type View123 { data: String }",
			expected: "View123",
		},
		{
			name:     "Empty SDL",
			sdl:      "",
			expected: "",
		},
		{
			name:     "Invalid SDL format",
			sdl:      "not a type definition",
			expected: "",
		},
		{
			name:     "SDL with complex directive",
			sdl:      "type ComplexView @materialized(if: true) @index(unique: true) { id: String }",
			expected: "ComplexView",
		},
		{
			name:     "SDL with newlines and formatting",
			sdl:      "type\n  MultiLineView\n  @materialized(if: false)\n  {\n    field: String\n  }",
			expected: "MultiLineView",
		},
		{
			name:     "SDL with tabs",
			sdl:      "type\tTabbedView\t@index(unique: true)\t{\tname: String\t}",
			expected: "TabbedView",
		},
		{
			name:     "SDL with mixed case",
			sdl:      "type MixedCaseView { field: String }",
			expected: "MixedCaseView",
		},
		{
			name:     "SDL with special characters in name",
			sdl:      "type View$Special { field: String }",
			expected: "", // $ is not a word character, so regex won't match
		},
		{
			name:     "SDL with only type keyword",
			sdl:      "type",
			expected: "",
		},
		{
			name:     "SDL with type but no name",
			sdl:      "type { field: String }",
			expected: "",
		},
		{
			name:     "SDL with type and name but no braces",
			sdl:      "type MyView",
			expected: "",
		},
		{
			name:     "SDL with multiple type definitions",
			sdl:      "type FirstView { field: String } type SecondView { field: String }",
			expected: "FirstView",
		},
		{
			name:     "SDL with interface instead of type",
			sdl:      "interface MyInterface { field: String }",
			expected: "",
		},
		{
			name:     "SDL with comment before type",
			sdl:      "// This is a comment\ntype CommentedView { field: String }",
			expected: "CommentedView",
		},
		{
			name:     "SDL with whitespace only",
			sdl:      "   \t\n   ",
			expected: "",
		},
		{
			name:     "SDL with type and directive but no braces",
			sdl:      "type IncompleteView @materialized(if: false)",
			expected: "IncompleteView",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := &view.View{Sdl: &tt.sdl}
			ExtractNameFromSDL(view)

			if view.Name != tt.expected {
				t.Errorf("ExtractNameFromSDL() = %v, want %v", view.Name, tt.expected)
			}
		})
	}
}

func TestSubscribeToView(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "FilteredAndDecodedLogs",
	}

	myDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	err = testView.SubscribeTo(context.Background(), myDefra)
	require.NoError(t, err)
}

func TestSubscribeToInvalidViewFails(t *testing.T) {
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs @materialized(if: false) {transactionHash: String}"
	testView := view.View{
		Query:     &query,
		Sdl:       &sdl,
		Transform: models.Transform{},
		Name:      "FilteredAndDecodedLogs",
	}

	myDefra, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	err = testView.SubscribeTo(context.Background(), myDefra)
	require.Error(t, err)
}
