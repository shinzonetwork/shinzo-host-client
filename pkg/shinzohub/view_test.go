package shinzohub

import (
	"context"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/host/pkg/view"
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
