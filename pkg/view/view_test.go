package view

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// TestView_SubscribeTo tests basic view subscription functionality
func TestView_SubscribeTo(t *testing.T) {
	ctx := context.Background()

	// Create a test view
	query := "Log {address topics data transactionHash blockNumber}"
	sdl := "type FilteredAndDecodedLogs {transactionHash: String}"
	testView := View{
		Name: "FilteredAndDecodedLogs",
		Data: viewbundle.View{
			Query: query,
			Sdl:   sdl,
		},
	}

	// Create a mock DefraDB node
	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, &defra.MockSchemaApplierThatSucceeds{})
	require.NoError(t, err)
	defer defraNode.Close(ctx)

	// SubscribeTo should fail because the collection doesn't exist yet
	err = testView.SubscribeTo(ctx, defraNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "collection does not exist")
}

// TestView_HasLenses tests lens detection
func TestView_HasLenses(t *testing.T) {
	tests := []struct {
		name     string
		view     View
		expected bool
	}{
		{
			name: "has lenses",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{{Path: "filter_transaction.wasm"}},
					},
				},
			},
			expected: true,
		},
		{
			name:     "empty transform",
			view:     View{},
			expected: false,
		},
		{
			name: "no lenses",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{},
					},
				},
			},
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

// TestView_NeedsWasmConversion tests WASM conversion detection
func TestView_NeedsWasmConversion(t *testing.T) {
	tests := []struct {
		name     string
		view     View
		expected bool
	}{
		{
			name: "base64 data needs conversion",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{{Path: "SGVsbG8gV29ybGQ="}},
					},
				},
			},
			expected: true,
		},
		{
			name: "file URL no conversion needed",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{{Path: "file://filter_transaction.wasm"}},
					},
				},
			},
			expected: false,
		},
		{
			name: "http URL no conversion needed",
			view: View{
				Data: viewbundle.View{
					Transform: viewbundle.Transform{
						Lenses: []viewbundle.Lens{{Path: "http://example.com/lens.wasm"}},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.view.needsWasmConversion()
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestView_ExtractNameFromSDL tests SDL name extraction
func TestView_ExtractNameFromSDL(t *testing.T) {
	tests := []struct {
		name     string
		sdl      string
		expected string
	}{
		{
			name:     "SDL with @materialized",
			sdl:      "type LogView @materialized(if: false) { address: String }",
			expected: "LogView",
		},
		{
			name:     "SDL with @index",
			sdl:      "type TestView @index(unique: [\"address\"]) { address: String }",
			expected: "TestView",
		},
		{
			name:     "simple SDL",
			sdl:      "type SimpleView { name: String }",
			expected: "SimpleView",
		},
		{
			name:     "empty SDL",
			sdl:      "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := &View{
				Data: viewbundle.View{
					Sdl: tt.sdl,
				},
			}

			view.ExtractNameFromSDL()
			require.Equal(t, tt.expected, view.Name)
		})
	}
}
