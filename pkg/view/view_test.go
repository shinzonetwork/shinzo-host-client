package view

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/viewbundle-go"
	"github.com/stretchr/testify/require"
)

// TestView_SubscribeTo tests basic view subscription functionality.
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
	defer func() { _ = defraNode.Close(ctx) }()

	// SubscribeTo should fail because the collection doesn't exist yet
	err = testView.SubscribeTo(ctx, defraNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "collection does not exist")
}

// TestView_HasLenses tests lens detection.
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

// TestView_NeedsWasmConversion tests WASM conversion detection.
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

// TestView_ExtractNameFromSDL tests SDL name extraction.
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

// TestAttemptQueryCorrection_WholeWordReplacementOnly makes sure the
// corrector doesn't mangle identifiers that merely contain the incorrect
// field as a substring. A plain strings.ReplaceAll would have.
func TestAttemptQueryCorrection_WholeWordReplacementOnly(t *testing.T) {
	cases := []struct {
		name     string
		query    string
		errMsg   string
		expected string
	}{
		{
			name:     "single occurrence gets corrected",
			query:    "Log { address }",
			errMsg:   `Cannot query field "Log" on type "Query". Did you mean "Ethereum__Mainnet__Log"`,
			expected: "Ethereum__Mainnet__Log { address }",
		},
		{
			// "Log" as a substring of "Logger" must not be replaced.
			name:     "substring in a different identifier is untouched",
			query:    "Log { Logger address }",
			errMsg:   `Cannot query field "Log" on type "Query". Did you mean "Ethereum__Mainnet__Log"`,
			expected: "Ethereum__Mainnet__Log { Logger address }",
		},
		{
			// "inputData" fix must not clip "inputDataLength".
			name:     "substring suffix is untouched",
			query:    "Log { inputData inputDataLength }",
			errMsg:   `Cannot query field "inputData" on type "Log". Did you mean "input"`,
			expected: "Log { input inputDataLength }",
		},
		{
			// A genuine repeat of the same identifier should all be fixed.
			name:     "multiple whole-word occurrences all get corrected",
			query:    "Log { nested { Log } }",
			errMsg:   `Cannot query field "Log" on type "Query". Did you mean "Ethereum__Mainnet__Log"`,
			expected: "Ethereum__Mainnet__Log { nested { Ethereum__Mainnet__Log } }",
		},
		{
			// Error string that doesn't match the expected pattern
			// leaves the query untouched.
			name:     "unparseable error leaves query alone",
			query:    "Log { address }",
			errMsg:   "some unrelated error",
			expected: "Log { address }",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			v := &View{Data: viewbundle.View{Query: tt.query}}
			got := v.attemptQueryCorrection(tt.errMsg)
			require.Equal(t, tt.expected, got)
		})
	}
}
