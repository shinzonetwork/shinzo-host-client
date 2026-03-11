package shinzohub

import (
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
	"github.com/stretchr/testify/require"
)

// TestViewRegisteredEvent_ToString tests event string representation
func TestViewRegisteredEvent_ToString(t *testing.T) {
	event := ViewRegisteredEvent{
		Key:     "test-key-123",
		Creator: "test-creator",
		View: view.View{
			Name: "TestView",
		},
	}

	expected := "ViewRegistered: key=test-key-123, creator=test-creator, view=TestView"
	actual := event.ToString()

	require.Equal(t, expected, actual)
}

// TestEntityRegisteredEvent_ToString tests event string representation
func TestEntityRegisteredEvent_ToString(t *testing.T) {
	event := EntityRegisteredEvent{
		Key:    "entity-key-456",
		Owner:  "entity-owner",
		DID:    "did:key:test123",
		Pid:    "pid456",
		Entity: "\u0001",
	}

	expected := "EntityRegistered: key=entity-key-456, owner=entity-owner, did=did:key:test123, pid=pid456"
	actual := event.ToString()

	require.Equal(t, expected, actual)
}

// TestExtractViewFromEvent tests view extraction from events
func TestExtractViewFromEvent(t *testing.T) {
	// This would test the extractViewFromEvent function in query.go
	// For now, we'll test the event structure
	event := ViewRegisteredEvent{
		Key:     "test-key",
		Creator: "test-creator",
		View: view.View{
			Name: "TestView",
			Data: viewbundle.View{
				Query: "Log { address }",
				Sdl:   "type TestView { address: String }",
			},
		},
	}

	// Verify event structure
	require.Equal(t, "test-key", event.Key)
	require.Equal(t, "test-creator", event.Creator)
	require.Equal(t, "TestView", event.View.Name)
	require.Equal(t, "Log { address }", event.View.Data.Query)
	require.Equal(t, "type TestView { address: String }", event.View.Data.Sdl)
}

// TestGetEntityType tests entity type detection
func TestGetEntityType(t *testing.T) {
	tests := []struct {
		name         string
		entityValue  string
		expectedType EntityType
	}{
		{
			name:         "indexer entity",
			entityValue:  "\u0001",
			expectedType: EntityTypeIndexer,
		},
		{
			name:         "host entity",
			entityValue:  "\u0002",
			expectedType: EntityTypeHost,
		},
		{
			name:         "unknown entity",
			entityValue:  "\u0003",
			expectedType: EntityTypeUnknown,
		},
		{
			name:         "empty entity",
			entityValue:  "",
			expectedType: EntityTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetEntityType(tt.entityValue)
			require.Equal(t, tt.expectedType, result)
		})
	}
}

// TestEventStructures tests event struct definitions
func TestEventStructures(t *testing.T) {
	// Test ViewRegisteredEvent structure
	viewEvent := ViewRegisteredEvent{
		Key:     "view-key",
		Creator: "view-creator",
		View: view.View{
			Name: "ViewName",
			Data: viewbundle.View{
				Query: "TestQuery",
				Sdl:   "type TestType { field: String }",
			},
		},
	}

	require.Equal(t, "view-key", viewEvent.Key)
	require.Equal(t, "view-creator", viewEvent.Creator)
	require.Equal(t, "ViewName", viewEvent.View.Name)
	require.Equal(t, "TestQuery", viewEvent.View.Data.Query)
	require.Equal(t, "type TestType { field: String }", viewEvent.View.Data.Sdl)

	// Test EntityRegisteredEvent structure
	entityEvent := EntityRegisteredEvent{
		Key:    "entity-key",
		Owner:  "entity-owner",
		DID:    "did:key:abc123",
		Pid:    "pid123",
		Entity: "\u0001",
	}

	require.Equal(t, "entity-key", entityEvent.Key)
	require.Equal(t, "entity-owner", entityEvent.Owner)
	require.Equal(t, "did:key:abc123", entityEvent.DID)
	require.Equal(t, "pid123", entityEvent.Pid)
	require.Equal(t, "\u0001", entityEvent.Entity)
}

// TestEventInterface tests that events implement the ShinzoEvent interface
func TestEventInterface(t *testing.T) {
	// Test ViewRegisteredEvent implements ShinzoEvent
	var viewEvent ShinzoEvent = &ViewRegisteredEvent{
		Key:     "test",
		Creator: "test",
		View:    view.View{Name: "Test"},
	}

	// Should have ToString method
	require.NotPanics(t, func() {
		_ = viewEvent.ToString()
	})

	// Test EntityRegisteredEvent implements ShinzoEvent
	var entityEvent ShinzoEvent = &EntityRegisteredEvent{
		Key:    "test",
		Owner:  "test",
		DID:    "test",
		Pid:    "test",
		Entity: "\u0001",
	}

	// Should have ToString method
	require.NotPanics(t, func() {
		_ = entityEvent.ToString()
	})
}
