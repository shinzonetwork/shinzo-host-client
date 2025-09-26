package host

import (
	"context"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/views"
	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/shinzohub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostHandlesViewRegisteredEvents(t *testing.T) {
	// Create a mock event subscription
	mockEventSub := shinzohub.NewMockEventSubscription()

	// Create test config with a dummy websocket URL
	testConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url", // Dummy URL to trigger event subscription
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
	}
	testConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	// Start hosting with the mock
	host, err := StartHostingWithEventSubscription(testConfig, mockEventSub)
	require.NoError(t, err)
	defer host.Close(context.Background())

	// Verify initial state
	assert.Empty(t, host.HostedViews, "Host should start with no hosted views")

	// Create a test view
	testView := views.View{
		Name: "TestView",
		Sdl:  "type TestView { id: String }",
	}

	// Create a ViewRegisteredEvent
	registeredEvent := &shinzohub.ViewRegisteredEvent{
		Key:     "test-key-123",
		Creator: "test-creator",
		View:    testView,
	}

	// Send the event through the mock
	mockEventSub.SendEvent(registeredEvent)

	// Give the host time to process the event
	time.Sleep(100 * time.Millisecond)

	// Verify the host received and processed the event
	assert.Len(t, host.HostedViews, 1, "Host should have one hosted view after receiving event")
	assert.Equal(t, testView.Name, host.HostedViews[0].Name, "Hosted view should have correct name")
	assert.Equal(t, testView.Sdl, host.HostedViews[0].Sdl, "Hosted view should have correct SDL")
}

func TestHostHandlesMultipleViewRegisteredEvents(t *testing.T) {
	// Create a mock event subscription
	mockEventSub := shinzohub.NewMockEventSubscription()

	// Create test config
	testConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url",
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
	}
	testConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	// Start hosting with the mock
	host, err := StartHostingWithEventSubscription(testConfig, mockEventSub)
	require.NoError(t, err)
	defer host.Close(context.Background())

	// Send multiple events
	events := []*shinzohub.ViewRegisteredEvent{
		{Key: "key1", Creator: "creator1", View: views.View{Name: "View1", Sdl: "type View1 { id: String }"}},
		{Key: "key2", Creator: "creator2", View: views.View{Name: "View2", Sdl: "type View2 { id: String }"}},
		{Key: "key3", Creator: "creator3", View: views.View{Name: "View3", Sdl: "type View3 { id: String }"}},
	}

	for _, event := range events {
		mockEventSub.SendEvent(event)
	}

	// Give the host time to process all events
	time.Sleep(200 * time.Millisecond)

	// Verify all events were processed
	assert.Len(t, host.HostedViews, 3, "Host should have three hosted views")

	for i, expectedEvent := range events {
		assert.Equal(t, expectedEvent.View.Name, host.HostedViews[i].Name)
		assert.Equal(t, expectedEvent.View.Sdl, host.HostedViews[i].Sdl)
	}
}

// UnknownEvent is a mock event type for testing
type UnknownEvent struct{}

func (u UnknownEvent) ToString() string { return "unknown" }

func TestHostHandlesUnknownEvents(t *testing.T) {
	// Create a mock event subscription
	mockEventSub := shinzohub.NewMockEventSubscription()

	// Create test config
	testConfig := &config.Config{
		Shinzo: config.ShinzoConfig{
			MinimumAttestations: 1,
			WebSocketUrl:        "ws://dummy-url",
		},
		ShinzoAppConfig: DefaultConfig.ShinzoAppConfig,
	}
	testConfig.ShinzoAppConfig.DefraDB.Store.Path = t.TempDir()
	testConfig.ShinzoAppConfig.DefraDB.Url = "127.0.0.1:0"

	// Start hosting with the mock
	host, err := StartHostingWithEventSubscription(testConfig, mockEventSub)
	require.NoError(t, err)
	defer host.Close(context.Background())

	// Send unknown event
	mockEventSub.SendEvent(&UnknownEvent{})

	// Give the host time to process the event
	time.Sleep(100 * time.Millisecond)

	// Verify the host didn't add any views
	assert.Empty(t, host.HostedViews, "Host should not add views for unknown events")
}
