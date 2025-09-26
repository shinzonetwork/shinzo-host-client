package host

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/file"
	"github.com/shinzonetwork/app-sdk/pkg/views"
	"github.com/shinzonetwork/host/config"
	"github.com/shinzonetwork/host/pkg/shinzohub"
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
	require.Empty(t, host.HostedViews, "Host should start with no hosted views")

	// Create a test view
	sdl := "type TestView { id: String }"
	testView := views.View{
		Name: "TestView",
		Sdl:  &sdl,
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
	require.Len(t, host.HostedViews, 1, "Host should have one hosted view after receiving event")
	require.Equal(t, testView.Name, host.HostedViews[0].Name, "Hosted view should have correct name")
	require.Equal(t, testView.Sdl, host.HostedViews[0].Sdl, "Hosted view should have correct SDL")
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
	sdl1 := "type View1 { id: String }"
	sdl2 := "type View2 { id: String }"
	sdl3 := "type View3 { id: String }"
	events := []*shinzohub.ViewRegisteredEvent{
		{Key: "key1", Creator: "creator1", View: views.View{Name: "View1", Sdl: &sdl1}},
		{Key: "key2", Creator: "creator2", View: views.View{Name: "View2", Sdl: &sdl2}},
		{Key: "key3", Creator: "creator3", View: views.View{Name: "View3", Sdl: &sdl3}},
	}

	for _, event := range events {
		mockEventSub.SendEvent(event)
	}

	// Give the host time to process all events
	time.Sleep(200 * time.Millisecond)

	// Verify all events were processed
	require.Len(t, host.HostedViews, 3, "Host should have three hosted views")

	for i, expectedEvent := range events {
		require.Equal(t, expectedEvent.View.Name, host.HostedViews[i].Name)
		require.Equal(t, expectedEvent.View.Sdl, host.HostedViews[i].Sdl)
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
	require.Empty(t, host.HostedViews, "Host should not add views for unknown events")
}

func TestHostHandlesRealLensEventFromFile(t *testing.T) {
	mockEventSub := shinzohub.NewMockEventSubscription()

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

	// Read event json from file
	var rawJSON []byte
	path, err := file.FindFile("viewWithLensEvent.txt")
	require.NoError(t, err)
	rawJSON, err = os.ReadFile(path)
	require.NoError(t, err, "Failed to read viewWithLensEvent.txt from any of the attempted paths")

	// Send the raw JSON message as if it came from the WebSocket
	err = mockEventSub.SendRawJSONMessage(string(rawJSON))
	require.NoError(t, err, "Failed to process raw JSON message")

	// Wait a bit for the event to be processed
	time.Sleep(200 * time.Millisecond)

	// Verify the host processed the event
	require.Len(t, host.HostedViews, 1, "Host should have one hosted view after receiving event")

	hostedView := host.HostedViews[0]
	require.NotEmpty(t, hostedView.Name, "View should have a name extracted from SDL")
	require.NotNil(t, hostedView.Transform)
	require.NotNil(t, hostedView.Transform.Lenses)
	require.Len(t, hostedView.Transform.Lenses, 1)
	require.Equal(t, "filter", hostedView.Transform.Lenses[0].Label)
}
