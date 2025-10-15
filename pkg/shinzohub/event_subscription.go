package shinzohub

import (
	"context"
	"encoding/json"
	"fmt"
)

// RealEventSubscription is the real implementation that connects to Tendermint
type RealEventSubscription struct{}

func (r *RealEventSubscription) StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	return StartEventSubscription(tendermintURL)
}

// MockEventSubscription allows you to control what events are sent during testing
type MockEventSubscription struct {
	Events chan ShinzoEvent
}

func NewMockEventSubscription() *MockEventSubscription {
	return &MockEventSubscription{
		Events: make(chan ShinzoEvent, 10), // Buffered channel for testing
	}
}

func (m *MockEventSubscription) StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	cancel := func() {
		close(m.Events)
	}
	return cancel, m.Events, nil
}

// SendEvent allows you to send events to the mock during testing
func (m *MockEventSubscription) SendEvent(event ShinzoEvent) {
	m.Events <- event
}

// SendRawJSONMessage simulates receiving a raw JSON message from the WebSocket
// and processes it through the normal parsing logic
func (m *MockEventSubscription) SendRawJSONMessage(jsonMessage string) error {
	// Parse the JSON message using the same logic as the real WebSocket handler
	var msg RPCResponse
	if err := json.Unmarshal([]byte(jsonMessage), &msg); err != nil {
		return fmt.Errorf("failed to parse JSON message: %w", err)
	}

	// Extract Registered events using the same logic as the real handler
	registeredEvents := extractRegisteredEvents(msg)

	// Send each event to the channel
	for _, event := range registeredEvents {
		select {
		case m.Events <- &event:
			// Event sent successfully
		default:
			// Channel is full, skip this event
			fmt.Printf("Warning: Event channel is full, skipping event: %s\n", event.ToString())
		}
	}

	return nil
}
