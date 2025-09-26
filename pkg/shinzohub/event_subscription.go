package shinzohub

import "context"

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
