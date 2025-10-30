package shinzohub

import (
	"context"
)

// RealEventSubscription is the real implementation that connects to Tendermint
type RealEventSubscription struct{} // Implements EventSubscription interface

func (r *RealEventSubscription) StartEventSubscription(tendermintURL string) (context.CancelFunc, <-chan ShinzoEvent, error) {
	return StartEventSubscription(tendermintURL)
}
