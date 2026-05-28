package server

import "errors"

var (
	errPeerInfo             = errors.New("peer info error")
	errSigning              = errors.New("signing error")
	errSigningNotConfigured = errors.New("signing not configured")
	errSimulatedWrite       = errors.New("simulated write error")
)
