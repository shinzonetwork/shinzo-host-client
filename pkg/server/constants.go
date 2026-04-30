package server

import "time"

const (
	healthTimeoutMultiplier = 2
	defaultTimeout          = 5 * time.Second
	statusUnhealthy         = "unhealthy"
)
