package pruner

import "errors"

var (
	// ErrInvalidDocIDFormat is returned when a docID string does not match the expected format.
	ErrInvalidDocIDFormat = errors.New("invalid docID format")
	// ErrInvalidUUID is returned when a string cannot be parsed as a valid UUID.
	ErrInvalidUUID = errors.New("invalid UUID")
)
