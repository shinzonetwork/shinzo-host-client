package host

import "errors"

// ErrDefraAlreadyStarted is returned by a second call to Defra.Start.
var ErrDefraAlreadyStarted = errors.New("host: defra already started")
