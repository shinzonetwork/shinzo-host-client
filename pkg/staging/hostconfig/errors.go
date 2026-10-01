package hostconfig

import "errors"

var (
	errAlreadyExists = errors.New("config already exists")
	errInvalidLevel  = errors.New("invalid log level")
	errInvalidAddr   = errors.New("invalid http address")
)
