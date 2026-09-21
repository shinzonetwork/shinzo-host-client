package hostconfig

import "errors"

var (
	errEmptyName   = errors.New("name must not be empty")
	errInvalidName = errors.New("invalid name: use only letters, digits, hyphens and underscores, starting with a letter or digit")
)
