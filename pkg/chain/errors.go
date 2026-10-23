package chain

import "errors"

var ( //nolint:revive
	ErrInvalidPrefix       = errors.New("prefix must be <Name>__<Network>, each part a letter followed by letters or digits") //nolint:revive
	ErrDuplicatePrefix     = errors.New("prefixes must differ ignoring case")                                                 //nolint:revive
	ErrInvalidGeneratorURL = errors.New("generator URL must be an http or https URL with a host")                             //nolint:revive
)
