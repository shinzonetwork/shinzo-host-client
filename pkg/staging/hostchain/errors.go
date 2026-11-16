package hostchain

import "errors"

var (
	// ErrInvalidID means a network ID isn't <Name>__<Network>.
	ErrInvalidID = errors.New("network ID must be <Name>__<Network>, each part an upper-case letter followed by letters or digits")

	// ErrDuplicateID means two network IDs differ only in case.
	ErrDuplicateID = errors.New("network IDs must differ ignoring case")

	// ErrInvalidMaxBlocks means a chain's max_blocks is negative.
	ErrInvalidMaxBlocks = errors.New("max_blocks must not be negative")

	// ErrInvalidGeneratorURL means a generator's url isn't an http(s) URL with a host.
	ErrInvalidGeneratorURL = errors.New("generator url must be an http or https URL with a host")

	// ErrNoChains means no chains are configured.
	ErrNoChains = errors.New("no chains configured")
)
