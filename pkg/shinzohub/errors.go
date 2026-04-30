package shinzohub

import "errors"

var ( //nolint:revive
	ErrTendermintConnection = errors.New("tenderMint connection failure") //nolint:revive
	ErrHTTPErrorResponse    = errors.New("HTTP error response")           //nolint:revive
	ErrViewHasNoQuery       = errors.New("view has no query")             //nolint:revive
)
