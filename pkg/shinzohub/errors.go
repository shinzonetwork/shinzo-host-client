package shinzohub

import "errors"

//nolint:revive // package-level error sentinels — names follow Err* convention
var (
	ErrTendermintConnection          = errors.New("tenderMint connection failure")
	ErrHTTPErrorResponse             = errors.New("HTTP error response")
	ErrViewHasNoQuery                = errors.New("view has no query")
	ErrLCDNotConfigured              = errors.New("LCD URL not configured")
	ErrEventNoContract               = errors.New("event has no contract_address")
	ErrLCDEmptyData                  = errors.New("LCD response carries empty data")
	ErrLCDHTTPStatus                 = errors.New("LCD returned non-OK HTTP status")
	errDecodeBundleInvalidWireFormat = errors.New("decode bundle: invalid wire format")
	errInvalidBalanceAmount          = errors.New("invalid balance amount")
)
