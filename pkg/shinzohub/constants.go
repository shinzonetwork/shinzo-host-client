package shinzohub

import "time"

const (
	pingIntervalSecs = 15
	pingMessageID    = 999
	defaultPageSize  = 5
	defaultTimeout   = 5 * time.Second
)

// CometBFT WebSocket envelope and event-attribute identifiers used by the
// host's registry subscriber. Exposed so tests can construct identical
// fixtures without re-duplicating the literals.
const (
	jsonRPCMsgKey  = "jsonrpc"
	jsonRPCVersion = "2.0"
	tmEventTxType  = "tendermint/event/Tx"

	eventTypeViewRegistered           = "view.view_registered"
	eventTypeViewRegistrationFailed   = "view.view_registration_failed"
	eventTypeViewRegistrationTimedOut = "view.view_registration_timed_out"

	attrViewID          = "view_id"
	attrContractAddress = "contract_address"
	attrCreator         = "creator"
	attrError           = "error"
)

// LCD response JSON field names (mirrors the JSON tags on LCDView and the
// pagination wrap). Tests build response fixtures with these keys.
const (
	lcdFieldView       = "view"
	lcdFieldViews      = "views"
	lcdFieldPagination = "pagination"
	lcdFieldNextKey    = "next_key"
)
