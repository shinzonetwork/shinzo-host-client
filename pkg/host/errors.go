package host

import "errors"

var ( //nolint:revive
	ErrDefraNodeUnavailable = errors.New("DefraNode not available")                      //nolint:revive
	ErrViewManagerNil       = errors.New("ViewManager not initialized")                  //nolint:revive
	ErrViewMissingQuery     = errors.New("view missing query")                           //nolint:revive
	ErrViewMissingSDL       = errors.New("view missing SDL")                             //nolint:revive
	ErrDefraDBNotReady      = errors.New("DefraDB failed to become ready")               //nolint:revive
	ErrUnsupportedPlatform  = errors.New("unsupported platform")                         //nolint:revive
	ErrInvalidIPAddress     = errors.New("invalid IP address")                           //nolint:revive
	ErrNoPeerIDMismatchInfo = errors.New("error does not contain peer ID mismatch info") //nolint:revive
	ErrEmptyMerkleRoot      = errors.New("empty merkleRoot")

	errHubBaseURLMissing = errors.New("billing middleware enabled but the Shinzo hub base URL is not configured")
	errNoChainSchema     = errors.New("the built-in schema holds only Ethereum mainnet's tables")
	errNoGeneratorSchema = errors.New("no configured generator served a usable schema")
	errUnlistedChain     = errors.New("the store holds a chain the config does not list")
)
