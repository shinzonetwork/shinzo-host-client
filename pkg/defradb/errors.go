package defradb

import "errors"

var (
	// defra.go.

	// ErrEmptyConfig is returned when a nil config is provided.
	ErrEmptyConfig = errors.New("config cannot be nil")
	// ErrNilKeyringSecret is returned when no keyring secret is provided for keyring-based key management.
	ErrNilKeyringSecret = errors.New("keyringSecret is required for keyring-based key management")
	// ErrIdentityNotFull is returned when an identity does not implement FullIdentity and a private key is required.
	ErrIdentityNotFull = errors.New("identity is not a FullIdentity, cannot extract private key")
	// ErrPrivateKeyFailure is returned when extracting a private key from an identity fails.
	ErrPrivateKeyFailure = errors.New("failed to get private key from identity")
	// ErrPrivateKeyNoBytes is returned when a private key yields no raw bytes.
	ErrPrivateKeyNoBytes = errors.New("private key has no raw bytes")
	// ErrKeyLengthError is returned when a secp256k1 key is not exactly 32 bytes.
	ErrKeyLengthError = errors.New("expected 32-byte secp256k1 key")
	// ErrNilSubscriptionChannel is returned when a subscription channel is nil.
	ErrNilSubscriptionChannel = errors.New("subscription channel is nil")
	// ErrClientStarted is returned when attempting to start a client that is already running.
	ErrClientStarted = errors.New("client already started")
	// ErrClientMustBeStarted is returned when an operation requires the client to be started first.
	ErrClientMustBeStarted = errors.New("client must be started before applying schema")
	// ErrEmptySchema is returned when an empty schema string is provided.
	ErrEmptySchema = errors.New("schema cannot be empty")
	// ErrNoPeerFound is returned when a requested peer cannot be located.
	ErrNoPeerFound = errors.New("peer not found")
	// ErrNoP2PMesh is returned when all active peers have been lost and the P2P mesh is down.
	ErrNoP2PMesh = errors.New("P2P mesh lost - no active peers")
	// ErrPeerDisconnected is returned when a peer unexpectedly disconnects.
	ErrPeerDisconnected = errors.New("peer disconnected")
	// ErrPeerAlreadyAdded is returned when attempting to add a peer that already exists.
	ErrPeerAlreadyAdded = errors.New("peer already exists")
	// ErrInvalidBootstrapPeer is returned when a bootstrap peer entry is malformed or unusable.
	ErrInvalidBootstrapPeer = errors.New("bootstrap peer is invalid and will be skipped")
	// ErrPeerEmptyID is returned when a bootstrap peer has no peer ID.
	ErrPeerEmptyID = errors.New("peer has empty ID and will be skipped")
	// ErrPeerNoAddresses is returned when a bootstrap peer has no addresses to dial.
	ErrPeerNoAddresses = errors.New("peer has no addresses and will be skipped")

	// query.go.

	// ErrDefraNodeNil is returned when a nil defraNode is passed to a query function.
	ErrDefraNodeNil = errors.New("defraNode parameter cannot be nil")
	// ErrQueryEmpty is returned when an empty query string is provided.
	ErrQueryEmpty = errors.New("query parameter is empty")
	// ErrGraphQLErrors is returned when the GraphQL response contains one or more errors.
	ErrGraphQLErrors = errors.New("graphql errors")
	// ErrUnexpectedDataFormat is returned when the response data is not in the expected format.
	ErrUnexpectedDataFormat = errors.New("unexpected data format")
	// ErrResultNotPointer is returned when the result parameter is not a pointer.
	ErrResultNotPointer = errors.New("result must be a pointer")

	// writer.go.

	// ErrNotMutation is returned when a non-mutation query is passed to a mutation writer.
	ErrNotMutation = errors.New("query must be a mutation")
	// ErrMutationErrors is returned when the GraphQL mutation response contains errors.
	ErrMutationErrors = errors.New("encountered errors posting mutation")
	// ErrPostingMutation is returned when posting a GraphQL mutation fails.
	ErrPostingMutation = errors.New("error posting mutation")
	// ErrNoArrayData is returned when a mutation result contains no array data.
	ErrNoArrayData = errors.New("no array data found in mutation result")
)
