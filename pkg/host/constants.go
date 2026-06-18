package host

import "time"

const (
	// defaultMaxP2PRetries is a const for the max p2p retries.
	defaultMaxP2PRetries = 5
	// defaultRetryBaseDelayMs is a const for a base delay in ms.
	defaultRetryBaseDelayMs = 1000
	// defaultReconnectIntervalMs is a const for reconnect interval in ms.
	defaultReconnectIntervalMs = 60000
	// blockSignatureCacheSize is a const for blockSig cache size.
	blockSignatureCacheSize = 10000
	// openBrowserDelaySecs is a delay for opening the browser.
	openBrowserDelaySecs = 2
	// healthServerShutdownTimeoutSecs is a const for shutdown timeout in s.
	healthServerShutdownTimeoutSecs = 5
	// defaultMaxProcessingDepth is a const for max processing depth.
	defaultMaxProcessingDepth = 5
	// knownCollectionIDs is a const for known collection ids is a const for the number of known collection ids.
	knownCollectionIDs = 5
	// maxAttestationRetries is a const for the number of max attestation retries.
	maxAttestationRetries = 5
	// attestationRetryDelayMs is a const for the attestation retry delay in ms.
	attestationRetryDelayMs = 2
	// attestationBackoffBase is the base number of backoff for attestations.
	attestationBackoffBase = 10
	// identityTruncateLength is a const for the identity tracation.
	identityTruncateLength = 16
	// nanosecondsPerMillisecond is a const for the number of ns per ms.
	nanosecondsPerMillisecond = 1_000_000.0
	// defaultTimeout is a const for the default timeout in s.
	defaultTimeout = 5 * time.Second
	// shinzoHeightPollInterval is how often the host refreshes the cached ShinzoHub
	// height stamped onto block attestations. Epoch binning is coarse, so a few
	// seconds of staleness is acceptable and avoids a chain call per attestation.
	shinzoHeightPollInterval = 10 * time.Second
)

// Short document-type aliases. The provenance + processing pipelines
// recognise both the fully-qualified Ethereum__Mainnet__* form and the
// short suffix; both map to the same required-field list and replication
// behaviour.
const (
	docTypeTransaction     = "Transaction"
	docTypeBlock           = "Block"
	docTypeLog             = "Log"
	docTypeAccessListEntry = "AccessListEntry"

	// Lowercase forms used by replication filter as collection-type tags.
	colTypeTransaction     = "transaction"
	colTypeLog             = "log"
	colTypeAccessListEntry = "accessListEntry"
)

// Replication filter modes.
const (
	filterModeAllowlist = "allowlist"
	filterModeBlocklist = "blocklist"
)

// Provenance-tracking metadata field names stored on processed documents.
const (
	provenanceFieldProcessingDepth = "processing_depth"
	provenanceFieldIsViewOutput    = "is_view_output"
	provenanceFieldSourceColl      = "source_collection"
	provenanceFieldViewID          = "view_id"
	provenanceFieldProcessingChain = "processing_chain"
	provenanceFieldChainLength     = "chain_length"
	provenanceFieldOriginalDocID   = "original_doc_id"
)

// DefraDB JSON metadata field names referenced from the host's GraphQL
// query builders and tests.
const (
	defraFieldDocID   = "_docID"
	defraFieldVersion = "_version"
)

// defaultDefraURL is the DefraDB endpoint baked into DefaultConfig.
const defaultDefraURL = "localhost:9181"

// Common GraphQL field names referenced by the query builder and tests.
const (
	gqlFieldAddress     = "address"
	gqlFieldFrom        = "from"
	gqlFieldTo          = "to"
	gqlFieldHash        = "hash"
	gqlFieldBlockNumber = "blockNumber"
	gqlFieldNumber      = "number"
	gqlFieldTopics      = "topics"
	gqlFieldStorageKeys = "storageKeys"
)
