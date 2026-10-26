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
	// maxAttestationRetries is a const for the number of max attestation retries.
	maxAttestationRetries = 5
	// attestationRetryDelayMs is a const for the attestation retry delay in ms.
	attestationRetryDelayMs = 2
	// attestationBackoffBase is the base number of backoff for attestations.
	attestationBackoffBase = 10
	// identityTruncateLength is a const for the identity tracation.
	identityTruncateLength = 16
	// defaultTimeout is a const for the default timeout in s.
	defaultTimeout = 5 * time.Second
)

const (
	// docTypeBlock is the doc_type stored on block attestation records.
	docTypeBlock = "Block"

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

// defaultDefraURL is the DefraDB endpoint baked into DefaultConfig.
const defaultDefraURL = "localhost:9181"
