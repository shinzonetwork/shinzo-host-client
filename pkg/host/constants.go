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
	// peerResolutionTimeoutSecs is a const for resolution timeout in s.
	peerResolutionTimeoutSecs = 5
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
)
