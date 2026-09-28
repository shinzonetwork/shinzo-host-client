package defradb

import "time"

const (
	// MaxRetries is the maximum number of retry attempts before giving up.
	MaxRetries = 5
	// RetryBaseDelayMs is the base delay in milliseconds between retry attempts.
	RetryBaseDelayMs = 1000
	// ReconnectIntervalsMs is the interval in milliseconds between reconnection attempts.
	ReconnectIntervalsMs = 60000
	// BlockCacheMB is the default block cache size in megabytes for the Badger store.
	BlockCacheMB = 20
	// MemTableMB is the default memtable size in megabytes for the Badger store.
	MemTableMB = 20
	// IndexCacheMB is the default index cache size in megabytes for the Badger store.
	IndexCacheMB = 20
	// BadgerFileSize is the default value-log file size in megabytes for the Badger store.
	BadgerFileSize = 20
	// DefaultResourceMemoryMiB is the memory budget handed to the libp2p resource
	// manager when the config leaves it unset. It is deliberately a fraction of a
	// node's own memory limit: badger and the indexer need the rest.
	DefaultResourceMemoryMiB = 10240
	// DefaultResourceFileDescriptors is the file descriptor budget handed to the
	// libp2p resource manager when the config leaves it unset.
	DefaultResourceFileDescriptors = 8192
	// DefaultMaxStreamsPerPeer caps the concurrent streams a single peer may open
	// in each direction. It sits well above the handful of in-flight CAR requests
	// defradb queues per peer, leaving headroom for bitswap and pubsub on the
	// same connection.
	DefaultMaxStreamsPerPeer = 4096
	// BaseDelay is the base duration used for exponential backoff calculations.
	BaseDelay = 30 * time.Second
)
