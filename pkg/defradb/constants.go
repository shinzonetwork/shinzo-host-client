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
	// BaseDelay is the base duration used for exponential backoff calculations.
	BaseDelay = 30 * time.Second
)
