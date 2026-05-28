package host

import "errors"

var (
	errPeerIDMismatchDial = errors.New(
		"all dials failed\n  * [/ip4/34.9.109.135/tcp/9171] failed to negotiate security protocol: " +
			"peer id mismatch: expected 12D3KooWCS9HHoiqfu1YRBiLM5spAbDGiHb2NtXEGUoHYTcCtEfy, " +
			"but remote key matches 12D3KooWPBbKmsSsFiTW2X4sY4uuCUEazSFZkAdY8Egm4mQsiSEF",
	)
	errConnectionRefused    = errors.New("connection refused")
	errTransactionConflict  = errors.New("transaction conflict: cannot write")
	errOperationFailedRetry = errors.New("operation failed: Please retry")
	errEmptyMessage         = errors.New("")
)
