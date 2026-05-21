package snapshot

import "errors"

var ( //nolint:revive
	ErrListSnapshotsStatus    = errors.New("list snapshots non-OK status")                    //nolint:revive
	ErrDownloadSnapshotStatus = errors.New("download snapshot non-OK status")                 //nolint:revive
	ErrInvalidSnapshotMagic   = errors.New("invalid snapshot magic")                          //nolint:revive
	ErrNoBlockSignatures      = errors.New("no block signatures found in KV snapshot header") //nolint:revive
	ErrMerkleRootMismatch     = errors.New("merkle root mismatch")                            //nolint:revive
	ErrBlockSigCountMismatch  = errors.New("block sig root count mismatch")                   //nolint:revive
	ErrBlockSigRootMismatch   = errors.New("block sig root mismatch")                         //nolint:revive
	ErrUnsupportedSigType     = errors.New("unsupported signature type")                      //nolint:revive
	ErrInvalidSignature       = errors.New("invalid signature")                               //nolint:revive
)
