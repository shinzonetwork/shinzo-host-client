package attestation

import "errors"

var ( //nolint:revive
	ErrBlockSignatureNil        = errors.New("block signature is nil")                                         //nolint:revive
	ErrUnsupportedSignatureType = errors.New("unsupported signature type")                                     //nolint:revive
	ErrBlockSigVerifyFailed     = errors.New("block signature verification failed")                            //nolint:revive
	ErrComputeMerkleRoot        = errors.New("failed to compute merkle root from CIDs")                        //nolint:revive
	ErrEmptyIdentityOrCID       = errors.New("empty identity or CID")                                          //nolint:revive
	ErrInvalidSignatureType     = errors.New("invalid signature type")                                         //nolint:revive
	ErrDefraDBUnavailable       = errors.New("defradb node or DB is not available for signature verification") //nolint:revive
	ErrDifferentAttestedDocIDs  = errors.New("cannot merge records with different attested document IDs")      //nolint:revive
)
