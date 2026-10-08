package attestation

import "errors"

var ( //nolint:revive
	ErrBlockSignatureNil        = errors.New("block signature is nil")                                    //nolint:revive
	ErrUnsupportedSignatureType = errors.New("unsupported signature type")                                //nolint:revive
	ErrBlockSigVerifyFailed     = errors.New("block signature verification failed")                       //nolint:revive
	ErrComputeMerkleRoot        = errors.New("failed to compute merkle root from CIDs")                   //nolint:revive
	ErrDifferentAttestedDocIDs  = errors.New("cannot merge records with different attested document IDs") //nolint:revive
	ErrDocumentNotFound         = errors.New("document not found")                                        //nolint:revive
)
