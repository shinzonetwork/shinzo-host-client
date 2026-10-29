package chain

import "errors"

var ( //nolint:revive
	ErrInvalidPrefix        = errors.New("prefix must be <Name>__<Network>, each part an upper-case letter followed by letters or digits") //nolint:revive
	ErrDuplicatePrefix      = errors.New("prefixes must differ ignoring case")                                                             //nolint:revive
	ErrInvalidGeneratorURL  = errors.New("generator URL must be an http or https URL with a host")                                         //nolint:revive
	ErrInvalidGeneratorPeer = errors.New("generator peer must be a multiaddr or an IP address with an optional port")                      //nolint:revive
	ErrInvalidIPAddress     = errors.New("invalid IP address")                                                                             //nolint:revive
	ErrInvalidMaxBlocks     = errors.New("max_blocks must not be negative")                                                                //nolint:revive
	ErrMissingChainID       = errors.New("chain_id must be set")                                                                           //nolint:revive
	ErrNoSigners            = errors.New("signers must list at least one generator")                                                       //nolint:revive
	ErrInvalidSigner        = errors.New("signer must be a public key in hex")                                                             //nolint:revive
)
