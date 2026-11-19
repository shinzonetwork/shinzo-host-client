package chain

import "errors"

var ( //nolint:revive
	ErrInvalidPrefix        = errors.New("prefix must be <Name>__<Network>, each part an upper-case letter followed by letters or digits") //nolint:revive
	ErrDuplicatePrefix      = errors.New("prefixes must differ ignoring case")                                                             //nolint:revive
	ErrInvalidGeneratorURL  = errors.New("generator URL must be an http or https URL with a host")                                         //nolint:revive
	ErrInvalidGeneratorPeer = errors.New("generator peer must be a multiaddr or an IP address with an optional port")                      //nolint:revive
	ErrInvalidIPAddress     = errors.New("invalid IP address")                                                                             //nolint:revive
	ErrInvalidMaxBlocks     = errors.New("max_blocks must not be negative")                                                                //nolint:revive
	ErrInvalidMode          = errors.New("mode must be full or tokens")                                                                    //nolint:revive
	ErrTokenKeyInFullMode   = errors.New("only a tokens-mode chain takes this key")                                                        //nolint:revive
	ErrMissingTokenKey      = errors.New("a tokens-mode chain requires this key")                                                          //nolint:revive
	ErrInvalidStartBlock    = errors.New("start.block must not be negative")                                                               //nolint:revive
	ErrDuplicateCAIP2       = errors.New("caip2 must differ between chains")                                                               //nolint:revive
)
