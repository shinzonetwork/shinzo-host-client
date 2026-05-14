package view

import "errors"

var ( //nolint:revive
	ErrViewNameRequired      = errors.New("view name is required")             //nolint:revive
	ErrViewQueryRequired     = errors.New("view query is required")            //nolint:revive
	ErrViewSDLRequired       = errors.New("view SDL is required")              //nolint:revive
	ErrViewQuerySDLRequired  = errors.New("view query and SDL are required")   //nolint:revive
	ErrLensEmptyPath         = errors.New("lens has empty path")               //nolint:revive
	ErrLensInvalidBase64     = errors.New("lens contains invalid base64 data") //nolint:revive
	ErrInvalidWASMFormat     = errors.New("invalid WASM file format for lens") //nolint:revive
	ErrCollectionNotFound    = errors.New("collection does not exist")         //nolint:revive
	ErrViewAlreadyRegistered = errors.New("view already registered")           //nolint:revive
	ErrWASMDownloadFailed    = errors.New("failed to download WASM files")     //nolint:revive
	ErrHTTPErrorResponse     = errors.New("HTTP error response")               //nolint:revive
)
