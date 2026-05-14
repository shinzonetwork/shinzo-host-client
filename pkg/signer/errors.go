package signer

import "errors"

var (
	// ErrKeyFileNotFound is returned when the defra_identity.key file cannot be located in any common path.
	ErrKeyFileNotFound = errors.New("could not find defra_identity.key in any common location, please ensure the key file exists or provide a store path in config")
	// ErrNoPrivateKey is returned when an identity does not contain a private key.
	ErrNoPrivateKey = errors.New("identity does not have a private key")
	// ErrUnexpectedKeyLength is returned when an Ed25519 key is neither 32 nor 64 bytes.
	ErrUnexpectedKeyLength = errors.New("unexpected Ed25519 key length: expected 32 or 64 bytes")
	// ErrNoPublicKey is returned when an identity does not contain a public key.
	ErrNoPublicKey = errors.New("identity does not have a public key")
	// ErrSignatureVerification is returned when a signature fails to verify against the public key.
	ErrSignatureVerification = errors.New("signature verification failed")
	// ErrNotEd25519Key is returned when a public key is not of the Ed25519 type.
	ErrNotEd25519Key = errors.New("public key is not Ed25519")
)
