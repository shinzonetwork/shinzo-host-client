package main

import "errors"

var (
	errNotImplemented              = errors.New("not implemented")
	errUnsupportedOverrideFlagType = errors.New("unsupported override flag type")
)
