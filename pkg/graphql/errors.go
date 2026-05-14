package graphql

import "errors"

var ( //nolint:revive
	ErrQueryEmpty          = errors.New("query cannot be empty")                                //nolint:revive
	ErrParseCollectionName = errors.New("unable to parse collection name from query")           //nolint:revive
	ErrNoClosingParen      = errors.New("unable to find matching closing parenthesis in query") //nolint:revive
	ErrNoSelectionSet      = errors.New("unable to find selection set in query")                //nolint:revive
)
