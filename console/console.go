// Package console embeds the placeholder node console page, structured the
// same way the playground package embeds its own static assets.
package console

import "embed"

//go:embed dist
var Dist embed.FS
