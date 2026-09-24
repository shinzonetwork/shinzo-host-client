//go:build !windows

//go:generate ../tools/scripts/download_console.sh

package console

import (
	"embed"
)

//go:embed dist
var Dist embed.FS
