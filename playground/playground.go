//go:build hostplayground && !windows

//go:generate ../tools/scripts/download_playground.sh

package playground

import (
	"embed"
)

//go:embed dist
var Dist embed.FS

