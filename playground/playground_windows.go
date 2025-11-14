//go:build hostplayground

//go:generate powershell -ExecutionPolicy Bypass -File ../tools/scripts/download_playground.ps1

package playground

import (
	"embed"
)

//go:embed dist
var Dist embed.FS

