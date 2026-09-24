//go:generate powershell -ExecutionPolicy Bypass -File ../tools/scripts/download_console.ps1

package console

import (
	"embed"
)

//go:embed dist
var Dist embed.FS
