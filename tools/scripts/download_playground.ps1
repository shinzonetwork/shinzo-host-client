# Download static assets from: `github.com/sourcenetwork/defradb-playground`.
#
# Bump the release tag in the URL below to change versions.

$ErrorActionPreference = "Stop"

$playgroundDir = Join-Path $PSScriptRoot "..\..\playground"
Set-Location $playgroundDir

$url = "https://github.com/sourcenetwork/defradb-playground/releases/download/v1.0.0/dist.tar.gz"
$tempFile = [System.IO.Path]::GetTempFileName() + ".tar.gz"

try {
    Invoke-WebRequest -Uri $url -OutFile $tempFile
    tar -xzf $tempFile
} finally {
    if (Test-Path $tempFile) {
        Remove-Item $tempFile
    }
}

