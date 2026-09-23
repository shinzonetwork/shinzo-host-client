# Install node-console static assets into console/dist.
#
# Prefer a sibling ..\node-console checkout. Otherwise set CONSOLE_DIST_URL
# to a dist.tar.gz GitHub release.

$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$consoleDir = Join-Path $root "console"
$uiDir = Join-Path (Split-Path $root -Parent) "node-console"

Set-Location $consoleDir

if (Test-Path $uiDir) {
    Write-Host "building node-console from $uiDir"
    Push-Location $uiDir
    npm run build
    Pop-Location
    if (Test-Path dist) { Remove-Item -Recurse -Force dist }
    New-Item -ItemType Directory -Path dist | Out-Null
    Copy-Item -Recurse -Force (Join-Path $uiDir "dist\*") dist
    Write-Host "copied $uiDir\dist -> $consoleDir\dist"
    exit 0
}

if ($env:CONSOLE_DIST_URL) {
    $tempFile = [System.IO.Path]::GetTempFileName() + ".tar.gz"
    try {
        Invoke-WebRequest -Uri $env:CONSOLE_DIST_URL -OutFile $tempFile
        tar -xzf $tempFile
    } finally {
        if (Test-Path $tempFile) { Remove-Item $tempFile }
    }
    exit 0
}

Write-Error @"
node-console assets not found.

Clone a sibling checkout at ..\node-console or set CONSOLE_DIST_URL to a dist.tar.gz release.
"@
