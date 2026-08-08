$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location (Join-Path $repoRoot "backend")
try {
    $unformatted = @(& gofmt -l .)
    if ($LASTEXITCODE -ne 0) {
        throw "gofmt failed with exit code $LASTEXITCODE."
    }
    if ($unformatted.Count -gt 0) {
        throw "Go files require formatting:`n$($unformatted -join [Environment]::NewLine)"
    }
}
finally {
    Pop-Location
}
