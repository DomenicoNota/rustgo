$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
foreach ($line in Get-Content (Join-Path $repoRoot "Makefile")) {
    if ($line -match '^(?<target>[a-z0-9-]+):.*## (?<description>.+)$') {
        Write-Host ("{0,-14} {1}" -f $Matches.target, $Matches.description)
    }
}
