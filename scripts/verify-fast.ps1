[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    Write-Host "==> $Description"
    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

Push-Location (Join-Path $repoRoot "backend")
$previousDatabaseURL = [Environment]::GetEnvironmentVariable("TEST_DATABASE_URL", "Process")
$previousKafkaBrokers = [Environment]::GetEnvironmentVariable("TEST_KAFKA_BROKERS", "Process")
try {
    [Environment]::SetEnvironmentVariable("TEST_DATABASE_URL", $null, "Process")
    [Environment]::SetEnvironmentVariable("TEST_KAFKA_BROKERS", $null, "Process")

    Write-Host "==> Go formatting check"
    & (Join-Path $PSScriptRoot "check-go-format.ps1")

    Invoke-Checked "Go vet" { go vet ./... }
    Invoke-Checked "Go tests" { go test -count=1 ./... }
    Invoke-Checked "Go race tests" { go test -race -count=1 ./... }
}
finally {
    [Environment]::SetEnvironmentVariable("TEST_DATABASE_URL", $previousDatabaseURL, "Process")
    [Environment]::SetEnvironmentVariable("TEST_KAFKA_BROKERS", $previousKafkaBrokers, "Process")
    Pop-Location
}

Push-Location (Join-Path $repoRoot "agent")
try {
    Invoke-Checked "Rust formatting check" { cargo fmt --check }
    Invoke-Checked "Rust Clippy" { cargo clippy --locked --all-targets --all-features -- -D warnings }
    Invoke-Checked "Rust tests" { cargo test --locked }
}
finally {
    Pop-Location
}

Push-Location (Join-Path $repoRoot "dashboard")
try {
    Invoke-Checked "Dashboard dependency install" { npm ci }
    Invoke-Checked "Dashboard dependency audit" { npm audit --audit-level=high }
    Invoke-Checked "Dashboard formatting check" { npm run format:check }
    Invoke-Checked "Dashboard lint" { npm run lint }
    Invoke-Checked "Dashboard typecheck" { npm run typecheck }
    Invoke-Checked "Dashboard tests" { npm run test }
    Invoke-Checked "Dashboard build" { npm run build }
}
finally {
    Pop-Location
}

Push-Location $repoRoot
try {
    Invoke-Checked "Docker Compose configuration" { docker compose config --quiet }
}
finally {
    Pop-Location
}

Write-Host "PASS: fast verification"
