[CmdletBinding()]
param(
    [switch]$SkipFastChecks,
    [switch]$KeepStack,
    [ValidateRange(10, 900)][int]$TimeoutSeconds = 180,
    [ValidateRange(30, 1800)][int]$StackStartTimeoutSeconds = 600
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
$failure = $null
$stackAttempted = $false

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

function Invoke-CheckedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][int]$ProcessTimeoutSeconds
    )

    Write-Host "==> $Description"
    $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -WorkingDirectory $WorkingDirectory -NoNewWindow -PassThru
    if (-not $process.WaitForExit($ProcessTimeoutSeconds * 1000)) {
        try {
            $process.Kill()
            $process.WaitForExit()
        }
        catch {
            Write-Warning "Could not stop timed-out process $($process.Id): $_"
        }
        throw "$Description exceeded its $ProcessTimeoutSeconds-second timeout."
    }
    if ($process.ExitCode -ne 0) {
        throw "$Description failed with exit code $($process.ExitCode)."
    }
}

function Get-ComposeConfiguration {
    $json = (& docker compose config --format json | Out-String)
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose config failed with exit code $LASTEXITCODE."
    }
    return $json | ConvertFrom-Json
}

try {
    if (-not $SkipFastChecks) {
        & (Join-Path $PSScriptRoot "verify-fast.ps1")
        if ($LASTEXITCODE -ne 0) {
            throw "Fast verification failed with exit code $LASTEXITCODE."
        }
    }

    Invoke-CheckedProcess `
        -Description "Check Docker daemon" `
        -FilePath "docker" `
        -ArgumentList @("info", "--format", "{{.ServerVersion}}") `
        -WorkingDirectory $repoRoot `
        -ProcessTimeoutSeconds 15

    $stackAttempted = $true
    Invoke-CheckedProcess `
        -Description "Build and start the real stack" `
        -FilePath "docker" `
        -ArgumentList @("compose", "up", "--build", "-d", "--wait") `
        -WorkingDirectory $repoRoot `
        -ProcessTimeoutSeconds $StackStartTimeoutSeconds

    Push-Location $repoRoot
    try {
        $composeConfig = Get-ComposeConfiguration
    }
    finally {
        Pop-Location
    }
    $workerPort = [string]$composeConfig.services.worker.environment.WORKER_OBSERVABILITY_PORT
    & (Join-Path $PSScriptRoot "smoke.ps1") `
        -TimeoutSeconds $TimeoutSeconds `
        -WorkerBaseUrl "http://localhost:$workerPort"

    $databaseURL = [string]$composeConfig.services.migrate.environment.DATABASE_URL
    $testDatabaseURL = $databaseURL.Replace("@postgres:", "@127.0.0.1:")
    if ($testDatabaseURL -eq $databaseURL) {
        throw "Could not convert the Compose PostgreSQL URL to its host-side integration-test URL."
    }

    $previousDatabaseURL = [Environment]::GetEnvironmentVariable("TEST_DATABASE_URL", "Process")
    $previousKafkaBrokers = [Environment]::GetEnvironmentVariable("TEST_KAFKA_BROKERS", "Process")
    Push-Location (Join-Path $repoRoot "backend")
    try {
        $env:TEST_DATABASE_URL = $testDatabaseURL
        $env:TEST_KAFKA_BROKERS = "localhost:29092"
        Invoke-Checked "PostgreSQL and Kafka integration tests" { go test -race -count=1 ./tests/integration }
    }
    finally {
        [Environment]::SetEnvironmentVariable("TEST_DATABASE_URL", $previousDatabaseURL, "Process")
        [Environment]::SetEnvironmentVariable("TEST_KAFKA_BROKERS", $previousKafkaBrokers, "Process")
        Pop-Location
    }

    & (Join-Path $PSScriptRoot "demo.ps1") -TimeoutSeconds $TimeoutSeconds
}
catch {
    $failure = $_.Exception
    [Console]::Error.WriteLine("Full verification failed: $($failure.Message)")
    if ($stackAttempted) {
        try {
            Invoke-CheckedProcess `
                -Description "Collect Compose status" `
                -FilePath "docker" `
                -ArgumentList @("compose", "--profile", "ui", "ps") `
                -WorkingDirectory $repoRoot `
                -ProcessTimeoutSeconds 15
            Invoke-CheckedProcess `
                -Description "Collect bounded Compose logs" `
                -FilePath "docker" `
                -ArgumentList @("compose", "logs", "--no-color", "--tail", "100", "api", "worker", "agent", "kafka", "postgres") `
                -WorkingDirectory $repoRoot `
                -ProcessTimeoutSeconds 15
        }
        catch {
            Write-Warning "Could not collect Docker diagnostics: $_"
        }
    }
}
finally {
    if (-not $KeepStack -and $stackAttempted) {
        try {
            Invoke-CheckedProcess `
                -Description "Stop verification stack" `
                -FilePath "docker" `
                -ArgumentList @("compose", "--profile", "ui", "down", "--remove-orphans") `
                -WorkingDirectory $repoRoot `
                -ProcessTimeoutSeconds 30
        }
        catch {
            if ($null -eq $failure) {
                $failure = $_.Exception
            }
            else {
                Write-Warning "Could not stop the verification stack: $_"
            }
        }
    }
}

if ($null -ne $failure) {
    exit 1
}

Write-Host "PASS: full verification"
