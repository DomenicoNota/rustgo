param(
    [string]$ApiBaseUrl = "http://localhost:8080",
    [string]$ApiKey = "",
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$demoDirectory = Join-Path $repoRoot "demo-data"
$demoLog = Join-Path $demoDirectory "demo.log"
$token = "logstreamdemo$([Guid]::NewGuid().ToString('N'))"
$barrierToken = "logstreambarrier$([Guid]::NewGuid().ToString('N'))"

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    if (-not [string]::IsNullOrWhiteSpace($env:LOGSTREAM_API_KEY)) {
        $ApiKey = $env:LOGSTREAM_API_KEY
    }
    elseif (-not [string]::IsNullOrWhiteSpace($env:API_KEYS)) {
        $ApiKey = ($env:API_KEYS -split ',')[0].Trim()
    }
    else {
        Push-Location $repoRoot
        try {
            $composeJSON = (& docker compose config --format json | Out-String)
            if ($LASTEXITCODE -ne 0) {
                throw "docker compose config failed with exit code $LASTEXITCODE"
            }
            $composeConfig = $composeJSON | ConvertFrom-Json
            $ApiKey = (($composeConfig.services.api.environment.API_KEYS -split ',')[0]).Trim()
        }
        finally {
            Pop-Location
        }
    }
}
if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw "No API key was supplied and none could be resolved from the environment or Compose configuration."
}
$headers = @{ Authorization = "Bearer $ApiKey" }

New-Item -ItemType Directory -Force -Path $demoDirectory | Out-Null

function Get-MatchingItems {
    param([string]$Message)

    $encoded = [Uri]::EscapeDataString($Message)
    $response = Invoke-RestMethod -Method Get -Uri "$ApiBaseUrl/v1/logs?service=demo-service&q=$encoded&limit=100"
    return @($response.items | Where-Object { $_.message -eq $Message })
}

function Wait-ForSingleItem {
    param([string]$Message)

    $waitDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $waitDeadline) {
        try {
            $items = @(Get-MatchingItems -Message $Message)
        }
        catch {
            if ([DateTime]::UtcNow -ge $waitDeadline) {
                throw
            }
            Start-Sleep -Milliseconds 500
            continue
        }
        if ($items.Count -eq 1) {
            return $items[0]
        }
        if ($items.Count -gt 1) {
            throw "Expected one event for '$Message', found $($items.Count)."
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Timed out waiting for '$Message' through GET /v1/logs."
}

function Send-Event {
    param([object]$Event)

    $body = @{ events = @($Event) } | ConvertTo-Json -Depth 20 -Compress
    $response = Invoke-RestMethod -Method Post -Uri "$ApiBaseUrl/v1/ingest" -Headers $headers -ContentType "application/json" -Body $body
    if ($response.accepted -ne 1 -or $response.rejected -ne 0) {
        throw "Ingest did not accept exactly one event: $($response | ConvertTo-Json -Compress)"
    }
}

$line = [ordered]@{
    timestamp = [DateTime]::UtcNow.ToString("o")
    level = "info"
    message = $token
    demo_token = $token
} | ConvertTo-Json -Compress
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::AppendAllText($demoLog, $line + [Environment]::NewLine, $utf8NoBom)

$agentEvent = Wait-ForSingleItem -Message $token
if ($agentEvent.schema_version -ne 1) {
    throw "Agent emitted schema version '$($agentEvent.schema_version)', expected 1."
}
if ([string]::IsNullOrWhiteSpace($agentEvent.id)) {
    throw "Agent event did not contain a stable ID."
}
if ($agentEvent.attributes.demo_token -ne $token) {
    throw "Agent did not preserve the structured demo attribute."
}
if ($agentEvent.source.agent -ne "docker-demo-agent") {
    throw "Agent source metadata is missing or incorrect."
}

$duplicate = [ordered]@{
    schema_version = $agentEvent.schema_version
    id = $agentEvent.id
    timestamp = ([DateTime]$agentEvent.timestamp).ToUniversalTime().ToString("o")
    service = $agentEvent.service
    level = $agentEvent.level
    message = $agentEvent.message
    attributes = $agentEvent.attributes
    source = $agentEvent.source
}
Send-Event -Event $duplicate
Send-Event -Event $duplicate

$barrier = [ordered]@{
    schema_version = 1
    id = "barrier-$([Guid]::NewGuid().ToString('N'))"
    timestamp = [DateTime]::UtcNow.ToString("o")
    service = "demo-service"
    level = "info"
    message = $barrierToken
    attributes = @{ purpose = "duplicate-delivery-barrier" }
    source = @{ agent = "demo-script"; file = "scripts/demo.ps1" }
}
Send-Event -Event $barrier
$null = Wait-ForSingleItem -Message $barrierToken

$finalItems = @(Get-MatchingItems -Message $token)
if ($finalItems.Count -ne 1) {
    throw "Idempotency check failed: event ID '$($agentEvent.id)' produced $($finalItems.Count) stored rows."
}
if ($finalItems[0].id -ne $agentEvent.id) {
    throw "Queried event ID changed between agent delivery and persistence."
}

Write-Host "PASS: agent -> API -> Kafka -> worker -> PostgreSQL -> query API"
Write-Host "Event ID: $($agentEvent.id)"
Write-Host "Message: $token"
Write-Host "Duplicate deliveries stored as rows: $($finalItems.Count)"
