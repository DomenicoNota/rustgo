param(
    [string]$ApiBaseUrl = "http://localhost:8080",
    [string]$WorkerBaseUrl = "http://localhost:9091",
    [string]$AgentBaseUrl = "http://localhost:9090",
    [ValidateRange(1, 900)][int]$TimeoutSeconds = 120,
    [ValidateRange(50, 5000)][int]$PollIntervalMilliseconds = 500
)

$ErrorActionPreference = "Stop"
$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
$lastFailure = "no response"

while ([DateTime]::UtcNow -lt $deadline) {
    try {
        $health = Invoke-RestMethod -Uri "$ApiBaseUrl/healthz"
        if ($health.status -ne "ok") {
            throw "liveness returned '$($health.status)'"
        }

        $ready = Invoke-RestMethod -Uri "$ApiBaseUrl/readyz"
        if ($ready.status -ne "ready") {
            throw "readiness returned '$($ready.status)'"
        }

        $workerHealth = Invoke-RestMethod -Uri "$WorkerBaseUrl/healthz"
        if ($workerHealth.status -ne "ok") {
            throw "worker liveness returned '$($workerHealth.status)'"
        }
        $workerReady = Invoke-RestMethod -Uri "$WorkerBaseUrl/readyz"
        if ($workerReady.status -ne "ready") {
            throw "worker readiness returned '$($workerReady.status)'"
        }
        $agentHealth = Invoke-RestMethod -Uri "$AgentBaseUrl/healthz"
        if ($agentHealth.status -ne "ok") {
            throw "agent liveness returned '$($agentHealth.status)'"
        }

        Write-Host "LogStream API, worker, and agent are healthy; required dependencies are ready."
        return
    }
    catch {
        $lastFailure = $_.Exception.Message
    }
    Start-Sleep -Milliseconds $PollIntervalMilliseconds
}

throw "Stack did not become ready within $TimeoutSeconds seconds. Last failure: $lastFailure"
