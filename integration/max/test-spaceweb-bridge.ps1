$ErrorActionPreference = 'Stop'
$envPath = Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) '.env'
$line = Get-Content $envPath | Where-Object { $_ -match '^LOCAL_STAGE4_AGENT_TOKEN=' } | Select-Object -First 1
if (-not $line) { throw 'LOCAL_STAGE4_AGENT_TOKEN is missing' }
$token = $line.Substring($line.IndexOf('=') + 1).Trim()
$headers = @{
    'X-Local-Agent-Token' = $token
    'Content-Type' = 'application/json'
    'Accept' = 'application/json'
}
$body = '{"action":"ping"}'
foreach ($mode in @('relay', 'forward')) {
    $uri = "https://shina-moskva.ru/max-webhook.php?render_bridge=$mode"
    try {
        $result = Invoke-RestMethod $uri -Method Post -Headers $headers -Body $body -TimeoutSec 25
        Write-Output "MODE=$mode OK=$($result.ok) RETURN_MODE=$($result.mode)"
    }
    catch {
        Write-Output "MODE=$mode ERROR=$($_.Exception.Message)"
    }
}
$token = $null
