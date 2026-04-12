$ErrorActionPreference = 'Continue'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$fetchScript = Join-Path $scriptDir 'fetch_kp_march.py'

while ($true) {
  try {
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Updating data..."
    c:/python314/python.exe $fetchScript
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Done"
  } catch {
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Error: $($_.Exception.Message)"
  }

  Start-Sleep -Seconds 60
}
