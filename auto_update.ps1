$ErrorActionPreference = 'Continue'

while ($true) {
  try {
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Updating data..."
    c:/python314/python.exe c:\Users\Server\Documents\API\fetch_kp_march.py
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Done"
  } catch {
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Error: $($_.Exception.Message)"
  }

  Start-Sleep -Seconds 60
}
