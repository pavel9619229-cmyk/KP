# Registers watcher task for near-real-time light polling.
# Run once: powershell -ExecutionPolicy Bypass -File tools\schedule_watcher.ps1

$ProjectRoot = "C:\Users\Server\Documents\API"
$PythonExe   = (Get-Command python).Source
$ScriptPath  = "$ProjectRoot\tools\watch_changes.py"
$TaskName    = "KP_WatchChanges"

$Action  = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument $ScriptPath `
    -WorkingDirectory $ProjectRoot

# Start once now; script itself stays alive and polls every WATCH_POLL_SECONDS.
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date)

$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 24) `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable

$Principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Force

Write-Host "Task '$TaskName' registered."
Write-Host "Start now: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "Stop: Stop-ScheduledTask -TaskName '$TaskName'"
