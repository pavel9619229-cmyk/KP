# Регистрирует задание в Windows Task Scheduler:
# каждые 30 минут запускает refresh_seed.py
# Запустить один раз от имени администратора: .\tools\schedule_refresh.ps1

$ProjectRoot = "C:\Users\Server\Documents\API"
$PythonExe   = (Get-Command python).Source
$ScriptPath  = "$ProjectRoot\tools\refresh_seed.py"
$LogFile     = "$ProjectRoot\tools\refresh_seed.log"
$TaskName    = "KP_RefreshSeed"

$Action  = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument $ScriptPath `
    -WorkingDirectory $ProjectRoot

$Trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 30) -Once -At (Get-Date)

$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 2) `
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

Write-Host "Задание '$TaskName' зарегистрировано. Запуск каждые 30 минут."
Write-Host "Проверить: Get-ScheduledTask -TaskName '$TaskName'"
Write-Host "Запустить вручную: Start-ScheduledTask -TaskName '$TaskName'"
