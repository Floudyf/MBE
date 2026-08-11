@echo off
setlocal EnableExtensions
set "SCRIPT_DIR=%~dp0"
set "REPO=%~1"
if "%REPO%"=="" set "REPO=C:\Projects\Metaverse_Blockchain_Env"

if /I "%~2"=="apply" goto APPLY

echo Running MBE formal runtime cache DRY-RUN...
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%cleanup_v5_formal_runtime_cache.ps1" -RepoRoot "%REPO%" -MinAgeHours 1
set "RC=%ERRORLEVEL%"
goto END

:APPLY
echo Applying MBE formal runtime cache cleanup...
echo Completed RunGroup metadata and formal artifacts.zip are preserved.
echo Completed child raw runtime cache older than 1 hour may be deleted.
echo Failed/cancelled child diagnostics are preserved.
echo.
set /P CONFIRM=Type DELETE_RUNTIME_CACHE to continue:
if /I not "%CONFIRM%"=="DELETE_RUNTIME_CACHE" (
  echo Cancelled.
  exit /b 2
)
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%cleanup_v5_formal_runtime_cache.ps1" -RepoRoot "%REPO%" -MinAgeHours 1 -Apply
set "RC=%ERRORLEVEL%"

:END
echo.
if not "%RC%"=="0" echo Cache cleanup tool failed with exit code %RC%.
pause
exit /b %RC%
