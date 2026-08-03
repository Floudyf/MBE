@echo off
setlocal EnableExtensions
chcp 65001 >nul
title MBE Smart Launcher

set "PROJECT_ROOT=%~dp0"
set "LAUNCHER=%PROJECT_ROOT%scripts\start_v0_dev.ps1"

if not exist "%LAUNCHER%" (
    echo [ERROR] Missing launcher: "%LAUNCHER%"
    echo Keep start_mbe.bat in the MBE repository root.
    pause
    exit /b 1
)

echo ========================================================
echo  MBE Smart Launcher
echo  It will repair copied environments and start both apps.
echo ========================================================
echo.

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%LAUNCHER%"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo [ERROR] MBE startup failed. Review the message above.
    pause
)

exit /b %EXIT_CODE%
