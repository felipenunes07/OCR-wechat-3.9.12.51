@echo off
setlocal enableextensions

:loop
cls
echo STATUS AO VIVO (Ctrl+C para sair)
echo =================================
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0STATUS_WECHAT_OCR.ps1"
echo.
timeout /t 3 /nobreak >nul
goto loop
