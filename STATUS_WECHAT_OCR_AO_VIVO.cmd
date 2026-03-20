@echo off
setlocal enableextensions

:loop
echo.
echo ================================================================
echo STATUS AO VIVO - %date% %time%  (Ctrl+C para sair)
echo ================================================================
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0STATUS_WECHAT_OCR.ps1"
echo.
timeout /t 3 /nobreak >nul
goto loop
