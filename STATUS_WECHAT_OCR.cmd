@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0STATUS_WECHAT_OCR.ps1"
echo.
pause
