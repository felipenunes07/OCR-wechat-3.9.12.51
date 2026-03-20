@echo off
setlocal
set "DIR=%~dp0"
set "PS1=%DIR%INICIAR_WECHAT_OCR.ps1"

if not exist "%PS1%" (
  echo Script nao encontrado: "%PS1%"
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
if errorlevel 1 (
  echo.
  echo Falha ao iniciar WeChat OCR.
  echo Verifique a mensagem acima e o arquivo wechat_receipt.err.log.
  pause
  exit /b 1
)

echo.
echo WeChat OCR iniciado. Esta janela pode ser fechada.
exit /b 0
