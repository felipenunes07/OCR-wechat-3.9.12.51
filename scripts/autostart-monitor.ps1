<#
  Disparado pela tarefa agendada "WeChat OCR - Iniciar monitor" no logon.
  Espera o WeChat abrir (o resolver do daemon le a chave do banco da memoria
  do WeChat logado) e chama o INICIAR_WECHAT_OCR.ps1, que ja sai sozinho com
  JA_EM_EXECUCAO se o monitor estiver rodando.
#>
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$log = Join-Path $root ".runtime\autostart.log"
$iniciar = Join-Path $root "INICIAR_WECHAT_OCR.ps1"

function Write-Log([string]$msg) {
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
  Add-Content -Path $log -Value $line -Encoding utf8
}

Write-Log "logon detectado, aguardando WeChat.exe"

# Espera ate 30 min pelo WeChat. Se nao abrir, inicia mesmo assim: o daemon
# tenta de novo ler a chave do banco a cada ciclo.
$deadline = (Get-Date).AddMinutes(30)
$wechat = $null
while ((Get-Date) -lt $deadline) {
  $wechat = Get-Process -Name "WeChat" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($wechat) { break }
  Start-Sleep -Seconds 10
}

if ($wechat) {
  Write-Log "WeChat.exe encontrado (PID=$($wechat.Id)), aguardando 45s para o login terminar"
  Start-Sleep -Seconds 45
} else {
  Write-Log "WeChat.exe nao abriu em 30 min, iniciando monitor assim mesmo"
}

$out = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $iniciar 2>&1 | Out-String
Write-Log ("INICIAR exit={0}`n{1}" -f $LASTEXITCODE, $out.Trim())
