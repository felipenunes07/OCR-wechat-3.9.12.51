# Helper compartilhado para ler o wechat_receipt.pid com seguranca.
# Depois de reiniciar o PC o Windows reaproveita PIDs, entao o numero gravado
# antes do boot quase sempre pertence a outro programa. Sem conferir a
# identidade do processo o INICIAR desiste com JA_EM_EXECUCAO, o STATUS mostra
# RODANDO e o PARAR mata o processo alheio.

function Read-WeChatDaemonPid {
  param([Parameter(Mandatory = $true)][string]$PidFile)

  if (-not (Test-Path $PidFile)) { return 0 }
  $raw = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
  if (-not $raw) { return 0 }
  $parsed = 0
  if (-not [int]::TryParse($raw.Trim(), [ref]$parsed)) { return 0 }
  if ($parsed -le 0) { return 0 }
  return $parsed
}

function Get-WeChatDaemonProcess {
  # Retorna o processo somente se o PID gravado ainda for o daemon; senao $null.
  param([Parameter(Mandatory = $true)][string]$PidFile)

  $daemonPid = Read-WeChatDaemonPid -PidFile $PidFile
  if ($daemonPid -le 0) { return $null }

  $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $daemonPid" -ErrorAction SilentlyContinue
  if (-not $proc) { return $null }
  if ([string]$proc.CommandLine -notlike "*wechat_receipt_daemon.py*") { return $null }
  return $proc
}
