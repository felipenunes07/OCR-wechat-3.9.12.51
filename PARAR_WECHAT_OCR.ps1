$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pidf = Join-Path $dir "wechat_receipt.pid"
. (Join-Path $dir "wechat_pid_lib.ps1")
if (!(Test-Path $pidf)) {
  Write-Output "NAO_HA_PID"
  exit 0
}
$daemonProc = Get-WeChatDaemonProcess -PidFile $pidf
if ($daemonProc) {
  Stop-Process -Id $daemonProc.ProcessId -Force
  Write-Output "PARADO PID=$($daemonProc.ProcessId)"
} else {
  # PID reciclado por outro programa: apenas limpa o arquivo, nunca mata o intruso.
  $daemonPid = Read-WeChatDaemonPid -PidFile $pidf
  Write-Output "PROCESSO_NAO_ENCONTRADO PID=$daemonPid"
}
Remove-Item $pidf -Force -ErrorAction SilentlyContinue
