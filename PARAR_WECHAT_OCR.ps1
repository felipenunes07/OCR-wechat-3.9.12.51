$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pidf = Join-Path $dir "wechat_receipt.pid"
if (!(Test-Path $pidf)) {
  Write-Output "NAO_HA_PID"
  exit 0
}
$daemonPid = (Get-Content $pidf | Select-Object -First 1)
if ($daemonPid) {
  $p = Get-Process -Id $daemonPid -ErrorAction SilentlyContinue
  if ($p) {
    Stop-Process -Id $daemonPid -Force
    Write-Output "PARADO PID=$daemonPid"
  } else {
    Write-Output "PROCESSO_NAO_ENCONTRADO PID=$daemonPid"
  }
}
Remove-Item $pidf -Force -ErrorAction SilentlyContinue
