$ErrorActionPreference = "Stop"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $dir "wechat_receipt_daemon.py"
$db = Join-Path $dir "wechat_receipt_state.db"
$excel = Join-Path $dir "pagamentos_wechat.xlsx"
$logOut = Join-Path $dir "wechat_receipt.out.log"
$logErr = Join-Path $dir "wechat_receipt.err.log"
$log = $logOut
$pidf = Join-Path $dir "wechat_receipt.pid"
$watch = "C:\Users\admin\Documents\WeChat Files\wxid_xd3703k0ih2p22\FileStorage"

if (!(Test-Path $script)) { throw "Script nao encontrado: $script" }
if (!(Test-Path $watch)) { throw "Pasta WeChat nao encontrada: $watch" }

# Atualiza automaticamente o mapa hash->nome de grupo antes de iniciar.
$mapUpdater = Join-Path $dir "refresh_group_map.py"
if (Test-Path $mapUpdater) {
  python -X utf8 $mapUpdater | Out-Null
}

if (Test-Path $pidf) {
  $oldPid = (Get-Content $pidf -ErrorAction SilentlyContinue | Select-Object -First 1)
  if ($oldPid) {
    $pOld = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
    if ($pOld) {
      Write-Output "JA_EM_EXECUCAO PID=$oldPid"
      exit 0
    }
  }
}

$arguments = "-X utf8 -u `"$script`" --watch-root `"$watch`" --db-path `"$db`" --excel-path `"$excel`" --reconcile-seconds 90 --min-confidence 0.55 --client-map-path `"$dir\clientes_grupos.json`""
$p = Start-Process -FilePath "python" -ArgumentList $arguments -WorkingDirectory $dir -RedirectStandardOutput $logOut -RedirectStandardError $logErr -PassThru
$p.Id | Set-Content -Path $pidf -Encoding ascii
Start-Sleep -Seconds 2
if (Get-Process -Id $p.Id -ErrorAction SilentlyContinue) {
  Write-Output "INICIADO PID=$($p.Id)"
  Write-Output "LOG=$log"
  Write-Output "EXCEL=$excel"
} else {
  Write-Output "FALHOU_INICIAR. Veja log: $log"
}

