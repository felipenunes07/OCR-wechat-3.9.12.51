$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pidf = Join-Path $dir "wechat_receipt.pid"
$logOut = Join-Path $dir "wechat_receipt.out.log"
$logErr = Join-Path $dir "wechat_receipt.err.log"
$excel = Join-Path $dir "pagamentos_wechat.xlsx"
$db = Join-Path $dir "wechat_receipt_state.db"

if (Test-Path $pidf) {
  $daemonPid = (Get-Content $pidf | Select-Object -First 1)
  $p = Get-Process -Id $daemonPid -ErrorAction SilentlyContinue
  if ($p) {
    Write-Output "STATUS=RODANDO PID=$daemonPid"
  } else {
    Write-Output "STATUS=PARADO (PID antigo: $daemonPid)"
  }
} else {
  Write-Output "STATUS=PARADO"
}

Write-Output "EXCEL=$excel"
Write-Output "DB=$db"
Write-Output "LOG_OUT=$logOut"
Write-Output "LOG_ERR=$logErr"

if (Test-Path $excel) { Write-Output "EXCEL_TAMANHO=$((Get-Item $excel).Length)" }
if (Test-Path $db) { Write-Output "DB_TAMANHO=$((Get-Item $db).Length)" }

if (Test-Path $db) {
  $py = @"
import sqlite3
from datetime import datetime
p = r'''$db'''
conn = sqlite3.connect(p)
cur = conn.cursor()
rows = cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status ORDER BY 2 DESC").fetchall()
print('FILA_STATUS=', rows)
pending = cur.execute("SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing')").fetchone()[0]
print('FILA_PENDENTE=', pending)
last = cur.execute("SELECT source_path, amount, txn_date, txn_time, datetime(ingested_at,'unixepoch','localtime') FROM receipts ORDER BY ingested_at DESC LIMIT 1").fetchone()
print('ULTIMO=', last)
conn.close()
"@
  $py | python -
}

if (Test-Path $logOut) {
  Write-Output "----- ULTIMAS LINHAS OUT -----"
  Get-Content $logOut -Tail 20
}
if (Test-Path $logErr) {
  Write-Output "----- ULTIMAS LINHAS ERR -----"
  Get-Content $logErr -Tail 20
}
