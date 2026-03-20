$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$db = Join-Path $dir "wechat_receipt_state.db"
if (!(Test-Path $db)) {
  Write-Output "DB nao encontrado: $db"
  exit 1
}
$pyExe = Join-Path $dir ".venv\\Scripts\\python.exe"
if (!(Test-Path $pyExe)) { $pyExe = "python" }

$py = @"
import sqlite3
from collections import Counter
from pathlib import Path
p = Path("wechat_receipt_state.db").resolve()
conn = sqlite3.connect(p)
cur = conn.cursor()
rows = cur.execute("SELECT last_error FROM files WHERE last_error LIKE 'MISSING_CLIENT_MAP:%' AND status IN ('pending','retry','processing')").fetchall()
ctr = Counter()
for (e,) in rows:
    gid = (e or '').split(':',1)[1] if ':' in (e or '') else 'SEM_GRUPO'
    ctr[gid] += 1
print('GRUPOS_SEM_MAPA:')
for gid, n in ctr.most_common(50):
    print(f"{gid} | pendentes={n}")
conn.close()
"@
Push-Location $dir
try {
  $py | & $pyExe -X utf8 -
} finally {
  Pop-Location
}
