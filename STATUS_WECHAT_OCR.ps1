$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pidf = Join-Path $dir "wechat_receipt.pid"
$logOut = Join-Path $dir "wechat_receipt.out.log"
$logErr = Join-Path $dir "wechat_receipt.err.log"
$sinkConfigPath = Join-Path $dir "sink_config.json"
$excel = Join-Path $dir "pagamentos_wechat.xlsx"
$db = Join-Path $dir "wechat_receipt_state.db"
$isExcelSink = $true

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

if (Test-Path $sinkConfigPath) {
  $sinkConfig = Get-Content $sinkConfigPath -Raw | ConvertFrom-Json
  $sinkMode = [string]($sinkConfig.sink_mode)
  if ([string]::IsNullOrWhiteSpace($sinkMode)) { $sinkMode = "excel" }
  Write-Output "SINK_MODE=$sinkMode"
  if ($sinkMode -eq "google-sheets") {
    $isExcelSink = $false
    $gsheetRef = [string]($sinkConfig.spreadsheet_url)
    if ([string]::IsNullOrWhiteSpace($gsheetRef)) { $gsheetRef = [string]($sinkConfig.spreadsheet_id) }
    $googleCredentialsPath = [string]($sinkConfig.google_credentials_path)
    if ([string]::IsNullOrWhiteSpace($googleCredentialsPath)) {
      $googleCredentialsPath = "google_service_account.json"
    }
    if (-not [System.IO.Path]::IsPathRooted($googleCredentialsPath)) {
      $googleCredentialsPath = Join-Path $dir $googleCredentialsPath
    }
    $dbMergePath = [string]($sinkConfig.db_merge_path)
    if ([string]::IsNullOrWhiteSpace($dbMergePath)) { $dbMergePath = ".runtime\\wechat_merge.db" }
    if (-not [System.IO.Path]::IsPathRooted($dbMergePath)) {
      $dbMergePath = Join-Path $dir $dbMergePath
    }
    $resolutionMode = [string]($sinkConfig.resolution_mode)
    if ([string]::IsNullOrWhiteSpace($resolutionMode)) { $resolutionMode = "db-first" }
    $verificationColumnName = [string]($sinkConfig.verification_column_name)
    if ([string]::IsNullOrWhiteSpace($verificationColumnName)) { $verificationColumnName = "STATUS_VERIFICACAO" }
    $originalWaitSeconds = [int]($sinkConfig.original_wait_seconds)
    if ($originalWaitSeconds -le 0) { $originalWaitSeconds = 90 }
    $tempCorrelationSeconds = [int]($sinkConfig.temp_correlation_seconds)
    if ($tempCorrelationSeconds -le 0) { $tempCorrelationSeconds = 30 }
    $uiForceDownloadEnabled = [bool]$sinkConfig.ui_force_download_enabled
    $uiForceDelaySeconds = [int]($sinkConfig.ui_force_delay_seconds)
    if ($uiForceDelaySeconds -le 0) { $uiForceDelaySeconds = 15 }
    $uiForceScope = [string]($sinkConfig.ui_force_scope)
    if ([string]::IsNullOrWhiteSpace($uiForceScope)) { $uiForceScope = "mapped-groups" }
    $uiFocusPolicy = [string]($sinkConfig.ui_focus_policy)
    if ([string]::IsNullOrWhiteSpace($uiFocusPolicy)) { $uiFocusPolicy = "immediate" }
    $uiBatchMode = [string]($sinkConfig.ui_batch_mode)
    if ([string]::IsNullOrWhiteSpace($uiBatchMode)) { $uiBatchMode = "group-sequential" }
    $uiItemTimeoutSeconds = [int]($sinkConfig.ui_item_timeout_seconds)
    if ($uiItemTimeoutSeconds -le 0) { $uiItemTimeoutSeconds = 5 }
    $uiRetryBackoffSeconds = ($sinkConfig.ui_retry_backoff_seconds | ForEach-Object { [string]$_ }) -join ","
    if ([string]::IsNullOrWhiteSpace($uiRetryBackoffSeconds)) { $uiRetryBackoffSeconds = "5,10,20,40" }
    $uiWindowBackends = ($sinkConfig.ui_window_backends | ForEach-Object { [string]$_ }) -join ","
    if ([string]::IsNullOrWhiteSpace($uiWindowBackends)) { $uiWindowBackends = "win32,uia" }
    $uiWindowClasses = ($sinkConfig.ui_window_classes | ForEach-Object { [string]$_ }) -join ","
    if ([string]::IsNullOrWhiteSpace($uiWindowClasses)) { $uiWindowClasses = "WeChatMainWndForPC,Base_PowerMessageWindow,Chrome_WidgetWin_0" }
    $sheetOrderScope = [string]($sinkConfig.sheet_order_scope)
    if ([string]::IsNullOrWhiteSpace($sheetOrderScope)) { $sheetOrderScope = "per_talker" }
    $sheetMaterializationOrder = [string]($sinkConfig.sheet_materialization_order)
    if ([string]::IsNullOrWhiteSpace($sheetMaterializationOrder)) { $sheetMaterializationOrder = "desc" }
    $sheetCommitOrder = [string]($sinkConfig.sheet_commit_order)
    if ([string]::IsNullOrWhiteSpace($sheetCommitOrder)) { $sheetCommitOrder = "asc" }
    Write-Output "GOOGLE_SHEETS_REF=$gsheetRef"
    Write-Output "GOOGLE_CREDENTIALS=$googleCredentialsPath"
    Write-Output "GOOGLE_CREDENTIALS_OK=$(Test-Path $googleCredentialsPath)"
    Write-Output "RESOLUTION_MODE=$resolutionMode"
    Write-Output "DB_MERGE_PATH=$dbMergePath"
    Write-Output "DB_MERGE_OK=$(Test-Path $dbMergePath)"
    Write-Output "ORIGINAL_WAIT_SECONDS=$originalWaitSeconds"
    Write-Output "TEMP_CORRELATION_SECONDS=$tempCorrelationSeconds"
    Write-Output "VERIFICATION_COLUMN=$verificationColumnName"
    Write-Output "UI_FORCE_DOWNLOAD_ENABLED=$uiForceDownloadEnabled"
    Write-Output "UI_FORCE_DELAY_SECONDS=$uiForceDelaySeconds"
    Write-Output "UI_FORCE_SCOPE=$uiForceScope"
    Write-Output "UI_FOCUS_POLICY=$uiFocusPolicy"
    Write-Output "UI_BATCH_MODE=$uiBatchMode"
    Write-Output "UI_ITEM_TIMEOUT_SECONDS=$uiItemTimeoutSeconds"
    Write-Output "UI_RETRY_BACKOFF_SECONDS=$uiRetryBackoffSeconds"
    Write-Output "UI_WINDOW_BACKENDS=$uiWindowBackends"
    Write-Output "UI_WINDOW_CLASSES=$uiWindowClasses"
    Write-Output "SHEET_ORDER_SCOPE=$sheetOrderScope"
    Write-Output "SHEET_MATERIALIZATION_ORDER=$sheetMaterializationOrder"
    Write-Output "SHEET_COMMIT_ORDER=$sheetCommitOrder"
  } else {
    Write-Output "EXCEL=$excel"
  }
} else {
  Write-Output "SINK_MODE=excel"
  Write-Output "EXCEL=$excel"
}

Write-Output "DB=$db"
Write-Output "LOG_OUT=$logOut"
Write-Output "LOG_ERR=$logErr"

if ($isExcelSink -and (Test-Path $excel)) { Write-Output "EXCEL_TAMANHO=$((Get-Item $excel).Length)" }
if (Test-Path $db) { Write-Output "DB_TAMANHO=$((Get-Item $db).Length)" }


$pyExe = Join-Path $dir ".venv\\Scripts\\python.exe"
if (!(Test-Path $pyExe)) { $pyExe = "python" }

if (Test-Path $db) {
  $py = @"
import sqlite3
import time
from pathlib import Path
p = Path("wechat_receipt_state.db").resolve()
conn = sqlite3.connect(p)
cur = conn.cursor()
receipt_cols = {str(row[1]).lower() for row in cur.execute("PRAGMA table_info(receipts)").fetchall()}
rows = cur.execute("SELECT status, COUNT(*) FROM files GROUP BY status ORDER BY 2 DESC").fetchall()
print('FILA_STATUS=', rows)
pending = cur.execute("SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing')").fetchone()[0]
pending_24h = cur.execute(
    "SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing') AND mtime < ?",
    (time.time() - 24 * 3600,),
).fetchone()[0]
waiting_original = cur.execute(
    "SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing') AND last_error='WAITING_ORIGINAL_MEDIA'"
).fetchone()[0]
waiting_temp = cur.execute(
    "SELECT COUNT(*) FROM files WHERE status IN ('pending','retry','processing') AND last_error IN ('WAITING_TEMP_CONTEXT','WAITING_TEMP_DB_MATCH')"
).fetchone()[0]
exceptions = cur.execute(
    "SELECT COUNT(*) FROM files WHERE status='exception'"
).fetchone()[0]
print('FILA_PENDENTE=', pending)
print('FILA_PENDENTE_24H=', pending_24h)
print('AGUARDANDO_IMAGEM_MELHOR=', waiting_original)
print('AGUARDANDO_TEMP_UNICO=', waiting_temp)
print('EXCECOES=', exceptions)
recent_review = cur.execute(
    "SELECT review_needed, COUNT(*) FROM receipts WHERE ingested_at >= ? GROUP BY review_needed ORDER BY review_needed",
    (time.time() - 24 * 3600,),
).fetchall()
message_jobs_exists = cur.execute(
    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_jobs'"
).fetchone()[0]
print('RECEIPTS_24H_REVIEW=', recent_review)
direct_24h = cur.execute(
    "SELECT COUNT(*) FROM receipts WHERE ingested_at >= ? AND resolution_source='direct_image'",
    (time.time() - 24 * 3600,),
).fetchone()[0]
ui_success_24h = cur.execute(
    """
    SELECT COUNT(*)
    FROM receipts r
    JOIN message_jobs m ON m.msg_svr_id = r.msg_svr_id
    WHERE r.ingested_at >= ?
      AND m.last_ui_result LIKE 'ui_%'
    """,
    (time.time() - 24 * 3600,),
).fetchone()[0] if message_jobs_exists else 0
ui_failed_24h = cur.execute(
    """
    SELECT COUNT(*)
    FROM message_jobs
    WHERE last_seen_at >= ?
      AND last_ui_result IN (
        'hover_without_materialized_media',
        'photo_open_action_failed',
        'context_menu_no_open_action'
      )
    """,
    (time.time() - 24 * 3600,),
).fetchone()[0] if message_jobs_exists else 0
print('AUTO_DOWNLOADED_DIRECT_24H=', direct_24h)
print('UI_FORCED_DOWNLOAD_SUCCESS_24H=', ui_success_24h)
print('UI_FORCED_DOWNLOAD_FAILED_24H=', ui_failed_24h)
last_fields = [
    "source_kind",
    "source_path",
    "amount",
    "txn_date",
    "txn_time",
    "excel_sheet",
    "datetime(ingested_at,'unixepoch','localtime')",
]
if "client" in receipt_cols:
    last_fields.insert(2, "client")
if "bank" in receipt_cols:
    last_fields.insert(3 if "client" in receipt_cols else 2, "bank")
if "msg_svr_id" in receipt_cols:
    last_fields.insert(1, "msg_svr_id")
if "talker" in receipt_cols:
    insert_at = 2 if "msg_svr_id" in receipt_cols else 1
    last_fields.insert(insert_at, "talker")
if "resolved_media_path" in receipt_cols:
    last_fields.insert(2 if "talker" in receipt_cols else 1, "resolved_media_path")
if "resolution_source" in receipt_cols:
    last_fields.insert(3 if "resolved_media_path" in receipt_cols else 2, "resolution_source")
if "verification_status" in receipt_cols:
    last_fields.insert(4 if "resolution_source" in receipt_cols else 3, "verification_status")
last = cur.execute(f"SELECT {', '.join(last_fields)} FROM receipts ORDER BY ingested_at DESC LIMIT 1").fetchone()
print('ULTIMO=', last)
recent = cur.execute(
    f"SELECT {', '.join(last_fields)} FROM receipts ORDER BY ingested_at DESC LIMIT 5"
).fetchall()
print('ULTIMOS_5=')
for row in recent:
    print('  ', row)
last_resolution = cur.execute(
    "SELECT value FROM meta WHERE key='last_resolution_source' LIMIT 1"
).fetchone()
last_verification = cur.execute(
    "SELECT value FROM meta WHERE key='last_verification_status' LIMIT 1"
).fetchone()
ui_force_pending = 0
ui_force_running = 0
batch_groups_pending = 0
if message_jobs_exists:
    ui_force_pending = cur.execute(
        "SELECT COUNT(*) FROM message_jobs WHERE state='UI_FORCE_PENDING'"
    ).fetchone()[0]
    ui_force_running = cur.execute(
        "SELECT COUNT(*) FROM message_jobs WHERE state='UI_FORCE_RUNNING'"
    ).fetchone()[0]
    batch_groups_pending = cur.execute(
        "SELECT COUNT(DISTINCT talker) FROM message_jobs WHERE state='UI_FORCE_PENDING'"
    ).fetchone()[0]
last_exception = cur.execute(
    "SELECT value FROM meta WHERE key='last_exception_reason' LIMIT 1"
).fetchone()
last_ui_result = cur.execute(
    "SELECT value FROM meta WHERE key='last_ui_result' LIMIT 1"
).fetchone()
last_ui_talker = cur.execute(
    "SELECT value FROM meta WHERE key='last_ui_talker' LIMIT 1"
).fetchone()
print('LAST_RESOLUTION_SOURCE=', None if last_resolution is None else last_resolution[0])
print('LAST_VERIFICATION_STATUS=', None if last_verification is None else last_verification[0])
print('UI_FORCE_PENDING=', ui_force_pending)
print('UI_FORCE_RUNNING=', ui_force_running)
print('BATCH_GROUPS_PENDING=', batch_groups_pending)
print('LAST_UI_RESULT=', None if last_ui_result is None else last_ui_result[0])
print('LAST_UI_TALKER=', None if last_ui_talker is None else last_ui_talker[0])
print('LAST_EXCEPTION_REASON=', None if last_exception is None else last_exception[0])
print('LAST_EXCEPTION_REASON_IS_HISTORICAL=', True)
conn.close()
"@
  Push-Location $dir
  try {
    $py | & $pyExe -X utf8 -
  } finally {
    Pop-Location
  }
}

if (Test-Path $logOut) {
  Write-Output "----- ULTIMAS LINHAS OUT -----"
  Get-Content $logOut -Tail 20
}
if (Test-Path $logErr) {
  Write-Output "----- ULTIMAS LINHAS ERR -----"
  Get-Content $logErr -Tail 20
}
