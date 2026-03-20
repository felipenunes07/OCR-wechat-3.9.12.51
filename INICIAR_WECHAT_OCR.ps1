$ErrorActionPreference = "Stop"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $dir "wechat_receipt_daemon.py"
$db = Join-Path $dir "wechat_receipt_state.db"
$excel = Join-Path $dir "pagamentos_wechat.xlsx"
$sinkConfigPath = Join-Path $dir "sink_config.json"
$logOut = Join-Path $dir "wechat_receipt.out.log"
$logErr = Join-Path $dir "wechat_receipt.err.log"
$log = $logOut
$pidf = Join-Path $dir "wechat_receipt.pid"

# Prefer the project venv python, but fall back to system python if needed.
$py = Join-Path $dir ".venv\\Scripts\\python.exe"
if (!(Test-Path $py)) { $py = "python" }

function Get-WeChatFileStorageRoot {
  $doc = [Environment]::GetFolderPath("MyDocuments")
  $wechatFiles = Join-Path $doc "WeChat Files"
  if (!(Test-Path $wechatFiles)) { return $null }

  # Pick the most recently changed account folder that has FileStorage.
  $accounts = Get-ChildItem -Path $wechatFiles -Directory -ErrorAction SilentlyContinue | Where-Object {
    Test-Path (Join-Path $_.FullName "FileStorage")
  } | Sort-Object LastWriteTime -Descending

  if ($accounts -and $accounts.Count -gt 0) {
    return (Join-Path $accounts[0].FullName "FileStorage")
  }
  return $null
}

function Convert-ToCliArg([string]$value) {
  if ($null -eq $value) { return '""' }
  if ($value -notmatch '[\s"]') { return $value }
  return '"' + ($value -replace '(\\*)"', '$1$1\"') + '"'
}

$watch = Get-WeChatFileStorageRoot

if (!(Test-Path $script)) { throw "Script nao encontrado: $script" }
if (!$watch -or !(Test-Path $watch)) {
  throw "Pasta WeChat nao encontrada. Verifique se existe 'Documentos\\WeChat Files\\<wxid>\\FileStorage'."
}

$sinkMode = "excel"
$gsheetRef = ""
$gsheetWorksheet = ""
$gsheetReviewWorksheet = "Revisar"
$recentFilesHours = 24
$originalWaitSeconds = 90
$tempCorrelationSeconds = 30
$resolutionMode = "db-first"
$verificationColumnName = "STATUS_VERIFICACAO"
$uiForceDownloadEnabled = $true
$uiForceDelaySeconds = 15
$uiForceScope = "mapped-groups"
$uiFocusPolicy = "immediate"
$uiBatchMode = "group-sequential"
$uiItemTimeoutSeconds = 5
$uiRetryBackoffSeconds = "5,10,20,40"
$dbMergePath = Join-Path $dir ".runtime\\wechat_merge.db"
$googleCredentialsPath = Join-Path $dir "google_service_account.json"
if (Test-Path $sinkConfigPath) {
  $sinkConfig = Get-Content $sinkConfigPath -Raw | ConvertFrom-Json
  if ($sinkConfig.sink_mode) { $sinkMode = [string]$sinkConfig.sink_mode }
  if ($sinkConfig.spreadsheet_url) { $gsheetRef = [string]$sinkConfig.spreadsheet_url }
  if ($sinkConfig.spreadsheet_id -and [string]::IsNullOrWhiteSpace($gsheetRef)) { $gsheetRef = [string]$sinkConfig.spreadsheet_id }
  if ($sinkConfig.worksheet) { $gsheetWorksheet = [string]$sinkConfig.worksheet }
  if ($sinkConfig.review_worksheet) { $gsheetReviewWorksheet = [string]$sinkConfig.review_worksheet }
  if ($sinkConfig.recent_files_hours) { $recentFilesHours = [int]$sinkConfig.recent_files_hours }
  if ($sinkConfig.original_wait_seconds) { $originalWaitSeconds = [int]$sinkConfig.original_wait_seconds }
  if ($sinkConfig.temp_correlation_seconds) { $tempCorrelationSeconds = [int]$sinkConfig.temp_correlation_seconds }
  if ($sinkConfig.resolution_mode) { $resolutionMode = [string]$sinkConfig.resolution_mode }
  if ($sinkConfig.verification_column_name) { $verificationColumnName = [string]$sinkConfig.verification_column_name }
  if ($null -ne $sinkConfig.ui_force_download_enabled) { $uiForceDownloadEnabled = [bool]$sinkConfig.ui_force_download_enabled }
  if ($sinkConfig.ui_force_delay_seconds) { $uiForceDelaySeconds = [int]$sinkConfig.ui_force_delay_seconds }
  if ($sinkConfig.ui_force_scope) { $uiForceScope = [string]$sinkConfig.ui_force_scope }
  if ($sinkConfig.ui_focus_policy) { $uiFocusPolicy = [string]$sinkConfig.ui_focus_policy }
  if ($sinkConfig.ui_batch_mode) { $uiBatchMode = [string]$sinkConfig.ui_batch_mode }
  if ($sinkConfig.ui_item_timeout_seconds) { $uiItemTimeoutSeconds = [int]$sinkConfig.ui_item_timeout_seconds }
  if ($sinkConfig.ui_retry_backoff_seconds) { $uiRetryBackoffSeconds = (($sinkConfig.ui_retry_backoff_seconds | ForEach-Object { [string]$_ }) -join ",") }
  if ($sinkConfig.db_merge_path) { $dbMergePath = [string]$sinkConfig.db_merge_path }
  if ($sinkConfig.google_credentials_path) {
    $googleCredentialsPath = [string]$sinkConfig.google_credentials_path
    if (-not [System.IO.Path]::IsPathRooted($googleCredentialsPath)) {
      $googleCredentialsPath = Join-Path $dir $googleCredentialsPath
    }
  }
}
if (-not [System.IO.Path]::IsPathRooted($dbMergePath)) {
  $dbMergePath = Join-Path $dir $dbMergePath
}

# Atualiza automaticamente o mapa hash->nome de grupo antes de iniciar.
$mapUpdater = Join-Path $dir "refresh_group_map.py"
if (Test-Path $mapUpdater) {
  & $py -X utf8 $mapUpdater | Out-Null
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

$arguments = @(
  "-X", "utf8",
  "-u", $script,
  "--watch-root", $watch,
  "--db-path", $db,
  "--reconcile-seconds", "90",
  "--recent-files-hours", "$recentFilesHours",
  "--min-confidence", "0.55",
  "--resolution-mode", $resolutionMode,
  "--db-merge-path", $dbMergePath,
  "--original-wait-seconds", "$originalWaitSeconds",
  "--temp-correlation-seconds", "$tempCorrelationSeconds",
  "--verification-column-name", $verificationColumnName,
  "--ui-force-download-enabled", ($(if ($uiForceDownloadEnabled) { "true" } else { "false" })),
  "--ui-force-delay-seconds", "$uiForceDelaySeconds",
  "--ui-force-scope", $uiForceScope,
  "--ui-focus-policy", $uiFocusPolicy,
  "--ui-batch-mode", $uiBatchMode,
  "--ui-item-timeout-seconds", "$uiItemTimeoutSeconds",
  "--ui-retry-backoff-seconds", $uiRetryBackoffSeconds,
  "--client-map-path", (Join-Path $dir "clientes_grupos.json")
)

if ($sinkMode -eq "google-sheets") {
  if ([string]::IsNullOrWhiteSpace($gsheetRef)) {
    throw "sink_config.json esta com Google Sheets ativo, mas sem spreadsheet_url/spreadsheet_id."
  }
  if (!(Test-Path $googleCredentialsPath)) {
    throw "Credencial Google nao encontrada: $googleCredentialsPath. Coloque o JSON da service account nesse caminho e compartilhe a planilha com o e-mail dela."
  }
  $arguments += @("--sink-mode", "google-sheets", "--gsheet-ref", $gsheetRef, "--google-credentials-path", $googleCredentialsPath)
  if (-not [string]::IsNullOrWhiteSpace($gsheetWorksheet)) {
    $arguments += @("--gsheet-worksheet", $gsheetWorksheet)
  }
  if (-not [string]::IsNullOrWhiteSpace($gsheetReviewWorksheet)) {
    $arguments += @("--gsheet-review-worksheet", $gsheetReviewWorksheet)
  }
} else {
  $arguments += @("--sink-mode", "excel", "--excel-path", $excel)
}

$argumentLine = ($arguments | ForEach-Object { Convert-ToCliArg ([string]$_) }) -join " "
$p = Start-Process -FilePath $py -ArgumentList $argumentLine -WorkingDirectory $dir -RedirectStandardOutput $logOut -RedirectStandardError $logErr -PassThru
$p.Id | Set-Content -Path $pidf -Encoding ascii
Start-Sleep -Seconds 2
if (Get-Process -Id $p.Id -ErrorAction SilentlyContinue) {
  Write-Output "INICIADO PID=$($p.Id)"
  Write-Output "LOG=$log"
  if ($sinkMode -eq "google-sheets") {
    Write-Output "DESTINO=GOOGLE_SHEETS"
    Write-Output "PLANILHA=$gsheetRef"
  } else {
    Write-Output "EXCEL=$excel"
  }
  Write-Output "RESOLUTION_MODE=$resolutionMode"
  Write-Output "DB_MERGE_PATH=$dbMergePath"
  Write-Output "UI_FORCE_DOWNLOAD_ENABLED=$uiForceDownloadEnabled"
  Write-Output "UI_FORCE_DELAY_SECONDS=$uiForceDelaySeconds"
} else {
  Write-Output "FALHOU_INICIAR. Veja log: $log"
}

