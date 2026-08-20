<#
.SYNOPSIS
    Apaga os intermediarios de merge vazados em .runtime.

.DESCRIPTION
    Cada ciclo de merge do wechat_receipt_daemon descriptografa cada shard do
    WeChat num arquivo intermediario de ~200 MB chamado
    "de_msgN_wechat_merge.db.tmp<seq>". Ate 2026-08-11 nenhum caminho de saida
    apagava os extras: 1482 deles encheram um disco de 465 GB em tres dias.

    O daemon agora limpa os proprios arquivos e varre orfaos no boot. Este
    script e a rede de seguranca independente do processo -- roda mesmo com o
    daemon morto, travado ou substituido.

    NUNCA remove: wechat_merge.db (o indice vivo), seus sidecars -wal/-shm,
    os caches base_msg*.cache.db nem qualquer coisa fora dos padroes abaixo.

.PARAMETER DryRun
    Lista o que seria apagado sem apagar nada.

.PARAMETER KeepDays
    Quantos dias manter, contando hoje. Padrao: 1 -- nada escrito HOJE e
    tocado; so apaga do dia anterior para tras. Regra definida pelo Felipe em
    2026-08-11: arquivo do dia corrente pode estar em uso, entao nunca some.
    KeepDays=2 preserva hoje e ontem, e assim por diante.

.EXAMPLE
    .\cleanup-runtime-tmp.ps1 -DryRun
    .\cleanup-runtime-tmp.ps1
    .\cleanup-runtime-tmp.ps1 -KeepDays 3
#>
[CmdletBinding()]
param(
    [switch]$DryRun,
    [ValidateRange(1, 365)]
    [int]$KeepDays = 1
)

$ErrorActionPreference = 'Stop'

$RuntimeDir = Join-Path (Split-Path -Parent $PSScriptRoot) '.runtime'
$LogFile    = Join-Path $RuntimeDir 'cleanup-tmp.log'
$MergeName  = 'wechat_merge.db'

# Protegidos explicitamente, mesmo que um padrao futuro passe a casar.
$Keep = @($MergeName, "$MergeName-wal", "$MergeName-shm")

function Write-Log {
    param([string]$Message)
    $stamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    $line  = "[$stamp] $Message"
    Write-Output $line
    try { Add-Content -LiteralPath $LogFile -Value $line -Encoding UTF8 } catch { }
}

if (-not (Test-Path -LiteralPath $RuntimeDir)) {
    Write-Log "ERRO: .runtime nao encontrado em $RuntimeDir"
    exit 1
}

# Meia-noite de hoje menos os dias extras a preservar. Com o padrao KeepDays=1
# o corte e hoje 00:00: qualquer arquivo escrito hoje sobrevive, aconteca o que
# acontecer, e so o dia anterior para tras e removido.
$cutoff = (Get-Date).Date.AddDays(-($KeepDays - 1))

# "<qualquer>.db.tmp<seq>" cobre o tmp da tentativa E os intermediarios
# "de_msgN_..." escopados nele. "de_msgN_wechat_merge.db" cobre o merge que
# escreveu direto no indice vivo (sem sufixo .tmp).
$targets = Get-ChildItem -LiteralPath $RuntimeDir -Force -File -ErrorAction SilentlyContinue |
    Where-Object {
        $Keep -notcontains $_.Name -and
        $_.LastWriteTime -lt $cutoff -and
        ($_.Name -match '\.db\.tmp\d*$' -or $_.Name -match "^de_msg\d+_$([regex]::Escape($MergeName))$")
    }

$cutoffLabel = $cutoff.ToString('yyyy-MM-dd HH:mm')

if (-not $targets) {
    Write-Log "nada a limpar (0 orfaos anteriores a $cutoffLabel)"
    exit 0
}

$totalGB = [math]::Round((($targets | Measure-Object Length -Sum).Sum) / 1GB, 2)

if ($DryRun) {
    Write-Log "DRY-RUN: $($targets.Count) orfaos anteriores a $cutoffLabel, $totalGB GB seriam liberados"
    $targets | Sort-Object Length -Descending |
        Select-Object Name, @{n='MB';e={[math]::Round($_.Length/1MB,1)}}, LastWriteTime |
        Format-Table -AutoSize | Out-String | Write-Output
    exit 0
}

$removed = 0
$freed   = 0
$locked  = 0
foreach ($f in $targets) {
    try {
        $size = $f.Length
        Remove-Item -LiteralPath $f.FullName -Force -ErrorAction Stop
        $removed++
        $freed += $size
    } catch {
        # Em uso por um merge vivo: fica para a proxima passada.
        $locked++
    }
}

$freeGB = [math]::Round((Get-Volume -DriveLetter C).SizeRemaining / 1GB, 1)
Write-Log ("removidos={0} liberados={1} GB em_uso={2} corte=<{3} livre_em_C={4} GB" -f `
    $removed, [math]::Round($freed / 1GB, 2), $locked, $cutoffLabel, $freeGB)
