Set-StrictMode -Version Latest

function Die([string]$msg) {
  # 役割: エラー理由を表示して即終了する
  Write-Error "[run_real_logs] ERROR: $msg"
  exit 1
}

function Require-Env([string]$name) {
  # 役割: 必須の環境変数が空なら止める
  $v = [string]([Environment]::GetEnvironmentVariable($name))
  if ([string]::IsNullOrWhiteSpace($v)) { Die "env '$name' is required" }
}

function Get-GitSha {
  # 役割: git sha を取得する（取得できなければ unknown）
  try {
    $sha = (& git rev-parse HEAD 2>$null)
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($sha)) { return $sha.Trim() }
  } catch {}
  return "unknown"
}

function Write-Meta([string]$path, [hashtable]$obj) {
  # 役割: 実行メタ情報をjsonで保存する（追跡と監査のため）
  $json = ($obj | ConvertTo-Json -Compress)
  Set-Content -Path $path -Value $json -Encoding utf8
}

$logDir = [Environment]::GetEnvironmentVariable("LOG_DIR")
if ([string]::IsNullOrWhiteSpace($logDir)) { $logDir = [Environment]::GetEnvironmentVariable("LOG_PATH") }
if ([string]::IsNullOrWhiteSpace($logDir)) { $logDir = "logs" }
New-Item -ItemType Directory -Force -Path $logDir | Out-Null  # 役割: ログ保存先を必ず作る

$dryRun = [Environment]::GetEnvironmentVariable("DRY_RUN")
if ([string]::IsNullOrWhiteSpace($dryRun)) { $dryRun = "1" }  # 役割: 既定は安全側

if ($dryRun -eq "0") {
  # 役割: 実運用誤爆防止（明示許可が無いと止める）
  $ok = [Environment]::GetEnvironmentVariable("REAL_RUN_OK")
  if ($ok -ne "1") { Die "DRY_RUN=0 detected. Set REAL_RUN_OK=1 to allow real run." }
}

$tsUtc = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")  # 役割: 実行時刻（UTC）
$runId = [Environment]::GetEnvironmentVariable("RUN_ID")
if ([string]::IsNullOrWhiteSpace($runId)) { $runId = "real_${tsUtc}_$PID" }  # 役割: 実行ID（上書き可）

$outLog = Join-Path $logDir ("real_run_{0}.log" -f $runId)  # 役割: 実行ログ
$metaJson = Join-Path $logDir ("real_run_{0}.meta.json" -f $runId)  # 役割: メタ情報

$cmdString = ""
if ($args.Count -gt 0) {
  # 役割: 引数があればそれをコマンドとして扱う
  $cmdString = ($args -join " ")
} else {
  # 役割: 引数が無ければ REAL_LOG_CMD を使う
  Require-Env "REAL_LOG_CMD"
  $cmdString = [Environment]::GetEnvironmentVariable("REAL_LOG_CMD")
}

$gitSha = Get-GitSha  # 役割: git sha を取得する

Write-Meta $metaJson @{
  run_id = $runId
  ts_utc = $tsUtc
  log_dir = $logDir
  cmd = $cmdString
  git_sha = $gitSha
}  # 役割: メタ情報を書き出す

"[run_real_logs] LOG_DIR=$logDir" | Tee-Object -FilePath $outLog -Append  # 役割: 実行環境をログに残す
"[run_real_logs] RUN_ID=$runId" | Tee-Object -FilePath $outLog -Append  # 役割: 実行IDをログに残す
"[run_real_logs] DRY_RUN=$dryRun" | Tee-Object -FilePath $outLog -Append  # 役割: DRY_RUNをログに残す
"[run_real_logs] CMD=$cmdString" | Tee-Object -FilePath $outLog -Append  # 役割: 実行コマンドをログに残す

& cmd.exe /c $cmdString 2>&1 | Tee-Object -FilePath $outLog -Append  # 役割: コマンドを実行し、出力をファイルへ保存する
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) { exit $exitCode }  # 役割: 失敗時は同じ終了コードで落とす
