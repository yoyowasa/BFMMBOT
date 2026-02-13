# このスクリプトが何をするか: ENTRY_GUARD の require 環境変数をセットして paper を起動する
if (-not $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE) { $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE="0" }
if (-not $env:ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC) { $env:ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC="999999" }
if (-not $env:ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99) { $env:ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99="999999" }
if (-not $env:ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX) { $env:ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX="999999999" }
if (-not $env:ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE) { $env:ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE="1.0" }
if (-not $env:ENTRY_GUARD_REQUIRE_ACTIVE_COUNT) { $env:ENTRY_GUARD_REQUIRE_ACTIVE_COUNT="0" }
if (-not $env:ENTRY_GUARD_REQUIRE_ERRORS) { $env:ENTRY_GUARD_REQUIRE_ERRORS="0" }

# このスクリプトが何をするか: paper を起動する（必要ならここをあなたの普段の起動コマンドに置換）
.\.venv\Scripts\python.exe -m src.app.run_paper

# このスクリプトが何をするか: 最新ログの PROMOTE_GUARD_RESULT と SUMMARY を表示して、pass=false を見逃さない
$dir="C:\BOT\stall_then_strike\logs\runtime"
$log = Get-ChildItem -Path $dir -File | Where-Object { $_.Name -match '^paper-\d{8}-\d{6}\.log$' } | Sort-Object LastWriteTime -Descending | Select-Object -First 1
"LOG="+$log.Name
Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT|ENTRY_GUARD_ENFORCE_SUMMARY" | ForEach-Object { $_.Line }

# このスクリプトが何をするか: pass=false のときは exit 2 で異常終了させる
$pr = Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT" | Select-Object -Last 1
if ($pr -and ($pr.Line -match 'pass=false')) { exit 2 }
