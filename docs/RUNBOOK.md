# RUNBOOK（live運用）

## 定義（このプロジェクトでの用語）
- **live（実稼働）**：`env=live` かつ **実注文が1件でも送信される状態**
- **paper（紙）**：`mode="paper"` または `--paper`（観察・疑似約定。実注文は出ない）
- **dry-run**：実注文なしで、接続・権限・注文経路（発注口）を確認する直前テスト
- **canary**：実注文ありの最小運転（**時間とサイズで必ず止まる**）

## Go/No-Go（最低限）
1) **作業ツリーが再現可能**
- `git status -sb` が把握できている（差分はコミット or 退避）
- `git rev-parse HEAD` をメモ（ロールバックの起点）

2) **鍵が入っている（.env）**
- `BF_API_KEY` / `BF_API_SECRET` / `BF_REGION=jp`
- ヘルスチェックでOKが出る（下記）

3) **止まるための鍵が入っている（設定）**
- `configs/live.yml` に `canary_minutes` がある（初回は **15分推奨**）
- `configs/live.yml` に `dry_run_max_sec` がある（dry-runの自動停止）

## 手順（推奨順）

### 1) health（実発注なし）
```powershell
poetry run python -m src.cli.health --product-code FX_BTC_JPY
```

### 2) dry-run（実発注なし）
```powershell
poetry run python -m src.cli.trade --config configs/live.yml --strategy stall_then_strike --dry-run
```
確認（最小）：
- `logs/runtime/run.log` に `live(dry-run): ...` が出る
- dry-run中は「発注が出ない」こと（placeのskipログはOK）

### 3) canary（実発注あり・必ず止まる）
前提：`configs/live.yml` の `canary_minutes` が 15（初回推奨）。
```powershell
poetry run python -m src.cli.trade --config configs/live.yml --strategy stall_then_strike
```
確認（最小）：
- `logs/runtime/run.log` に live系ログが出る
- `logs/orders/order_log.ndjson` に `action:"place"` が出る（＝送信経路が通った）
- `canary_minutes` 到達で自動停止し、終了シーケンスで全取消を試行する

### 4) 継続運転
- canaryが問題なければ、`canary_minutes` を延長 or 無効化して再起動
  - 無効化は `canary_minutes: 0`（またはキー削除）

## 監視（最低限）
その場で追う（PowerShell）：
```powershell
Get-Content logs\runtime\run.log -Tail 50 -Wait
Get-Content logs\runtime\heartbeat.ndjson -Tail 20 -Wait
Get-Content logs\orders\order_log.ndjson -Tail 20 -Wait
Get-Content logs\trades\trade_log.ndjson -Tail 20 -Wait
Get-Content logs\analytics\decision_log.ndjson -Tail 20 -Wait
```
まとめて確認：
```powershell
python tools\monitor_heartbeat.py --duration 600 --print-every 30
poetry run python -m src.cli.export --kind analytics --format csv --limit 200 --out data/results/decision_preview.csv
```

## 停止・復旧
### 通常停止
- canary：時間で自動停止
- 手動：`Ctrl+C`（liveは終了シーケンスで全取消を試行）

### 強制停止（最終手段）
```powershell
taskkill /IM python.exe /F
```
注意：
- 強制停止だと未約定が残る可能性があるため、次回起動時に `cancel_all_on_start: true` で掃除（または取引所UIで手動取消）

### 二重起動ロック（trade.lock）
- `src.cli.trade` は `logs/runtime/trade_<env>_<config>.lock` で多重起動を抑止
- 異常終了で残った場合のみ削除（中で動いていないことを確認してから）
```powershell
Remove-Item logs\runtime\trade_live_live_yml.lock
```

## ロールバック（戻し方）
- 事前にコミットしていれば `git reset --hard <hash>` で即戻せる
- 未コミットなら `git diff` を保存してから `git checkout -- .` などで戻す
