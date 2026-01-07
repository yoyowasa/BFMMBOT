# HANDOFF TEMPLATE（運用引継ぎの最小テンプレ）

## 0) 目的
- 誰が見ても「いま何が動いていて、どう止めて、どう戻すか」が分かる状態にする。

## 1) 現在地
- 日付：
- 運用者：
- 取引所／銘柄：FX_BTC_JPY
- 実行モード：`env=live` / `env=paper` / `dry-run` / `--paper`
- 実行コマンド：
- Git（ロールバック起点）：
  - `git rev-parse HEAD`：

## 2) 設定（この運転で効かせた鍵）
- `configs/live.yml`：
  - `canary_minutes`：
  - `dry_run_max_sec`：
  - `cancel_all_on_start`：
  - `risk.max_inventory`：
  - `risk.kill.*`：

## 3) 監視（見る場所）
- `logs/runtime/run.log`
- `logs/runtime/heartbeat.ndjson`
- `logs/orders/order_log.ndjson`
- `logs/trades/trade_log.ndjson`
- `logs/analytics/decision_log.ndjson`

## 4) 結果（canaryの結果）
- 実注文が出たか（Yes/No）：
- 注文/取消/約定の件数（概算）：
- PnL/手数料（概算）：
- 停止理由（canary / signal / kill / exception）：
- 事故/ヒヤリ（あれば）：

## 5) 停止・復旧
- 通常停止：`Ctrl+C` / canary自動停止
- 強制停止：`taskkill /IM python.exe /F`
- 二重起動ロックが残った場合：`logs/runtime/trade_*.lock` を確認してから削除

## 6) 次にやること
- 例：dry-run → canary → 継続、閾値調整、ログ分析、コミット整理 など
