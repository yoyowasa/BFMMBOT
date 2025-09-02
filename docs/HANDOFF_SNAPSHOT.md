# HANDOFF SNAPSHOT（BF‑MMBOT / live 可動版）

## 1) 現在地（結論）
- **録る → 再生 → 紙トレ（paper） → 本番（live 小ロット）**まで導線が揃いました。  
- **live 実装**：exchange adapter（REST送信口）／イベントループ／TTL取消／在庫上限／ミッド変化ガード／Funding/メンテ窓停止／Kill‑Switch（日次PnL・DD）／**Canary自動停止**（分指定）／**安全停止（Ctrl+C/SIGTERMで全取消）**を実装。  
- **可観測性**：orders/trades/decision（Parquet＋**NDJSONミラー**）／**ハートビート**（start/pause/place/fill/kill＋定期 status=Q/A/R）／**窓イベントCSV**／回転 **run.log**。  
- **設定**：`configs/live.yml` に **tick_size / size{min,default,step} / fees.bps / tx.min_interval_ms / canary_minutes / cancel_all_on_start / logging.heartbeat_status_sec** を追加済み。  
（設計と用語はワークフロー文書／引継ぎサマリに準拠。戦略は #1/#2 稼働＋#3 追加に対応。） <!-- 参照: WORKFLOW_CFD / 引継ぎまとめ / 戦略リスト -->
  
## 2) 重要ファイルと“何をするか”（表なし）
- `src/core/exchange.py`：**bitFlyer REST送信口**（署名／sendchildorder／cancel／getchildorders／getpositions／429/5xxバックオフ）。  
- `src/runtime/live.py`：**live本体**（WS→板→戦略→注文、TTL取消、窓停止、在庫ガード、Kill、Canary、**ハートビート**、**窓CSV**、orders/trades/decision の記録、**手数料fee反映**、価格/サイズの**丸め**、送信スロットリング、**安全停止**）。  
- `src/cli/trade.py`：**起動CLI**（回転 run.log、`env: live` 分岐、`--dry-run` で実発注抑止）。  
- `src/cli/health.py`：**ヘルスチェックCLI**（`.env` / REST疎通確認、**実発注なし**）。  
- `configs/live.yml`：**本番設定**（小ロット・ガードON・可観測性ON）。  
- ログ出力：`logs/orders/*`、`logs/trades/*`、`logs/analytics/*`、`logs/runtime/heartbeat.ndjson`、`logs/events/*.csv`、`logs/runtime/run.log`。  
（構成・ログ仕様はワークフロー文書の定義どおりで固定スキーマ。）  

## 3) 運転コマンド（Go/No‑Goの順）
- **ヘルス確認（実発注なし）**  
  `poetry run python -m src.cli.health --product-code FX_BTC_JPY`
- **live：疎通だけ（実発注なし）**  
  `poetry run python -m src.cli.trade --config configs/live.yml --strategy stall_then_strike --dry-run`
- **live：小ロット本番（Canary 30–60分）**  
  `poetry run python -m src.cli.trade --config configs/live.yml --strategy stall_then_strike`  
  → Canary 到達・Kill・Signal いずれでも **全取消→停止**。心拍に `kill` を記録。

## 4) 安全装置（効く順）
- **窓停止**：メンテ／Funding 計算・授受の各窓で新規停止、既存は整理（enter/exit を CSV 記録）。  
- **ミッド変化ガード**：30秒比のbp変化が大きい間は停止。  
- **在庫上限**：`risk.max_inventory` 超で新規停止。  
- **Kill‑Switch**：`risk.kill.daily_pnl_jpy` / `risk.kill.max_dd_jpy` 到達で全取消→停止。  
- **Canary**：`canary_minutes` 超で全取消→停止（reason=`"canary"`）。  
- **安全停止**：Ctrl+C / SIGTERM で全取消→停止（reason=`"signal"`）。  
- **送信スロットリング**：`tx.min_interval_ms` 未満の連打を抑止（429回避）。  

## 5) 可観測性（“見える化”の柱）
- **ハートビート NDJSON**：`start/pause/place/fill/kill` と **status(Q/A/R)** を数秒ごと。  
- **orders**：`place/cancel/fill/partial` を固定列で記録。  
- **trades**：`fee` を含む **手数料込みPnL** を記録。  
- **decision**：`place/hold` 等の判断ログ（features_json は空でも可）。  
- **窓イベントCSV**：maintenance / funding の enter/exit。  
- **回転 run.log**：INFO/DEBUG、rotate_mb でローテーション。  

## 6) 設定の要点（live.yml）
- `tick_size`: 価格の最小刻み（例：1）。  
- `size: { min, default, step }`: **下限・既定・刻み丸め**を一元管理。  
- `fees.bps`: 手数料の bps（負値でリベート）。  
- `tx.min_interval_ms`: 新規送信の最小間隔。  
- `canary_minutes`: 最長運転時間（分）。  
- `cancel_all_on_start`: 起動直後に**既存注文を全取消**して開始。  
- `logging.heartbeat_status_sec`: 心拍 status の間隔（秒）。  

## 7) 戦略（ON / 追加）
- **#1 静止→一撃（Stall‑Then‑Strike）**、**#2 キャンセル比ゲート（Cancel‑Add Gate）**：ON。  
- **#3 エイジ×マイクロプライス**：追加済（A/B用）。  
（いずれも **板＋約定の素データ**だけで駆動。後続 OFF 戦略は雛形に準拠して段階導入可。）  

## 8) 次の改善候補（任意）
- bitFlyerの**fills照会**の強化（`getexecutions` 連携）による Fill 追跡の高精度化。  
- レポ自動化（EOD KPI／session_report の定期実行）。  

---

**根拠**：  
- 構成・運用手順・ログ仕様はワークフロー文書に準拠。  
- 到達点と残課題の区分、可観測性の柱は引継ぎサマリの整理と一致。  
- 戦略の定義・優先度は戦略リストの要点に基づく。  
