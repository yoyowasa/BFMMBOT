# WORKFLOW_CFD（録る→再生→紙→本番）

このプロジェクトの「実稼働までの道筋」を、手順と成果物（ログ）で固定します。

## 0) 設定の読み方（重要）
- `configs/base.yml` を土台に、`configs/paper.yml` / `configs/live.yml` / `configs/backtest.yml` を **deep merge** します（実体：`src/core/utils.py`）。
- その後、`env_overrides.<env>` があれば最後に上書きされます。

## 1) 録る（WS → NDJSONテープ）
目的：board/executions を **そのまま**保存し、後で再生できる状態にする。
```powershell
poetry run python -m src.cli.ingest --config configs/paper.yml --outfile data/raw/FX_BTC_JPY_YYYYMMDD.ndjson --duration_min 30
```
成果物：
- `data/raw/*.ndjson`（1行=1イベント）

## 2) 再生（テープ → 要約/最小シミュ）
目的：期間指定で読み、イベント件数や最小シミュの動作を確認する。
```powershell
poetry run python -m src.cli.backtest --config configs/backtest.yml --tape data/raw/FX_BTC_JPY_YYYYMMDD.ndjson
poetry run python -m src.cli.backtest --config configs/backtest.yml --tape data/raw/FX_BTC_JPY_YYYYMMDD.ndjson --simulate
```
成果物：
- `data/results/backtest_summary_*.json`
- `--simulate` の場合は `logs/*/*.parquet` も生成（orders/trades/analytics）

## 3) 紙（観察・疑似約定）
目的：**実注文なし**で、判断・ログ・監視・停止手順を固める。
```powershell
poetry run python -m src.cli.trade --config configs/paper.yml
poetry run python -m src.cli.trade --config configs/live.yml --paper
```
成果物（共通）：
- `logs/orders/*`（発注ログ：Parquet＋NDJSONミラー）
- `logs/trades/*`（約定/PnLログ：Parquet＋NDJSONミラー）
- `logs/analytics/*`（decisionログ：Parquet＋NDJSONミラー）
- `logs/runtime/*`（run.log / heartbeat）

## 4) 本番直前（dry-run）
目的：**実注文なし**で、鍵・権限・WS/REST経路の確認を行う。
```powershell
poetry run python -m src.cli.health --product-code FX_BTC_JPY
poetry run python -m src.cli.trade --config configs/live.yml --strategy stall_then_strike --dry-run
```
注意：
- dry-runの自動停止は `dry_run_max_sec`（秒）で制御します。

## 5) 初回実発注（canary）
目的：**実注文を1回でも通す**が、必ず止まる（時間制限）状態で踏む。
```powershell
poetry run python -m src.cli.trade --config configs/live.yml --strategy stall_then_strike
```
前提：
- `configs/live.yml` に `canary_minutes`（分）が設定されている
