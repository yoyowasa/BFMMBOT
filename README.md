![CI](https://github.com/yoyowasa/BFMMBOT/actions/workflows/ci.yml/badge.svg)

- [CODEX: Zero→Reopen Pop（スプレッド0→再拡大の1拍だけ取る戦略）](docs/CODEX_ZERO_REOPEN_POP.md)  <!-- 何をするか：戦略の詳細仕様と運用ワークフローの導線 -->

### 起動コマンド（シンプル）

#### デフォルトONリスト（`configs/*.yml` の `strategies:` 配列）をまとめて起動
- 紙トレード: `poetry run python -m src.cli.trade --config configs/paper.yml`
- 本番: `poetry run python -m src.cli.trade --config configs/live.yml`
  - 起動直後に `logs/runtime/run.log` を確認すると `paper start: ... strategy=<名前>` の行が出力され、実際に動いている戦略名を把握できます。

#### zero_reopen_pop
- 紙トレード: `./scripts/run_zero_reopen_paper.sh`
  - CLI直接実行: `poetry run python -m src.cli.trade --config configs/paper.yml --strategy zero_reopen_pop`
- 本番: `poetry run python -m src.cli.trade --config configs/live.yml --strategy zero_reopen_pop`

#### stall_then_strike
- 紙トレード: `poetry run python -m src.cli.trade --config configs/paper.yml --strategy stall_then_strike`
- 本番: `poetry run python -m src.cli.trade --config configs/live.yml --strategy stall_then_strike`

#### cancel_add_gate
- 紙トレード: `poetry run python -m src.cli.trade --config configs/paper.yml --strategy cancel_add_gate`
- 本番: `poetry run python -m src.cli.trade --config configs/live.yml --strategy cancel_add_gate`

#### age_microprice
- 紙トレード: `poetry run python -m src.cli.trade --config configs/paper.yml --strategy age_microprice`
- 本番: `poetry run python -m src.cli.trade --config configs/live.yml --strategy age_microprice`

説明：--live や環境変数は不要です。渡す --config の中身（接続先や鍵）だけで紙／本番が決まります。

### テキストログの切り分け設定

- 共通の運用ログ（起動・ガードレール・致命的エラーなど）は `logging.run_log_template` の値に書き出されます。既定値は `configs/base.yml` にある `logs/runtime/run.log` で、各環境の設定ファイル（`configs/paper.yml` / `configs/live.yml` など）から上書き可能です。
- 戦略ごとの意思決定ログを分けて保存したい場合は、同じく `configs/*` の `logging.strategy_log_template` にテンプレートを設定してください。例: `logs/runtime/strategies/{strategy}.log` とすると、`zero_reopen_pop` なら `logs/runtime/strategies/zero_reopen_pop.log` が生成されます。
- 戦略用ログのレベルを共通ログと変えたいときは `logging.strategy_level` を指定します。未指定（`null`）の場合は `logging.level` の値がそのまま使われます。
- これらのテンプレートは `{strategy}` を埋め込み可能な通常の `str.format` 互換です。不正な書式を指定した場合は警告を残した上で原文どおりのファイル名で出力されます。
