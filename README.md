![CI](https://github.com/yoyowasa/BFMMBOT/actions/workflows/ci.yml/badge.svg)

- [CODEX: Zero→Reopen Pop（スプレッド0→再拡大の1拍だけ取る戦略）](docs/CODEX_ZERO_REOPEN_POP.md)  <!-- 何をするか：戦略の詳細仕様と運用ワークフローの導線 -->

### 起動コマンド（シンプル）

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
