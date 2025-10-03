#!/usr/bin/env bash
# 何をするスクリプトか：
#   Zero→Reopen Pop 戦略を "紙トレ設定" で最短ワンコマンド起動する。
#   追加の引数はそのまま CLI に渡る（例：--log-level DEBUG など）。

set -euo pipefail

# プロジェクトルートへ移動（どこから実行しても安全）
cd "$(dirname "$0")/.."

# 紙トレ起動コマンド（何をするか：configs/paper.yml を使って zero_reopen_pop を起動）
poetry run python -m src.cli.trade --config configs/paper.yml --strategy zero_reopen_pop "$@"
