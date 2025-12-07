@echo off
rem このバッチは「開発用の trade 起動口」を 1 本に固定するためのものです。
rem 必ずリポジトリ直下（このバッチファイルがあるフォルダ）をカレントにしてから
rem poetry 経由で src.cli.trade を起動します。

rem 自分自身が置かれているフォルダに移動する
cd /d "%~dp0.."

rem 仮想環境(poetry)を使って trade.py を実行する
poetry run python -m src.cli.trade --config configs/live.yaml --strategy stall_then_strike cancel_add_gate
