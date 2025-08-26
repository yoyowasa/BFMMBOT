# src/cli/trade.py
# 役割：paper実行のCLI（設定読込→エンジン起動→Ctrl+Cで安全終了）
# 文書の 8.3 ペーパー運用に対応（--config / --strategy）。:contentReference[oaicite:11]{index=11}
from __future__ import annotations

import argparse  # 引数処理
import asyncio  # 非同期ランタイム
from loguru import logger  # 実行ログ

from src.core.utils import load_config  # 【関数】設定ローダー（base＋上書き）:contentReference[oaicite:12]{index=12}
from src.runtime.engine import PaperEngine  # 【関数】paperエンジン（本ステップ）

def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：--config（必須）/ --strategy（#1 既定 か #2）"""
    p = argparse.ArgumentParser(description="Run paper trading (real-time)")
    p.add_argument("--config", required=True, help="configs/paper.yml など")
    p.add_argument("--strategy", default="stall_then_strike", choices=["stall_then_strike", "cancel_add_gate"],
                   help="どの戦略で動かすか（#1 or #2）")
    return p.parse_args()

def main() -> None:
    """【関数】エントリ：設定を読み、paperエンジンを走らせる"""
    args = _parse_args()
    cfg = load_config(args.config)
    if cfg.env != "paper":
        logger.warning(f"env is '{cfg.env}' (expected 'paper') - 続行はします")
    engine = PaperEngine(cfg, args.strategy)
    try:
        asyncio.run(engine.run_paper())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
