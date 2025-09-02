# src/cli/trade.py
# 役割：paper実行のCLI（設定読込→エンジン起動→Ctrl+Cで安全終了）
# 文書の 8.3 ペーパー運用に対応（--config / --strategy）。:contentReference[oaicite:11]{index=11}
from __future__ import annotations

import argparse  # 引数処理
import asyncio  # 非同期ランタイム
from loguru import logger  # 実行ログ
from pathlib import Path  # run.log の保存先を扱う

from src.core.utils import load_config  # 【関数】設定ローダー（base＋上書き）:contentReference[oaicite:12]{index=12}
from src.runtime.engine import PaperEngine  # 【関数】paperエンジン（本ステップ）
from src.runtime.live import run_live  # 何をするか：本番（live）の最小導線（疎通確認）を呼び出す

def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：--config（必須）/ --strategy（#1 既定 か #2）"""
    p = argparse.ArgumentParser(description="Run paper trading (real-time)")
    p.add_argument("--config", required=True, help="configs/paper.yml など")
    p.add_argument("--strategy", default="stall_then_strike",
                choices=["stall_then_strike", "cancel_add_gate", "age_microprice"],
                help="どの戦略で動かすか（#1/#2/#3）")
    p.add_argument("--dry-run", action="store_true", help="何をするか：liveでも実発注せず疎通確認だけ行う（安全テスト）")

    return p.parse_args()

def main() -> None:
    """【関数】エントリ：設定を読み、paperエンジンを走らせる"""
    args = _parse_args()
    cfg = load_config(args.config)
    # 何をするか：設定の env を見て live/paper を切り替える（ワークフローの 8.3→8.4 切替）
    if getattr(cfg, "env", "paper") == "live":
        run_live(cfg, args.strategy, dry_run=args.dry_run)  # 何をするか：CLIの --dry-run 指定で実発注を抑止（可観測性はそのまま）
        return  # 何をするか：live 分岐ではここで終了（paper へは進まない）

    log_path = Path("logs/runtime/run.log")  # 【関数】ログファイルの出力先
    log_path.parent.mkdir(parents=True, exist_ok=True)  # フォルダ作成
    rotate_mb = getattr(getattr(cfg, "logging", None), "rotate_mb", 128)  # 既定128MB
    level = getattr(getattr(cfg, "logging", None), "level", "INFO")  # 既定INFO
    logger.add(log_path, level=level, rotation=f"{int(rotate_mb)} MB", enqueue=True)  # ローテ付きで出力

    if cfg.env != "paper":
        logger.warning(f"env is '{cfg.env}' (expected 'paper') - 続行はします")
    engine = PaperEngine(cfg, args.strategy)
    try:
        asyncio.run(engine.run_paper())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
