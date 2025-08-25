# src/cli/ingest.py
# 役割：設定を読み→WS購読を開始→NDJSONに録画するCLI（Ctrl+Cで安全に終了）
# 文書「8.1 テープ録画（Board/Executions）」の最小実装（--config と --outfile）。:contentReference[oaicite:6]{index=6}
from __future__ import annotations

import argparse  # CLI引数
import asyncio  # 非同期実行＆Ctrl+C
from pathlib import Path  # パス検査
from loguru import logger  # ログ

from src.core.utils import load_config  # 【関数】設定ローダー（前ステップ）:contentReference[oaicite:7]{index=7}
from src.core.realtime import event_stream  # 【関数】WSイベントストリーム（本ステップ）
from src.core.recorder import NDJSONRecorder  # 【関数】NDJSON録画器（本ステップ）

def _parse_args() -> argparse.Namespace:
    """【関数】引数定義：--config（必須）と --outfile（必須）"""
    p = argparse.ArgumentParser(description="Record bitFlyer WS tape to NDJSON")
    p.add_argument("--config", required=True, help="configs/paper.yml など")
    p.add_argument("--outfile", required=True, help="data/raw/2025-08-20_FX_BTC_JPY.ndjson など")
    return p.parse_args()

async def _run(config_path: str, outfile: str) -> None:
    """【関数】実行本体：設定→購読→録画の流れ"""
    cfg = load_config(config_path)  # base＋上書き合成済みConfigを取得
    product = cfg.product_code or "FX_BTC_JPY"
    logger.info(f"env={cfg.env} product={product}")

    rec = NDJSONRecorder(outfile)  # 出力ファイルを用意
    try:
        # board/executions を購読し続け、受けたイベントを逐次録画
        async for ev in event_stream(product_code=product):
            rec.write(ev)
    except asyncio.CancelledError:
        logger.info("stopping by cancel...")
        raise
    except KeyboardInterrupt:
        logger.info("Ctrl+C received, closing...")
    finally:
        rec.close()

def main() -> None:
    """【関数】エントリーポイント：非同期イベントループを起動"""
    args = _parse_args()
    try:
        asyncio.run(_run(args.config, args.outfile))
    except KeyboardInterrupt:
        # Windows の Ctrl+C 二度押し等でも静かに抜ける
        pass

if __name__ == "__main__":
    main()
