# src/cli/ingest.py
# 役割：WS（board/executions）を購読し、テープ（NDJSON）として data/raw に保存するCLI
# - 【関数】_parse_args：引数（config/outfile/duration_min）を読む（workflow 8.1に準拠）:contentReference[oaicite:2]{index=2}
# - 【関数】_run：WSイベントを受けて1行JSONで追記する（Ctrl+Cで安全停止）
# - 【関数】main：設定を読み、録画を実行する

from __future__ import annotations

import argparse  # CLI引数
import asyncio   # 非同期実行
from pathlib import Path  # 出力ファイルの作成
from datetime import datetime, timezone, timedelta  # 録画の制限時間
import orjson  # 1行JSONでの高速書き込み
from loguru import logger  # 進捗ログ

from src.core.utils import load_config  # 【関数】設定ローダー（base→paper/liveの順に上書き）:contentReference[oaicite:3]{index=3}
from src.core.realtime import event_stream  # 【関数】WSイベント購読（board/executions）:contentReference[oaicite:4]{index=4}

def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：--config と --outfile（必須/推奨の命名に準拠）、--duration_min（任意）"""
    p = argparse.ArgumentParser(description="Record WS events (board/executions) to NDJSON")
    p.add_argument("--config", required=True, help="configs/paper.yml または configs/live.yml")
    p.add_argument(
        "--outfile",
        required=True,
        help="録画先 NDJSON（例：data/raw/2025-08-27_FX_BTC_JPY.ndjson）",
    )
    p.add_argument(
        "--duration_min",
        type=int,
        default=None,
        help="録画分数。指定しなければ手動停止（Ctrl+C）まで続ける",
    )
    return p.parse_args()

async def _run(cfg, outfile: Path, duration_min: int | None) -> None:
    """【関数】実体：WSを購読し、1イベント=1行JSONで逐次追記する"""
    outfile.parent.mkdir(parents=True, exist_ok=True)
    # 期限（あれば）を計算
    deadline = None
    if duration_min is not None:
        deadline = datetime.now(timezone.utc) + timedelta(minutes=duration_min)

    product = getattr(cfg, "product_code", "FX_BTC_JPY") or "FX_BTC_JPY"
    logger.info(f"ingest start: product={product} → {outfile}")

    count = 0
    try:
        async for ev in event_stream(product_code=product):
            # 期限チェック（時間到達で終了）
            if deadline and datetime.now(timezone.utc) >= deadline:
                logger.info("duration reached; stopping ingest")
                break

            # 1行JSONで追記（PowerShellの -Wait で確認可能）
            with outfile.open("ab") as f:
                f.write(orjson.dumps(ev))
                f.write(b"\n")
            count += 1

            # たまに進捗（1,000行ごと目安）
            if count % 1000 == 0:
                logger.info(f"written events: {count}")

    except KeyboardInterrupt:
        logger.info("Ctrl+C - stopping ingest")
    finally:
        logger.info(f"ingest end: events={count} file={outfile}")

def main() -> None:
    """【関数】エントリ：設定を読み、録画タスクを開始する"""
    args = _parse_args()
    cfg = load_config(args.config)
    outfile = Path(args.outfile)
    asyncio.run(_run(cfg, outfile, args.duration_min))

if __name__ == "__main__":
    main()
