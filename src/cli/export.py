# src/cli/export.py
# 役割：Parquet ログ（orders/trades）を CSV または NDJSON に“閲覧用”として書き出すCLI
# - 【関数】_src_path(kind)：入力Parquetの場所を決める（仕様に従いlogs配下を参照）:contentReference[oaicite:2]{index=2}
# - 【関数】_default_out(kind, fmt)：出力ファイル名（時刻入り）を決める
# - 【関数】main()：引数処理→読込→書き出し（limitで行数を絞る簡易プレビュー）

from __future__ import annotations

import argparse  # CLI引数の処理
from pathlib import Path  # パス操作
from datetime import datetime  # 出力ファイル名に時刻を入れる
import polars as pl  # Parquet/CSV/NDJSON I/O（pyprojectに準拠）:contentReference[oaicite:3]{index=3}
from loguru import logger  # 進捗表示

def _src_path(kind: str) -> Path:
    """【関数】入力Parquetのパスを返す（orders/tradesの仕様名に合わせる）"""
    names = {"orders": "order_log.parquet", "trades": "trade_log.parquet"}
    if kind not in names:
        raise ValueError("kind must be 'orders' or 'trades'")
    return Path("logs") / kind / names[kind]

def _default_out(kind: str, fmt: str) -> Path:
    """【関数】出力ファイルの既定パス（data/results/に保存）"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("data") / "results" / f"{kind}_export_{ts}.{fmt}"

def _parse_args() -> argparse.Namespace:
    """【関数】引数定義：どのログを、どの形式で、どこに出すかを決める"""
    p = argparse.ArgumentParser(description="Export Parquet logs to CSV or NDJSON")
    p.add_argument("--kind", choices=["orders", "trades"], required=True, help="どのログを出すか")
    p.add_argument("--format", choices=["csv", "ndjson"], required=True, help="出力形式")
    p.add_argument("--out", default=None, help="出力ファイルパス（省略可）")
    p.add_argument("--limit", type=int, default=None, help="先頭から何行だけ出すか（省略で全件）")
    return p.parse_args()

def main() -> None:
    """【関数】本体：Parquetを読んでCSV/NDJSONに書き出す"""
    args = _parse_args()
    src = _src_path(args.kind)
    if not src.exists():
        raise FileNotFoundError(f"source not found: {src}")

    df = pl.read_parquet(src)  # 読み込み
    if args.limit is not None:
        df = df.head(args.limit)  # プレビュー用に行数を絞る

    out = Path(args.out) if args.out else _default_out(args.kind, args.format)
    out.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "csv":
        df.write_csv(out)       # CSVで保存
    else:
        df.write_ndjson(out)    # NDJSONで保存

    logger.info(f"exported rows={len(df)} → {out}")

if __name__ == "__main__":
    main()
