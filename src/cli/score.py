# src/cli/score.py
# 役割：trades（約定ログ）から“戦略ごとの ざっくり成績”を集計して CSV に出すCLI
# - 【関数】_parse_args：引数を読む（どの軸で集計するか、出力先など）
# - 【関数】_load_trades：Parquet の trades を読み、期間フィルタを適用
# - 【関数】_calc_metrics：件数・勝率・合計PnL・平均PnL・合計サイズを集計
# - 【関数】main：一連の処理を流し、data/results/ に CSV を保存して要約を表示
#   ※ trades の列定義は文書の「ログ仕様（固定スキーマ）」に準拠（ts, side, px, sz, fee, pnl, strategy, tag, ...）。:contentReference[oaicite:1]{index=1}

from __future__ import annotations

import argparse  # 【関数】CLI引数の処理を行う
from pathlib import Path  # 【関数】ファイルパス操作
from datetime import datetime  # 【関数】出力ファイル名に時刻を入れる
import polars as pl  # 【関数】Parquet読込と集計（文書の推奨どおり）:contentReference[oaicite:2]{index=2}
from loguru import logger  # 【関数】進捗ログ

def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：集計軸（strategy/tag）や期間、出力パスを指定"""
    p = argparse.ArgumentParser(description="Quick score from trades parquet")
    p.add_argument("--by", choices=["strategy", "tag"], default="strategy", help="どの軸で集計するか（戦略 or タグ）")
    p.add_argument("--start", default=None, help="開始UTC（例: 2025-08-25T00:00:00+00:00）")
    p.add_argument("--end", default=None, help="終了UTC（例: 2025-08-26T00:00:00+00:00）")
    p.add_argument("--trades", default="logs/trades/trade_log.parquet", help="trades Parquet のパス")
    p.add_argument("--out", default=None, help="出力CSV（省略時は data/results/score_YYYYmmdd_HHMMSS.csv）")
    return p.parse_args()

def _load_trades(path: str, start: str | None, end: str | None) -> pl.DataFrame:
    """【関数】trades（Parquet）を読み、必要なら ts の期間でフィルタする"""
    df = pl.read_parquet(path)
    if start:
        df = df.filter(pl.col("ts") >= start)
    if end:
        df = df.filter(pl.col("ts") < end)
    return df

def _calc_metrics(df: pl.DataFrame, group_col: str) -> pl.DataFrame:
    """【関数】件数・勝率・合計PnL・平均PnL・合計サイズを group_col で集計する"""
    # 勝ち= pnl>0 を 1 として合計 → win_count、件数で割って win_rate を出す
    agg = (
        df.with_columns(
            (pl.col("pnl") > 0).cast(pl.Int64).alias("win_flag")
        )
        .group_by(group_col)  # 【関数】集計：軸（strategy/tag）ごとにKPIをまとめる
        .agg([
            pl.len().alias("trades"),                                     # 件数
            pl.col("win_flag").sum().alias("wins"),                       # 勝ち件数
            pl.col("pnl").sum().alias("pnl_sum"),                         # 合計PnL
            pl.col("pnl").mean().alias("pnl_avg"),                        # 1件平均PnL
            pl.col("pnl").std(ddof=1).alias("pnl_std"),                   # PnL標準偏差
            pl.col("pnl").filter(pl.col("pnl") > 0).mean().alias("avg_win"),   # 平均勝ち
            pl.col("pnl").filter(pl.col("pnl") <= 0).mean().alias("avg_loss"), # 平均負け（≤0）
            pl.col("sz").sum().alias("size_sum"),                         # 体積合計（BTC）
            pl.col("pnl").filter(pl.col("pnl") > 0).sum().alias("gross_profit"), # 総利益
            pl.col("pnl").filter(pl.col("pnl") <= 0).sum().alias("gross_loss"),  # 総損失（≤0）
        ])
        .with_columns([
            (pl.col("wins") / pl.col("trades")).alias("win_rate"),        # 勝率
            pl.when(pl.col("avg_loss") != 0)
              .then(pl.col("avg_win") / pl.col("avg_loss").abs())
              .otherwise(None)
              .alias("payoff"),                                           # 平均勝ち/平均負け
            (pl.col("pnl_sum") / pl.col("size_sum")).alias("pnl_per_btc"),# 1BTCあたりPnL
            (pl.col("pnl_avg") + (1 - (pl.col("wins")/pl.col("trades"))) * 0)  # 形式上の項
              .alias("_tmp_ignore"),                                      # （説明用ダミー：実値は下のexpectancyで再計算）
        ])
        .drop(["_tmp_ignore"])
        .with_columns([
            # 期待値（1トレードあたり）：win_rate*avg_win + (1-win_rate)*avg_loss
            (pl.col("win_rate") * pl.col("avg_win") + (1 - pl.col("win_rate")) * pl.col("avg_loss")).alias("expectancy"),
            # 簡易Sharpe（1件単位）：pnl_avg / pnl_std
            pl.when(pl.col("pnl_std") > 0)
              .then(pl.col("pnl_avg") / pl.col("pnl_std"))
              .otherwise(None)
              .alias("sharpe_per_trade"),
        ])
        .sort(group_col)

    )
    return agg

def main() -> None:
    """【関数】本体：trades 読込 → 集計 → CSV 保存 → 画面に要約を表示"""
    args = _parse_args()
    src = Path(args.trades)
    if not src.exists():
        raise FileNotFoundError(f"source not found: {src}")

    df = _load_trades(str(src), args.start, args.end)
    if df.is_empty():
        logger.warning("trades is empty for given range")
        return

    group_col = args.by
    if group_col not in df.columns:
        raise ValueError(f"column not found in trades: {group_col}")

    score = _calc_metrics(df, group_col)

    out = Path(args.out) if args.out else Path("data") / "results" / f"score_{datetime.now():%Y%m%d_%H%M%S}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    score.write_csv(out)

    # 画面にも一言（行数と保存先）
    logger.info(f"groups={score.height} saved → {out}")
    # 先頭数行を簡易表示（表ではなく行テキストでOK）
    for row in score.head(5).to_dicts():
        logger.info(row)

if __name__ == "__main__":
    main()
