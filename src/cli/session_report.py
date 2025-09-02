# src/cli/session_report.py
# 役割：trades（約定ログ）から“戦略別／タグ別”のざっくり結果を Markdown で出力するCLI
# - 【関数】_parse_args：引数を読む（期間や出力先）
# - 【関数】_load_trades：Parquet の trades を読み、期間で絞る
# - 【関数】_mk_section：1つのグループ（strategy/tag）集計をMarkdownの箇条書きに整形
# - 【関数】main：全体の流れ（読込→集計→MD文字列→data/results/に保存）
#   ※ 列定義・置き先は文書のログ仕様/成果物構成に準拠（logs/trades/*.parquet → data/results/*）。:contentReference[oaicite:1]{index=1}

from __future__ import annotations  # 未来のアノテーション
from pathlib import Path  # ファイルパス操作
from datetime import datetime  # 出力ファイル名に時刻を入れる
import argparse  # CLI引数の処理
import polars as pl  # Parquet読込と集計（仕様どおり）:contentReference[oaicite:2]{index=2}
from loguru import logger  # 進捗ログ

def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：期間/出力先/集計軸（strategy/tag）"""
    p = argparse.ArgumentParser(description="Make quick Markdown session report from trades")
    p.add_argument("--trades", default="logs/trades/trade_log.parquet", help="trades Parquet のパス")
    p.add_argument("--start", default=None, help="開始UTC（例: 2025-08-25T00:00:00+00:00）")
    p.add_argument("--end", default=None, help="終了UTC（例: 2025-08-26T00:00:00+00:00）")
    p.add_argument("--by", choices=["strategy", "tag"], default="strategy", help="集計の軸")
    p.add_argument("--top", type=int, default=5, help="上位何グループを出すか（pnl_sum順）")
    p.add_argument("--out", default=None, help="出力MD（省略時は data/results/session_report_*.md）")
    return p.parse_args()

def _load_trades(path: str, start: str | None, end: str | None) -> pl.DataFrame:
    """【関数】trades を読み、必要なら ts で期間を絞る（ログ仕様の固定スキーマを前提）。:contentReference[oaicite:3]{index=3}"""
    df = pl.read_parquet(path)
    if start:
        df = df.filter(pl.col("ts") >= start)
    if end:
        df = df.filter(pl.col("ts") < end)
    return df

def _mk_section(df: pl.DataFrame, group_col: str, top_n: int) -> str:
    """【関数】group_col（strategy/tag）ごとに集計してMarkdown箇条書きを作る"""
    if group_col not in df.columns:
        return f"\n（注）tradesに列 {group_col!r} がありません。\n"
    g = (
        df.with_columns((pl.col("pnl") > 0).cast(pl.Int64).alias("win_flag"))
          .group_by(group_col)
          .agg([
              pl.len().alias("trades"),
              pl.col("win_flag").sum().alias("wins"),
              pl.col("pnl").sum().alias("pnl_sum"),
              pl.col("pnl").mean().alias("pnl_avg"),
              pl.col("pnl").std(ddof=1).alias("pnl_std"),
              pl.col("pnl").filter(pl.col("pnl") > 0).mean().alias("avg_win"),
              pl.col("pnl").filter(pl.col("pnl") <= 0).mean().alias("avg_loss"),
              pl.col("sz").sum().alias("size_sum"),
          ])
          .with_columns([
              (pl.col("wins") / pl.col("trades")).alias("win_rate"),
              pl.when(pl.col("pnl_std") > 0).then(pl.col("pnl_avg") / pl.col("pnl_std")).otherwise(None).alias("sharpe_per_trade"),
          ])
          .sort(by="pnl_sum", descending=True)  # 役割：合計PnLの降順で並べ替え（Polars 1.x 互換）
          .head(top_n)
    )
    lines = []
    for r in g.to_dicts():
        key = r[group_col]
        # 説明：数値は読みやすい丸め（小数点2〜3桁）
        lines.append(
            f"- **{group_col} = {key}**｜件数 {r['trades']}｜勝率 {r['win_rate']:.2%}｜"
            f"合計PnL {r['pnl_sum']:.2f}｜平均PnL {r['pnl_avg']:.3f}｜Sharpe(1trade) "
            f"{('%.3f' % r['sharpe_per_trade']) if r['sharpe_per_trade'] is not None else 'NA'}｜"
            f"合計サイズ {r['size_sum']:.4f}"
        )
    return "\n".join(lines) if lines else "\n（該当データなし）\n"

def main() -> None:
    """【関数】本体：読込→集計→Markdown文字列→ data/results に保存"""
    args = _parse_args()
    src = Path(args.trades)
    if not src.exists():
        raise FileNotFoundError(f"source not found: {src}")

    df = _load_trades(str(src), args.start, args.end)
    if df.is_empty():
        logger.warning("trades is empty for given range")
        return

    # 見出し（ログと戦略の仕様に準拠）:contentReference[oaicite:4]{index=4} :contentReference[oaicite:5]{index=5}
    title = "# Session Report (Quick)\n"
    period = f"- Range(UTC): start={args.start or 'ALL'} / end={args.end or 'ALL'}\n"
    body = "## Summary by " + args.by + "\n" + _mk_section(df, args.by, args.top) + "\n"

    content = title + period + body

    out = Path(args.out) if args.out else Path("data") / "results" / f"session_report_{datetime.now():%Y%m%d_%H%M%S}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(content, encoding="utf-8")
    logger.info(f"saved → {out}")

if __name__ == "__main__":
    main()
