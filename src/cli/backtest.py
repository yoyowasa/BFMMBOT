# src/cli/backtest.py
# 役割：設定を読み→periodでテープを切り出し→件数を要約（最小）
# - 後続で戦略(#1/#2)や約定シミュに接続するための“足場”
from __future__ import annotations

import argparse  # 引数処理
from collections import Counter  # 件数要約
from loguru import logger  # ログ
from pathlib import Path  # 結果ファイルの保存先を扱う
from datetime import datetime  # ファイル名に時刻スタンプを付ける
import orjson  # 要約をJSONで高速保存する

from src.core.utils import load_config  # 【関数】設定ローダー（前ステップ）:contentReference[oaicite:5]{index=5}
from src.backtest.loader import iter_tape  # 【関数】テープ読取（本ステップ）
from src.backtest.runner import run_backtest_min  # 【関数】最小ランナー（戦略#1を接続）

def _parse_args() -> argparse.Namespace:
    """【関数】引数定義：--config と --tape（戦略名は後続用で受けるだけ）"""
    p = argparse.ArgumentParser(description="Backtest (tape playback minimal)")
    p.add_argument("--config", required=True, help="configs/backtest.yml")
    p.add_argument("--tape", required=True, help="data/raw/....ndjson")
    p.add_argument("--strategy", default="stall_then_strike", help="strategy name (placeholder)")
    p.add_argument("--simulate", action="store_true", help="戦略(#1)と最小シミュで再生する")

    p.add_argument("--outdir", default="data/results", help="結果を書き出すフォルダ")

    return p.parse_args()

def _save_summary(outdir, cfg, strategy, total, per_ch):
    """【関数】要約保存：結果を data/results/ に JSON で保存する"""
    outdir_path = Path(outdir)
    outdir_path.mkdir(parents=True, exist_ok=True)  # フォルダが無ければ作る

    # Pydanticモデルは dict 化してから保存（orjson で直に落とせる形にする）
    period = cfg.period.model_dump() if getattr(cfg, "period", None) else None
    latency = cfg.latency.model_dump() if getattr(cfg, "latency", None) else None
    slippage = cfg.slippage.model_dump() if getattr(cfg, "slippage", None) else None

    payload = {
        "env": cfg.env,
        "strategy": strategy,
        "events": total,
        "per_channel": dict(per_ch),
        "period": period,
        "latency": latency,
        "slippage": slippage,
        "product_code": getattr(cfg, "product_code", None),
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outpath = outdir_path / f"backtest_summary_{ts}.json"
    outpath.write_bytes(orjson.dumps(payload))  # 1ファイルに保存
    return outpath

def main() -> None:
    """【関数】エントリ：periodで切り出して件数をログ表示（まずは動作確認）"""
    args = _parse_args()
    cfg = load_config(args.config)  # env/backtest などが入る。period/latency/slippageも取得。:contentReference[oaicite:6]{index=6}

    start = cfg.period.start if cfg.period else None
    end = cfg.period.end if cfg.period else None

    total = 0
    per_ch = Counter()
    for ev in iter_tape(args.tape, start=start, end=end):
        total += 1
        per_ch[ev["channel"]] += 1

    logger.info(f"env={cfg.env} strategy={args.strategy} events={total} per_channel={dict(per_ch)}")
    logger.info(f"period=({start} → {end}) latency={cfg.latency} slippage={cfg.slippage}")  # 後続で使用予定
    
    if args.simulate:
        run_backtest_min(cfg, args.tape, args.strategy)  # 【関数】最小ランナー：サマリは内部でログ出力済み


    saved = _save_summary(args.outdir, cfg, args.strategy, total, per_ch)  # 要約を書き出す
    logger.info(f"saved summary: {saved}")  # どこに保存したかログで知らせる

if __name__ == "__main__":
    main()
