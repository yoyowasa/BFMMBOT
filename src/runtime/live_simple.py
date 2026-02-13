from __future__ import annotations

import os
import atexit
import signal
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Sequence

from loguru import logger

from src.core.realtime import stream_events
from src.strategy.base import build_strategy_from_cfg


def _now_utc() -> datetime:
    return datetime.now(ZoneInfo("Asia/Tokyo"))


def _hb_write(path: Path, **fields) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = {"ts": _now_utc().isoformat(), **fields}
        path.open("ab").write((str(line).replace("'", '"') + "\n").encode("utf-8"))
    except Exception:
        logger.exception("heartbeat write failed (simple)")


def run_live(
    cfg: Any,
    strategy_name: str,
    dry_run: bool = True,
    *,
    strategies: Sequence[str] | str | None = None,
    strategy_cfg=None,
) -> None:
    """最小のlive起動（DRYRUN疎通確認用）。
    - WSイベントを受信しつつ、心拍(start/status/kill)を出力する。
    - 危険な実発注は一切行わない。
    """

    # APIキーは一応チェック（存在しなくてもDRYRUNは続行可能）
    api_key = os.getenv("BF_API_KEY")
    api_secret = os.getenv("BF_API_SECRET")
    if not api_key or not api_secret:
        logger.warning("BF_API_KEY / BF_API_SECRET が見つかりません（DRYRUNは続行）")

    product_code = getattr(cfg, "product_code", "FX_BTC_JPY")

    # 心拍出力先
    hb_path = Path("logs/runtime/heartbeat.ndjson")
    hb_path.parent.mkdir(parents=True, exist_ok=True)

    # strategy名の整形（analytics/logの見栄え用）
    try:
        cfg_payload = dict(cfg) if not hasattr(cfg, "model_dump") else cfg.model_dump()
    except Exception:
        cfg_payload = {}
    if strategies:
        try:
            cfg_payload["strategies"] = list(strategies) if not isinstance(strategies, str) else [strategies]
        except Exception:
            pass
    try:
        strat = build_strategy_from_cfg(cfg_payload, strategy_cfg=strategy_cfg)
        summary_name = getattr(strat, "strategy_name", getattr(strat, "name", "unknown"))
        strategy_list = [
            getattr(child, "strategy_name", getattr(child, "name", "unknown"))
            for child in getattr(strat, "children", [])
        ] or [summary_name]
    except Exception:
        # 失敗しても最小表記で続行
        summary_name = str(strategy_name)
        strategy_list = list(strategies) if isinstance(strategies, (list, tuple)) else [summary_name]

    logger.info(
        f"live: starting loop product={product_code} strategy={summary_name} strategies={strategy_list}"
    )

    # atexit: 停止記録
    def _on_exit():
        try:
            _hb_write(hb_path, event="stop")
        except Exception:
            pass

    atexit.register(_on_exit)

    # start 心拍
    _hb_write(
        hb_path,
        event="start",
        product=product_code,
        strategy=summary_name,
        strategies=strategy_list,
        reason="launch",
    )

    # signal: Ctrl+Cで停止
    stop = {"flag": False}

    def _sig_handler(signum, frame):
        logger.warning(f"signal received: {signum} -> halt")
        stop["flag"] = True

    try:
        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)
    except Exception:
        pass

    # ループ（WSイベント受信 + status心拍）
    started_at = _now_utc()
    hb_interval_s = int(getattr(getattr(cfg, "health", None), "heartbeat_interval_sec", 1) or 1)
    next_hb = started_at + timedelta(seconds=hb_interval_s)
    dry_limit_s = getattr(cfg, "dry_run_limit_sec", None)

    for ev in stream_events(product_code=product_code):
        now = _now_utc()
        if stop["flag"]:
            break

        # 周期 status
        if now >= next_hb:
            _hb_write(
                hb_path,
                event="status",
                Q=0.0,
                A=0,
                R=0.0,
            )
            next_hb = now + timedelta(seconds=hb_interval_s)

        # DRYRUN タイムリミット
        if dry_run and dry_limit_s and (now - started_at).total_seconds() >= float(dry_limit_s):
            _hb_write(
                hb_path,
                event="kill",
                reason="dryrun_done",
                runtime_sec=int((now - started_at).total_seconds()),
            )
            logger.info("live(dry-run): time limit reached → halt")
            break

    # 終了時のkill（重複しても可）
    _hb_write(
        hb_path,
        event="kill",
        reason=("signal" if stop["flag"] else "done"),
        runtime_sec=int((_now_utc() - started_at).total_seconds()),
    )
