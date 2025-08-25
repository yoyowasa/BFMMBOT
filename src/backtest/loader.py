# src/backtest/loader.py
# 役割：録画NDJSONテープを「期間・チャンネル」でフィルタし、イベントを1件ずつ返す“読み取り係”
# - 【関数】iter_tape：NDJSONを読み、tsで期間フィルタ、channelで種別フィルタ
# - 注意：ここでは“読むだけ”。約定シミュや戦略接続は後続ステップで実装する
from __future__ import annotations

from pathlib import Path  # パスの安全な扱い
from typing import Iterator, Dict, Any, Iterable  # 型ヒント
from datetime import datetime  # ISO時刻の比較
from collections import Counter  # 進捗要約で使用
import orjson  # 高速JSON（録画と同じライブラリ）
from loguru import logger  # ログ出力

def _parse_iso(ts: str) -> datetime:
    """【関数】ISO文字列→datetime（'Z'も+00:00に正規化）"""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def iter_tape(
    tape_path: str | Path,
    start: str | None = None,
    end: str | None = None,
    channels: Iterable[str] | None = None,
) -> Iterator[Dict[str, Any]]:
    """
    【関数】NDJSONテープを順に返す（期間とチャンネルでフィルタ）
    - 入力：tape_path=NDJSONファイル、start/end=ISO文字列（例 "2025-08-01T00:00:00+09:00"）
    - 出力：{"ts": "...", "channel": "...", "message": ..., "source": "..."} を1件ずつyield
    - 仕様：文書 8.2「バックテスト（録画再生）」の“テープ再生”に対応（最小）。:contentReference[oaicite:4]{index=4}
    """
    path = Path(tape_path)
    if not path.is_file():
        raise FileNotFoundError(f"tape not found: {path}")

    start_dt = _parse_iso(start) if isinstance(start, str) else None
    end_dt = _parse_iso(end) if isinstance(end, str) else None
    ch_filter = set(channels) if channels else None

    kept = 0
    skipped = Counter()

    # 録画は1行=1イベントのNDJSON。バイナリで開き、orjsonで高速に読む
    with path.open("rb") as fh:
        for raw in fh:
            if not raw.strip():
                continue
            try:
                ev = orjson.loads(raw)  # bytes→dict
            except Exception:
                skipped["json_error"] += 1
                continue

            ts = ev.get("ts")
            ch = ev.get("channel")
            if not isinstance(ts, str) or ch is None:
                skipped["bad_shape"] += 1
                continue

            try:
                dt = _parse_iso(ts)
            except Exception:
                skipped["bad_ts"] += 1
                continue

            if start_dt and dt < start_dt:
                continue
            if end_dt and dt >= end_dt:
                continue
            if ch_filter and ch not in ch_filter:
                continue

            kept += 1
            yield ev

    logger.info(f"tape iterate done: kept={kept}, skipped={dict(skipped)}")
