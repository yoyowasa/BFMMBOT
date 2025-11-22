#!/usr/bin/env python3
# 何をするスクリプトか：
# - Heartbeat NDJSON（最新ファイルを自動選択）を1時間など指定時間フォローし、
#   event の内訳（status/pause/kill）と pause 理由別カウントを集計して逐次/最終サマリを表示します。
# - 併せて run.log の "hb status Q=" 行も追跡して直近1行を表示し、到達確認を容易にします。

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional, Dict, List


def _latest_hb_file(repo: Path) -> Optional[Path]:
    """最新の heartbeat ファイルを返す。非日付→日付付きの順で探す。"""
    p1 = repo / "logs/runtime/heartbeat.ndjson"
    if p1.exists():
        return p1
    cands = sorted((repo / "logs/runtime").glob("*heartbeat*.ndjson"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None


class NdjsonWatcher:
    """NDJSONファイルの追跡係。前回からの追加行だけを返す。
    - 文字化け/途中行は安全側でスキップ
    - ファイルが無くてもOK（後から出来たら追跡）
    """

    def __init__(self, path: Path):
        self.path = path
        self._pos = 0  # 読み取り位置（bytes）

    def read_new(self) -> List[str]:
        p = self.path
        try:
            size = p.stat().st_size
        except FileNotFoundError:
            return []
        if size < self._pos:
            # ローテーションなどで小さくなったら先頭から読み直し
            self._pos = 0
        if size == self._pos:
            return []
        out: List[str] = []
        try:
            with p.open("rb") as f:
                f.seek(self._pos)
                chunk = f.read()
                self._pos = f.tell()
        except Exception:
            return []
        try:
            text = chunk.decode("utf-8", errors="ignore")
        except Exception:
            return []
        for line in text.splitlines():
            if line:
                out.append(line)
        return out


def _find_runlog(repo: Path) -> Optional[Path]:
    p = repo / "logs/runtime/run.log"
    return p if p.exists() else None


def monitor(duration_sec: int, print_every_sec: int, repo: Path) -> int:
    hb = _latest_hb_file(repo)
    if not hb:
        print("[monitor] heartbeat file not found", file=sys.stderr)
        return 1
    runlog = _find_runlog(repo)
    orders_nd = repo / "logs/orders/order_log.ndjson"
    trades_nd = repo / "logs/trades/trade_log.ndjson"

    print(f"[monitor] heartbeat = {hb}")
    if runlog:
        print(f"[monitor] run.log   = {runlog}")
    if orders_nd.exists():
        print(f"[monitor] orders    = {orders_nd}")
    if trades_nd.exists():
        print(f"[monitor] trades    = {trades_nd}")

    stop_at = time.time() + max(1, int(duration_sec))
    next_progress = time.time() + max(1, int(print_every_sec))

    # Heartbeat集計
    hb_counts: Dict[str, int] = {"status": 0, "pause": 0, "kill": 0, "place": 0}
    pause_reason: Dict[str, int] = {}
    last_status = None

    # Orders/Trades 集計
    ord_counts: Dict[str, int] = {"place": 0, "cancel": 0, "other": 0}
    trd_count = 0

    # ウォッチャ（逐次追随）
    hb_w = NdjsonWatcher(hb)
    ord_w = NdjsonWatcher(orders_nd)
    trd_w = NdjsonWatcher(trades_nd)

    # run.log 側は進捗ごとに read で直近1行を拾う（tail -f 相当の簡易版）
    def _read_last_runlog_status() -> Optional[str]:
        if not runlog:
            return None
        try:
            lines = runlog.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            return None
        for line in reversed(lines[-500:]):
            if "hb status Q=" in line:
                return line
        return None

    last_runlog_status = None

    # ポーリングループ
    while time.time() < stop_at:
        # Heartbeat の新着処理
        for line in hb_w.read_new():
            try:
                obj = json.loads(line)
            except Exception:
                continue
            ev = obj.get("event")
            if ev in hb_counts:
                hb_counts[ev] += 1
            if ev == "status":
                last_status = obj
            elif ev == "pause":
                r = obj.get("reason", "(unknown)")
                pause_reason[r] = pause_reason.get(r, 0) + 1

        # Orders の新着処理
        for line in ord_w.read_new():
            try:
                obj = json.loads(line)
            except Exception:
                continue
            act = str(obj.get("action", "")).lower()
            if act in ("place", "cancel"):
                ord_counts[act] += 1
            else:
                ord_counts["other"] += 1

        # Trades の新着処理
        for _ in trd_w.read_new():
            trd_count += 1

        # 進捗表示
        now = time.time()
        if now >= next_progress:
            last_runlog_status = _read_last_runlog_status() or last_runlog_status
            print(
                "[progress] HB status={s} pause={p} kill={k} place={pl} | ORD place={op} cancel={oc} other={oo} | TRD={t} | last_runlog='{}'".format(
                    (last_runlog_status or ""),
                    s=hb_counts["status"], p=hb_counts["pause"], k=hb_counts["kill"], pl=hb_counts["place"],
                    op=ord_counts["place"], oc=ord_counts["cancel"], oo=ord_counts["other"], t=trd_count,
                )
            )
            next_progress = now + max(1, int(print_every_sec))

        time.sleep(1.0)

    # 終了サマリ
    last_runlog_status = _read_last_runlog_status() or last_runlog_status
    print("[summary] HB events:", hb_counts)
    if pause_reason:
        print("[summary] HB pause reasons:")
        for k, v in sorted(pause_reason.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"  - {k}: {v}")
    if last_status:
        q = last_status.get("Q")
        a = last_status.get("A")
        r = last_status.get("R")
        print(f"[summary] last HB status: Q={q} A={a} R={r} ts={last_status.get('ts')}")
    print("[summary] ORD:", ord_counts)
    print(f"[summary] TRD: {trd_count}")
    if last_runlog_status:
        print(f"[summary] last run.log status: {last_runlog_status}")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Heartbeat監視（NDJSON）とrun.logのstatus観測を行う")
    ap.add_argument("--duration", type=int, default=3600, help="監視する秒数（既定1時間）")
    ap.add_argument("--print-every", type=int, default=60, help="進捗表示の秒間隔（既定60s）")
    args = ap.parse_args()
    repo = Path.cwd()
    sys.exit(monitor(duration_sec=args.duration, print_every_sec=args.print_every, repo=repo))


if __name__ == "__main__":
    main()
