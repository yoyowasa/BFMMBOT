# src/core/simulator.py
# 役割: 最小の約定シミュレータ。board/exec を受けて open 注文を管理し、Fill/Cancel を返す。
from __future__ import annotations

from datetime import datetime  # 時刻・TTL判定
from typing import List, Dict, Any
from loguru import logger

from src.core.orders import Order


class MiniSimulator:
    """最小シミュレータ
    - open 注文を保持し、TTL や executions に応じて Fill/Cancel を処理する。
    - 価格が当たったら Fill とみなす簡易版。
    """

    def __init__(self) -> None:
        self.open: List[Order] = []
        self.placed = 0
        self.cancelled = 0
        self.filled = 0

    def _parse_iso(self, ts: str) -> datetime:
        """ISO 文字列を datetime（timezone UTC）へ."""
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def place(self, order: Order, now: datetime) -> None:
        """注文を板に置く（作成時刻を刻む）。"""
        order.created_ts = now
        self.open.append(order)
        self.placed += 1

    @staticmethod
    def _tag_matches(order_tag: str | None, query: str | None) -> bool:
        """タグ一致を緩く判定（base と base|corr:xxx のような付加情報付きも拾う）。"""
        if not query:
            return False
        if order_tag == query:
            return True
        try:
            return str(order_tag).startswith(f"{query}|")
        except Exception:
            return False

    def cancel_by_tag(self, tag: str) -> list[Order]:
        """タグで一括キャンセルして、取消した注文リストを返す。"""
        cancelled: list[Order] = []
        kept: list[Order] = []
        for o in self.open:
            if self._tag_matches(getattr(o, "tag", None), tag):
                self.cancelled += 1
                cancelled.append(o)
                continue
            kept.append(o)
        self.open = kept
        return cancelled

    def has_open_tag(self, tag: str) -> bool:
        """指定タグの未約定注文が残っているかを確認（重複発注防止のゲート）。"""
        return any(self._tag_matches(getattr(o, "tag", None), tag) for o in self.open)

    def on_time(self, now: datetime) -> list[Order]:
        """TTL チェックを行い、期限切れを返す（ログ用）。"""
        if not self.open:
            return []
        expired: list[Order] = []
        kept: list[Order] = []
        for o in self.open:
            if o.ttl_ms is not None and o.created_ts is not None:
                age_ms = int((now - o.created_ts).total_seconds() * 1000)
                if age_ms > o.ttl_ms:
                    self.cancelled += 1
                    expired.append(o)
                    continue
            kept.append(o)
        self.open = kept
        return expired

    def on_executions(self, prints: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
        """紁E���E適用し、成立した Fill イベント明細を返す（PnL/ログ用）。"""
        fills: list[dict[str, Any]] = []
        for p in prints:
            try:
                px = float(p["price"])
                sz = float(p.get("size") or p.get("exec_size") or 0.0)
            except Exception:
                continue
            remaining_sz = sz
            for o in list(self.open):
                if remaining_sz <= 0:
                    break
                hit = (o.side == "buy" and o.remaining > 0 and px <= o.price) or \
                    (o.side == "sell" and o.remaining > 0 and px >= o.price)
                if not hit:
                    continue
                fill = min(o.remaining, remaining_sz)
                o.remaining -= fill
                remaining_sz -= fill
                self.filled += 1
                partial = o.remaining > 0
                fills.append({
                    "ts": now.isoformat(),
                    "side": o.side,
                    "price": px,
                    "size": fill,
                    "order": o,
                    "tag": o.tag,
                    "partial": partial,
                })
                if o.remaining <= 0:
                    self.open.remove(o)
        return fills
