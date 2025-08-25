# src/core/simulator.py
# 役割：最小の約定シミュレータ（録画の executions を見て、自分の指値がタッチしたら Fill とみなす）
from __future__ import annotations

from datetime import datetime, timezone  # 時刻・TTL判定
from typing import List, Dict, Any  # 型ヒント
from loguru import logger  # 進捗ログ

from src.core.orders import Order  # 注文モデル

class MiniSimulator:
    """【関数】最小シミュレータ
    - 役割: open注文の集合を持ち、TTLや executions に応じて Fill/Cancel を進める
    - 簡略化: 価格タッチ＝Fill とみなす（キュー順位は後続で強化）
    """
    def __init__(self) -> None:
        self.open: List[Order] = []   # 未約定の注文
        self.placed = 0               # Place件数（要約用）
        self.cancelled = 0            # Cancel件数（要約用）
        self.filled = 0               # Fill件数（要約用）

    def _parse_iso(self, ts: str) -> datetime:
        """【関数】ISO文字列→datetime（'Z' も +00:00 に正規化）"""
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def place(self, order: Order, now: datetime) -> None:
        """【関数】注文を板に置く（作成時刻を刻む）"""
        order.created_ts = now
        self.open.append(order)
        self.placed += 1

    def cancel_by_tag(self, tag: str) -> list[Order]:
        """【関数】タグで一括キャンセルして、取消した注文リストを返す"""
        cancelled: list[Order] = []
        kept: list[Order] = []
        for o in self.open:
            if o.tag == tag:
                self.cancelled += 1
                cancelled.append(o)
                continue
            kept.append(o)
        self.open = kept
        return cancelled


    def has_open_tag(self, tag: str) -> bool:
        """【関数】あるタグの未約定が残っているかを確認（重複発注防止のゲート）"""
        return any(o.tag == tag for o in self.open)

    def on_time(self, now: datetime) -> list[Order]:
        """【関数】TTLチェック：期限切れを返す（ログ用）"""
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
        """【関数】約定適用：Fillイベントの明細を返す（ログ/PnL用）
        返す要素：{'ts', 'side', 'price', 'size', 'order', 'tag', 'partial'}
        """
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

