# src/core/orderbook.py
# 役割：WSのboard差分をローカル板に適用して、Best/Spread/BestAge/C/A比などを取り出す“板エンジン”
# - 【関数】update_from_event(ev)：録画/実WSイベント（board）を受け取り適用
# - 【関数】apply_board(now, message)：bids/asks差分を適用
# - 【関数】best_age_ms(now)：両Bestのうち古い方の“静止時間”をmsで返す（#1で使用）
# - 【関数】spread_ticks()：現在のスプレッド（tick）を返す（#1/#2で使用）
# - 【関数】ca_ratio(now, window_ms)：Best層のCancel/Add比（直近窓）を返す（#2で使用）
# - 【関数】state_snapshot(now)：主要指標のスナップ（デバッグ/ログ向け）
from __future__ import annotations

from dataclasses import dataclass  # Best側の状態を1まとまりにする
from typing import Dict, Any, Deque, Tuple, Iterable  # 型ヒント
from collections import deque  # C/Aイベントのスライディング窓
from datetime import datetime, timezone  # ISO→datetime と経過ms計算
import time  # Bestの滞留時間(ms)を単調時計で測るために使用
from loguru import logger  # ログ（必要時）

# ─────────────────────────────────────────────────────────────
# 補助：ISO文字列をdatetimeへ（'Z'も+00:00として扱えるよう正規化）
def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))

def _age_ms(since: datetime | None, now: datetime) -> int:
    """【関数】経過ms計算：since から now までのミリ秒"""
    if not since:
        return 0
    return int((now - since).total_seconds() * 1000)

@dataclass
class _BestSide:
    """片側Bestの状態：価格/数量/静止開始時刻"""
    price: float | None = None
    size: float = 0.0
    since: datetime | None = None

# ─────────────────────────────────────────────────────────────
class OrderBook:
    """ローカル板の最小実装（Best/Spread/BestAge/C-A比）"""
    def __init__(self, tick_size: float = 1.0) -> None:
        self.tick = float(tick_size)         # 刻み（例：1 JPY）
        self.bids: Dict[float, float] = {}   # 価格→数量
        self.asks: Dict[float, float] = {}
        self.best_bid = _BestSide()
        self.best_ask = _BestSide()
        # C/A計測用： (時刻, side, kind, qty_abs) を貯める（kind: "add" | "cancel"）
        self.ca_events: Deque[Tuple[datetime, str, str, float]] = deque()
        self._last_ts: datetime | None = None  # 最後に適用したイベントの時刻
        self._last_best_bid_px = None  # Best Bidの直近価格（Ageリセット判定用）
        self._last_best_ask_px = None  # Best Askの直近価格（Ageリセット判定用）
        self._last_best_update_mono = time.monotonic()  # Age起点（単調時計）
        self._best_age_ms = 0.0  # Bestの滞留時間（ms）— 外部へ提供する指標
        self._mid_history: Deque[Tuple[float, float]] = deque()  # ミッド価格履歴（timestamp, mid）

    # ── 【関数】イベント受信→適用（boardチャンネルのみ拾う）
    def update_from_event(self, ev: Dict[str, Any]) -> None:
        ch = (ev or {}).get("channel") or ""
        if not ch.startswith("lightning_board_"):
            return
        now = _parse_iso((ev or {}).get("ts", ""))
        msg = (ev or {}).get("message") or {}
        self.apply_board(now, msg)

    # ── 【関数】board差分を適用（bids/asks の price/size を反映）
    def apply_board(self, now: datetime, message: Dict[str, Any]) -> None:
        self._last_ts = now
        self._apply_side("bid", now, message.get("bids") or [])
        self._apply_side("ask", now, message.get("asks") or [])
        self._refresh_best("bid", now)
        self._refresh_best("ask", now)
        bid_px = self.best_bid.price  # 現在のBest Bid価格（Ageリセット判定）
        ask_px = self.best_ask.price  # 現在のBest Ask価格（Ageリセット判定）
        current_mono = time.monotonic()  # 今回適用時点の単調時計を取得
        if self._last_best_bid_px is None or self._last_best_ask_px is None:
            self._last_best_update_mono = current_mono  # 初回は現在時刻を起点にする
        elif bid_px != self._last_best_bid_px or ask_px != self._last_best_ask_px:
            self._last_best_update_mono = current_mono  # どちらかのBest価格変化でAgeをリセット
        self._best_age_ms = (current_mono - self._last_best_update_mono) * 1000.0  # 単調時計差分から最新の滞留時間(ms)を算出
        self._last_best_bid_px = bid_px  # 次回比較に向けてBest Bid価格を保存
        self._last_best_ask_px = ask_px  # 次回比較に向けてBest Ask価格を保存
        if bid_px is not None and ask_px is not None:
            mid = (bid_px + ask_px) / 2.0
            self._mid_history.append((now.timestamp(), mid))

    def mid_change_bps(self, window_ms: int) -> float:
        """【関数】ミッド価格の変化率（bps）を直近window_msから算出"""
        if window_ms is None or window_ms <= 0:
            return 0.0
        if not self._mid_history:
            return 0.0
        now_ts, current_mid = self._mid_history[-1]
        cutoff = now_ts - (window_ms / 1000.0)
        # 古い履歴を整理（window外の完全に古いものは破棄）
        while len(self._mid_history) > 1 and self._mid_history[1][0] <= cutoff:
            self._mid_history.popleft()
        if len(self._mid_history) > 1 and self._mid_history[0][0] < cutoff:
            self._mid_history.popleft()
        base_ts, base_mid = self._mid_history[0]
        if base_ts < cutoff and len(self._mid_history) == 1:
            return 0.0
        if base_mid is None or base_mid == 0:
            return 0.0
        return ((current_mid - base_mid) / base_mid) * 10_000.0

    # ── 側ごとの差分適用（Best層のC/Aイベントもここで採集）
    def _apply_side(self, side: str, now: datetime, levels: Iterable[Dict[str, Any]]) -> None:
        book = self.bids if side == "bid" else self.asks
        for lv in levels:
            try:
                px = float(lv["price"])
                sz = float(lv.get("size", 0.0))
            except Exception:
                continue

            old = book.get(px, 0.0)
            if sz <= 0.0:
                # 0は削除（差分仕様）。C/A比では“cancel”として扱う
                if px in book:
                    del book[px]
                    delta = -old
                else:
                    delta = 0.0
            else:
                book[px] = sz
                delta = sz - old  # +はAdd, -はCancel

            # “いまのBest価格”に対する変化だけをC/A窓に記録（Best層合計）
            best_px = (max(book) if side == "bid" else (min(book) if book else None)) if book else None
            if best_px is not None and abs(px - best_px) < 1e-9 and abs(delta) > 0.0:
                kind = "add" if delta > 0 else "cancel"
                self.ca_events.append((now, side, kind, abs(delta)))

    # ── Bestの価格/数量/静止開始時刻を更新（価格が変わった時だけsinceをリセット）
    def _refresh_best(self, side: str, now: datetime) -> None:
        book = self.bids if side == "bid" else self.asks
        cur = self.best_bid if side == "bid" else self.best_ask
        new_px = (max(book) if side == "bid" else min(book)) if book else None
        if new_px != cur.price:
            # 価格が変わった＝“若返り”なのでsinceを更新
            new_sz = book.get(new_px, 0.0) if new_px is not None else 0.0
            updated = _BestSide(price=new_px, size=new_sz, since=(now if new_px is not None else None))
            if side == "bid":
                self.best_bid = updated
            else:
                self.best_ask = updated
        else:
            # 価格は同じ：数量だけ最新化（sinceは維持＝“静止継続”）
            if new_px is not None:
                new_sz = book.get(new_px, 0.0)
                if side == "bid":
                    self.best_bid.size = new_sz
                else:
                    self.best_ask.size = new_sz

    # ── 【関数】Best静止時間（ms）：両側のうち短い方（=より厳しい定義）を返す
    def best_age_ms(self, now: datetime | None = None) -> int:
        _ = now  # シグネチャ互換用（単調時計で管理するため未使用）
        return int(self._best_age_ms)  # 単調時計で算出した滞留時間(ms)を整数で返す

    # ── 【関数】スプレッド（tick単位）
    def spread_ticks(self) -> int:
        if self.best_bid.price is None or self.best_ask.price is None:
            return 0
        gap = max(0.0, self.best_ask.price - self.best_bid.price)
        return int(round(gap / self.tick))

    # ── 【関数】マイクロプライス（参考：#3で使用予定）
    def microprice(self) -> float | None:
        if self.best_bid.price is None or self.best_ask.price is None:
            return None
        bb, ba = self.best_bid, self.best_ask
        denom = (bb.size + ba.size)
        if denom <= 0:
            return (bb.price + ba.price) / 2.0
        return (ba.price * bb.size + bb.price * ba.size) / denom

    # ── 【関数】C/A比（Best層合計, 直近window_ms）
    def ca_ratio(self, now: datetime | None = None, window_ms: int = 500) -> float:
        now = now or self._last_ts or datetime.now(timezone.utc)
        cutoff = now.timestamp() - (window_ms / 1000.0)
        # 古いイベントを窓から落とす
        while self.ca_events and self.ca_events[0][0].timestamp() < cutoff:
            self.ca_events.popleft()
        adds = 0.0
        cancels = 0.0
        for _, _, kind, qty in self.ca_events:
            if kind == "add":
                adds += qty
            else:
                cancels += qty
        return (cancels / adds) if adds > 0 else float("inf")

    # ── 【関数】主要指標のスナップ（デバッグ/ログ用）
    def state_snapshot(self, now: datetime | None = None) -> Dict[str, Any]:
        now = now or self._last_ts or datetime.now(timezone.utc)
        return {
            "bid": {"px": self.best_bid.price, "sz": self.best_bid.size, "age_ms": _age_ms(self.best_bid.since, now) if self.best_bid.since else None},
            "ask": {"px": self.best_ask.price, "sz": self.best_ask.size, "age_ms": _age_ms(self.best_ask.since, now) if self.best_ask.since else None},
            "spread_tick": self.spread_ticks(),
            "mp": self.microprice(),
            "ca_ratio_500ms": self.ca_ratio(now, 500),
        }
