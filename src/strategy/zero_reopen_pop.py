# 何をするモジュールか：
#   「スプレッドが一瞬0になった直後、1tick以上に再拡大した“その一拍”だけ」片面で最小ロットを置き、
#   当たったら即IOCで+1tick利確して退出する“イベント駆動ワンショットMM”の本体実装。

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

# 何をするimportか：戦略の骨組み・板の読み取り・注文生成・時刻取得（すべて既存の共通層を利用）
from src.strategy.base import StrategyBase
from src.core.orderbook import OrderBook  # best/中値/tick/スプレッド/健全性 health_ok() を提供
from src.core.orders import Order        # Limit/IOC と TTL/タグを付けて発注する型
from src.core.utils import now_ms        # クールダウンや“直後”判定に使うミリ秒時刻


@dataclass
class ZeroReopenConfig:
    """何をする設定か：最小限の外だしパラメータ（YAML上書き前提）"""

    min_spread_tick: int = 1       # 再拡大の下限（1tick以上に開いていること）
    max_spread_tick: int = 2       # 何をする設定か：再拡大が広すぎる（毒性高い）ときは出さない上限tick
    ttl_ms: int = 800              # 指値の寿命（置きっぱなし防止・秒速撤退のため短め）
    size_min: float = 0.001        # 最小ロット（取引所の最小単位に合わせる）
    cooloff_ms: int = 250          # 連打禁止と毒性回避のための“息継ぎ”
    seen_zero_window_ms: int = 1000  # どれだけ“ゼロ直後”を有効とみなすか


def zero_reopen_config_from(cfg: Any) -> ZeroReopenConfig | None:
    """【関数】設定オブジェクトから zero_reopen_pop セクションを取り出し ZeroReopenConfig に変換する"""

    if cfg is None:
        return None

    features = getattr(cfg, "features", None)
    section: Any = None

    if isinstance(features, Mapping):
        section = features.get("zero_reopen_pop")
    elif features is not None:
        section = getattr(features, "zero_reopen_pop", None)
        if section is None:
            extra = getattr(features, "model_extra", None)
            if isinstance(extra, Mapping):
                section = extra.get("zero_reopen_pop")
            elif hasattr(features, "__dict__") and isinstance(features.__dict__, dict):
                section = features.__dict__.get("zero_reopen_pop")

    if not section:
        return None

    if isinstance(section, ZeroReopenConfig):
        return section

    if isinstance(section, Mapping):
        return ZeroReopenConfig(**section)

    raise TypeError("zero_reopen_pop config must be a mapping or ZeroReopenConfig instance")


class ZeroReopenPop(StrategyBase):
    """
    何をする戦略か：
      - 直近に spread==0（タッチ/クロス）を観測 → 記録
      - その直後（既定1秒以内）に spread >= 1tick に再拡大した“一拍だけ”片面を最小ロットで提示（GTC+TTL）
      - 約定したら即IOCで+1tick利確して退出
      - ガード（health_ok 等）が悪いときは何もしない
    """

    name: str = "zero_reopen_pop"

    def __init__(self, *, cfg: Optional[ZeroReopenConfig] = None) -> None:
        # 何をする関数か：設定と内部状態（直近ゼロ時刻／直近アクション時刻）の初期化
        super().__init__()
        self.cfg = cfg or ZeroReopenConfig()
        self._last_spread_zero_ms: int = -10**9
        self._last_action_ms: int = -10**9
        self._lock_until_ms: int = -10**9  # 何をするか：同時に複数枚を出さない“発注ロック”の期限ms（TTL中は新規禁止）
        self._entry_active: bool = False   # 何をするか：現在エントリー指値が生きているかを覚えて cancel_tag を制御する
        self._entry_tag: str = "zero_reopen"
        self._take_tag: str = "zero_reopen_take"

    # -------------------------
    # 内部ヘルパ（責務を明記）
    # -------------------------

    def _to_ms(self, now: datetime | int | float | None) -> int:
        """【関数】datetime/整数/None を ms(int) に正規化"""
        if isinstance(now, datetime):
            return int(now.timestamp() * 1000)
        if now is None:
            return now_ms()
        try:
            return int(now)
        except Exception:
            return now_ms()

    def _mark_zero(self, ob: OrderBook, now_ms: int) -> None:
        """【関数】ゼロ記録：spread==0 を見た“時刻”を記録して、のちほど“直後”判定に使う"""
        if ob.spread_ticks() == 0:
            self._last_spread_zero_ms = now_ms

    def _is_reopen(self, ob: OrderBook, now_ms: int) -> bool:
        """【関数】再拡大判定：“直近ゼロあり かつ 現在は≥min_spread_tick”かどうか"""
        seen_zero_recently = (now_ms - self._last_spread_zero_ms) <= self.cfg.seen_zero_window_ms
        return seen_zero_recently and (self.cfg.min_spread_tick <= ob.spread_ticks() <= self.cfg.max_spread_tick)  # 何をするか：1〜上限tickの“ちょうど良い開き”だけ許可

    def _pass_gates(self, ob: OrderBook, now_ms: int) -> bool:
        """【関数】安全ゲート：標準ガード（health_ok）・クールダウン・best存在チェックをまとめて判定"""
        health_check = getattr(ob, "health_ok", None)
        if callable(health_check) and not health_check():
            return False
        if (now_ms - self._last_action_ms) < self.cfg.cooloff_ms:
            return False
        if now_ms < self._lock_until_ms:
            return False  # 何をするか：まだTTL中＝前の注文が生きているので、新しい発注をロックして1枚運用を守る

        bid_px = getattr(getattr(ob, "best_bid", None), "price", None)
        ask_px = getattr(getattr(ob, "best_ask", None), "price", None)
        if bid_px is None or ask_px is None:
            return False
        return True

    def _choose_side(self, ob: OrderBook) -> str:
        """【関数】サイド決定：ミッドからのズレが大きい側（再拡大の外側）で“逆向きに1tick待つ”"""
        bid_px = getattr(getattr(ob, "best_bid", None), "price", None)
        ask_px = getattr(getattr(ob, "best_ask", None), "price", None)
        if bid_px is None or ask_px is None:
            return "buy"

        mid = None
        microprice = getattr(ob, "microprice", None)
        if callable(microprice):
            try:
                mid = microprice()
            except Exception:
                mid = None
        if mid is None:
            mid = (bid_px + ask_px) / 2.0

        bid_offset = abs(mid - bid_px)
        ask_offset = abs(ask_px - mid)

        if ask_offset > bid_offset:
            return "sell"
        return "buy"

    def _build_entry(self, ob: OrderBook, side: str) -> Dict[str, Any]:
        """【関数】エントリー生成：片面1発の指値（GTC+TTL・最小ロット・戦略タグ付）を作る"""
        tick = float(getattr(ob, "tick", 1.0))
        bid_px = getattr(getattr(ob, "best_bid", None), "price", None)
        ask_px = getattr(getattr(ob, "best_ask", None), "price", None)
        if bid_px is None or ask_px is None:
            raise ValueError("best bid/ask required for entry order")
        mid = (bid_px + ask_px) / 2.0
        px = mid - tick if side == "buy" else mid + tick
        order = Order(
            side=side,
            price=px,
            size=self.cfg.size_min,
            tif="GTC",
            ttl_ms=self.cfg.ttl_ms,
            tag=self._entry_tag,
        )
        return {"type": "place", "order": order}

    def _build_take_profit(self, ob: OrderBook, fill: Any) -> Dict[str, Any]:
        """【関数】利確生成：fillを受けたら反対側に+1tickのIOCを即返す（秒速撤退）"""
        tick = float(getattr(ob, "tick", 1.0))
        fill_side = getattr(fill, "side", None)
        fill_price = getattr(fill, "price", None)
        fill_size = getattr(fill, "size", None)
        if isinstance(fill, Mapping):
            fill_side = fill_side or str(fill.get("side", "")).lower()
            if fill_price is None:
                fill_price = fill.get("price")
            if fill_size is None:
                fill_size = fill.get("size")
            order_info = fill.get("order")
            if fill_price is None and order_info is not None:
                fill_price = getattr(order_info, "price", None)
            if fill_size is None and order_info is not None:
                fill_size = getattr(order_info, "size", None)
        else:
            if fill_side is not None:
                fill_side = str(fill_side).lower()

        if fill_price is None or fill_size is None:
            raise ValueError("fill price/size required for take profit")

        exit_side = "sell" if str(fill_side).lower() == "buy" else "buy"
        exit_price = (float(fill_price) + tick) if exit_side == "sell" else (float(fill_price) - tick)
        order = Order(
            side=exit_side,
            price=exit_price,
            size=float(fill_size),
            tif="IOC",
            ttl_ms=200,
            tag=self._take_tag,
        )
        return {"type": "place", "order": order}

    def _build_cancel(self) -> Dict[str, Any]:
        """【関数】エントリーの戦略タグを一括取消するアクション"""
        self._entry_active = False
        self._lock_until_ms = -10**9
        return {"type": "cancel_tag", "tag": self._entry_tag}

    # -------------------------
    # ランタイム・フック
    # -------------------------

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        """【関数】板イベント：ゼロを記録 → “直後の再拡大 & ゲート合格”ならアクションを返す"""
        now_ms = self._to_ms(now)
        actions: List[Dict[str, Any]] = []

        self._mark_zero(ob, now_ms)

        if self._entry_active and (now_ms >= self._lock_until_ms or not self._is_reopen(ob, now_ms)):
            actions.append(self._build_cancel())

        if self._is_reopen(ob, now_ms) and self._pass_gates(ob, now_ms):
            try:
                side = self._choose_side(ob)
                action = self._build_entry(ob, side)
            except ValueError:
                return actions
            actions.append(action)
            self._last_action_ms = now_ms
            self._lock_until_ms = now_ms + self.cfg.ttl_ms
            self._entry_active = True

        return actions

    def on_board(self, ob: OrderBook) -> List[Order]:
        """【関数】後方互換：旧IF（List[Order]）向けに place アクションだけを返す"""
        actions = self.evaluate(ob, datetime.now(timezone.utc), None)
        return [a["order"] for a in actions if a.get("type") == "place" and "order" in a]

    def on_fill(self, ob: OrderBook, my_fill: Any) -> List[Dict[str, Any]]:
        """【関数】約定イベント：+1tickのIOC利確を即返して“秒速で退出”する"""
        self._entry_active = False
        self._lock_until_ms = -10**9

        try:
            action = self._build_take_profit(ob, my_fill)
        except ValueError:
            return []
        return [action]
