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
    max_speed_ticks_per_s: float = 12.0  # 何をする設定か：midの速さ（tick/秒）がこの上限を超えたら発注しない
    flat_timeout_ms: int = 600         # 何をする設定か：利確IOCが通らない時の“時間でフラット”の締切ms
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
        self._tp_pending: bool = False        # 何をするか：利確IOC待ち（まだ手仕舞えていない）かどうか
        self._tp_deadline_ms: int = -10**9    # 何をするか：この時刻を過ぎたら“フラットIOC”を出す締切
        self._open_side: Optional[str] = None # 何をするか：保有している方向（BUY/SELL）
        self._open_size: float = 0.0          # 何をするか：保有しているサイズ
        self._last_mid_px: float = 0.0       # 何をするか：前回のmid価格を記録して速さ（速度）を測る
        self._last_mid_ts_ms: int = -10**9   # 何をするか：前回midの記録時刻（ms）
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
        # 何をするか：midの“速さ”（tick/秒）を計算し、上限を超えると危険なので発注を見送る
        mid = ob.mid_price()
        tick = ob.tick_size()
        if self._last_mid_ts_ms > 0:
            dt_ms = now_ms - self._last_mid_ts_ms
            if dt_ms > 0:
                speed_ticks_per_s = abs(mid - self._last_mid_px) / tick * (1000.0 / dt_ms)
                if speed_ticks_per_s > self.cfg.max_speed_ticks_per_s:
                    return False  # 速すぎる＝トレンド急進中と判断し、今回は出さない
        # 記録を更新（次回の速度計算のため）
        self._last_mid_px = mid
        self._last_mid_ts_ms = now_ms
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

        if self._tp_pending:
            if now_ms >= self._tp_deadline_ms:
                best_bid_fn = getattr(ob, "best_bid", None)
                best_ask_fn = getattr(ob, "best_ask", None)
                best_bid = best_bid_fn() if callable(best_bid_fn) else best_bid_fn
                best_ask = best_ask_fn() if callable(best_ask_fn) else best_ask_fn
                bid_px = getattr(best_bid, "price", best_bid)
                ask_px = getattr(best_ask, "price", best_ask)
                if (
                    bid_px is not None
                    and ask_px is not None
                    and self._open_side in {"BUY", "SELL"}
                    and self._open_size > 0.0
                ):
                    order_side = "sell" if self._open_side == "BUY" else "buy"
                    order_price = float(bid_px) if order_side == "sell" else float(ask_px)
                    order = Order(
                        side=order_side,
                        price=order_price,
                        size=self._open_size,
                        tif="IOC",
                        ttl_ms=200,
                        tag="zero_reopen_flat",
                    )
                    actions.append({"type": "place", "order": order})
                    self._tp_deadline_ms = now_ms + self.cfg.flat_timeout_ms
                    return actions
            return actions

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
        now = now_ms()  # 何をするか：フラット締切の計算に使う
        tag = getattr(my_fill, "tag", "")
        if tag is None:
            tag = ""
        tag_str = str(tag)

        if tag_str in ("zero_reopen_take", "zero_reopen_flat"):
            # 何をするか：利確IOC or フラットIOCが約定した＝ポジション解消、ロック解除
            self._tp_pending = False
            self._open_side = None
            self._open_size = 0.0
            self._entry_active = False
            self._lock_until_ms = now - 1
            return []

        self._entry_active = False
        self._lock_until_ms = -10**9

        # 何をするか：エントリー約定を受けたので、利確IOCの“締切”をセットして待機フラグON
        fill_side = getattr(my_fill, "side", None)
        fill_size = getattr(my_fill, "size", None)
        if isinstance(my_fill, Mapping):
            if fill_side is None:
                fill_side = my_fill.get("side")
            if fill_size is None:
                fill_size = my_fill.get("size")
            order_info = my_fill.get("order")
            if fill_side is None and order_info is not None:
                fill_side = getattr(order_info, "side", None)
            if fill_size is None and order_info is not None:
                fill_size = getattr(order_info, "size", None)
        side_str = str(fill_side).upper() if fill_side is not None else None
        size_val = float(fill_size) if fill_size is not None else 0.0
        self._open_side = side_str if side_str else None
        self._open_size = size_val if size_val > 0.0 else 0.0
        if self._open_side is not None and self._open_size > 0.0:
            self._tp_pending = True
            self._tp_deadline_ms = now + self.cfg.flat_timeout_ms
        else:
            self._tp_pending = False
            self._open_side = None
            self._open_size = 0.0

        try:
            action = self._build_take_profit(ob, my_fill)
        except ValueError:
            if self._tp_pending:
                self._tp_pending = False
                self._open_side = None
                self._open_size = 0.0
            return []
        return [action]
