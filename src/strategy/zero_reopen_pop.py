# 何をするモジュールか：
#   「スプレッドが一瞬0になった直後、1tick以上に再拡大した“その一拍”だけ」片面で最小ロットを置き、
#   当たったら即IOCで+1tick利確して退出する“イベント駆動ワンショットMM”の本体実装。

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional

# 何をするimportか：戦略の骨組み・板の読み取り・注文生成・時刻取得（すべて既存の共通層を利用）
from src.strategy.base import StrategyBase
from src.core.orderbook import OrderbookView  # best/中値/tick/スプレッド/健全性 health_ok() を提供
from src.core.orders import Order, TIF        # Limit/IOC と TTL/タグを付けて発注する型
from src.core.utils import now_ms             # クールダウンや“直後”判定に使うミリ秒時刻


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

    def __init__(self, *, cfg: Optional[ZeroReopenConfig] = None) -> None:
        # 何をする関数か：設定と内部状態（直近ゼロ時刻／直近アクション時刻）の初期化
        super().__init__()
        self.cfg = cfg or ZeroReopenConfig()
        self._last_spread_zero_ms: int = -10**9
        self._last_action_ms: int = -10**9
        self._lock_until_ms: int = -10**9  # 何をするか：同時に複数枚を出さない“発注ロック”の期限ms（TTL中は新規禁止）

    # -------------------------
    # 内部ヘルパ（責務を明記）
    # -------------------------

    def _mark_zero(self, ob: OrderbookView, now: int) -> None:
        """【関数】ゼロ記録：spread==0 を見た“時刻”を記録して、のちほど“直後”判定に使う"""
        if ob.spread_ticks() == 0:
            self._last_spread_zero_ms = now

    def _is_reopen(self, ob: OrderbookView, now: int) -> bool:
        """【関数】再拡大判定：“直近ゼロあり かつ 現在のspreadが[min,max]帯”かどうか"""
        seen_zero_recently = (now - self._last_spread_zero_ms) <= self.cfg.seen_zero_window_ms
        return seen_zero_recently and (self.cfg.min_spread_tick <= ob.spread_ticks() <= self.cfg.max_spread_tick)  # 何をするか：1〜上限tickの“ちょうど良い開き”だけ許可

    def _pass_gates(self, ob: OrderbookView, now: int) -> bool:
        """【関数】安全ゲート：標準ガード（health_ok）・クールダウン・best存在チェックをまとめて判定"""
        if not ob.health_ok():
            return False
        if (now - self._last_action_ms) < self.cfg.cooloff_ms:
            return False
        if now < self._lock_until_ms:
            return False  # 何をするか：まだTTL中＝前の注文が生きているので、新しい発注をロックして1枚運用を守る
        if ob.best_bid() is None or ob.best_ask() is None:
            return False
        return True

    def _choose_side(self, ob: OrderbookView) -> str:
        """【関数】サイド決定：ミッドからのズレが大きい側（再拡大の外側）で“逆向きに1tick待つ”"""
        mid = ob.mid_price()
        bid = ob.best_bid()
        ask = ob.best_ask()
        # ask−mid が大きい＝上に開いた ⇒ 戻りBUYを mid−1tick に置く
        if (ask - mid) >= (mid - bid):
            return "BUY"
        # それ以外＝下に開いた ⇒ 戻りSELLを mid＋1tick に置く
        return "SELL"

    def _build_entry(self, ob: OrderbookView, side: str) -> Order:
        """【関数】エントリー生成：片面1発の指値（GTC+TTL・最小ロット・戦略タグ付）を作る"""
        tick = ob.tick_size()
        mid = ob.mid_price()
        px = mid - tick if side == "BUY" else mid + tick
        return Order.limit(
            side=side,
            price=px,
            size=self.cfg.size_min,
            tif=TIF.GTC,
            ttl_ms=self.cfg.ttl_ms,
            tag="zero_reopen",
        )

    def _build_take_profit(self, ob: OrderbookView, fill) -> Order:
        """【関数】利確生成：fillを受けたら反対側に+1tickのIOCを即時に返す（秒速撤退）"""
        tick = ob.tick_size()
        if fill.side == "BUY":
            return Order.limit(
                side="SELL",
                price=fill.price + tick,
                size=fill.size,
                tif=TIF.IOC,
                ttl_ms=200,
                tag="zero_reopen_take",
            )
        return Order.limit(
            side="BUY",
            price=fill.price - tick,
            size=fill.size,
            tif=TIF.IOC,
            ttl_ms=200,
            tag="zero_reopen_take",
        )

    # -------------------------
    # ランタイム・フック
    # -------------------------

    def on_board(self, ob: OrderbookView) -> List[Order]:
        """【関数】板イベント：ゼロを記録 → “直後の再拡大 & ゲート合格”なら片面1発だけ返す"""
        now = now_ms()
        self._mark_zero(ob, now)
        if self._is_reopen(ob, now) and self._pass_gates(ob, now):
            side = self._choose_side(ob)
            order = self._build_entry(ob, side)
            self._last_action_ms = now
            self._lock_until_ms = now + self.cfg.ttl_ms  # 何をするか：このTTLの間は新規発注を禁止して“同時1枚だけ”を保証する
            return [order]
        # ふだんは何もしない（Idle）
        return []

    def on_fill(self, ob: OrderbookView, my_fill) -> List[Order]:
        """【関数】約定イベント：+1tickのIOC利確を即返して“秒速で退出”する"""
        self._lock_until_ms = now_ms() - 1  # 何をするか：約定で用件完了→ロックを即解除して次のチャンスを待てるようにする
        return [self._build_take_profit(ob, my_fill)]
