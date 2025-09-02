# src/strategy/age_microprice.py
# 役割：#3 エイジ×マイクロプライス（Age×MP）
# - 【関数】evaluate：|MP-mid| ≥ αtick かつ best_age_ms ≥ βms（かつ spread≥1）なら、
#   MP寄り側に片面でリーンして Best に指値（TTL付き）。条件外はタグ一括キャンセル。
from __future__ import annotations

from typing import List, Dict, Any  # 戻り値の型
from datetime import datetime      # 戦略判断の時刻

from src.core.orderbook import OrderBook  # BestAge / Microprice / Spread を参照
from src.core.orders import Order         # 指値モデル
from src.strategy.base import StrategyBase

class AgeMicroprice(StrategyBase):
    """【関数】#3 エイジ×マイクロプライス（最小実装）
    トリガ：|MP - mid| ≥ αtick かつ best_age_ms ≥ βms（かつ spread ≥ 1tick）
    動作：MP寄り側（MP≥mid→sell, MP<mid→buy）に Best で片面提示（TTL付き）
    撤退：条件外→タグ("age_mp")を一括キャンセル（TTLはシミュ側で自動処理）
    """
    name: str = "age_microprice"

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        feats = getattr(cfg, "features", None)
        beta_ms = getattr(feats, "age_ms", 200)                 # β：最小静止時間
        alpha_tick = getattr(feats, "mp_offset_tick", 1.0)      # α：MP偏りのしきい値
        min_sp = getattr(feats, "min_spread_tick", 1)           # 必要最小スプレッド
        ttl_ms = getattr(feats, "ttl_ms", 800)
        lot = getattr(getattr(cfg, "size", None), "default", 0.01)
        tick = float(getattr(cfg, "tick_size", 1.0))

        # Best未確定やスプレッド不足は撤退
        if ob.best_bid.price is None or ob.best_ask.price is None:
            return []
        if ob.spread_ticks() < min_sp:
            return [{"type": "cancel_tag", "tag": "age_mp"}]

        # 指標計算（AgeとMP）
        age = ob.best_age_ms(now)
        mp = ob.microprice()
        if mp is None:
            return [{"type": "cancel_tag", "tag": "age_mp"}]

        mid = (ob.best_bid.price + ob.best_ask.price) / 2.0
        offset_tick = abs(mp - mid) / tick

        # トリガ成立 → 片面で Best に置く（MP寄り側）
        if age >= beta_ms and offset_tick >= alpha_tick:
            side = "sell" if mp >= mid else "buy"
            px = ob.best_ask.price if side == "sell" else ob.best_bid.price
            return [{"type": "place", "order": Order(side=side, price=px, size=lot,
                                                    tif="GTC", ttl_ms=ttl_ms, tag="age_mp")}]

        # 条件外 → タグ一括キャンセル
        return [{"type": "cancel_tag", "tag": "age_mp"}]
