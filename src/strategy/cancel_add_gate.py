# src/strategy/cancel_add_gate.py
# 役割：#2 キャンセル比ゲート（Best層の C/A 比が低い＝落ち着いた時だけ片面で出す）
from __future__ import annotations

from typing import List, Dict, Any  # 戻り値の型
from datetime import datetime  # 戦略判断のタイムスタンプ

from src.core.orderbook import OrderBook  # C/A 比・MP・Best/Spreadを参照
from src.core.orders import Order  # 指値の表現
from src.strategy.base import StrategyBase  # 共通IF

class CancelAddGate(StrategyBase):
    """【関数】#2 キャンセル比ゲートの最小実装
    - トリガ：ca_ratio(window_ms) ≤ θ かつ spread_tick ≥ 1
    - 動作：トリガ成立中のみ片面でリーン（MP寄り側）。条件外はタグ("ca_gate")を一括取消。
    参照：features.ca_ratio_win_ms / ca_threshold / min_spread_tick / ttl_ms / size.default。:contentReference[oaicite:3]{index=3} :contentReference[oaicite:4]{index=4}
    """
    name: str = "cancel_add_gate"

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        # 設定（無ければ文書の最小値にフォールバック）
        feats = getattr(cfg, "features", None)
        win_ms = getattr(feats, "ca_ratio_win_ms", 500)
        theta = getattr(feats, "ca_threshold", 1.3)
        min_sp = getattr(feats, "min_spread_tick", 1)
        ttl_ms = getattr(feats, "ttl_ms", 800)
        lot = getattr(getattr(cfg, "size", None), "default", 0.01)

        # Best未確定またはスプレッド不足→撤退（タグ一括取消）
        if ob.best_bid.price is None or ob.best_ask.price is None or ob.spread_ticks() < min_sp:
            return [{"type": "cancel_tag", "tag": "ca_gate"}]

        # 直近windowのBest層 C/A 比を取得（adds<=0なら∞扱い）
        ratio = ob.ca_ratio(now, window_ms=win_ms)
        if ratio <= theta:
            # MP寄り側にリーン：MP≥mid→sell側、MP<mid→buy側
            mid = (ob.best_bid.price + ob.best_ask.price) / 2.0
            mp = ob.microprice()
            if mp is None:
                # MP計算不可なら中立：とりあえず買い側に寄せる
                side = "buy"
            else:
                side = "sell" if mp >= mid else "buy"

            # 価格は「その側のBest」に置く（tickずれ防止の最小実装）
            if side == "buy":
                px = ob.best_bid.price
            else:
                px = ob.best_ask.price

            return [{"type": "place", "order": Order(side=side, price=px, size=lot,
                                                    tif="GTC", ttl_ms=ttl_ms, tag="ca_gate")}]

        # 条件外（比が悪化）→タグ一括取消
        return [{"type": "cancel_tag", "tag": "ca_gate"}]
