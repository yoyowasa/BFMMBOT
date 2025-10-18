# src/strategy/stall_then_strike.py
# 役割：#1 静止→一撃（BestがTms静止 & Spread>=Sの直後にミッド±1tickへ最小ロット両面、条件外は即撤退）
from __future__ import annotations

from typing import List, Dict, Any  # 返り値の型
from collections.abc import Mapping
from datetime import datetime  # 戦略判断時刻

from loguru import logger  # 何をするか：ゲート理由を戦略ログに1行で出す

from src.core.orderbook import OrderBook  # Best/Spreadを参照
from src.core.orders import Order  # 置く指値の表現
from src.strategy.base import StrategyBase  # 共通IF
from src.core.utils import coerce_ms  # 追加：時間値をmsに正規化するユーティリティ

class StallThenStrike(StrategyBase):
    """【関数】#1 静止→一撃の最小実装（文書のトリガ/撤退に準拠）"""
    name: str = "stall_then_strike"

    def __init__(self, cfg=None, *, strategy_cfg=None):
        self.cfg = cfg
        self._strategy_cfg = strategy_cfg

    @staticmethod
    def _value_from(node, *keys):
        current = node
        for key in keys:
            if current is None:
                return None
            if isinstance(current, Mapping):
                current = current.get(key)
            else:
                current = getattr(current, key, None)
        return current

    def _resolve_size_default(self, cfg) -> float:
        override_default = self._value_from(self._strategy_cfg, "size", "default")
        if override_default is not None:
            try:
                return float(override_default)
            except (TypeError, ValueError):
                return override_default
        base_default = self._value_from(cfg, "size", "default")
        if base_default is not None:
            try:
                return float(base_default)
            except (TypeError, ValueError):
                return base_default
        return 0.01

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        engine = locals().get("engine", getattr(self, "engine", None))  # 何をするか：エンジン参照（引数or属性）
        gate = engine.gate_status() if (engine and hasattr(engine, "gate_status")) else {"mode": "healthy", "reason": "na", "limits": {}, "ts_ms": None}  # 何をするか：ゲート状態を取得
        if gate["mode"] == "halted":  # 何をするか：停止中は新規注文を作らず早期リターン（決済は別系で通す）
            logger.info(f"strategy:skip_new_orders mode=halted reason={gate.get('reason')}")  # 何をするか：スキップ理由を1行で記録
            return []  # 何をするか：このイベントでは何も出さない（新規ブロック）

        # 設定の読み出し（無ければ文書の最小値）:contentReference[oaicite:4]{index=4} :contentReference[oaicite:5]{index=5}
        feats = cfg.features
        stall_T = getattr(feats, "stall_T_ms", 250)
        min_sp = getattr(feats, "min_spread_tick", 1)
        ttl_ms = getattr(feats, "ttl_ms", 800)
        lot = self._resolve_size_default(cfg)
        tick = float(getattr(cfg, "tick_size", 1.0))

        # Bestが未確定のときは何もしない
        if ob.best_bid.price is None or ob.best_ask.price is None:
            return []

        # 現在の指標を取得（BestAge/Spread）:contentReference[oaicite:6]{index=6}
        age_ms = coerce_ms(ob.best_age_ms(now)) or 0.0

        sp_tick = ob.spread_ticks()

        # トリガ成立：ミッド±1tick に最小ロット両面
        if age_ms is not None and age_ms >= stall_T and sp_tick >= min_sp:
            mid = (ob.best_bid.price + ob.best_ask.price) / 2.0
            return [
                {"type": "place", "order": Order(side="buy",  price=mid - 1 * tick, size=lot, tif="GTC", ttl_ms=ttl_ms, tag="stall")},
                {"type": "place", "order": Order(side="sell", price=mid + 1 * tick, size=lot, tif="GTC", ttl_ms=ttl_ms, tag="stall")},
            ]
        # 条件外：この戦略タグの注文は撤退
        return [{"type": "cancel_tag", "tag": "stall"}]
