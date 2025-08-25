# src/strategy/base.py
# 役割：戦略クラスの共通インターフェース（evaluateで注文アクションを返す）
from __future__ import annotations

from typing import List, Dict, Any  # 返り値の型
from datetime import datetime  # 戦略判断の時刻
from src.core.orderbook import OrderBook  # ローカル板

class StrategyBase:
    """【関数】戦略の基底：on_event/evaluate の型だけ決める最小クラス"""
    name: str = "base"

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        """【関数】戦略評価：必要なときだけ [{'type':'place'|'cancel_tag', ...}] を返す"""
        return []
