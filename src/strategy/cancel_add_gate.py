# src/strategy/cancel_add_gate.py
# 役割: #2 キャンセル比ゲート（C/A比が低い静かな板だけ片側で出すMM）。毒性上昇や条件外なら即撤退。
from __future__ import annotations

from typing import List, Dict, Any
from collections.abc import Mapping
from datetime import datetime

from src.core.orderbook import OrderBook  # C/A比・Best/Spreadを参照
from src.core.orders import Order  # 置く指値
from src.strategy.base import StrategyBase  # 共通IF


class CancelAddGate(StrategyBase):
    """#2 キャンセル比ゲートの最小実装"""

    name: str = "cancel_add_gate"

    def __init__(self, cfg=None, *, strategy_cfg=None):
        self.cfg = cfg
        self._strategy_cfg = strategy_cfg
        self._inventory = 0.0  # 現在の在庫（Fillで更新）

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

    def _resolve_taker_z_thresh(self):
        # strategy_cfg > features.ca_taker_zscore_threshold の順で拾う
        cand = self._value_from(self._strategy_cfg, "taker_zscore_threshold")
        if cand is None:
            feats = getattr(self.cfg, "features", None)
            if feats is not None:
                cand = getattr(feats, "ca_taker_zscore_threshold", None)
                if cand is None and isinstance(feats, Mapping):
                    cand = feats.get("ca_taker_zscore_threshold")
        try:
            return float(cand)
        except Exception:
            return None

    def _taker_zscore(self):
        # エンジン側に毒性zscoreがあれば拾う（無ければNone）
        eng = getattr(self, "engine", None)
        for attr in ("taker_zscore", "toxicity_zscore", "recent_taker_zscore"):
            val = getattr(eng, attr, None)
            if val is not None:
                try:
                    return float(val)
                except Exception:
                    continue
        return None

    def on_fill(self, ob: OrderBook, fill: Any):
        """在庫をFillで更新（BUYは+／SELLは-）。撤退は共通リスク側へ委譲。"""
        try:
            side = getattr(fill, "side", None) if not isinstance(fill, dict) else (fill.get("side") or fill.get("action"))
            sz = None
            if isinstance(fill, dict):
                sz = fill.get("sz") if fill.get("sz") is not None else fill.get("size")
            else:
                sz = getattr(fill, "sz", None) if getattr(fill, "sz", None) is not None else getattr(fill, "size", None)
            qty = float(sz or 0.0)
            if side is not None and qty:
                s = str(side).lower()
                if s in ("buy", "bid", "b"):
                    self._inventory = float(getattr(self, "_inventory", 0.0)) + qty
                elif s in ("sell", "ask", "s"):
                    self._inventory = float(getattr(self, "_inventory", 0.0)) - qty
        except Exception:
            pass
        return []

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        feats = getattr(cfg, "features", None)
        win_ms = getattr(feats, "ca_ratio_win_ms", 500)
        theta = getattr(feats, "ca_threshold", 1.3)
        min_sp_cfg = getattr(cfg, "min_spread_tick", None)
        min_sp = getattr(feats, "min_spread_tick", 1) if min_sp_cfg is None else min_sp_cfg
        ttl_ms = getattr(feats, "ttl_ms", 800)
        lot = self._resolve_size_default(cfg)
        z_thresh = self._resolve_taker_z_thresh()

        spread_ticks = ob.spread_ticks()
        decision_features: Dict[str, Any] = {
            "ca_ratio_win_ms": win_ms,
            "ca_threshold": theta,
            "spread_tick": spread_ticks,
        }

        # Best未確定 or スプレッド不足 → 即撤退
        if ob.best_bid.price is None or ob.best_ask.price is None or spread_ticks < min_sp:
            self._set_decision_features(decision_features)
            return [{"type": "cancel_tag", "tag": "ca_gate"}]

        # 在庫帯ブロック
        feats_node = getattr(cfg, "features", None)
        feats_node = feats_node if feats_node is not None else (cfg.get("features") if isinstance(cfg, Mapping) else None)
        blk_min = getattr(feats_node, "ca_gate_block_min_abs_inv", None) if feats_node is not None else None
        if blk_min is None and isinstance(feats_node, Mapping):
            blk_min = feats_node.get("ca_gate_block_min_abs_inv")
        blk_max = getattr(feats_node, "ca_gate_block_max_abs_inv", None) if feats_node is not None else None
        if blk_max is None and isinstance(feats_node, Mapping):
            blk_max = feats_node.get("ca_gate_block_max_abs_inv")
        try:
            inv_abs = abs(float(getattr(self, "_inventory", 0.0) or 0.0))
        except Exception:
            inv_abs = 0.0
        if blk_min is not None and blk_max is not None:
            try:
                if inv_abs >= float(blk_min) and inv_abs < float(blk_max):
                    decision_features["ca_block_band"] = True
                    self._set_decision_features(decision_features)
                    return [{"type": "cancel_tag", "tag": "ca_gate"}]
            except Exception:
                pass

        ratio = ob.ca_ratio(now, window_ms=win_ms)
        decision_features["ca_ratio"] = ratio

        # 在庫帯ごとのしきい値補正
        try:
            abs_inv = abs(float(getattr(self, "_inventory", 0.0) or 0.0))
        except Exception:
            abs_inv = 0.0
        rules = None
        if feats is not None:
            rules = getattr(feats, "ca_threshold_by_abs_inv", None)
            if rules is None and isinstance(feats, Mapping):
                rules = feats.get("ca_threshold_by_abs_inv")
        if rules:
            try:
                for b in rules:
                    mx = b.get("max_abs_inv") if isinstance(b, Mapping) else getattr(b, "max_abs_inv", None)
                    th = b.get("theta") if isinstance(b, Mapping) else getattr(b, "theta", None)
                    if mx is None or th is None:
                        continue
                    if abs_inv <= float(mx):
                        theta = float(th)
                        break
            except Exception:
                pass
        decision_features["ca_threshold_effective"] = theta

        taker_z = self._taker_zscore()
        if taker_z is not None:
            decision_features["taker_zscore"] = taker_z
        if z_thresh is not None:
            decision_features["taker_zscore_threshold"] = z_thresh

        # 撤退条件: C/A超過 or taker zscore高騰
        if ratio > theta or (z_thresh is not None and taker_z is not None and taker_z >= z_thresh):
            self._set_decision_features(decision_features)
            return [{"type": "cancel_tag", "tag": "ca_gate"}]

        # 条件維持: 片側だけ提示（Microprice寄り）
        mid = (ob.best_bid.price + ob.best_ask.price) / 2.0
        mp = ob.microprice()
        if mp is None:
            side = "buy"
        else:
            side = "sell" if mp >= mid else "buy"

        px = ob.best_bid.price if side == "buy" else ob.best_ask.price
        self._set_decision_features(decision_features)
        return [{
            "type": "place",
            "order": Order(side=side, price=px, size=lot, tif="GTC", ttl_ms=ttl_ms, tag="ca_gate"),
        }]
