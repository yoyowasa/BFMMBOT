# src/strategy/cancel_add_gate.py
# 役割：#2 キャンセル比ゲート（Best層の C/A 比が低い＝落ち着いた時だけ片面で出す）
from __future__ import annotations

from typing import List, Dict, Any  # 戻り値の型
from collections.abc import Mapping
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

    def on_fill(self, ob: OrderBook, fill: Any):
        """在庫をFillで更新する（BUYで+、SELLで-）。既存のロジックに影響を与えない。"""
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
        # 設定（無ければ文書の最小値にフォールバック）
        feats = getattr(cfg, "features", None)
        win_ms = getattr(feats, "ca_ratio_win_ms", 500)
        theta = getattr(feats, "ca_threshold", 1.3)
        min_sp_cfg = getattr(cfg, "min_spread_tick", None)
        min_sp = getattr(feats, "min_spread_tick", 1) if min_sp_cfg is None else min_sp_cfg
        ttl_ms = getattr(feats, "ttl_ms", 800)
        lot = self._resolve_size_default(cfg)

        # Best未確定またはスプレッド不足→撤退（タグ一括取消）
        spread_ticks = ob.spread_ticks()
        if ob.best_bid.price is None or ob.best_ask.price is None or spread_ticks < min_sp:
            return [{"type": "cancel_tag", "tag": "ca_gate"}]

        # 直近windowのBest層 C/A 比を取得（adds<=0なら∞扱い）
        # 在庫帯のブレーキ（負けゾーンは新規発注しない）
        _feats = getattr(cfg, "features", None)
        _feats = (_feats if _feats is not None else (cfg.get("features") if isinstance(cfg, Mapping) else None))
        _blk_min = (getattr(_feats, "ca_gate_block_min_abs_inv", None) if _feats is not None else None)
        if _blk_min is None and isinstance(_feats, Mapping): _blk_min = _feats.get("ca_gate_block_min_abs_inv")
        _blk_max = (getattr(_feats, "ca_gate_block_max_abs_inv", None) if _feats is not None else None)
        if _blk_max is None and isinstance(_feats, Mapping): _blk_max = _feats.get("ca_gate_block_max_abs_inv")
        try:
            inv_abs = abs(float(getattr(self, "_inventory", 0.0) or 0.0))
        except Exception:
            inv_abs = 0.0
        if _blk_min is not None and _blk_max is not None:
            try:
                if inv_abs >= float(_blk_min) and inv_abs < float(_blk_max):
                    return [{"type": "cancel_tag", "tag": "ca_gate"}]
            except Exception:
                pass

        ratio = ob.ca_ratio(now, window_ms=win_ms)
        # 可変しきい値 θ（在庫帯に応じて調整）
        try:
            abs_inv = abs(float(getattr(self, "_inventory", 0.0) or 0.0))
        except Exception:
            abs_inv = 0.0
        _rules = None
        if feats is not None:
            _rules = getattr(feats, "ca_threshold_by_abs_inv", None)
            if _rules is None and isinstance(feats, Mapping): _rules = feats.get("ca_threshold_by_abs_inv")
        if _rules:
            try:
                for b in _rules:
                    _mx = None; _th = None
                    if isinstance(b, Mapping):
                        _mx = b.get("max_abs_inv"); _th = b.get("theta")
                    else:
                        _mx = getattr(b, "max_abs_inv", None); _th = getattr(b, "theta", None)
                    if _mx is None or _th is None: continue
                    if abs_inv <= float(_mx):
                        theta = float(_th); break
            except Exception:
                pass

        if ratio <= theta and spread_ticks >= min_sp:  # C/Aゲート: 最低スプレッドtickを設定値で制御（0〜1tick帯は通さない）
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
