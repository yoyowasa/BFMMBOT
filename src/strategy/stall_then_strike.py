# src/strategy/stall_then_strike.py
# 役割: #1 静止→一撃。Best が静止 & スプレッド開き時に mid±1tick を両面、条件外なら撤退。
from __future__ import annotations

from typing import List, Dict, Any, Callable
from collections.abc import Mapping
from datetime import datetime, timezone

from loguru import logger

from src.core.orderbook import OrderBook
from src.core.orders import Order
from src.strategy.base import StrategyBase
from src.core.utils import coerce_ms, normalize_ttl_bands


class StallThenStrike(StrategyBase):
    """#1 静止→一撃。最小実装をドキュメントのトリガ/決済に合わせる。"""

    name: str = "stall_then_strike"

    def __init__(self, cfg=None, *, strategy_cfg=None):
        self.cfg = cfg
        self._strategy_cfg = strategy_cfg
        self._last_cancel_ts_ms: int | None = None
        self.engine = None
        self._time_source: Callable[[], int] | None = None
        self._pos_entry_px: float | None = None
        self._pos_entry_ts_ms: int | None = None
        self._pos_side: str | None = None

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

    def _resolve_ttl_bands(self, cfg) -> list[dict[str, float | int]]:
        raw = self._value_from(self._strategy_cfg, "ttl_bands")
        if not raw:
            raw = self._value_from(getattr(cfg, "features", None), "stall_then_strike", "ttl_bands")
        return normalize_ttl_bands(raw)

    def _resolve_ttl_band_window_ms(self, cfg, default: int = 1000) -> int:
        win = self._value_from(self._strategy_cfg, "ttl_band_window_ms")
        if win is None:
            win = self._value_from(getattr(cfg, "features", None), "stall_then_strike", "ttl_band_window_ms")
        try:
            win_val = int(win) if win is not None else default
        except Exception:
            win_val = default
        if win_val <= 0:
            return default
        return win_val

    def set_time_source(self, time_source: Callable[[], int] | None) -> None:
        if callable(time_source):
            self._time_source = time_source
        else:
            self._time_source = None

    def _resolve_min_requote_interval_ms(self) -> int | None:
        raw = self._value_from(self._strategy_cfg, "min_requote_interval_ms")
        if raw is None:
            return None
        try:
            value = int(raw)
        except Exception:
            return None
        if value <= 0:
            return None
        return value

    def _resolve_buy_momentum_filter(self) -> dict[str, float | int | str] | None:
        node = self._value_from(self._strategy_cfg, "stall_then_strike", "buy_momentum_filter")
        if node is None:
            node = self._value_from(self._strategy_cfg, "buy_momentum_filter")
        if node is None:
            return None
        raw: dict[str, float | int | str] = {}
        if isinstance(node, Mapping):
            raw = dict(node)
        else:
            for key in ("window_ms", "threshold_bps", "mode", "behavior", "skip_mode"):
                if hasattr(node, key):
                    raw[key] = getattr(node, key)
        if not raw:
            return None
        window_ms = raw.get("window_ms")
        threshold = raw.get("threshold_bps")
        mode = (
            raw.get("mode")
            or raw.get("behavior")
            or raw.get("skip_mode")
            or raw.get("on_trigger")
            or raw.get("when_triggered")
        )
        result: dict[str, float | int | str] = {}
        try:
            if window_ms is not None:
                win_val = int(window_ms)
                if win_val > 0:
                    result["window_ms"] = win_val
        except Exception:
            pass
        try:
            if threshold is not None:
                result["threshold_bps"] = float(threshold)
        except Exception:
            pass
        if isinstance(mode, str):
            mode_val = mode.strip().lower()
            if mode_val in {"sell_only", "halt_both", "both"}:
                if mode_val == "both":
                    mode_val = "halt_both"
                result["mode"] = mode_val
        return result or None

    def _resolve_min_mp_edge_bp(self, default: float = 0.0) -> float:
        node = self._value_from(self._strategy_cfg, "stall_then_strike", "min_mp_edge_bp")
        if node is None:
            node = self._value_from(self._strategy_cfg, "min_mp_edge_bp")
        if node is None:
            node = self._value_from(getattr(self.cfg, "features", None), "stall_min_mp_edge_bp")
        try:
            return float(node) if node is not None else default
        except Exception:
            return default

    def _resolve_min_mp_edge_bp(self, default: float = 0.0) -> float:
        node = self._value_from(self._strategy_cfg, "stall_then_strike", "min_mp_edge_bp")
        if node is None:
            node = self._value_from(self._strategy_cfg, "min_mp_edge_bp")
        if node is None:
            node = self._value_from(getattr(self.cfg, "features", None), "stall_min_mp_edge_bp")
        try:
            return float(node) if node is not None else default
        except Exception:
            return default

    def _current_time_ms(self, now: datetime) -> int | None:
        if callable(self._time_source):
            try:
                ts = int(self._time_source())
            except Exception:
                ts = None
            else:
                if ts >= 0:
                    return ts
        engine = getattr(self, "engine", None)
        if engine is not None:
            for attr in ("now_ms", "monotonic_ms"):
                candidate = getattr(engine, attr, None)
                if callable(candidate):
                    try:
                        ts = int(candidate())
                    except Exception:
                        continue
                    if ts >= 0:
                        return ts
        if isinstance(now, datetime):
            try:
                if now.tzinfo is None:
                    epoch = now.replace(tzinfo=timezone.utc)
                else:
                    epoch = now.astimezone(timezone.utc)
                return int(epoch.timestamp() * 1000)
            except Exception:
                return None
        return None

    def _current_inventory(self) -> float:
        eng = getattr(self, "engine", None)
        try:
            return float(getattr(eng, "Q", 0.0))
        except Exception:
            return 0.0

    def _stall_orders(self) -> list[Order]:
        eng = getattr(self, "engine", None)
        sim = getattr(eng, "sim", None)
        if sim is None:
            return []
        result: list[Order] = []
        for o in getattr(sim, "open", []):
            tag = getattr(o, "tag", "")
            if tag == "stall" or str(tag).startswith("stall|"):
                result.append(o)
        return result

    def _clear_position_state(self) -> None:
        self._pos_entry_px = None
        self._pos_entry_ts_ms = None
        self._pos_side = None

    def _ensure_position_state(self, inventory: float, ob: OrderBook, now: datetime) -> None:
        if abs(inventory) <= 1e-12:
            self._clear_position_state()
            return
        side = "buy" if inventory > 0 else "sell"
        if self._pos_side and self._pos_side != side:
            self._clear_position_state()
        self._pos_side = side
        if self._pos_entry_px is None:
            entry_px = None
            try:
                entry_px = float(getattr(getattr(self, "engine", None), "A", None))
            except Exception:
                entry_px = None
            if entry_px is None or entry_px == 0.0:
                try:
                    entry_px = (ob.best_bid.price + ob.best_ask.price) / 2.0
                except Exception:
                    entry_px = None
            self._pos_entry_px = entry_px
            self._pos_entry_ts_ms = self._current_time_ms(now)

    def _position_age_ms(self, now: datetime) -> int | None:
        if self._pos_entry_ts_ms is None:
            return None
        ts = self._current_time_ms(now)
        if ts is None:
            return None
        return ts - int(self._pos_entry_ts_ms)

    def _resolve_take_profit_ticks(self) -> float:
        val = self._value_from(self._strategy_cfg, "stall_then_strike", "take_profit_ticks")
        if val is None:
            val = self._value_from(self._strategy_cfg, "take_profit_ticks")
        try:
            tp = float(val)
        except Exception:
            tp = 1.0
        return tp if tp > 0 else 1.0

    def _resolve_stop_ticks(self) -> float:
        val = self._value_from(self._strategy_cfg, "stall_then_strike", "stop_ticks")
        if val is None:
            val = self._value_from(self._strategy_cfg, "stop_ticks")
        try:
            st = float(val)
        except Exception:
            st = 2.0
        return st if st > 0 else 2.0

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        self.cfg = cfg
        engine = locals().get("engine", getattr(self, "engine", None))
        gate = engine.gate_status() if (engine and hasattr(engine, "gate_status")) else {"mode": "healthy", "reason": "na", "limits": {}, "ts_ms": None}
        if gate["mode"] == "halted":
            logger.info(f"strategy:skip_new_orders mode=halted reason={gate.get('reason')}")
            return []

        feats = cfg.features
        stall_T = getattr(feats, "stall_T_ms", 250)
        min_sp = getattr(feats, "min_spread_tick", 1)
        ttl_ms = getattr(feats, "ttl_ms", 800)
        lot = self._resolve_size_default(cfg)
        tick = float(getattr(cfg, "tick_size", 1.0))

        ttl_bands = self._resolve_ttl_bands(cfg)
        ttl_window_ms = self._resolve_ttl_band_window_ms(cfg)
        mid_change_bp = ob.mid_change_bps(ttl_window_ms)
        selected_band = None
        if ttl_bands:
            abs_change = abs(mid_change_bp)
            for band in ttl_bands:
                threshold = band.get("threshold_bp")
                ttl_candidate = band.get("ttl_ms")
                if threshold is None or ttl_candidate is None:
                    continue
                if abs_change <= float(threshold):
                    try:
                        ttl_ms = int(ttl_candidate)
                    except Exception:
                        ttl_ms = getattr(feats, "ttl_ms", 800)
                    else:
                        selected_band = band
                    break

        buy_filter_cfg = self._resolve_buy_momentum_filter() or {}
        buy_filter_window = buy_filter_cfg.get("window_ms")
        buy_filter_threshold = buy_filter_cfg.get("threshold_bps")
        buy_filter_mode = buy_filter_cfg.get("mode") or "sell_only"
        buy_filter_mid_change = None
        if buy_filter_window is not None:
            try:
                buy_filter_mid_change = ob.mid_change_bps(int(buy_filter_window))
            except Exception:
                buy_filter_mid_change = None

        ttl_st = getattr(feats, "stall_ttl_ms", None)
        if ttl_st is None:
            ttl_st = getattr(cfg, "stall_ttl_ms", None)
        try:
            ttl_st = int(ttl_st)
        except Exception:
            ttl_st = ttl_ms

        decision_features = {
            "stall_mid_change_bp": mid_change_bp,
            "stall_ttl_selected_ms": ttl_ms,
            "stall_ttl_window_ms": ttl_window_ms,
            "stall_T_ms": stall_T,
            "stall_min_spread_tick": min_sp,
            "tick_size": tick,
        }
        if selected_band is not None and selected_band.get("threshold_bp") is not None:
            decision_features["stall_ttl_band_threshold_bp"] = selected_band["threshold_bp"]
        if buy_filter_window is not None:
            decision_features["stall_buy_filter_window_ms"] = buy_filter_window
        if buy_filter_threshold is not None:
            decision_features["stall_buy_filter_threshold_bp"] = buy_filter_threshold
        if buy_filter_mid_change is not None:
            decision_features["stall_buy_filter_mid_change_bp"] = buy_filter_mid_change
        if buy_filter_cfg:
            decision_features["stall_buy_filter_mode"] = buy_filter_mode
        decision_features["stall_buy_filter_suppressed"] = False

        if ob.best_bid.price is None or ob.best_ask.price is None:
            self._set_decision_features(decision_features)
            return []

        age_ms = coerce_ms(ob.best_age_ms(now)) or 0.0
        sp_tick = ob.spread_ticks()
        decision_features["best_age_ms"] = age_ms
        decision_features["spread_tick"] = sp_tick
        mid = (ob.best_bid.price + ob.best_ask.price) / 2.0
        mp = ob.microprice()
        mp_edge_bp = None
        if mid not in (None, 0) and mp is not None:
            mp_edge_bp = (mp - mid) / mid * 1e4
            decision_features["mp_edge_bp"] = mp_edge_bp
        min_edge_bp_base = self._resolve_min_mp_edge_bp(0.1)
        # スプレッドとBest Ageで下限をスライド（0.1〜0.3bp）
        edge_bump = 0.0
        if sp_tick >= 2:
            edge_bump += 0.05
        if age_ms >= 500:
            edge_bump += 0.05
        if age_ms >= 1000:
            edge_bump += 0.05
        min_edge_bp = min(0.3, min_edge_bp_base + edge_bump)
        decision_features["stall_min_mp_edge_bp"] = min_edge_bp
        zero_min_edge_bp = self._value_from(getattr(self.cfg, "features", None), "zero_min_mp_edge_bp") or 1.0
        try:
            zero_min_edge_bp = float(zero_min_edge_bp)
        except Exception:
            zero_min_edge_bp = 1.0
        decision_features["zero_min_mp_edge_bp"] = zero_min_edge_bp

        inventory = self._current_inventory()
        has_position = abs(inventory) > 1e-12
        if has_position:
            self._ensure_position_state(inventory, ob, now)
        else:
            self._clear_position_state()

        stall_orders = self._stall_orders()
        has_reduce_order = any(getattr(o, "reduce_only", False) for o in stall_orders)
        needs_cancel_entries = any(not getattr(o, "reduce_only", False) for o in stall_orders)
        tp_ticks = self._resolve_take_profit_ticks()
        stop_ticks = self._resolve_stop_ticks()
        pos_age_ms = self._position_age_ms(now)
        if pos_age_ms is not None:
            decision_features["stall_position_age_ms"] = pos_age_ms

        if has_position:
            entry_px = self._pos_entry_px if self._pos_entry_px is not None else mid
            best_moved = age_ms < stall_T or sp_tick < min_sp
            adverse = False
            if entry_px is not None and mid is not None:
                drift = mid - entry_px
                if inventory > 0:
                    adverse = drift <= -stop_ticks * tick
                else:
                    adverse = drift >= stop_ticks * tick
            timeout = pos_age_ms is not None and ttl_ms is not None and pos_age_ms >= ttl_ms

            actions: List[Dict[str, Any]] = []
            if needs_cancel_entries:
                actions.append({"type": "cancel_tag", "tag": "stall"})
            if best_moved or adverse or timeout:
                qty = abs(inventory)
                close_side = "sell" if inventory > 0 else "buy"
                close_px = ob.best_bid.price if inventory > 0 else ob.best_ask.price
                if close_px is not None and qty > 0:
                    order = Order(
                        side=close_side,
                        price=close_px,
                        size=qty,
                        tif="IOC",
                        ttl_ms=ttl_ms,
                        tag="stall",
                    )
                    order.reduce_only = True
                    actions.append({"type": "place", "order": order, "allow_multiple": True})
                self._set_decision_features(decision_features)
                return actions

            if not has_reduce_order:
                qty = abs(inventory)
                target_px = None
                if entry_px is not None:
                    target_px = entry_px + tp_ticks * tick if inventory > 0 else entry_px - tp_ticks * tick
                if target_px is not None and qty > 0:
                    order = Order(
                        side="sell" if inventory > 0 else "buy",
                        price=target_px,
                        size=qty,
                        tif="IOC",
                        ttl_ms=ttl_ms,
                        tag="stall",
                    )
                    order.reduce_only = True
                    actions.append({"type": "place", "order": order, "allow_multiple": True})
            if actions:
                self._set_decision_features(decision_features)
                return actions
            self._set_decision_features(decision_features)
            return []

        # ここからは建玉なしのエントリー判定
        if age_ms is not None and age_ms >= stall_T and sp_tick >= min_sp and not stall_orders:
            apply_edge_block = (min_edge_bp > 0) and (min_sp > 0) and (stall_T > 0)
            allow_buy = True
            allow_sell = True
            if apply_edge_block:
                if mp_edge_bp is None:
                    decision_features["stall_mp_edge_blocked"] = True
                    self._set_decision_features(decision_features)
                    return [{"type": "cancel_tag", "tag": "stall"}]
                if mp_edge_bp < 0:
                    allow_buy = False
                    decision_features["stall_mp_edge_buy_blocked"] = True
                elif mp_edge_bp < min_edge_bp:
                    decision_features["stall_mp_edge_blocked"] = True
                    self._set_decision_features(decision_features)
                    return [{"type": "cancel_tag", "tag": "stall"}]
            if sp_tick == 0:
                if mp_edge_bp is None:
                    decision_features["stall_zero_mp_edge_blocked"] = True
                    self._set_decision_features(decision_features)
                    return [{"type": "cancel_tag", "tag": "stall"}]
                zero_min_edge_bp = decision_features.get("zero_min_mp_edge_bp") or 1.0
                if mp_edge_bp > -zero_min_edge_bp:
                    allow_sell = False
                    decision_features["stall_zero_mp_edge_sell_blocked"] = True
                if mp_edge_bp < zero_min_edge_bp:
                    allow_buy = False
                    decision_features["stall_zero_mp_edge_buy_blocked"] = True
            buy_suppressed = False
            if buy_filter_cfg and buy_filter_threshold is not None and buy_filter_mid_change is not None:
                try:
                    buy_suppressed = float(buy_filter_mid_change) >= float(buy_filter_threshold)
                except Exception:
                    buy_suppressed = False
            decision_features["stall_buy_filter_suppressed"] = bool(buy_suppressed)
            self._set_decision_features(decision_features)
            if buy_suppressed:
                actions: List[Dict[str, Any]] = [
                    {"type": "cancel_tag", "tag": "stall"},
                ]
                if buy_filter_mode != "halt_both":
                    actions.append(
                        {
                            "type": "place",
                            "order": Order(
                                side="sell",
                                price=mid + 1 * tick,
                                size=lot,
                                tif="GTC",
                                ttl_ms=ttl_st,
                                tag="stall",
                            ),
                            "allow_multiple": True,
                        }
                    )
                return actions
            actions: list[Dict[str, Any]] = []
            if allow_buy:
                actions.append(
                    {
                        "type": "place",
                        "order": Order(
                            side="buy",
                            price=mid - 1 * tick,
                            size=lot,
                            tif="GTC",
                            ttl_ms=ttl_st,
                            tag="stall",
                        ),
                        "allow_multiple": True,
                    }
                )
            actions.append(
                {
                    "type": "place",
                    "order": Order(
                        side="sell",
                        price=mid + 1 * tick,
                        size=lot,
                        tif="GTC",
                        ttl_ms=ttl_st,
                        tag="stall",
                    ),
                    "allow_multiple": True,
                }
            )
            return actions

        min_requote_interval_ms = self._resolve_min_requote_interval_ms()
        if min_requote_interval_ms is not None:
            current_ts_ms = self._current_time_ms(now)
            if (
                current_ts_ms is not None
                and self._last_cancel_ts_ms is not None
                and (current_ts_ms - self._last_cancel_ts_ms) < min_requote_interval_ms
            ):
                self._set_decision_features(decision_features)
                return []
            if current_ts_ms is not None:
                self._last_cancel_ts_ms = current_ts_ms
        self._set_decision_features(decision_features)
        return [{"type": "cancel_tag", "tag": "stall"}]
