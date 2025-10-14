# src/strategy/base.py
# 役割：戦略クラスの共通インターフェース（evaluateで注文アクションを返す）
from __future__ import annotations

import copy
import contextvars
from typing import Any, Dict, Iterable, List, Sequence  # 返り値の型
from datetime import datetime  # 戦略判断の時刻
from src.core.orderbook import OrderBook  # ローカル板

class StrategyBase:
    """【関数】戦略の基底：on_event/evaluate の型だけ決める最小クラス"""
    name: str = "base"

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        """【関数】戦略評価：必要なときだけ [{'type':'place'|'cancel_tag', ...}] を返す"""
        return []


current_strategy_ctx = contextvars.ContextVar("current_strategy_name", default=None)


class MultiStrategy(StrategyBase):
    """複数戦略をまとめて扱うラッパー"""

    def __init__(self, strategies: Sequence[StrategyBase]) -> None:
        children = list(strategies)
        if not children:
            raise ValueError("MultiStrategy requires at least one child strategy")
        self.children: List[StrategyBase] = children
        child_names = [getattr(child, "name", "") or "unknown" for child in self.children]
        self.name = self._compose_name(child_names)

    @staticmethod
    def _compose_name(child_names: Sequence[str]) -> str:
        if len(child_names) == 1:
            return str(child_names[0])
        joined = "+".join(str(name) for name in child_names)
        return f"multi:{joined}"

    @staticmethod
    def _prefixed_tag(child_name: str, tag: Any) -> str:
        base = "" if tag in (None, "") else str(tag)
        prefix = str(child_name) if child_name else ""
        if not prefix:
            return base
        if base == prefix or base.startswith(f"{prefix}:"):
            return base
        return f"{prefix}:{base}" if base else prefix

    @staticmethod
    def _strip_child_tag(child_name: str, tag: Any) -> tuple[str, bool]:
        base = "" if tag in (None, "") else str(tag)
        if not child_name:
            return base, True
        if not base:
            return "", True
        prefix = f"{child_name}:"
        if base == child_name:
            return "", True
        if base.startswith(prefix):
            return base[len(prefix):], True
        return base, False

    @staticmethod
    def _clone_fill_event(fill: Any) -> Any:
        if isinstance(fill, dict):
            return dict(fill)
        try:
            clone = copy.copy(fill)
        except Exception:
            return fill
        return clone

    @staticmethod
    def _set_tag(target: Any, tag: str) -> None:
        if isinstance(target, dict):
            target["tag"] = tag
            return
        if hasattr(target, "tag"):
            try:
                setattr(target, "tag", tag)
            except Exception:
                pass

    @staticmethod
    def _get_order_from_action(action: Dict[str, Any]) -> Any:
        return action.get("order") if isinstance(action, dict) else None

    def _wrap_actions(self, child: StrategyBase, actions: Iterable[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
        wrapped: List[Dict[str, Any]] = []
        if not actions:
            return wrapped
        child_name = getattr(child, "name", "")
        for action in actions:
            if not isinstance(action, dict):
                wrapped.append(action)
                continue
            item = dict(action)
            if item.get("type") == "place":
                order = self._get_order_from_action(item)
                if order is not None:
                    order.tag = self._prefixed_tag(child_name, getattr(order, "tag", ""))
            elif item.get("type") == "cancel_tag":
                item["tag"] = self._prefixed_tag(child_name, item.get("tag"))
            wrapped.append(item)
        return wrapped

    def _dispatch_actions(self, method_name: str, *args, **kwargs) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for child in self.children:
            method = getattr(child, method_name, None)
            if not callable(method):
                continue
            actions = method(*args, **kwargs)
            results.extend(self._wrap_actions(child, actions))
        return results

    def on_start(self, *args, **kwargs):
        return self._dispatch_actions("on_start", *args, **kwargs)

    def on_event(self, *args, **kwargs):
        results: List[Dict[str, Any]] = []
        for child in self.children:
            method = getattr(child, "on_event", None)
            if not callable(method):
                continue
            child_name = getattr(child, "strategy_name", None) or getattr(child, "name", "unknown")
            token = current_strategy_ctx.set(child_name or "unknown")
            try:
                actions = method(*args, **kwargs)
            finally:
                current_strategy_ctx.reset(token)
            results.extend(self._wrap_actions(child, actions))
        return results

    def on_stop(self, *args, **kwargs):
        return self._dispatch_actions("on_stop", *args, **kwargs)

    def evaluate(self, *args, **kwargs):
        return self._dispatch_actions("evaluate", *args, **kwargs)

    def on_fill(self, ob: OrderBook, fill_event: Any):
        results: List[Dict[str, Any]] = []
        for child in self.children:
            handler = getattr(child, "on_fill", None)
            if not callable(handler):
                continue
            child_name = getattr(child, "name", "")
            tag_value = getattr(fill_event, "tag", None)
            if isinstance(fill_event, dict):
                tag_value = fill_event.get("tag")
            stripped_tag, matched = self._strip_child_tag(child_name, tag_value)
            if not matched and tag_value not in (None, ""):
                continue
            event_copy = self._clone_fill_event(fill_event)
            self._set_tag(event_copy, stripped_tag)
            order = getattr(event_copy, "order", None)
            if isinstance(event_copy, dict):
                order = event_copy.get("order")
            if order is not None:
                try:
                    setattr(order, "tag", stripped_tag)
                except Exception:
                    pass
            actions = handler(ob, event_copy)
            results.extend(self._wrap_actions(child, actions))
        return results
