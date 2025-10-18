# src/strategy/base.py
# 役割：戦略クラスの共通インターフェース（evaluateで注文アクションを返す）
from __future__ import annotations

import copy
import contextvars
import uuid  # 何をするか：各子戦略の意思決定に固有の相関IDを割り当てる
import importlib  # 何をするか：戦略モジュールを動的に読み込むために使う
import inspect    # 何をするか：モジュール内から StrategyBase のサブクラスを見つけるために使う
from collections.abc import Mapping
from typing import Any, Dict, Iterable, List, Sequence  # 返り値の型
from datetime import datetime  # 戦略判断の時刻
from src.core.orderbook import OrderBook  # ローカル板

class StrategyBase:
    """【関数】戦略の基底：on_event/evaluate の型だけ決める最小クラス"""
    name: str = "base"

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        """【関数】戦略評価：必要なときだけ [{'type':'place'|'cancel_tag', ...}] を返す"""
        return []

    def _set_decision_features(self, features: Mapping[str, Any] | None) -> None:
        if features is None:
            self._decision_features = None
            return
        if isinstance(features, Mapping):
            self._decision_features = dict(features)
        else:
            self._decision_features = None

    def consume_decision_features(self) -> Dict[str, Any] | None:
        payload = getattr(self, "_decision_features", None)
        self._decision_features = None
        if isinstance(payload, Mapping):
            return dict(payload)
        return None


current_strategy_ctx = contextvars.ContextVar("current_strategy_name", default=None)
current_corr_ctx = contextvars.ContextVar("current_corr_id", default=None)  # 何をするか：現在の意思決定に割り当てた相関IDを共有する


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
        child = str(child_name) if child_name else ""
        suffix = f"|{child}" if child else ""
        if not suffix:
            return base
        if not base:
            return child
        if base.endswith(suffix):
            return base
        return f"{base}{suffix}"

    @staticmethod
    def _strip_child_tag(child_name: str, tag: Any) -> tuple[str, bool]:
        base = "" if tag in (None, "") else str(tag)
        child = str(child_name) if child_name else ""
        if not child:
            return base, True
        if not base:
            return "", True
        suffix = f"|{child}"
        if base == child:
            return "", True
        if base.endswith(suffix):
            return base[: -len(suffix)], True
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
        child_strategy_name = (
            getattr(child, "strategy_name", None) or child_name or "unknown"
        )
        corr_id = current_corr_ctx.get()
        for action in actions:
            if not isinstance(action, dict):
                if corr_id is not None:
                    try:
                        setattr(action, "_corr_id", corr_id)
                    except Exception:
                        pass
                try:
                    setattr(action, "_strategy", child_strategy_name)
                except Exception:
                    pass
                if hasattr(action, "tag"):
                    try:
                        current_tag = getattr(action, "tag", "")
                        prefixed = self._prefixed_tag(child_name, current_tag)
                        if prefixed != current_tag:
                            setattr(action, "tag", prefixed)
                    except Exception:
                        pass
                wrapped.append(action)
                continue
            item = dict(action)
            if corr_id is not None and "_corr_id" not in item:
                item["_corr_id"] = corr_id
            if item.get("type") == "place":
                order = self._get_order_from_action(item)
                if order is not None:
                    try:
                        setattr(order, "_strategy", child_strategy_name)
                    except Exception:
                        pass
                    if corr_id is not None:
                        setattr(order, "_corr_id", corr_id)
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
            child_name = getattr(child, "strategy_name", None) or getattr(child, "name", "unknown")
            token_strategy = current_strategy_ctx.set(child_name or "unknown")
            token_corr = current_corr_ctx.set(uuid.uuid4().hex)
            actions = None
            wrapped: List[Dict[str, Any]] = []
            try:
                actions = method(*args, **kwargs)
                wrapped = self._wrap_actions(child, actions)
            finally:
                current_corr_ctx.reset(token_corr)
                current_strategy_ctx.reset(token_strategy)
            results.extend(wrapped)
        return results

    def consume_decision_features(self) -> Dict[str, Any] | None:
        merged: Dict[str, Any] = {}
        for child in self.children:
            getter = getattr(child, "consume_decision_features", None)
            child_feats = getter() if callable(getter) else None
            if not isinstance(child_feats, Mapping):
                continue
            for key, value in child_feats.items():
                if key in merged:
                    continue
                merged[key] = value
        return merged or None

    def on_start(self, *args, **kwargs):
        return self._dispatch_actions("on_start", *args, **kwargs)

    def on_event(self, state):
        # 何をするか：全ての子戦略を順に実行し、返ってきた注文をひとつのリストに結合して返す
        merged = []  # 何をするか：全子戦略の発注をここに集める
        for c in self.children:  # 何をするか：子戦略を1つずつ実行
            token_strategy = current_strategy_ctx.set(getattr(c, "strategy_name", getattr(c, "name", "unknown")))  # 何をするか：実行中の子戦略名を合図にセット
            token_corr = current_corr_ctx.set(uuid.uuid4().hex)  # 何をするか：この子戦略の意思決定に一意の相関IDを振る
            try:
                out = c.on_event(state) or []  # 何をするか：子戦略の意思決定（[Order] または None）を取得
            finally:
                current_corr_ctx.reset(token_corr)  # 何をするか：相関IDの合図を元に戻す
                current_strategy_ctx.reset(token_strategy)  # 何をするか：子戦略名の合図を元に戻す
            child_name = getattr(c, "strategy_name", getattr(c, "name", "unknown"))  # 何をするか：この注文群の出所（子戦略名）
            for o in out:
                if not getattr(o, "_strategy", None):
                    o._strategy = child_name  # 何をするか：注文オブジェクトに子戦略名の印（ログ用）を刻む
                if getattr(o, "tag", None) and child_name not in str(o.tag):
                    o.tag = f"{o.tag}|{child_name}"  # 何をするか：tag末尾に「|子戦略名」を付けて order/trade ログで見分けやすくする
            merged.extend(out)  # 何をするか：この子からの注文を結合
        return merged  # 何をするか：全ての子戦略の注文をまとめて返す

    def on_stop(self, *args, **kwargs):
        return self._dispatch_actions("on_stop", *args, **kwargs)

    def evaluate(self, *args, **kwargs):
        return self._dispatch_actions("evaluate", *args, **kwargs)

    def on_fill(self, ob: OrderBook, fill: Any):
        results: List[Dict[str, Any]] = []
        for child in self.children:
            handler = getattr(child, "on_fill", None)
            if not callable(handler):
                continue
            child_name = getattr(child, "name", "")
            tag_value = getattr(fill, "tag", None)
            if isinstance(fill, dict):
                tag_value = fill.get("tag")
            stripped_tag, matched = self._strip_child_tag(child_name, tag_value)
            if not matched and tag_value not in (None, ""):
                continue
            event_copy = self._clone_fill_event(fill)
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


def _get_strategy_registry():
    # 何をするか：戦略名→モジュール名の対応表（設定の strategies 配列はこのキーで指定する）
    return {
        "stall_then_strike": "stall_then_strike",
        "cancel_add_gate": "cancel_add_gate",
        "age_microprice": "age_microprice",
        "gap_edge_refill": "gap_edge_refill",
        "tiny_prints_filter": "tiny_prints_filter",
        "queue_eta_gate": "queue_eta_gate",
        "zero_reopen_pop": "zero_reopen_pop",
    }  # 参照：7戦略の命名と配置（docsの構成と一致）。


def _load_strategy_class(module_name: str):
    # 何をするか：指定モジュールから StrategyBase を継承するクラスを1つ見つけて返す
    m = importlib.import_module(f"src.strategy.{module_name}")
    for _, obj in inspect.getmembers(m, inspect.isclass):
        try:
            if issubclass(obj, StrategyBase) and obj is not StrategyBase:
                return obj
        except Exception:
            continue
    raise ImportError(f"No StrategyBase subclass found in module: {module_name}")


def build_strategy_from_cfg(cfg: dict, *, strategy_cfg: Any = None):
    # 何をするか：cfg['strategies'] の配列から戦略を作り、複数なら MultiStrategy に束ねて返す
    names = list(cfg.get("strategies", []) or [])
    if not names:
        raise ValueError("No strategies specified in cfg['strategies']")  # 何をするか：必須チェック
    reg = _get_strategy_registry()
    children = []
    for name in names:
        module_name = reg.get(name)
        if not module_name:
            raise KeyError(f"Unknown strategy name: {name}")  # 何をするか：未登録名を早期に知らせる
        cls = _load_strategy_class(module_name)  # 何をするか：モジュールから戦略クラスを取得
        override = strategy_cfg
        if isinstance(strategy_cfg, Mapping):
            override = strategy_cfg.get(name)
        inst = None
        candidates = []
        if override is not None:
            candidates.extend([
                ((), {"cfg": cfg, "strategy_cfg": override}),
                ((), {"strategy_cfg": override}),
                ((override,), {}),
            ])
        candidates.extend([
            ((cfg,), {}),
            ((), {"cfg": cfg}),
            ((), {}),
        ])
        for args, kwargs in candidates:
            try:
                inst = cls(*args, **kwargs)
                break
            except TypeError:
                continue
        if inst is None:
            raise TypeError(f"failed to construct strategy '{name}' from cfg")
        if not getattr(inst, "strategy_name", None):
            inst.strategy_name = name  # 何をするか：ログ・タグ用に“正式名”を刻む（docsの命名に揃える）
        children.append(inst)
    if len(children) == 1:
        return children[0]  # 何をするか：1本だけならそのまま返す
    try:
        return MultiStrategy(children=children)  # 何をするか：複数本を束ねる（キーワード引数版）
    except TypeError:
        return MultiStrategy(children)           # 何をするか：位置引数版の互換フォールバック

