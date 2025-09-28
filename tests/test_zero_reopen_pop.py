from pathlib import Path
from types import SimpleNamespace
import importlib.util
import sys
import types


def _ensure_module(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        sys.modules[name] = module
    return module


def _ensure_package(name: str) -> types.ModuleType:
    module = _ensure_module(name)
    if not hasattr(module, "__path__"):
        module.__path__ = []  # type: ignore[attr-defined]
    return module


# Build minimal package structure so zero_reopen_pop can import its dependencies without
# requiring the full runtime stack (external deps such as pydantic/loguru/pyyaml).
src_pkg = _ensure_package("src")
strategy_pkg = _ensure_package("src.strategy")
core_pkg = _ensure_package("src.core")

base_module = _ensure_module("src.strategy.base")


class _StrategyBase:
    def __init__(self, *_, **__):
        pass


base_module.StrategyBase = _StrategyBase
setattr(strategy_pkg, "base", base_module)
setattr(src_pkg, "strategy", strategy_pkg)

orderbook_module = _ensure_module("src.core.orderbook")


class _OrderBook:
    pass


orderbook_module.OrderBook = _OrderBook

orders_module = _ensure_module("src.core.orders")


class _Order:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


orders_module.Order = _Order

utils_module = _ensure_module("src.core.utils")


def _now_ms() -> int:
    return 0


utils_module.now_ms = _now_ms

setattr(core_pkg, "orderbook", orderbook_module)
setattr(core_pkg, "orders", orders_module)
setattr(core_pkg, "utils", utils_module)
setattr(src_pkg, "core", core_pkg)


spec = importlib.util.spec_from_file_location(
    "src.strategy.zero_reopen_pop", Path(__file__).resolve().parents[1] / "src" / "strategy" / "zero_reopen_pop.py"
)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)
ZeroReopenPop = module.ZeroReopenPop


class _StubOrderBook:
    def __init__(self, bid_price: float, ask_price: float, microprice: float | None):
        self.best_bid = SimpleNamespace(price=bid_price)
        self.best_ask = SimpleNamespace(price=ask_price)
        self._microprice = microprice

    def microprice(self) -> float | None:  # pragma: no cover - trivial delegation
        return self._microprice


def test_choose_side_returns_sell_when_ask_farther_from_midpoint():
    ob = _StubOrderBook(bid_price=100.0, ask_price=102.0, microprice=100.8)
    strategy = ZeroReopenPop()

    side = strategy._choose_side(ob)

    assert side == "sell"


def test_choose_side_returns_buy_when_bid_farther_or_tied():
    ob = _StubOrderBook(bid_price=100.0, ask_price=102.0, microprice=101.4)
    strategy = ZeroReopenPop()

    side = strategy._choose_side(ob)

    assert side == "buy"


def test_choose_side_falls_back_to_arithmetic_mid_when_microprice_missing():
    ob = _StubOrderBook(bid_price=100.0, ask_price=102.0, microprice=None)
    strategy = ZeroReopenPop()

    side = strategy._choose_side(ob)

    assert side == "buy"
