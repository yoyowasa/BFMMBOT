from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import sys
import types

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if "loguru" not in sys.modules:
    stub = types.ModuleType("loguru")

    class _DummyLogger:
        def __getattr__(self, _):  # noqa: D401
            def _noop(*args, **kwargs):
                return None

            return _noop

    stub.logger = _DummyLogger()
    sys.modules["loguru"] = stub

if "yaml" not in sys.modules:
    yaml_stub = types.ModuleType("yaml")

    def _safe_load(_stream):
        return {}

    yaml_stub.safe_load = _safe_load
    sys.modules["yaml"] = yaml_stub

if "pydantic" not in sys.modules:
    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModel:
        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

        @classmethod
        def model_validate(cls, data):
            inst = cls()
            if isinstance(data, dict):
                for key, value in data.items():
                    setattr(inst, key, value)
            return inst

    def _field(default, description=None, **_kwargs):  # noqa: D401
        return default

    pydantic_stub.BaseModel = _BaseModel
    pydantic_stub.Field = _field
    sys.modules["pydantic"] = pydantic_stub

from src.core.orderbook import OrderBook
from src.strategy.stall_then_strike import StallThenStrike


def _make_cfg(ttl_ms: int = 800):
    features = SimpleNamespace(
        stall_T_ms=0,
        min_spread_tick=0,
        ttl_ms=ttl_ms,
        stall_then_strike=SimpleNamespace(),
    )
    size_cfg = SimpleNamespace(default=0.01)
    return SimpleNamespace(features=features, tick_size=1.0, size=size_cfg)


def test_ttl_falls_back_to_default_when_bands_missing():
    now = datetime.now(timezone.utc)
    ob = OrderBook(tick_size=1.0)
    ob.apply_board(
        now,
        {
            "bids": [{"price": 100.0, "size": 0.5}],
            "asks": [{"price": 101.0, "size": 0.5}],
        },
    )

    cfg = _make_cfg(ttl_ms=900)
    strat = StallThenStrike(cfg=cfg)

    actions = strat.evaluate(ob, now, cfg)
    orders = [a["order"] for a in actions if a.get("type") == "place"]
    assert orders, "expected place actions when stall conditions satisfied"
    assert {o.ttl_ms for o in orders} == {900}

    decision_features = strat.consume_decision_features() or {}
    assert decision_features.get("stall_ttl_selected_ms") == 900
    assert "stall_ttl_band_threshold_bp" not in decision_features
