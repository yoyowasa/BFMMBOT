"""strategy パッケージの初期化
- 役割: 戦略プラグイン（#1〜#7）を入れる箱
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

# 何をするimportか：エントリーポイント（CLI/ランタイム）から戦略クラスを取り出せるようにする
from .stall_then_strike import StallThenStrike  # 何をするか：#1 静止→一撃（Best静止後にミッド±1tick両面を置く）
from .cancel_add_gate import CancelAddGate    # 何をするか：#2 キャンセル比ゲート（落ち着いた瞬間だけ片面提示）
from .age_microprice import AgeMicroprice     # 何をするか：#3 エイジ×マイクロプライス（Age×MPで片面提示）
from .zero_reopen_pop import (
    ZeroReopenConfig,
    ZeroReopenPop,
    zero_reopen_config_from,
)  # 何をするか：ゼロ→再拡大の“一拍”だけ狙うワンショットMMのクラスと設定を読み込む

# 何をする定義か：戦略名→クラスの対応表（CLI --strategy などから参照）
STRATEGY_REGISTRY = {
    "stall_then_strike": StallThenStrike,  # 何をするか：Best静止→ミッド±1tick両面を置く戦略を登録
    "cancel_add_gate": CancelAddGate,      # 何をするか：C/A比ゲートで落ち着いた瞬間だけ片面提示する戦略を登録
    "age_microprice": AgeMicroprice,      # 何をするか：MP偏りとBest Ageで片面提示する戦略を登録
    "zero_reopen_pop": ZeroReopenPop,     # 何をするか：spread==0直後の再拡大に片面1tick→当たったら即+1tick利確の戦略を登録
}


def _merge_zero_reopen_cfg(
    base: ZeroReopenConfig | None, override: Any,
) -> ZeroReopenConfig | None:
    """何をする関数か：zero_reopen_pop の設定を base + override でマージする"""

    if override is None:
        return base

    base_dict = asdict(base) if isinstance(base, ZeroReopenConfig) else {}

    if isinstance(override, ZeroReopenConfig):
        merged = {**base_dict, **asdict(override)}
        return ZeroReopenConfig(**merged)

    if isinstance(override, Mapping):
        merged = {**base_dict, **dict(override)}
        return ZeroReopenConfig(**merged)

    raise TypeError("zero_reopen_pop override must be ZeroReopenConfig or mapping")


def build_strategy(name: str, cfg, *, strategy_cfg: Any = None):
    """何をする関数か：戦略名からインスタンスを生成する中央ファクトリ"""

    try:
        cls = STRATEGY_REGISTRY[name]
    except KeyError as exc:  # 何をするか：未知の戦略名なら明示的に止める
        raise ValueError(f"unknown strategy: {name}") from exc

    if name == "zero_reopen_pop":
        merged_cfg = _merge_zero_reopen_cfg(
            zero_reopen_config_from(cfg), strategy_cfg,
        )
        candidates = [
            ((), {"cfg": merged_cfg}),
            ((merged_cfg,), {}),
            ((), {}),
        ]
    else:
        candidates = []
        if strategy_cfg is not None:
            candidates.extend([((strategy_cfg,), {}), ((), {"cfg": strategy_cfg})])
        candidates.extend([((cfg,), {}), ((), {"cfg": cfg}), ((), {})])

    for args, kwargs in candidates:
        try:
            return cls(*args, **kwargs)
        except TypeError:
            continue

    raise TypeError(f"failed to construct strategy '{name}'")


__all__ = [
    "StallThenStrike",
    "CancelAddGate",
    "AgeMicroprice",
    "ZeroReopenPop",
    "ZeroReopenConfig",
    "zero_reopen_config_from",
    "STRATEGY_REGISTRY",
    "build_strategy",
]
