"""リスクゲート：在庫上限と安全マージンを扱う補助クラス"""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any


def _to_mapping(obj: Any) -> Mapping[str, Any]:
    """【関数】属性/辞書を読み取り専用の辞書風に正規化"""
    if obj is None:
        return {}
    if isinstance(obj, Mapping):
        return obj
    if hasattr(obj, "__dict__") and isinstance(obj.__dict__, MutableMapping):
        return obj.__dict__
    return {}


class RiskGate:
    """在庫ゲート（max_inventory と安全マージン inventory_eps を扱う）"""

    def __init__(self, cfg: Any | None = None) -> None:
        risk_section: Mapping[str, Any] = {}
        cfg_map = _to_mapping(cfg)
        if cfg_map:
            risk_section = _to_mapping(cfg_map.get("risk")) or cfg_map
        risk_section = risk_section or {}

        max_inv_raw = risk_section.get("max_inventory") if isinstance(risk_section, Mapping) else None
        self.max_inventory = float(max_inv_raw) if max_inv_raw is not None else None

        default_eps = 0.0
        if self.max_inventory is not None:
            default_eps = max(0.0, float(self.max_inventory) * 0.01)
        eps_raw = risk_section.get("inventory_eps") if isinstance(risk_section, Mapping) else None
        self.inventory_eps = float(eps_raw) if eps_raw is not None else default_eps
        self.market_mode = "healthy"  # 何をする行か：板の健康状態（healthy/caution/halted）を覚える

    def set_market_mode(self, mode: str):
        # 【関数】市場モードを受け取り、ゲートの振る舞いを切り替える（healthy/caution/halted）
        self.market_mode = mode

    def effective_inventory_limit(self) -> float | None:
        """【関数】新規発注の実効上限（max_inventory − inventory_eps）を返す"""
        if self.max_inventory is None:
            return None
        limit = float(self.max_inventory) - float(self.inventory_eps)
        return max(0.0, limit)

    def would_reduce_inventory(self, current_inventory: float, side: str | None, request_qty: float) -> bool:
        """【関数】注文が在庫|Q|を減らす（=決済）かどうかを判定"""
        if side is None:
            return False
        try:
            side_norm = str(side).strip().lower()
        except Exception:
            return False
        if side_norm not in {"buy", "sell"}:
            return False
        try:
            qty = float(request_qty)
        except (TypeError, ValueError):
            return False
        if qty <= 0.0:
            return False
        delta = qty if side_norm == "buy" else -qty
        return abs(current_inventory + delta) <= abs(current_inventory)

    def can_place(
        self,
        current_inventory: float,
        request_qty: float,
        side: str | None = None,
        reduce_only: bool = False,
        best_age_ms: float | None = None,
        **kwargs,
    ) -> bool:
        """【関数】新規発注の許可/不許可を判定する（在庫・安全装置の入口）。best_age_msは任意で健康判定に利用。"""
        if self.market_mode in ("caution", "halted"):  # 何をする行か：市場モードが注意/停止ならClose-Onlyを適用
            if reduce_only or (side and self.would_reduce_inventory(current_inventory, side, float(request_qty))):
                return True   # 何をする行か：在庫を減らす（決済）なら常に許可
            return False      # 何をする行か：在庫が増える方向の新規はブロック

        try:
            qty = abs(float(request_qty))
        except (TypeError, ValueError):
            return False

        eff_limit = self.effective_inventory_limit()
        if eff_limit is None:
            return True

        if abs(current_inventory) + qty <= eff_limit:
            return True

        if reduce_only or (side and self.would_reduce_inventory(current_inventory, side, float(request_qty))):
            return True

        return False
