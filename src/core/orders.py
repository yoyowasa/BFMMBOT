# src/core/orders.py
# 役割：戦略→実行系に渡す「注文オブジェクト」の最小定義（side/price/size/TTL/tag など）

from __future__ import annotations

from dataclasses import dataclass, field  # データクラス
from datetime import datetime  # 作成時刻やTTL管理に使用
from typing import Optional, Dict, Any  # 型/発注仕様(dict)
from decimal import Decimal, ROUND_FLOOR  # 刻み単位の安全切り下げ
@dataclass
class Order:
    """【関数】注文モデル（最小）
    - 役割: 戦略が「どこに/どれだけ/どんな条件で」出すかを表現する箱
    - 注意: ここではまだ取引所送信はしない（BT用の内輪の表現）
    """
    side: str          # "buy" or "sell"
    price: float       # 指値価格
    size: float        # 数量
    tif: str = "GTC"   # 有効期限(GTC/IOC/FOK)の最小表現
    ttl_ms: Optional[int] = None  # TTL（ミリ秒）: 超えたらキャンセル
    tag: str = ""      # どの戦略/意図の注文かの印（例: "stall"）
    client_id: Optional[str] = None  # 将来の冪等キー用
    created_ts: Optional[datetime] = None  # 生成時刻（ランナーが埋める）
    remaining: float = field(default=0.0)   # 残量（シミュ用）

    def __post_init__(self) -> None:
        # 作成時の残量は発注サイズと同じ（Fillで減らす）
        self.remaining = self.size



def _floor_to_step(x: Decimal, step: Decimal) -> Decimal:
    """刻みstepに従ってxを安全に切り下げる小さな補助関数"""
    if step is None or step == 0:
        return x
    try:
        return (x / step).to_integral_value(rounding=ROUND_FLOOR) * step
    except Exception:
        return Decimal("0")


def auto_reduce_build(
    *,
    side: str,
    q: Decimal,
    min_step_qty: Decimal,
    qty_step: Decimal,
    tick_size: Decimal,
    best_bid: Optional[Decimal],
    best_ask: Optional[Decimal],
    tif: str = "IOC",
    tag: str = "auto_reduce",
) -> Optional[Dict[str, Any]]:
    """在庫を減らす向きの安全サイズで Reduce‑Only + IOC の発注仕様(dict)を作る関数"""
    # 在庫の絶対値と最小ステップの小さい方を、刻みに合わせて切り下げて1口だけ作る
    try:
        req = abs(Decimal(q))
        base = Decimal(min_step_qty)
        step = Decimal(qty_step) if qty_step else base
    except Exception:
        return None

    raw = req if req < base else base
    size = _floor_to_step(raw, step)
    if size <= 0:
        return None  # 0サイズになったら発注しない（安全側）

    # taker化するため、SELL→best_bid / BUY→best_ask に当てる（無ければNone=成行相当）
    s = (side or "").upper()
    if s not in ("BUY", "SELL"):
        return None
    price = best_bid if s == "SELL" else best_ask
    try:
        px = Decimal(price) if price is not None else None
    except Exception:
        px = None

    # Reduce‑Only + IOC の“仕様”を返す（実送信はエンジン側が行う）
    return {
        "side": s,
        "tif": tif,
        "price": px,
        "size": size,
        "reduce_only": True,
        "tag": tag,
    }

