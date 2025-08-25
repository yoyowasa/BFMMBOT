# src/core/orders.py
# 役割：戦略→実行系に渡す「注文オブジェクト」の最小定義（side/price/size/TTL/tag など）
from __future__ import annotations

from dataclasses import dataclass, field  # 簡潔なデータ入れ物
from datetime import datetime  # 作成時刻・TTL判定に使用
from typing import Optional  # 省略可の型

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
