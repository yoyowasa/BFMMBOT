# src/core/logs.py
# 役割：orders/trades ログを Parquet に書き出す“記録係”
from __future__ import annotations

from pathlib import Path  # パス操作
from typing import List, Dict, Any  # 型ヒント
import polars as pl  # Parquet I/O（pyproject に準拠）
from loguru import logger  # 進捗ログ
import orjson  # NDJSONミラー出力（1行JSON）に使用

class OrderLog:
    """【関数】発注ログ（place/cancel/fill/partial）を Parquet に保存"""
    def __init__(self, path: str | Path, mirror_ndjson: str | Path | None = None) -> None:
        self.path = Path(path)
        self.rows: List[Dict[str, Any]] = []
        self._mirror = Path(mirror_ndjson) if mirror_ndjson else None  # NDJSONミラーの出力先
        if self._mirror: self._mirror.parent.mkdir(parents=True, exist_ok=True)

    def add(self, *, ts: str, action: str, tif: str | None, ttl_ms: int | None,
            px: float | None, sz: float | None, reason: str = "") -> None:
        # 説明：1件の発注イベントを追加（後でflushでまとめて保存）
        self.rows.append({
            "ts": ts, "action": action, "tif": tif, "ttl_ms": ttl_ms,
            "px": float(px) if px is not None else None,
            "sz": float(sz) if sz is not None else None,
            "reason": reason,
        })
        if self._mirror:
            # リアルタイム確認：1行JSONを追記（PowerShellの -Wait で見える）
            self._mirror.open("a", encoding="utf-8").write(orjson.dumps(self.rows[-1]).decode("utf-8") + "\n")


    def flush(self) -> Path:
        # 説明：貯めた行を Parquet に書き出す。既存があれば縦結合して書き直し。
        if not self.rows:
            return self.path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new_df = pl.DataFrame(self.rows)
        if self.path.exists():
            old_df = pl.read_parquet(self.path)
            new_df = old_df.vstack(new_df)
        new_df.write_parquet(self.path)
        logger.info(f"orders log saved: {self.path} rows={len(new_df)} (+{len(self.rows)})")
        self.rows.clear()
        return self.path

class TradeLog:
    """【関数】約定ログ（サイド/価格/サイズ/実現PnLなど）を Parquet に保存"""
    def __init__(self, path: str | Path, mirror_ndjson: str | Path | None = None) -> None:
        self.path = Path(path)
        self.rows: List[Dict[str, Any]] = []
        self._mirror = Path(mirror_ndjson) if mirror_ndjson else None  # NDJSONミラーの出力先
        if self._mirror: self._mirror.parent.mkdir(parents=True, exist_ok=True)


    def add(self, *, ts: str, side: str, px: float, sz: float,
            pnl: float, strategy: str, tag: str,
            inventory_after: float, fee: float = 0.0,
            window_funding: bool = False, window_maint: bool = False) -> None:
        # 説明：1件の約定（実現PnL含む）を追加
        self.rows.append({
            "ts": ts, "side": side, "px": float(px), "sz": float(sz),
            "fee": float(fee), "pnl": float(pnl),
            "strategy": strategy, "tag": tag,
            "inventory_after": float(inventory_after),
            "window_funding": bool(window_funding),
            "window_maint": bool(window_maint),
        })
        if self._mirror:
            # リアルタイム確認：1行JSONを追記
            self._mirror.open("a", encoding="utf-8").write(orjson.dumps(self.rows[-1]).decode("utf-8") + "\n")


    def flush(self) -> Path:
        if not self.rows:
            return self.path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new_df = pl.DataFrame(self.rows)
        if self.path.exists():
            old_df = pl.read_parquet(self.path)
            new_df = old_df.vstack(new_df)
        new_df.write_parquet(self.path)
        logger.info(f"trades log saved: {self.path} rows={len(new_df)} (+{len(self.rows)})")
        self.rows.clear()
        return self.path
