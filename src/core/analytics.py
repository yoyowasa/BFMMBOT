# src/core/analytics.py
# 役割：戦略の「判断」を Parquet（logs/analytics/decision_log.parquet）に記録する“決定ログ係”
from __future__ import annotations

from pathlib import Path  # パス操作
from typing import Any, Dict, List  # 型ヒント
import polars as pl  # Parquet I/O
import orjson  # features_json を高速JSON化
from loguru import logger  # 進捗ログ

class DecisionLog:
    """【関数】決定ログ（features と decision を保存）"""
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.rows: List[Dict[str, Any]] = []

    def add(
        self,
        *,
        ts: str,
        strategy: str,
        decision: str,
        features: Dict[str, Any],
        expected_edge_bp: float | None,
        eta_ms: int | None,
        ca_ratio: float | None,
        best_age_ms: int | None,
        spread_state: str,
    ) -> None:
        # 説明：判断1件を追加（features は JSON 文字列にして保存）
        self.rows.append({
            "ts": ts,
            "strategy": strategy,
            "features_json": orjson.dumps(features).decode("utf-8"),
            "decision": decision,
            "expected_edge_bp": expected_edge_bp,
            "eta_ms": eta_ms,
            "ca_ratio": ca_ratio,
            "best_age_ms": best_age_ms,
            "spread_state": spread_state,
        })

    def flush(self) -> Path:
        # 説明：貯めた判断を Parquet に書き出し（既存があれば縦結合）
        if not self.rows:
            return self.path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new_df = pl.DataFrame(self.rows)
        if self.path.exists():
            old = pl.read_parquet(self.path)
            new_df = old.vstack(new_df)
        new_df.write_parquet(self.path)
        logger.info(f"decision log saved: {self.path} rows={len(new_df)} (+{len(self.rows)})")
        self.rows.clear()
        return self.path
