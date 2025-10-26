# src/core/analytics.py
# 役割: 意思決定ログ（features と decision）を Parquet に保存。必要に応じて NDJSON ミラー。
from __future__ import annotations

from pathlib import Path  # パス
from typing import Any, Dict, List  # 型
import polars as pl  # Parquet I/O
import orjson  # features_json 直列化
from loguru import logger  # ログ
from datetime import datetime, timezone, timedelta  # JSTタグ

# ---- NDJSON ローテ（20MB + 日次） ----
_NDJSON_LIMIT_BYTES_DEFAULT = 20 * 1024 * 1024

def _jst_tag_from_iso(ts: str) -> str:
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    jst = timezone(timedelta(hours=9))
    return dt.astimezone(jst).strftime("%Y%m%d")


def _next_seq_path(base_dir: Path, prefix: str, start_from: int = 1) -> Path:
    max_seq = 0
    for p in base_dir.glob(f"{prefix}_*.ndjson"):
        try:
            n = int(p.stem.split("_")[-1])
            if n > max_seq:
                max_seq = n
        except Exception:
            continue
    seq = max(max_seq + 1, start_from)
    return base_dir / f"{prefix}_{seq:04d}.ndjson"


def _select_ndjson_target(base_dir: Path, kind_stem: str, ts_iso: str,
                          limit_bytes: int, last_path: Path | None) -> Path:
    tag = _jst_tag_from_iso(ts_iso)
    prefix = f"{tag}{kind_stem}"
    if last_path is not None and last_path.exists():
        if last_path.name.startswith(prefix):
            try:
                if last_path.stat().st_size < max(1, int(limit_bytes)):
                    return last_path
            except Exception:
                pass
            return _next_seq_path(base_dir, prefix)
    unsuffixed = base_dir / f"{prefix}.ndjson"
    try:
        if unsuffixed.exists() and unsuffixed.stat().st_size < max(1, int(limit_bytes)):
            return unsuffixed
    except Exception:
        pass
    return _next_seq_path(base_dir, prefix, start_from=1)


class DecisionLog:
    """[関数] 意思決定ログ（features と decision を保存）。"""
    def __init__(self, path: str | Path, mirror_ndjson: str | Path | None = None) -> None:
        self.path = Path(path)
        self.rows: List[Dict[str, Any]] = []
        self._mirror = Path(mirror_ndjson) if mirror_ndjson else None  # NDJSONミラーの出力先
        if self._mirror:
            self._mirror.parent.mkdir(parents=True, exist_ok=True)
        self._mirror_active: Path | None = None
        self._mirror_limit_bytes: int = _NDJSON_LIMIT_BYTES_DEFAULT
        self._kind_stem = "decision_log"

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
        # features は Parquet では JSON 文字列化、NDJSON ではオブジェクトのまま埋めてもOK
        row: Dict[str, Any] = {
            "ts": ts,
            "strategy": strategy,
            "features_json": orjson.dumps(features).decode("utf-8"),
            "decision": decision,
            "expected_edge_bp": expected_edge_bp,
            "eta_ms": eta_ms,
            "ca_ratio": ca_ratio,
            "best_age_ms": best_age_ms,
            "spread_state": spread_state,
        }
        self.rows.append(row)
        if self._mirror:
            try:
                base_dir = self._mirror.parent
                target = _select_ndjson_target(base_dir, self._kind_stem, ts,
                                              self._mirror_limit_bytes, self._mirror_active)
                self._mirror_active = target
                # NDJSONにはfeaturesもオブジェクトで入れる（ChatGPTでそのまま読めるように）
                payload = dict(row)
                payload["features"] = features
                target.open("a", encoding="utf-8").write(orjson.dumps(payload).decode("utf-8") + "\n")
            except Exception:
                pass

    def flush(self) -> Path:
        # メモリの rows を Parquet に書き出す
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
