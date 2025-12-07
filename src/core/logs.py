# src/core/logs.py
# 役割: orders/trades を Parquet に保存し、必要なら NDJSON にもミラーします。
# 追加: NDJSON は JST の日付タグ(YYYYMMDD)で日次ローテーションし、さらに20MBを超えたら分割します。
from __future__ import annotations

from pathlib import Path  # パス操作
from typing import List, Dict, Any  # 型
import polars as pl  # Parquet I/O
from loguru import logger  # ロギング
import orjson  # NDJSON ミラー出力（1行JSON）
from datetime import datetime, timezone, timedelta  # JSTタグ/ISO→dt
from zoneinfo import ZoneInfo  # JSTの現在時刻

# -------- NDJSON ローテーション補助 --------

_NDJSON_LIMIT_BYTES_DEFAULT = 20 * 1024 * 1024  # 20MB 目安


def _jst_tag_from_iso(ts: str) -> str:
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(ZoneInfo("Asia/Tokyo"))
    jst = timezone(timedelta(hours=9))
    return dt.astimezone(jst).strftime("%Y%m%d")


def _next_seq_path(base_dir: Path, prefix: str, start_from: int = 1) -> Path:
    # 既存の _####.ndjson を走査して最大+1を作る
    max_seq = 0
    for p in base_dir.glob(f"{prefix}_*.ndjson"):
        name = p.name
        try:
            s = name.split("_")[-1].split(".")[0]
            n = int(s)
            if n > max_seq:
                max_seq = n
        except Exception:
            continue
    seq = max(max_seq + 1, start_from)
    return base_dir / f"{prefix}_{seq:04d}.ndjson"


def _select_ndjson_target(base_dir: Path, kind_stem: str, ts_iso: str,
                          limit_bytes: int, last_path: Path | None) -> Path:
    # 例: kind_stem='order_log' → prefix='20251023order_log'
    tag = _jst_tag_from_iso(ts_iso)
    prefix = f"{tag}{kind_stem}"
    # 直前に使っていたファイルが同じ日付prefixなら続き/ローテ判定
    if last_path is not None and last_path.exists():
        if last_path.name.startswith(prefix):
            try:
                if last_path.stat().st_size < max(1, int(limit_bytes)):
                    return last_path
            except Exception:
                pass
            # 超過したので次のシーケンスへ
            return _next_seq_path(base_dir, prefix)
    # まだ無い/日付が変わった。まずは無印 → 超過してたら _0001
    unsuffixed = base_dir / f"{prefix}.ndjson"
    try:
        if unsuffixed.exists() and unsuffixed.stat().st_size < max(1, int(limit_bytes)):
            return unsuffixed
    except Exception:
        pass
    return _next_seq_path(base_dir, prefix, start_from=1)


class OrderLog:
    """[関数] 注文ログ（place/cancel/fill/partial）を Parquet に保存。
    mirror_ndjson が指定されていれば、NDJSON でも1行ずつミラーします。
    NDJSON は JST 日付で日次ローテーションし、20MB 超で分割します。
    """
    def __init__(self, path: str | Path, mirror_ndjson: str | Path | None = None) -> None:
        self.path = Path(path)
        self.rows: List[Dict[str, Any]] = []
        self._mirror = Path(mirror_ndjson) if mirror_ndjson else None
        if self._mirror:
            self._mirror.parent.mkdir(parents=True, exist_ok=True)
        # ローテ用キャッシュ
        self._mirror_active: Path | None = None
        self._mirror_limit_bytes: int = _NDJSON_LIMIT_BYTES_DEFAULT
        self._kind_stem = "order_log"

    def add(self, *, ts: str, action: str, tif: str | None, ttl_ms: int | None,
            px: float | None, sz: float | None, reason: str = "") -> None:
        # 1件をメモリに追加（flushでParquet保存） ＋ 必要ならNDJSONミラー
        rec = {
            "ts": ts, "action": action, "tif": tif, "ttl_ms": ttl_ms,
            "px": float(px) if px is not None else None,
            "sz": float(sz) if sz is not None else None,
            "reason": reason,
        }
        self.rows.append(rec)
        if self._mirror:
            try:
                base_dir = self._mirror.parent
                target = _select_ndjson_target(base_dir, self._kind_stem, ts,
                                              self._mirror_limit_bytes, self._mirror_active)
                self._mirror_active = target
                target.parent.mkdir(parents=True, exist_ok=True)
                target.open("a", encoding="utf-8").write(orjson.dumps(rec).decode("utf-8") + "\n")
            except Exception:
                # ミラーは失敗しても致命ではない（Parquetは保持）
                pass

    def flush(self) -> Path:
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
    """[関数] 約定ログ（サイド/サイズ/PnL 等）を Parquet に保存＋NDJSONミラー（任意）。"""
    def __init__(self, path: str | Path, mirror_ndjson: str | Path | None = None) -> None:
        self.path = Path(path)
        self.rows: List[Dict[str, Any]] = []
        self._mirror = Path(mirror_ndjson) if mirror_ndjson else None
        if self._mirror:
            self._mirror.parent.mkdir(parents=True, exist_ok=True)
        self._mirror_active: Path | None = None
        self._mirror_limit_bytes: int = _NDJSON_LIMIT_BYTES_DEFAULT
        self._kind_stem = "trade_log"

    def add(self, *, ts: str, side: str, px: float, sz: float,
            pnl: float, strategy: str, tag: str,
            inventory_after: float, fee: float = 0.0,
            window_funding: bool = False, window_maint: bool = False) -> None:
        rec = {
            "ts": ts, "side": side, "px": float(px), "sz": float(sz),
            "fee": float(fee), "pnl": float(pnl),
            "strategy": strategy, "tag": tag,
            "inventory_after": float(inventory_after),
            "window_funding": bool(window_funding),
            "window_maint": bool(window_maint),
        }
        self.rows.append(rec)
        if self._mirror:
            try:
                base_dir = self._mirror.parent
                target = _select_ndjson_target(base_dir, self._kind_stem, ts,
                                              self._mirror_limit_bytes, self._mirror_active)
                self._mirror_active = target
                target.parent.mkdir(parents=True, exist_ok=True)
                target.open("a", encoding="utf-8").write(orjson.dumps(rec).decode("utf-8") + "\n")
            except Exception:
                pass

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
