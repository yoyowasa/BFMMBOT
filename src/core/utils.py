# src/core/utils.py
# 役割：設定(YAML)の読み込み・base.ymlとの深いマージ・Pydanticでの型検査を行う“設定ローダー”
from __future__ import annotations

from pathlib import Path  # ファイルパスを安全に扱う
from typing import Any, Dict, Mapping, Sequence  # 型ヒント用
import os  # 実行ディレクトリの判定や環境変数利用
import copy  # 辞書のディープコピーで安全に合成
import yaml  # YAML読取（pyyaml）
from pydantic import BaseModel, Field  # 型検査モデル（v2）
import time  # 現在のUNIX時刻(ミリ秒)を取得するために使用
import math  # 単位推定にlog10などを使うため

# ─────────────────────────────────────────────────────────────
# Pydanticモデル定義（文書の設定キーを最小構成で網羅：env/size/risk/guard/窓/遅延/特徴/戦略/ログ/BT専用）
# 参考：configs/base.yml / paper.yml / live.yml / backtest.yml のキー一覧。:contentReference[oaicite:4]{index=4}

class SizeCfg(BaseModel):
    min: float = Field(..., description="最小ロット")
    default: float = Field(..., description="既定ロット")

class KillCfg(BaseModel):
    daily_pnl_jpy: float | int | None = None
    max_dd_jpy: float | int | None = None

class RiskCfg(BaseModel):
    max_inventory: float
    kill: KillCfg | None = None

class GuardCfg(BaseModel):
    max_mid_move_bp_30s: float | int | None = None

class HealthCfg(BaseModel):
    stale_sec_warn: float | int | None = None  # 何をする行か：Best静止の注意閾値（秒）
    stale_sec_halt: float | int | None = None  # 何をする行か：Best静止の停止閾値（秒）

class MaintWindowCfg(BaseModel):
    start: str
    end: str
    action: str

class ModeSwitchCfg(BaseModel):
    maintenance: MaintWindowCfg | None = None
    funding_calc_jst: list[str] | None = None
    funding_transfer_lag_hours: int | None = None

class LatencyCfg(BaseModel):
    rx_ms: int | float
    tx_ms: int | float

class FeaturesCfg(BaseModel):
    stall_T_ms: int | None = None
    ca_ratio_win_ms: int | None = None
    ca_threshold: float | None = None
    min_spread_tick: int | None = None
    ttl_ms: int | None = None
    age_ms: int | None = None
    mp_offset_tick: float | None = None
    tiny_prints_N: int | None = None
    tiny_prints_minCount: int | None = None
    eta_t_window_ms: int | None = None

class LoggingCfg(BaseModel):
    level: str
    rotate_mb: int | None = None

class SlippageCfg(BaseModel):
    taker_multiplier: float | int

class PeriodCfg(BaseModel):
    start: str
    end: str

class Config(BaseModel):
    """プロジェクト共通設定：base.yml を土台に各環境の差分を上書きして出来上がる最終形"""
    env: str = Field(..., description="paper | live | backtest")  # 実行モード
    product_code: str | None = None
    tick_size: float | int | None = None
    size: SizeCfg | None = None
    risk: RiskCfg | None = None
    guard: GuardCfg | None = None
    health: HealthCfg | None = None  # 何をする行か：市場ヘルス（Best静止しきい値）
    mode_switch: ModeSwitchCfg | None = None
    latency: LatencyCfg | None = None
    features: FeaturesCfg | None = None
    strategy_cfg: dict[str, Any] | None = None
    strategies: list[str] | None = None
    logging: LoggingCfg | None = None
    # backtest専用
    slippage: SlippageCfg | None = None
    period: PeriodCfg | None = None

# ─────────────────────────────────────────────────────────────
# 【関数】YAML読取：指定パスのYAMLを辞書で返す（空は {}）
def _read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)  # コメント以外を読み込む
    return data or {}

# ─────────────────────────────────────────────────────────────
# 【関数】深いマージ：base の上に override を重ねる（辞書は再帰、配列/スカラは置換）
def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = deep_merge(merged[k], v)  # 再帰的に辞書を合成
        else:
            merged[k] = copy.deepcopy(v)  # 配列・数値・文字列などは上書き
    return merged

# ─────────────────────────────────────────────────────────────
# 【関数】パス解決：与えられた config から base.yml と自身のフルパスを求める
def resolve_config_paths(config_path: str | os.PathLike[str]) -> tuple[Path, Path]:
    cpath = Path(config_path).resolve()
    # configs/ の直下にある想定。base.yml は同ディレクトリに存在。
    base = cpath.parent / "base.yml"
    if not base.is_file():
        # もし別ディレクトリから呼ばれても、プロジェクト直下の configs/base.yml を探す
        root = Path(__file__).resolve().parents[2]  # .../src/core/utils.py → プロジェクトルート
        alt = root / "configs" / "base.yml"
        if alt.is_file():
            base = alt
    return base, cpath

# ─────────────────────────────────────────────────────────────
# 【関数】設定ローダー：base.yml＋指定yml を合成し、Pydantic で型検査した Config を返す
def load_config(config_path: str | os.PathLike[str]) -> Config:
    """
    使い方：
      from src.core.utils import load_config
      cfg = load_config("configs/paper.yml")
    効能：
      - base.yml を土台に、paper/live/backtest の差分を“深く”上書き（文書の運用方針に準拠）。:contentReference[oaicite:5]{index=5}
      - Pydanticで型チェック済みのオブジェクトを返すので、後段は辞書より安全に参照できる。
    """
    base_path, cfg_path = resolve_config_paths(str(config_path))
    base = _read_yaml(base_path) if cfg_path.name != "base.yml" else {}
    override = _read_yaml(cfg_path)
    merged = deep_merge(base, override)
    return Config.model_validate(merged)


def normalize_ttl_bands(raw_bands: Any) -> list[dict[str, Any]]:
    """【関数】TTLバンド設定を安全なリスト(dict)へ正規化（threshold昇順に整列）"""
    if raw_bands is None:
        return []
    entries: list[dict[str, Any]] = []
    items: Sequence[Any]
    if isinstance(raw_bands, Mapping):
        items = list(raw_bands.values()) if not isinstance(raw_bands.get("threshold_bp"), (int, float)) else [raw_bands]
    elif isinstance(raw_bands, Sequence) and not isinstance(raw_bands, (str, bytes, bytearray)):
        items = list(raw_bands)
    else:
        return []
    for item in items:
        threshold = None
        ttl = None
        window = None
        if isinstance(item, Mapping):
            threshold = item.get("threshold_bp")
            ttl = item.get("ttl_ms")
            window = item.get("window_ms")
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
            if len(item) >= 2:
                threshold, ttl = item[0], item[1]
            if len(item) >= 3:
                window = item[2]
        if threshold is None or ttl is None:
            continue
        try:
            threshold_val = float(threshold)
            ttl_val = int(ttl)
        except Exception:
            continue
        record: dict[str, Any] = {"threshold_bp": threshold_val, "ttl_ms": ttl_val}
        if window is not None:
            try:
                record["window_ms"] = int(window)
            except Exception:
                pass
        entries.append(record)
    entries.sort(key=lambda x: x["threshold_bp"])
    return entries


def now_ms() -> int:
    """【関数】現在のUNIX時刻をミリ秒で返す（戦略のTTLやログ時刻用）。"""
    return int(time.time() * 1000)


def monotonic_ms() -> int:
    """【関数】単調増加の経過時間(ms)。相対時間の測定に使う（NTPずれの影響を避ける）。"""
    return int(time.perf_counter_ns() // 1_000_000)


def coerce_ms(v) -> float | None:
    """【関数】秒/ミリ秒/マイクロ秒/ナノ秒の“っぽい数字”を安全にmsへ正規化。
    - 例: 0.25(秒)→250ms, 250(たぶんms)→250ms, 250000(µs)→250ms, 250000000(ns)→250ms
    - Noneや不正値は None を返して呼び元で無視できるようにする。
    """
    if v is None:
        return None
    try:
        x = float(v)
    except Exception:
        return None

    if not math.isfinite(x):
        return None
    if x < 0:
        x = 0.0
    abs_x = abs(x)
    if abs_x < 1.0:  # 小さな値は秒として解釈
        return x * 1_000.0
    if abs_x < 100_000.0:  # 0.1s未満～100s未満はそのままmsとみなす
        return x
    if abs_x < 100_000_000.0:  # 100s以上1e5ms超級はµs想定で1/1000
        return x / 1_000.0
    return x / 1_000_000.0  # それより大きければns（もしくはそれ以上）とみなして1/1e6

