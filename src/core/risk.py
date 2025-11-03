"""リスクゲート：在庫上限と安全マージンを扱う補助クラス"""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any, Optional, Sequence
from decimal import Decimal  # 高精度で在庫・価格を扱う
from dataclasses import dataclass  # 判定結果DTO
import math  # 刻み計算で切り下げに使う（在庫連動ロットのため）
import time  # 60秒ロールアップで現在時刻を使う


_INV_CAP_CFG_BANNER_ONCE = False  # 起動バナーを一度だけ出すためのフラグ（モジュール内で共有）

# 60秒ロールアップ用の集計カウンタと設定経路の保持
_INV_CAP_ROLLUP = {"events": 0, "shrink": 0, "no_room": 0, "last_ts": 0.0}
_INV_CAP_CFG_PATH_LAST = "undefined"
_ROLLUP_WINDOW_SEC = 60.0


def _inv_cap_rollup_count(kind: str, eff_limit: float, cfg_path: str) -> None:
    """inv_capping 60秒ロールアップ: 種別(kind)を数え、60秒ごとにINFOでサマリ1行を出す"""
    # 関数内インポート（他所のトップimportに依存しない）
    from loguru import logger

    try:
        now = time.time()
        # イベント/結果の内訳をカウント
        if kind == "event":
            _INV_CAP_ROLLUP["events"] = int(_INV_CAP_ROLLUP.get("events", 0)) + 1
        elif kind in ("shrink", "no_room"):
            _INV_CAP_ROLLUP[kind] = int(_INV_CAP_ROLLUP.get(kind, 0)) + 1

        # 直近の設定経路を記録（観測の自己説明）
        global _INV_CAP_CFG_PATH_LAST
        _INV_CAP_CFG_PATH_LAST = cfg_path or "undefined"

        last = float(_INV_CAP_ROLLUP.get("last_ts", 0.0) or 0.0)
        if last == 0.0:
            _INV_CAP_ROLLUP["last_ts"] = now
            return
        if (now - last) >= _ROLLUP_WINDOW_SEC:
            try:
                logger.info(
                    "inv_capping.rollup60s | events={} shrink={} no_room={} eff_limit={:.6f} cfg_path={}",
                    int(_INV_CAP_ROLLUP.get("events", 0)),
                    int(_INV_CAP_ROLLUP.get("shrink", 0)),
                    int(_INV_CAP_ROLLUP.get("no_room", 0)),
                    float(eff_limit) if eff_limit is not None else 0.0,
                    _INV_CAP_CFG_PATH_LAST,
                )
            except Exception:
                pass
            _INV_CAP_ROLLUP.update({"events": 0, "shrink": 0, "no_room": 0, "last_ts": now})
    except Exception:
        # ロールアップは可観測の補助なので、失敗しても主処理を邪魔しない
        pass

def _to_mapping(obj: Any) -> Mapping[str, Any]:
    """【関数】属性/辞書を読み取り専用の辞書風に正規化"""
    if obj is None:
        return {}
    if isinstance(obj, Mapping):
        return obj
    if hasattr(obj, "__dict__") and isinstance(obj.__dict__, MutableMapping):
        return obj.__dict__
    return {}


@dataclass(frozen=True)
class AutoReduceDecision:
    """自動薄め（Reduce-Only/IOC）の発火判定結果を表す小さな入れ物"""
    fire: bool            # このティックで発火するか
    side: Optional[str]   # "SELL"（ロング縮小）/ "BUY"（ショート縮小）/ None
    reason: str           # なぜその判定か（運用ログに出す説明）


def auto_reduce_should_fire(
    *,
    enabled: bool,
    profit_only: bool,
    force_on_risk: bool,
    q: Decimal,
    a: Decimal,
    mark: Optional[Decimal],
    mode: str,
    recent_errors: Sequence[str],
    eff_limit: Decimal,
) -> AutoReduceDecision:
    """
    在庫と安全装置の“状態だけ”で、自動決済（Reduce‑Only）を今すぐ出すべきかを判定。
    ルール:
      1) 機能OFF or フラット(Q=0)なら発火しない
      2) force_on_risk 有効かつ 以下のいずれかで“損益不問で”発火
         - |Q| >= eff_limit（在庫上限ヒット）
         - 直近に "-205"（証拠金不足）を観測
         - mode == "halted"
      3) profit_only=True のときは“方向利益”（ロング: mark>=A / ショート: mark<=A）のときだけ発火
      4) それ以外（profit_only=False）は利益不問で発火
    戻り: AutoReduceDecision(fire, side, reason)
    """
    # 1) 機能OFF/フラット
    try:
        if not enabled:
            return AutoReduceDecision(False, None, "disabled")
        if q == 0:
            return AutoReduceDecision(False, None, "flat")
    except Exception:
        return AutoReduceDecision(False, None, "invalid_input")

    # 減らす向き（ロング→SELL / ショート→BUY）
    side = "SELL" if q > 0 else "BUY"

    # 2) リスク違反（強制発火）
    try:
        breaching_inv = (abs(q) >= eff_limit)
    except Exception:
        breaching_inv = False
    try:
        has_margin_reject = any(("-205" in (e or "")) or (" 205" in (e or "")) for e in recent_errors or [])
    except Exception:
        has_margin_reject = False
    risk_mode_block = str(mode or "").lower() == "halted"

    if force_on_risk and (breaching_inv or has_margin_reject or risk_mode_block):
        reason_bits: list[str] = []
        if breaching_inv:
            reason_bits.append(f"|Q|>={eff_limit}")
        if has_margin_reject:
            reason_bits.append("margin(-205)")
        if risk_mode_block:
            reason_bits.append("mode=halted")
        return AutoReduceDecision(True, side, "force_on_risk:" + ",".join(reason_bits))

    # 3) 通常: profit_only のときは方向利益のみ
    if profit_only:
        if mark is None:
            return AutoReduceDecision(False, None, "no_mark_for_profit_check")
        try:
            pnl_ok = ((mark - a) * (Decimal(1) if q > 0 else Decimal(-1))) >= Decimal("0")
        except Exception:
            pnl_ok = False
        if pnl_ok:
            return AutoReduceDecision(True, side, "profit_only_ok")
        return AutoReduceDecision(False, None, "profit_only_block")

    # 4) 利益不問で薄める
    return AutoReduceDecision(True, side, "profit_not_required")

class RiskGate:
    """在庫ゲート（max_inventory と安全マージン inventory_eps を扱う）"""

    def __init__(self, cfg: Any | None = None) -> None:
        # 起動時1回だけ：在庫連動ロット(inv_capping)の設定ソースと数値をINFOで自己紹介（可観測性の確保）
        # RiskGate は起動時に構築されるため、ここでバナーを出すのが最も早い
        try:
            global _INV_CAP_CFG_BANNER_ONCE
            if not _INV_CAP_CFG_BANNER_ONCE and cfg is not None:
                # 関数内インポート（トップのimport変更に依存しない）
                from loguru import logger  # ログ出力（INFO）

                # 設定フォールバック：risk.inv_capping が無ければ features.inv_capping を試す
                cfg_risk = getattr(cfg, "risk", None)
                cfg_feat = getattr(cfg, "features", None)

                inv_risk = getattr(cfg_risk, "inv_capping", None) if cfg_risk is not None else None
                if inv_risk is None:
                    extra_r = getattr(cfg_risk, "model_extra", None) if cfg_risk is not None else None
                    if isinstance(extra_r, dict):
                        inv_risk = extra_r.get("inv_capping")

                inv_feat = getattr(cfg_feat, "inv_capping", None) if cfg_feat is not None else None
                if inv_feat is None:
                    extra_f = getattr(cfg_feat, "model_extra", None) if cfg_feat is not None else None
                    if isinstance(extra_f, dict):
                        inv_feat = extra_f.get("inv_capping")

                cfg_path = "undefined"
                inv_enabled = False
                target_ratio = 1.0
                eff_limit_margin = 1.0

                if inv_risk is not None and getattr(inv_risk, "enabled", None) is not None:
                    try:
                        inv_enabled = bool(getattr(inv_risk, "enabled"))
                        tr = getattr(inv_risk, "target_ratio", None)
                        if tr is not None:
                            target_ratio = float(tr)
                        mg = getattr(inv_risk, "eff_limit_margin", None)
                        if mg is not None:
                            eff_limit_margin = float(mg)
                        cfg_path = "risk.inv_capping"
                    except Exception:
                        pass
                elif inv_feat is not None and getattr(inv_feat, "enabled", None) is not None:
                    try:
                        inv_enabled = bool(getattr(inv_feat, "enabled"))
                        tr = getattr(inv_feat, "target_ratio", None)
                        if tr is not None:
                            target_ratio = float(tr)
                        mg = getattr(inv_feat, "eff_limit_margin", None)
                        if mg is not None:
                            eff_limit_margin = float(mg)
                        cfg_path = "features.inv_capping"
                    except Exception:
                        pass

                max_inv_val = None
                try:
                    mv = getattr(cfg_risk, "max_inventory", None) if cfg_risk is not None else None
                    if mv is not None:
                        max_inv_val = float(mv)
                except Exception:
                    max_inv_val = None

                eff_limit = (max_inv_val * eff_limit_margin) if (max_inv_val is not None) else None
                try:
                    logger.info(
                        "inv_capping.cfg_path={} enabled={} target_ratio={:.2f} eff_limit_margin={:.2f} eff_limit={} max_inventory={}",
                        cfg_path, inv_enabled, target_ratio, eff_limit_margin,
                        (f"{eff_limit:.6f}" if eff_limit is not None else None),
                        (f"{max_inv_val:.6f}" if max_inv_val is not None else None),
                    )
                except Exception:
                    pass
                _INV_CAP_CFG_BANNER_ONCE = True
        except Exception:
            # バナー失敗は致命的ではないため握りつぶす
            pass
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


def cap_order_size_by_inventory(
    req_raw: float,
    abs_q: float,
    eff_limit: float,
    min_lot: float,
    target_ratio: float = 0.90,
) -> float:
    """
    在庫と新規サイズの合計が target_ratio*eff_limit を超えないように、
    最小ロット刻みで req を【切り下げ】て返す関数。
    - req_raw : 戦略が希望する元の新規サイズ
    - abs_q   : いまの在庫の絶対値 |Q|
    - eff_limit : 実効在庫上限（例：max_inventory * 0.99）
    - min_lot : 取引所の最小ロット刻み
    - target_ratio : 目標比（既定=0.90）。( |Q|+req ) / eff_limit ≤ 目標 になるよう制御

    戻り値:
      ・発注可能なら、刻みを満たしたサイズ（req_raw 以下に切り下げ）
      ・発注不可なら 0.0（この場合は上流で新規をスキップ/ROだけにする）
    """
    # 非常時・異常値の安全側
    if req_raw <= 0.0 or eff_limit <= 0.0 or min_lot <= 0.0 or target_ratio <= 0.0:
        return 0.0

    # 1) 目標以内で追加できる“最大許容増分”を計算（負なら新規ゼロ）
    allowed_add = target_ratio * eff_limit - abs_q
    if allowed_add <= 0.0:
        return 0.0

    # 2) ロット刻みに合わせて“切り下げ”し、元の希望サイズとも比較
    #    （切り上げ禁止＝規約超過や在庫超過を防ぐ）
    allowed_lots = math.floor(allowed_add / min_lot)
    if allowed_lots <= 0:
        return 0.0
    allowed_size = allowed_lots * min_lot

    # 3) 実際に出すサイズは「元の希望」か「許容サイズ」の小さい方
    sized = min(req_raw, allowed_size)

    # 4) ごく小さい端数（浮動小数誤差等）を安全に丸め落とし
    lots = math.floor(sized / min_lot)
    if lots <= 0:
        return 0.0
    return lots * min_lot


def inv_capping_preflight(
    *,
    req_raw: float,
    abs_q: float,
    eff_limit: float,
    min_lot: float,
    target_ratio: float = 0.90,
    cfg_path: str | None = None,
) -> tuple[float, bool]:
    """
    在庫ガード直前の“在庫連動ロット”前処理。
    - 入力: req_raw（希望サイズ）, abs_q（現|Q|）, eff_limit（実効上限）, min_lot, target_ratio
    - 出力: (safe_size, should_skip)
      should_skip=True の場合は固定文言で DEBUG を記録済み。
    """
    # 関数内インポート（トップの import 変更を避ける）
    from loguru import logger  # 在庫由来スキップの固定文言をDEBUGで残す（集計用）

    try:
        eff = float(eff_limit)
        lotsz = float(min_lot)
        tgt = float(target_ratio)
        rq = float(req_raw)
        aq = float(abs_q)
    except Exception:
        return 0.0, True

    safe_size = cap_order_size_by_inventory(
        req_raw=rq,
        abs_q=aq,
        eff_limit=eff,
        min_lot=lotsz,
        target_ratio=tgt,
    )
    r_pre = (aq + rq) / eff if eff > 0 else float("inf")
    # headroomの観測ログ（target_ratio×eff_limit 基準）
    target_eff = tgt * eff
    headroom = max(target_eff - aq, 0.0)
    try:
        logger.debug(
            "inv_capping.headroom | abs_q={:.6f} target_eff={:.6f} headroom={:.6f} cfg_path={}",
            aq, target_eff, headroom, (cfg_path or "n/a"),
        )
    except Exception:
        pass

    # 観測イベントを1件カウント（60秒でINFO集計）
    try:
        _inv_cap_rollup_count("event", eff, (cfg_path or "n/a"))
    except Exception:
        pass

    # 余地ゼロ、または丸め後も上限超過 → 固定文言で見送り
    if (safe_size <= 0.0) or ((aq + safe_size) > eff):
        logger.debug(
            "skip place: capped_by_inventory (no_room) | r_pre={:.3f} abs_q={:.6f} req_raw={:.6f} eff_limit={:.6f} target={:.2f} headroom={:.6f} cfg_path={}",
            r_pre, aq, rq, eff, tgt, headroom, (cfg_path or "n/a"),
        )
        # 余地ゼロ件数をカウント（60秒でINFO集計）
        try:
            _inv_cap_rollup_count("no_room", eff, (cfg_path or "n/a"))
        except Exception:
            pass
        return 0.0, True

    # 縮小の可視化（DEBUG）
    if safe_size < (rq - 1e-12):
        logger.debug(
            "capped_by_inventory | r_pre={:.3f} -> r_post={:.3f} size:{:.6f}->{:.6f} headroom={:.6f} cfg_path={}",
            r_pre, (aq + safe_size) / eff if eff > 0 else float("inf"), rq, safe_size, headroom, (cfg_path or "n/a"),
        )
        # 実縮小件数をカウント（60秒でINFO集計）
        try:
            _inv_cap_rollup_count("shrink", eff, (cfg_path or "n/a"))
        except Exception:
            pass
    return safe_size, False
