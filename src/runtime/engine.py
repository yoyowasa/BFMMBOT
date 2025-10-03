# src/runtime/engine.py
# 役割：リアルタイムの“paper実行”エンジン（WS→ローカル板→戦略→最小シミュ→ログ保存）
# - 【関数】run_paper：WSイベントを流し込み、#1/#2戦略を評価→発注/取消→Fill反映→ログ保存
# - 【関数】_guard_midmove_bp：30秒のミッド変化(bps)を監視し、閾値超なら新規停止＋全取消
# - ログは文書仕様どおり logs/orders・logs/trades・logs/analytics にParquetで出力する
from __future__ import annotations

import asyncio  # 非同期ループ/キャンセル
from collections import deque  # 30sミッド履歴でガード
from datetime import datetime, timezone, timedelta  # ts解析と現在時刻 JST日付の境界計算にtimedeltaを使う
from typing import Deque, Optional, Tuple  # 型ヒント
import csv  # 役割：窓イベントをCSVに1行追記するために使用
import time  # 何をするか：現在時刻(ms)を取得してHB間隔を測る
from loguru import logger  # 実行ログ
from pathlib import Path  # ハートビートNDJSONのファイル出力に使用
import orjson  # 1行JSON化（高速）
from src.core.realtime import event_stream  # 【関数】WS購読（board/executions）:contentReference[oaicite:2]{index=2}
from src.core.orderbook import OrderBook  # 【関数】ローカル板（Best/Spread/C-A）:contentReference[oaicite:3]{index=3}
from src.core.simulator import MiniSimulator  # 【関数】最小約定シミュ（価格タッチ）:contentReference[oaicite:4]{index=4}
from src.core.logs import OrderLog, TradeLog  # 【関数】発注/約定ログ（Parquet）:contentReference[oaicite:5]{index=5}
from src.core.analytics import DecisionLog  # 【関数】意思決定ログ（Parquet）:contentReference[oaicite:6]{index=6}
from src.strategy import build_strategy  # 何をするか：戦略生成を中央ファクトリに委譲する
from src.core.risk import RiskGate  # 何をするか：在庫ゲート（市場モードでClose-Onlyを切り替える）


def _eval_feed_health(cfg: dict | object,
                      best_age_ms: Optional[float],
                      hb_gap_sec: Optional[float]) -> Tuple[str, str]:
    """何をするか：板エイジ/ハートビート間隔から 'healthy' / 'caution' / 'halted' を決めて理由を返す"""

    def _get(node, key: str):
        if node is None:
            return None
        if isinstance(node, dict):
            return node.get(key)
        extra = getattr(node, "model_extra", None)
        if isinstance(extra, dict) and key in extra:
            return extra[key]
        return getattr(node, key, None)

    guard = _get(cfg, "guard") or {}
    fh = _get(guard, "feed_health") or {}

    age_cfg = _get(fh, "age_ms") or {}
    gap_cfg = _get(fh, "heartbeat_gap_sec") or {}

    age_caution = _get(age_cfg, "caution")
    age_halted = _get(age_cfg, "halted")
    gap_caution = _get(gap_cfg, "caution")
    gap_halted = _get(gap_cfg, "halted")

    age_caution = float(age_caution) if age_caution is not None else 3000.0
    age_halted = float(age_halted) if age_halted is not None else 10000.0
    gap_caution = float(gap_caution) if gap_caution is not None else 3.0
    gap_halted = float(gap_halted) if gap_halted is not None else 10.0

    if best_age_ms is not None and best_age_ms >= age_halted:
        return "halted", f"age_ms={int(best_age_ms)}>= {int(age_halted)}"
    if hb_gap_sec is not None and hb_gap_sec >= gap_halted:
        return "halted", f"hb_gap_sec={round(hb_gap_sec, 3)}>= {gap_halted}"

    if best_age_ms is not None and best_age_ms >= age_caution:
        return "caution", f"age_ms={int(best_age_ms)}>= {int(age_caution)}"
    if hb_gap_sec is not None and hb_gap_sec >= gap_caution:
        return "caution", f"hb_gap_sec={round(hb_gap_sec, 3)}>= {gap_caution}"

    return "healthy", "ok"

def _parse_iso(ts: str) -> datetime:
    """【関数】ISO→datetime（'Z'も+00:00に正規化）"""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _now_utc() -> datetime:
    """【関数】現在UTC（実行時刻の印）"""
    return datetime.now(timezone.utc)

class PaperEngine:
    """リアルタイム“paper”の最小エンジン"""

    def __init__(self, cfg, strategy_name: str, *, strategy_cfg=None) -> None:
        # 設定（製品コード/刻み/ガード閾値）
        self.cfg = cfg
        self.product = getattr(cfg, "product_code", "FX_BTC_JPY") or "FX_BTC_JPY"
        self.tick = float(getattr(cfg, "tick_size", 1.0))
        self.max_inv = getattr(getattr(cfg, "risk", None), "max_inventory", None)
        inv_eps_default = 0.0 if self.max_inv is None else max(0.0, float(self.max_inv) * 0.01)
        self.inventory_eps = float(getattr(getattr(cfg, "risk", None), "inventory_eps", inv_eps_default))
        self.guard_bp = None
        if getattr(cfg, "guard", None) is not None:
            self.guard_bp = getattr(cfg.guard, "max_mid_move_bp_30s", None)
            self.kill_daily = getattr(getattr(getattr(cfg, "risk", None), "kill", None), "daily_pnl_jpy", None)  # 【関数】Kill: 日次PnL閾値
            self.kill_dd = getattr(getattr(getattr(cfg, "risk", None), "kill", None), "max_dd_jpy", None)        # 【関数】Kill: 日次DD閾値
            self.halted = False  # 【関数】Kill発火後は停止
        self.risk = RiskGate(cfg)  # 何をする行か：設定を元に在庫ゲートを構築（市場モード連携の受け皿）
        health_cfg = getattr(cfg, "health", None)  # 何をする行か：Best静止しきい値（healthセクション）を安全に取得
        self._stale_warn_ms = None  # 何をする行か：Best静止でCautionへ入る閾値(ms)
        self._stale_halt_ms = None  # 何をする行か：Best静止でHaltedへ入る閾値(ms)
        if health_cfg is not None:  # 何をする行か：healthセクションが定義されているときのみ閾値を読み込む
            warn_sec = getattr(health_cfg, "stale_sec_warn", None)  # 何をする行か：Cautionへ入る秒数を読む
            halt_sec = getattr(health_cfg, "stale_sec_halt", None)  # 何をする行か：Haltedへ入る秒数を読む
            self._stale_warn_ms = float(warn_sec) * 1000.0 if warn_sec is not None else None  # 何をする行か：秒→ms換算
            self._stale_halt_ms = float(halt_sec) * 1000.0 if halt_sec is not None else None  # 何をする行か：秒→ms換算

        # 戦略（#1/#2/#3）を選択
        self.strat = build_strategy(strategy_name, cfg, strategy_cfg=strategy_cfg)


        # ローカル板・シミュ・ログ器
        self.ob = OrderBook(tick_size=self.tick)
        self.sim = MiniSimulator()
        self._feed_mode = "healthy"          # 何をするか：現在のフィード状態（healthy/caution/halted）を保持
        self._last_feed_reason = "init"      # 何をするか：直近の判定理由（ログや監視で参照）
        self._last_heartbeat_ms: int | None = None  # 何をするか：ハートビート/board受信時刻(ms)を保持
        self._last_gate_status = {"mode": "healthy", "reason": "init", "limits": {}, "ts_ms": None}  # 何をするか：直近のゲート状態（戦略から参照するため）
        self._last_place_ts_ms = 0       # 何をするか：直近の新規発注時刻（Cautionの発注レート制御に使う）
        self._orig_place = None               # 何をするか：元のplace関数を保存してラップ後に呼び戻す
        if hasattr(self, "sim") and hasattr(self.sim, "place"):
            self._orig_place = self.sim.place
            self.sim.place = self._place_with_feed_guard  # 何をするか：発注時にフィード健全性を確認するラップ関数へ差し替え
        self.order_log = OrderLog("logs/orders/order_log.parquet", mirror_ndjson="logs/orders/order_log.ndjson")  # NDJSONミラー有効化
        self.trade_log = TradeLog("logs/trades/trade_log.parquet", mirror_ndjson="logs/trades/trade_log.ndjson")  # NDJSONミラー有効化
        self.decision_log = DecisionLog("logs/analytics/decision_log.parquet", mirror_ndjson="logs/analytics/decision_log.ndjson")  # NDJSONミラー有効化
        self._hb_path = Path("logs/runtime/heartbeat.ndjson")  # 【関数】ハートビート出力先（NDJSON）
        self._events_dir = Path("logs/events")  # 役割：窓イベントのCSVフォルダ
        (self._events_dir / "maintenance.csv").touch(exist_ok=True)  # 初回起動でも tail できるよう空ファイルを作る
        (self._events_dir / "funding_schedule.csv").touch(exist_ok=True)  # 同上
        self._events_dir.mkdir(parents=True, exist_ok=True)  # 役割：フォルダを作成
        self._maint_prev, self._fund_prev = False, False  # 役割：直前の窓状態（enter/exit検出用）

        self._hb_path.parent.mkdir(parents=True, exist_ok=True)  # 親フォルダを用意
        self._midguard_paused = False  # 直近の“ミッド変化ガード”状態を持つ


        # PnL最小モデルの内部状態（自炊Q/A/Rのミニ版）
        self.Q = 0.0  # 在庫（+ロング/−ショート）
        self.A = 0.0  # 平均建値
        self.R = 0.0  # 実現PnL累計
        self._JST = timezone(timedelta(hours=9))  # 【関数】日次境界（JST）
        jst_now = _now_utc().astimezone(self._JST)
        jst_midnight = jst_now.replace(hour=0, minute=0, second=0, microsecond=0)
        self._day_start_utc = jst_midnight.astimezone(timezone.utc)  # 【関数】当日JST 0時（UTC）
        self._daily_R, self._R_HWM = 0.0, 0.0  # 【関数】日次PnLとそのHWM（DD計算に使用）

        # 30秒ミッド履歴（ガード用）：(epoch_sec, mid)
        self._midwin: Deque[Tuple[float, float]] = deque()

    def effective_inventory_limit(self) -> float | None:
        """【関数】新規発注の可否判定に使う実効在庫上限（上限−安全マージン）を返す"""
        if self.max_inv is None:
            return None
        limit = float(self.max_inv) - float(self.inventory_eps)
        return max(0.0, limit)

    def _place_with_feed_guard(self, *args, **kwargs):
        """何をするか：発注直前にフィード健全性を判定し、Haltedでは新規をブロック（決済のみ許可）する"""
        now_ms = int(time.time() * 1000)
        prev_mode = getattr(self, "_feed_mode", "healthy")  # 何をするか：モード変更の検知（ログを増やしすぎない）

        best_age_ms: float | None = None
        hb_gap_sec: float | None = None

        ob = getattr(self, "ob", None)
        last_rx_dt = None
        if ob is not None:
            ba_attr = getattr(ob, "best_age_ms", None)
            if callable(ba_attr):
                try:
                    best_age_ms = float(ba_attr())
                except Exception:
                    best_age_ms = None
            elif isinstance(ba_attr, (int, float)):
                best_age_ms = float(ba_attr)
            last_rx_dt = getattr(ob, "_last_ts", None)

        if last_rx_dt is not None:
            try:
                hb_gap_sec = max(0.0, (datetime.now(timezone.utc) - last_rx_dt).total_seconds())
            except Exception:
                hb_gap_sec = None
        elif isinstance(self._last_heartbeat_ms, (int, float)):
            hb_gap_sec = max(0.0, (now_ms - float(self._last_heartbeat_ms)) / 1000.0)

        cfg_obj = getattr(self, "cfg", {})
        mode, reason = _eval_feed_health(cfg_obj, best_age_ms, hb_gap_sec)
        self._feed_mode = mode
        self._last_feed_reason = reason
        if mode in ("healthy", "halted"):  # 何をするか：制限の無い2モードはここで一括更新（limitsは空）
            self._last_gate_status = {"mode": mode, "reason": reason, "limits": {}, "ts_ms": now_ms}
            if prev_mode != mode:
                logger.info(f"guard:mode_change {prev_mode}->{mode} reason={reason} limits={{}}")  # 何をするか：モード変化を1行で記録
        if hasattr(self, "risk") and hasattr(self.risk, "set_market_mode"):
            try:
                self.risk.set_market_mode(mode)
            except Exception:
                pass

        is_reduce = bool(kwargs.get("reduce_only"))
        if not is_reduce and args:
            first = args[0]
            is_reduce = getattr(first, "reduce_only", False) or getattr(first, "close_only", False)

        if mode == "halted" and not is_reduce:
            logger.warning(f"guard:block_new_order mode=halted reason={reason}")
            return None

        if mode == "caution":
            def _cfg_get(node, key):
                if node is None:
                    return None
                if isinstance(node, dict):
                    return node.get(key)
                extra = getattr(node, "model_extra", None)
                if isinstance(extra, dict) and key in extra:
                    return extra[key]
                return getattr(node, key, None)

            guard_cfg = _cfg_get(cfg_obj, "guard") or {}
            caution_cfg = _cfg_get(guard_cfg, "caution") or {}

            size_cfg = _cfg_get(cfg_obj, "size") or {}
            if isinstance(size_cfg, dict):
                sz_min_val = size_cfg.get("min")
            else:
                sz_min_val = getattr(size_cfg, "min", None)
            try:
                sz_min = float(sz_min_val)
                if sz_min <= 0:
                    raise ValueError
            except Exception:
                sz_min = 0.001

            max_sz_raw = _cfg_get(caution_cfg, "max_order_size")
            try:
                max_sz = float(max_sz_raw if max_sz_raw is not None else 3 * sz_min)
            except Exception:
                max_sz = 3 * sz_min
            if max_sz <= 0:
                max_sz = 3 * sz_min

            rate_raw = _cfg_get(caution_cfg, "max_order_rate_per_sec")
            try:
                rate = float(rate_raw if rate_raw is not None else 2.0)
            except Exception:
                rate = 2.0
            if rate <= 0:
                rate = 2.0
            min_interval_ms = 1000.0 / max(0.001, rate)
            limits = {"max_order_size": max_sz, "max_order_rate_per_sec": rate}  # 何をするか：Caution時の制限（戦略へ伝える数字）
            self._last_gate_status = {"mode": "caution", "reason": reason, "limits": limits, "ts_ms": now_ms}  # 何をするか：最新ゲート情報を更新
            if prev_mode != "caution":
                logger.info(f"guard:mode_change {prev_mode}->caution reason={reason} limits={limits}")  # 何をするか：モード変化を1行で記録

            if not is_reduce:
                if now_ms - self._last_place_ts_ms < min_interval_ms:
                    limit_rate = 0.0
                    if min_interval_ms > 0:
                        limit_rate = round(1000.0 / min_interval_ms, 3)
                    logger.warning(f"guard:throttle_new_order mode=caution reason=rate_limit {limit_rate}req/s")
                    return None

                if args:
                    first = args[0]
                    if hasattr(first, "size") and isinstance(getattr(first, "size"), (int, float)) and first.size > max_sz:
                        logger.warning(f"guard:shrink_size mode=caution from={first.size} to={max_sz}")
                        first.size = max_sz
                    elif hasattr(first, "sz") and isinstance(getattr(first, "sz"), (int, float)) and first.sz > max_sz:
                        logger.warning(f"guard:shrink_size mode=caution from={first.sz} to={max_sz}")
                        first.sz = max_sz
                if "size" in kwargs and isinstance(kwargs["size"], (int, float)) and kwargs["size"] > max_sz:
                    logger.warning(f"guard:shrink_size mode=caution from={kwargs['size']} to={max_sz}")
                    kwargs["size"] = max_sz
                if "sz" in kwargs and isinstance(kwargs["sz"], (int, float)) and kwargs["sz"] > max_sz:
                    logger.warning(f"guard:shrink_size mode=caution from={kwargs['sz']} to={max_sz}")
                    kwargs["sz"] = max_sz

        if self._orig_place is None:
            return None
        if not is_reduce:
            self._last_place_ts_ms = now_ms  # 何をするか：新規発注が通る直前に“最後に出した時刻”を更新（Cautionのレート制御に使う）
        return self._orig_place(*args, **kwargs)

    def gate_status(self):
        """何をするか：戦略が今のゲート状態（mode/理由/制限）を取得するためのアクセサ"""
        return getattr(self, "_last_gate_status", {"mode": "healthy", "reason": "na", "limits": {}, "ts_ms": None})
    def _normalize_side(self, side: str | None) -> str | None:
        """【関数】side表現を "buy" / "sell" に正規化（それ以外はNone）"""
        if side is None:
            return None
        try:
            s = str(side).strip().lower()
        except Exception:
            return None
        if s in ("buy", "sell"):
            return s
        return None

    def would_reduce_inventory(self, current_inventory: float, side: str | None, request_qty: float) -> bool:
        """【関数】注文が在庫|Q|を縮める（=決済）か判定し、縮めるならTrue"""
        side_norm = self._normalize_side(side)
        if side_norm not in ("buy", "sell"):
            return False
        try:
            qty = float(request_qty)
        except (TypeError, ValueError):
            return False
        if qty <= 0.0:
            return False
        delta = qty if side_norm == "buy" else -qty
        return abs(current_inventory + delta) <= abs(current_inventory)

    # ─────────────────────────────────────────────────────────────
    def _guard_midmove_bp(self, now: datetime) -> bool:
        """【関数】30sのミッド変化(bps)を監視：閾値超ならTrue（新規停止＋全取消）
        - 文書のguard方針（速すぎるときは出さない）に合わせた最小実装。:contentReference[oaicite:9]{index=9}
        """
        if self.ob.best_bid.price is None or self.ob.best_ask.price is None:
            return False
        mid = (self.ob.best_bid.price + self.ob.best_ask.price) / 2.0
        t = now.timestamp()
        self._midwin.append((t, mid))
        # 30秒より古いものを落とす
        cutoff = t - 30.0
        while self._midwin and self._midwin[0][0] < cutoff:
            self._midwin.popleft()
        if not self.guard_bp or len(self._midwin) < 2:
            return False
        oldest_mid = self._midwin[0][1]
        if oldest_mid <= 0:
            return False
        move_bp = abs(mid - oldest_mid) / oldest_mid * 1e4
        return move_bp >= float(self.guard_bp)

    def _in_maintenance(self, now: datetime) -> bool:
        """【関数】JSTのメンテ窓に入っているかを判定（跨日にも対応）"""
        m = getattr(getattr(self.cfg, "mode_switch", None), "maintenance", None)
        if not m:
            return False
        try:
            s, e = m.start, m.end  # "HH:MM:SS"
        except AttributeError:
            return False
        jst = now.astimezone(self._JST)
        start = jst.replace(hour=int(s[0:2]), minute=int(s[3:5]), second=int(s[6:8]), microsecond=0)
        end = jst.replace(hour=int(e[0:2]), minute=int(e[3:5]), second=int(e[6:8]), microsecond=0)
        if end >= start:
            return start <= jst < end              # 同日内
        else:
            return jst >= start or jst < end       # 日跨ぎ(例: 23:55→00:05)

    def _in_funding_calc(self, now: datetime) -> bool:
        """【関数】Funding“計算”窓（JST）にいるか？（±5分で判定）"""
        times = getattr(getattr(self.cfg, "mode_switch", None), "funding_calc_jst", None)
        if not times:
            return False
        jst = now.astimezone(self._JST)
        for s in times:  # "HH:MM:SS"
            tgt = jst.replace(hour=int(s[0:2]), minute=int(s[3:5]), second=int(s[6:8]), microsecond=0)
            if abs((jst - tgt).total_seconds()) <= 300:  # 5分以内なら窓中
                return True
        return False

    def _in_funding_transfer(self, now: datetime) -> bool:
        """【関数】Funding“授受”窓（計算+ラグ時間、JST）にいるか？（±5分で判定）"""
        times = getattr(getattr(self.cfg, "mode_switch", None), "funding_calc_jst", None)
        lag_h = getattr(getattr(self.cfg, "mode_switch", None), "funding_transfer_lag_hours", None)
        if not times or lag_h is None:
            return False
        jst = now.astimezone(self._JST)
        for s in times:
            base = jst.replace(hour=int(s[0:2]), minute=int(s[3:5]), second=int(s[6:8]), microsecond=0)
            tgt = base + timedelta(hours=int(lag_h))
            if abs((jst - tgt).total_seconds()) <= 300:  # 5分以内なら窓中
                return True
        return False

    def _roll_daily(self, now: datetime) -> None:
        """【関数】日次境界（JST）を跨いだら R_day/HWM をリセット"""
        jst = now.astimezone(self._JST)
        jst_mid = jst.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_utc = jst_mid.astimezone(timezone.utc)
        if day_start_utc != self._day_start_utc:
            self._day_start_utc = day_start_utc
            self._daily_R, self._R_HWM = 0.0, 0.0  # 新しい日としてリセット

    def _maybe_trigger_kill(self) -> tuple[bool, str | None, float, float]:
        """【関数】Kill判定：Trueなら停止（理由, 日次R, 日次DDを返す）"""
        daily_R = self._daily_R
        dd = daily_R - self._R_HWM  # ≤ 0（下振れがDD）
        # 日次PnL（下限到達でKill）
        if self.kill_daily is not None and daily_R <= float(self.kill_daily):
            return True, "daily", daily_R, dd
        # 日次DD（下限到達でKill）
        if self.kill_dd is not None and dd <= float(self.kill_dd):
            return True, "dd", daily_R, dd
        return False, None, daily_R, dd

    # ─────────────────────────────────────────────────────────────
    def _record_decision(self, now: datetime, actions, features: dict | None = None) -> None:
        """【関数】意思決定ログへ記録（featuresと結論の一行）"""
        # 特徴量を収集
        feats_win = getattr(getattr(self.cfg, "features", None), "ca_ratio_win_ms", 500)
        feats = features if isinstance(features, dict) else {
            "best_age_ms": self.ob.best_age_ms(now),
            "ca_ratio": self.ob.ca_ratio(now, window_ms=feats_win),
            "spread_tick": self.ob.spread_ticks(),
        }
        # 結論を要約
        if not actions:
            decision = "none"
        else:
            places = [a for a in actions if a.get("type") == "place"]
            cancels = [a for a in actions if a.get("type") == "cancel_tag"]
            if places:
                sides = {p["order"].side for p in places if "order" in p}
                decision = "place_both" if sides == {"buy", "sell"} else f"place_{list(sides)[0]}"
            elif cancels:
                decision = "cancel"
            else:
                decision = "none"

        self.decision_log.add(
            ts=now.isoformat(),
            strategy=self.strat.name,
            decision=decision,
            features=feats,
            expected_edge_bp=None,  # 最小実装では未算出
            eta_ms=None,            # 最小実装では未算出
            ca_ratio=feats["ca_ratio"],
            best_age_ms=feats["best_age_ms"],
            spread_state=("zero" if feats["spread_tick"] == 0 else "ge1"),
        )

    def _heartbeat(self, now: datetime, event: str, reason: str | None = None) -> None:
        """【関数】ハートビート：Q/A/R・日次R・各ガード/窓の状態を1行JSONで追記する"""
        j = {
            "ts": now.isoformat(),
            "event": event,           # "place" / "fill" / "pause" など直近イベント
            "reason": reason,         # "inventory_guard" / "midmove_guard" / "maintenance" / "funding" など
            "strategy": self.strat.name,
            "Q": self.Q,              # 現在在庫（+ロング/−ショート）
            "A": self.A,              # 平均建値
            "R": self.R,              # 累計実現PnL
            "R_day": self._daily_R,   # 日次実現PnL
            "guard": {                # ガードのON/OFF（Trueで“新規停止中”）
                "inventory": (self.effective_inventory_limit() is not None and abs(self.Q) >= float(self.effective_inventory_limit())),
                "midmove": self._midguard_paused,
            },
            "window": {
                "maint": self._in_maintenance(now),
                "funding": self._in_funding_calc(now) or self._in_funding_transfer(now),
            },
        }
        self._hb_path.open("a", encoding="utf-8").write(orjson.dumps(j).decode("utf-8") + "\n")

    # ─────────────────────────────────────────────────────────────
    def _apply_fill_and_log(self, ts_iso: str, side: str, px: float, sz: float, tag: str) -> None:
        """【関数】Fillを在庫Q/A/Rに適用し、orders/tradesへ記録（最小PnL）"""
        # 1) orders：fill行
        self.order_log.add(ts=ts_iso, action="fill", tif="GTC", ttl_ms=None, px=px, sz=sz, reason=tag)
        dt = _parse_iso(ts_iso)  # 【関数】この約定時刻で窓フラグを判定するためにdatetime化
        is_maint = self._in_maintenance(dt)  # 【関数】メンテ窓か？
        is_fund = self._in_funding_calc(dt) or self._in_funding_transfer(dt)  # 【関数】Funding窓か？

        # 2) PnL更新（最小）：ショート買い戻し/ロング利確を片側ずつ
        realized = 0.0
        if side == "sell":
            if self.Q > 0:
                matched = min(sz, self.Q)
                realized += (px - self.A) * matched
                self.Q -= matched
                if sz > matched:
                    self.A = px
                    self.Q -= (sz - matched)
            else:
                self.A = (self.A * abs(self.Q) + px * sz) / (abs(self.Q) + sz) if self.Q < 0 else px
                self.Q -= sz
        else:  # buy
            if self.Q < 0:
                matched = min(sz, -self.Q)
                realized += (self.A - px) * matched
                self.Q += matched
                if sz > matched:
                    self.A = px
                    self.Q += (sz - matched)
            else:
                self.A = (self.A * self.Q + px * sz) / (self.Q + sz) if self.Q > 0 else px
                self.Q += sz
        self.R += realized
        self._daily_R += realized  # 【関数】日次PnLを更新
        self._R_HWM = max(self._R_HWM, self._daily_R)  # 【関数】HWM更新（DD計算用）

        # 3) trades：約定行
        self.trade_log.add(
            ts=ts_iso, side=side, px=px, sz=sz, pnl=realized,
            strategy=self.strat.name, tag=tag, inventory_after=self.Q,
            window_funding=is_fund, window_maint=is_maint  # 【関数】どの窓中の約定かを明示
        )
        self._heartbeat(dt, "fill", reason=tag)  # ハートビート：約定を要約

    def _log_window_event(self, window_type: str, action: str, ts: datetime) -> None:
        """役割：メンテ/ファンディング等の“窓”の入退を1行CSVで記録する"""
        events_dir = Path("logs") / "events"  # 役割：イベントログの保存先
        events_dir.mkdir(parents=True, exist_ok=True)  # 役割：ディレクトリを必ず作成

        # 役割：窓の種類ごとに出力ファイル名を選ぶ（仕様：maintenance.csv / funding_schedule.csv）
        if window_type == "maintenance":
            outfile = events_dir / "maintenance.csv"
        elif window_type == "funding":
            outfile = events_dir / "funding_schedule.csv"
        else:
            outfile = events_dir / f"{window_type}.csv"

        is_new = not outfile.exists()  # 役割：ファイルが無いときはヘッダ行を書く

        # 役割：ts を UTC ISO8601 に正規化（naive なら UTC を付与して保存）
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts_iso = ts.astimezone(timezone.utc).isoformat()

        # 役割：CSV へ追記（列：ts, window, action）
        with outfile.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(["ts", "window", "action"])
            w.writerow([ts_iso, window_type, action])


    # ─────────────────────────────────────────────────────────────
    async def run_paper(self) -> None:
        """【関数】paper実行の本体：WS→板→戦略→シミュ→ログ（Ctrl+Cで安全終了）
        - 文書の 8.3 ペーパー運用の最小形。:contentReference[oaicite:10]{index=10}
        """
        logger.info(f"paper start: product={self.product} strategy={self.strat.name}")
        try:
            async for ev in event_stream(product_code=self.product):
                now = _parse_iso(ev["ts"])
                ch = ev.get("channel", "")

                if ch.startswith("lightning_board_"):
                    # ローカル板更新
                    self._last_heartbeat_ms = int(now.timestamp() * 1000)  # 何をするか：最新board受信時刻を記録しHB間隔を測る
                    # 日次境界（JST）を跨いだら R_day/HWM をリセット
                    self._roll_daily(now)

                    # Kill‑Switch 判定（trueで全キャンセル→停止）
                    hit, why, rday, dd = self._maybe_trigger_kill()
                    if hit:
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="kill")
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="kill")
                        self.halted = True
                        logger.error(f"Kill‑Switch({why}) fired: R_day={rday:.2f}, DD={dd:.2f} → stopping")
                        return  # 安全停止（finallyでログflush）  # 文書の“Kill到達で停止”に準拠

                    self.ob.update_from_event(ev)
                    # 窓の現在状態を判定（true/false）し、前回から変わったらCSVに記録
                    maint_now = self._in_maintenance(now)
                    fund_now = self._in_funding_calc(now) or self._in_funding_transfer(now)
                    if maint_now != self._maint_prev:
                        self._log_window_event("maintenance", "enter" if maint_now else "exit", now)  # 役割：メンテ窓の出入りを記録
                        self._maint_prev = maint_now
                    if fund_now != self._fund_prev:
                        self._log_window_event("funding", "enter" if fund_now else "exit", now)  # 役割：Funding窓の出入りを記録
                        self._fund_prev = fund_now

                    # メンテ窓：新規禁止＋同タグ一括Cancel（reason="window"）
                    if self._in_maintenance(now):
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="window")
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="window")
                            self._heartbeat(now, "pause", reason="maintenance")
                        continue  # このboardイベントでは新規Placeを行わない
                    
                    # Funding窓（計算 or 授受）：新規禁止＋同タグ一括Cancel（reason="funding"）
                    if self._in_funding_calc(now) or self._in_funding_transfer(now):
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="funding")  # 【関数】Funding窓で停止
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="funding")  # 【関数】Funding窓で停止
                            self._heartbeat(now, "pause", reason="funding")
                        continue  # このboardイベントでは新規Placeを行わない

                    # TTL失効を処理（取消ログ）
                    for o in self.sim.on_time(now):
                        self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                           px=o.price, sz=o.remaining, reason="ttl")

                    # ガード（速すぎるときは新規停止＋全取消）
                    paused = self._guard_midmove_bp(now)
                    self._midguard_paused = paused  # 直近のミッド移動ガード状態を保持（ハートビートに載せる）

                    if paused:
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                               px=o.price, sz=o.remaining, reason="guard")
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                               px=o.price, sz=o.remaining, reason="guard")
                            self._heartbeat(now, "pause", reason="midmove_guard")  # 直近イベントを要約（ミッド変化ガードで停止）
                        continue  # 新規は出さない
                    
                    # 戦略評価→意思決定ログ→アクション適用
                    # 在庫上限ガード：|Q| が上限以上なら新規はClose-Onlyに切り替え、既存の指値は整理
                    eff_limit = self.effective_inventory_limit()
                    close_only_mode = False
                    if eff_limit is not None and abs(self.Q) >= eff_limit:
                        close_only_mode = True
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="risk")  # 何を/なぜ記録したか（在庫上限）
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                            px=o.price, sz=o.remaining, reason="risk")  # 何を/なぜ記録したか（在庫上限）
                        logger.warning(f"risk guard: |Q|>={eff_limit} → new orders paused")  # 画面でも分かるように一言
                        self._heartbeat(now, "pause", reason="inventory_guard")  # ハートビート：在庫上限で停止


                    actions = self.strat.evaluate(self.ob, now, self.cfg)
                    feats_win = getattr(getattr(self.cfg, "features", None), "ca_ratio_win_ms", 500)  # 何をする行か：CA比率集計窓(ms)を取得
                    features = {
                        "best_age_ms": self.ob.best_age_ms(now),  # 何をする行か：Best静止時間(ms)を記録
                        "ca_ratio": self.ob.ca_ratio(now, window_ms=feats_win),  # 何をする行か：C/A比率を記録
                        "spread_tick": self.ob.spread_ticks(),  # 何をする行か：現在スプレッド(tick)を記録
                    }
                    self._record_decision(now, actions, features=features)
                    for act in actions:
                        if act.get("type") == "place":
                            # 同タグの重複を最小抑止
                            if self.sim.has_open_tag(act["order"].tag):
                                continue
                            o = act["order"]
                            age = None  # 何をする行か：best_age_msをfeatures/decisionから拾う準備
                            if isinstance(locals().get("features"), dict):  # 何をする行か：戦略特徴量が存在する場合
                                age = features.get("best_age_ms")
                            elif isinstance(locals().get("decision"), dict):  # 何をする行か：決定ペイロードに含まれる場合
                                age = decision.get("best_age_ms")
                            if isinstance(age, (int, float)):
                                if self._stale_halt_ms is not None and age >= self._stale_halt_ms:
                                    self.risk.set_market_mode("halted")  # 何をする行か：Best静止が閾値超→Haltedに切替
                                elif self._stale_warn_ms is not None and age >= self._stale_warn_ms:
                                    self.risk.set_market_mode("caution")  # 何をする行か：Best静止が注意閾値超→Cautionに切替
                                else:
                                    self.risk.set_market_mode("healthy")  # 何をする行か：静止時間が短いので通常モード
                            else:
                                self.risk.set_market_mode("healthy")  # 何をする行か：best_ageが無ければ通常モードに戻す
                            if self.risk.market_mode in ("caution", "halted"):
                                close_only_mode = True  # 何をする行か：市場モードが注意/停止ならClose-Only扱いにする
                            eff_limit = self.effective_inventory_limit()
                            req_qty = float(getattr(o, "size", 0.0) or 0.0)
                            side_val = getattr(o, "side", None)
                            reduce_only = bool(getattr(o, "reduce_only", False))
                            allow_place = self.risk.can_place(
                                self.Q,
                                req_qty,
                                side=side_val,
                                reduce_only=reduce_only,
                                best_age_ms=age,
                            )  # 何をする行か：市場モードと在庫から発注可否を判定
                            if not allow_place and self.risk.market_mode in ("caution", "halted"):
                                logger.debug(f"skip place: market_mode={self.risk.market_mode}")  # 何をする行か：静止検知で新規停止を記録
                                self._heartbeat(now, "pause", reason="market_mode")  # 何をする行か：ハートビートに市場停止を記録
                                continue
                            if eff_limit is not None:
                                if abs(self.Q) + req_qty > eff_limit:
                                    if reduce_only or self.would_reduce_inventory(self.Q, side_val, req_qty):
                                        pass  # 決済方向なのでClose-Only中でも許可
                                    else:
                                        logger.debug(
                                            f"skip place: |Q|+req={abs(self.Q) + req_qty:.6f} > eff_limit={eff_limit:.6f}"
                                        )
                                        self._heartbeat(now, "pause", reason="inventory_guard")
                                        continue
                                elif close_only_mode and not (reduce_only or self.would_reduce_inventory(self.Q, side_val, req_qty)):
                                    logger.debug("skip place: close_only_mode (inventory_guard)")
                                    self._heartbeat(now, "pause", reason="inventory_guard")
                                    continue
                            self.sim.place(o, now)
                            self.order_log.add(ts=now.isoformat(), action="place", tif=o.tif, ttl_ms=o.ttl_ms, px=o.price, sz=o.size, reason=o.tag)  # placeでも“注文タグ”（stall / ca_gate）を記録する

                            self._heartbeat(now, "place", reason=o.tag)  # ハートビート：発注を要約

                        elif act.get("type") == "cancel_tag":
                            for o in self.sim.cancel_by_tag(act["tag"]):
                                self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                                   px=o.price, sz=o.remaining, reason="strategy")

                elif ch.startswith("lightning_executions_"):
                    # 約定でシミュを進め、Fill明細を受け取る→PnL/ログ反映
                    fills = self.sim.on_executions(ev.get("message") or [], now)
                    for f in fills:
                        self._apply_fill_and_log(
                            ts_iso=f["ts"], side=f["side"], px=float(f["price"]),
                            sz=float(f["size"]), tag=f["tag"]
                        )
                    # TTLチェックをもう一度（成約後の期限切れ）
                    for o in self.sim.on_time(now):
                        self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                           px=o.price, sz=o.remaining, reason="ttl")

        except asyncio.CancelledError:
            logger.info("paper cancelled")
            raise
        except KeyboardInterrupt:
            logger.info("Ctrl+C - stopping paper")
        finally:
            # ログの確定保存
            self.order_log.flush()
            self.trade_log.flush()
            self.decision_log.flush()
            logger.info(f"paper end: realized_pnl={self.R}, open_orders={len(self.sim.open)}")
