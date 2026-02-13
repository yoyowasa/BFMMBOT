# src/runtime/engine.py
# 役割：リアルタイムの“paper実行”エンジン（WS→ローカル板→戦略→最小シミュ→ログ保存）
# - 【関数】run_paper：WSイベントを流し込み、#1/#2戦略を評価→発注/取消→Fill反映→ログ保存
# - 【関数】_guard_midmove_bp：30秒のミッド変化(bps)を監視し、閾値超なら新規停止＋全取消
# - ログは文書仕様どおり logs/orders・logs/trades・logs/analytics にParquetで出力する
from __future__ import annotations

import asyncio  # 非同期ループ/キャンセル
import uuid  # 何をするか：corr_idのフォールバック生成に使用
from collections import OrderedDict, deque  # 30sミッド履歴でガード＋client_order_id↔corr_id対応の保持
from collections.abc import Mapping  # 戦略別設定の判定に使用
from datetime import datetime, timezone, timedelta  # ts解析と現在時刻 JST日付の境界計算にtimedeltaを使う
from zoneinfo import ZoneInfo  # 何をするか：JSTタイムゾーンを扱う
from typing import Deque, Optional, Sequence, Tuple  # 型ヒント
import csv  # 役割：窓イベントをCSVに1行追記するために使用
import time  # 何をするか：現在時刻(ms)を取得してHB間隔を測る
import math  # 何をするか：QueueETAの推定結果（infなど）を安全に判定するため
from time import monotonic  # これは「auto_reduceのクールダウン用タイマー」です
from loguru import logger  # 実行ログ
from functools import wraps  # 再入防止デコレータ用（_maybe_auto_reduce の重複呼び出し抑止）
from pathlib import Path  # ハートビートNDJSONのファイル出力に使用
import orjson  # 1行JSON化（高速）
from src.core.realtime import event_stream  # 【関数】WS購読（board/executions）:contentReference[oaicite:2]{index=2}
from src.core.orderbook import OrderBook  # 【関数】ローカル板（Best/Spread/C-A）:contentReference[oaicite:3]{index=3}
from src.features.queue_eta import QueueETA  # 何をするか：QueueのETA推定器（executionsから推定）を使えるようにする
from src.core.simulator import MiniSimulator  # 【関数】最小約定シミュ（価格タッチ）:contentReference[oaicite:4]{index=4}
from src.core.logs import OrderLog, TradeLog  # 【関数】発注/約定ログ（Parquet）:contentReference[oaicite:5]{index=5}
from src.core.analytics import DecisionLog  # 【関数】意思決定ログ（Parquet）:contentReference[oaicite:6]{index=6}
from src.strategy import build_strategy  # 何をするか：戦略生成を中央ファクトリに委譲する
from src.strategy.base import (
    MultiStrategy,
    current_corr_ctx,
    current_strategy_ctx,
)  # 何をするか：複数戦略を束ねるラッパーと子戦略名・相関IDの合図
from src.core.risk import RiskGate  # 何をするか：在庫ゲート（市場モードでClose-Onlyを切り替える）
from src.core.utils import monotonic_ms
from src.core.risk import inv_capping_preflight  # 在庫前処理（ログ: headroom/shrink/no_room を一元管理）
from src.core.orders import Order  # Reduce-Only+IOCの仕様とOrder
from src.core.position_replay import http_replay_for_position  # HTTP約定リプレイでQ/A/R/Fの欠損を埋める関数

_CORR_MAP_MAX = 8192  # 何をするか：client_order_id→corr_id の保持件数上限
_coid_to_corr: OrderedDict[str, str] = OrderedDict()  # 何をするか：corr参照用のLRUマップ


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


def _cfg_pick(root, dotted, default=None):
    # ドット区切りパス（例: "risk.auto_reduce.enabled"）をたどって
    # dict / Pydantic(BaseModel) / SimpleNamespace などを安全に横断して値を取り出すユーティリティ。
    cur = root
    for key in dotted.split("."):
        if cur is None:
            return default
        if isinstance(cur, dict):
            # dict は通常どおりキーでたどる
            cur = cur.get(key)
        else:
            # Pydantic v2 では未知のキーが model_extra に入るので、まずそこを見る
            extra = getattr(cur, "model_extra", None)
            if isinstance(extra, dict) and key in extra:
                cur = extra[key]
            else:
                # なければ通常の属性アクセスを試す
                cur = getattr(cur, key, None)
    return default if cur is None else cur


def _ar_non_reentrant(fn):
    """関数目的：auto_reduceの再入（再帰/重複呼び出し）を1回に制限するデコレータ。
    実行中に同じ関数が再度呼ばれたらノーオペで戻す。"""
    @wraps(fn)
    def _wrap(self, *args, **kwargs):
        # いま実行中なら再入をブロック（目的：sim.place()→on_fill→_maybe_auto_reduce() の再帰ループ遮断）
        if getattr(self, "_ar_locked", False):
            logger.debug("auto_reduce: skip reentry_guard_on")
            return  # ノーオペで即終了（決済フローを壊さない）
        # ロックを立ててから本体を実行
        self._ar_locked = True  # このフラグは本インスタンス内だけで有効
        try:
            return fn(self, *args, **kwargs)
        finally:
            # 例外時でも必ず解放（目的：ロック取りっぱなしを防ぐ）
            self._ar_locked = False
    return _wrap

def _parse_iso(ts: str) -> datetime:
    """【関数】ISO→datetime（'Z'も+00:00に正規化）"""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _now_utc() -> datetime:
    """【関数】現在時刻（JST）"""
    return datetime.now(ZoneInfo("Asia/Tokyo"))

class PaperEngine:
    """リアルタイム“paper”の最小エンジン"""

    @staticmethod
    def _normalize_strategy_names(
        primary: str,
        strategies: Sequence[str] | str | None,
    ) -> list[str]:
        if strategies is None:
            names = [primary]
        elif isinstance(strategies, str):
            names = [strategies]
        else:
            names = list(strategies)
        return names or [primary]

    @staticmethod
    def _strategy_cfg_for(strategy_cfg, name: str):
        if isinstance(strategy_cfg, Mapping):
            return strategy_cfg.get(name)
        return strategy_cfg

    def __init__(
        self,
        cfg,
        strategy_name: str,
        *,
        strategies: Sequence[str] | str | None = None,
        strategy_cfg=None,
    ) -> None:
        if strategy_cfg is None:
            if isinstance(cfg, Mapping):
                strategy_cfg = cfg.get("strategy_cfg")
            else:
                strategy_cfg = getattr(cfg, "strategy_cfg", None)
        # 設定（製品コード/刻み/ガード閾値）
        self.cfg = cfg
        self.logger = logger
        self._ar_last_ts = 0.0  # これは「auto_reduceの前回発火時刻（秒）」です
        self._ar_recent = deque(maxlen=32)  # auto_reduceの直近発火時刻リスト
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
        setattr(self.risk, "engine", self)  # RiskGate から現在在庫やモードを参照できるようにつないでおく
        health_cfg = getattr(cfg, "health", None)  # 何をする行か：Best静止しきい値（healthセクション）を安全に取得
        self._stale_warn_ms = None  # 何をする行か：Best静止でCautionへ入る閾値(ms)
        self._stale_halt_ms = None  # 何をする行か：Best静止でHaltedへ入る閾値(ms)
        if health_cfg is not None:  # 何をする行か：healthセクションが定義されているときのみ閾値を読み込む
            warn_sec = getattr(health_cfg, "stale_sec_warn", None)  # 何をする行か：Cautionへ入る秒数を読む
            halt_sec = getattr(health_cfg, "stale_sec_halt", None)  # 何をする行か：Haltedへ入る秒数を読む
            self._stale_warn_ms = float(warn_sec) * 1000.0 if warn_sec is not None else None  # 何をする行か：秒→ms換算
            self._stale_halt_ms = float(halt_sec) * 1000.0 if halt_sec is not None else None  # 何をする行か：秒→ms換算

        # 戦略（#1/#2/#3）を選択
        self.strategies = self._normalize_strategy_names(strategy_name, strategies)
        if len(self.strategies) == 1:
            selected = self.strategies[0]
            cfg_override = self._strategy_cfg_for(strategy_cfg, selected)
            self.strat = build_strategy(selected, cfg, strategy_cfg=cfg_override)
        else:
            children = [
                build_strategy(name, cfg, strategy_cfg=self._strategy_cfg_for(strategy_cfg, name))
                for name in self.strategies
            ]
            self.strat = MultiStrategy(children)

        self._attach_strategy_context(self.strat)


        # ローカル板・シミュ・ログ器
        self.ob = OrderBook(tick_size=self.tick)
        try:
            self._queue_eta = QueueETA(window_sec=30.0)  # 何をするか：直近30秒の約定流量からQueueのETAを推定する
        except TypeError:
            self._queue_eta = QueueETA()  # 何をするか：QueueETAの__init__引数が違う実装でも落ちないようにする（保険）
        self._queue_eta_last = None  # 何をするか：直近のQueueETA入力/生ETAを保持してdecision_logへ同梱する
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
        self._hb_path.parent.mkdir(parents=True, exist_ok=True)  # 先にフォルダを用意
        self._events_dir = Path("logs/events")  # 役割：窓イベントのCSVフォルダ
        self._events_dir.mkdir(parents=True, exist_ok=True)  # 先にフォルダを作成（touch前に必要）
        (self._events_dir / "maintenance.csv").touch(exist_ok=True)  # 初回起動でも tail できるよう空ファイルを作る
        (self._events_dir / "funding_schedule.csv").touch(exist_ok=True)  # 同上
        self._maint_prev, self._fund_prev = False, False  # 役割：直前の窓状態（enter/exit検出用）

        self._hb_path.parent.mkdir(parents=True, exist_ok=True)  # 親フォルダを用意
        self._midguard_paused = False  # 直近の“ミッド変化ガード”状態を持つ
        # Sliding 30s mid-price window for guard (epoch_sec, mid)
        self._midwin: Deque[Tuple[float, float]] = deque()
        # 板フリーズ検知用（spread=0が続いたらKill-Switch）
        self._freeze_since_ms: float | None = None
        self._freeze_halted: bool = False
        self._freeze_recover_since_ms: float | None = None
        # フィード健全性・Fill監視
        self._feed_unhealthy_since_ms: float | None = None
        self._last_fill_ms: float | None = None
        self._early_exit_triggered: bool = False
        # auto_reduceのレート/量リミット用
        self._ar_min_bucket_ms: float | None = None
        self._ar_min_count: int = 0
        self._ar_hour_bucket_ms: float | None = None
        self._ar_hour_qty: float = 0.0


        # PnL最小モデルの内部状態（自炊Q/A/Rのミニ版）
        self.Q = 0.0  # 在庫（+ロング/−ショート）
        self.A = 0.0  # 平均建値
        self.R = 0.0  # 実現PnL累計
        self._inv_guard_state: str = "healthy"  # 在庫ガードの現在モード（warning/caution/hard）

        self._JST = timezone(timedelta(hours=9))  # 【関数】日次境界（JST）
        jst_now = _now_utc().astimezone(self._JST)
        jst_midnight = jst_now.replace(hour=0, minute=0, second=0, microsecond=0)
        self._day_start_utc = jst_midnight.astimezone(timezone.utc)  # 【関数】当日JST 0時（UTC）
        self._daily_R, self._R_HWM = 0.0, 0.0  # 【関数】日次PnLとそのHWM（DD計算に使用）

    async def _http_replay_position(self) -> None:
        # この関数は「WSが再接続したあとにHTTPで自分の約定履歴を取り寄せて、
        # position が覚えている last_fill_id 以降の Q/A/R/F の“穴”をまとめて埋める」係です。
        # 実際のバリアID→HTTPリプレイ→適用のロジックは core.position_replay 側にまとめてあります。
        await http_replay_for_position(
            exchange=self.exchange,        # 取引所へのHTTPクライアント（Engineが持っている想定）
            product_code=getattr(self, "product", getattr(self, "product_code", None)),  # このEngineが扱っている銘柄
            position=self.position,        # Q/A/R/Fとlast_fill_idを持っているポジションオブジェクト
        )

    def _ts_jst(self, dt: datetime) -> str:
        """ログ用のJST ISO文字列を返す（タイムゾーン未指定ならUTC扱い）。"""
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(self._JST).isoformat()



    def effective_inventory_limit(self) -> float | None:
        """【関数】新規発注の可否判定に使う実効在庫上限（上限−安全マージン）を返す"""
        if self.max_inv is None:
            return None
        limit = float(self.max_inv) - float(self.inventory_eps)
        return max(0.0, limit)


    def _place_with_feed_guard(self, *args, **kwargs):
        """フィードの健全性と在庫ガードを踏まえて発注をフィルタする（Haltedでは新規を止める）。"""
        now_ms = int(time.time() * 1000)
        prev_mode = getattr(self, "_feed_mode", "healthy")
        best_age_ms: float | None = None
        hb_gap_sec: float | None = None
        spread_tick: int | None = None

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
            try:
                spread_tick = int(ob.spread_ticks())
            except Exception:
                spread_tick = None

        if last_rx_dt is not None:
            try:
                hb_gap_sec = max(0.0, (datetime.now(timezone.utc) - last_rx_dt).total_seconds())
            except Exception:
                hb_gap_sec = None
        elif isinstance(getattr(self, "_last_heartbeat_ms", None), (int, float)):
            hb_gap_sec = max(0.0, (now_ms - float(self._last_heartbeat_ms)) / 1000.0)

        cfg_obj = getattr(self, "cfg", {}) or {}
        freeze_cfg = _cfg_pick(cfg_obj, "guard.freeze", {}) or {}
        freeze_age_ms = float(freeze_cfg.get("age_ms", 3000.0) or 3000.0)
        freeze_streak_ms = float(freeze_cfg.get("streak_ms", 3000.0) or 3000.0)
        resume_spread_tick = int(freeze_cfg.get("resume_spread_tick", 1) or 1)
        resume_age_ms = float(freeze_cfg.get("resume_age_ms", 800.0) or 800.0)
        resume_window_ms = float(freeze_cfg.get("resume_window_ms", 4000.0) or 4000.0)
        # 早期終了用のゆるい閾値（長めに設定しておき、ログを見ながら詰める想定）
        try:
            freeze_timeout_min = float(freeze_cfg.get("max_unhealthy_min", 10.0) or 0.0)
        except Exception:
            freeze_timeout_min = 0.0
        try:
            nofill_timeout_min = float(freeze_cfg.get("max_no_fill_min", 10.0) or 0.0)
        except Exception:
            nofill_timeout_min = 0.0

        spread_freeze = (spread_tick == 0) and (best_age_ms is not None and best_age_ms >= freeze_age_ms)
        if spread_freeze:
            if self._freeze_since_ms is None:
                self._freeze_since_ms = now_ms
        else:
            self._freeze_since_ms = None
            self._freeze_recover_since_ms = None

        freeze_halt = False
        if self._freeze_since_ms is not None and (now_ms - self._freeze_since_ms) >= freeze_streak_ms:
            freeze_halt = True

        mode, reason = _eval_feed_health(cfg_obj, best_age_ms, hb_gap_sec)
        if freeze_halt:
            mode = "halted"
            reason = f"freeze:spread0_age>={int(freeze_age_ms)}ms"
        elif getattr(self, "_freeze_halted", False):
            healthy_spread = (spread_tick is not None and spread_tick >= resume_spread_tick)
            healthy_age = (best_age_ms is None) or (best_age_ms <= resume_age_ms)
            if healthy_spread and healthy_age:
                if self._freeze_recover_since_ms is None:
                    self._freeze_recover_since_ms = now_ms
                if (now_ms - self._freeze_recover_since_ms) >= resume_window_ms:
                    mode = "caution"
                    reason = "freeze_recovered"
                    self._freeze_halted = False
                    self._freeze_since_ms = None
            else:
                self._freeze_recover_since_ms = None

        self._feed_mode = mode
        self._last_feed_reason = reason
        if mode in ("healthy", "halted"):
            self._last_gate_status = {"mode": mode, "reason": reason, "limits": {}, "ts_ms": now_ms}
            if prev_mode != mode:
                logger.info(f"guard:mode_change {prev_mode}->{mode} reason={reason} limits={{}}")

        # 早期終了判定（板不健康が長時間続き、かつ Fill が一定時間無い場合）
        # 早期終了判定：フリーズ継続かつノーフィルが長時間続いた場合にRunを終わらせる
        freeze_dur_ms = (now_ms - self._freeze_since_ms) if self._freeze_since_ms is not None else 0.0
        last_fill_age = (now_ms - self._last_fill_ms) if self._last_fill_ms is not None else None
        freeze_timeout = freeze_timeout_min > 0 and self._freeze_since_ms is not None and freeze_dur_ms >= freeze_timeout_min * 60_000.0
        nofill_timeout = nofill_timeout_min > 0 and (last_fill_age is None or last_fill_age >= nofill_timeout_min * 60_000.0)
        if freeze_timeout and nofill_timeout and not self._early_exit_triggered:
            self._early_exit_triggered = True
            unhealthy_min = freeze_dur_ms / 60000.0
            nofill_min = float("inf") if last_fill_age is None else last_fill_age / 60000.0
            logger.warning(
                f"guard:early_exit reason=freeze_timeout unhealthy_min={unhealthy_min:.2f} no_fill_min={nofill_min:.2f}"
            )
            try:
                sim = getattr(self, "sim", None)
                canceller = getattr(sim, "cancel_all", None)
                if callable(canceller):
                    canceller()
                # 簡易的にopenが空くまで少し待つ
                if sim is not None and hasattr(sim, "open"):
                    for _ in range(10):
                        if not getattr(sim, "open", None):
                            break
                        time.sleep(0.05)
            except Exception as e:
                logger.warning(f"guard:early_exit_cancel_failed {e}")
            try:
                self._last_gate_status = {"mode": "halted", "reason": "freeze_timeout", "limits": {}, "ts_ms": now_ms}
            except Exception:
                pass
            raise SystemExit("early_exit_freeze_timeout")

        if hasattr(self, "risk") and hasattr(self.risk, "set_market_mode"):
            try:
                self.risk.set_market_mode(mode)
            except Exception as e:
                logger.warning(f"risk.set_market_mode failed: {e}")

        is_reduce = bool(kwargs.get("reduce_only"))
        if not is_reduce and args:
            first = args[0]
            is_reduce = getattr(first, "reduce_only", False) or getattr(first, "close_only", False)

        if mode == "halted" and not is_reduce:
            if freeze_halt and not getattr(self, "_freeze_halted", False):
                try:
                    sim = getattr(self, "sim", None)
                    canceller = getattr(sim, "cancel_all", None)
                    if callable(canceller):
                        canceller()
                except Exception as e:
                    logger.warning(f"guard:freeze_cancel_failed {e}")
                self._freeze_halted = True
            self._last_gate_status = {"mode": "halted", "reason": reason, "limits": {}, "ts_ms": now_ms}
            logger.warning(f"guard:block_new_order mode=halted reason={reason}")
            return None

        if mode == "caution" and not is_reduce:
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

            limits = {"max_order_size": max_sz, "max_order_rate_per_sec": rate}
            self._last_gate_status = {"mode": "caution", "reason": reason, "limits": limits, "ts_ms": now_ms}
            if prev_mode != "caution":
                logger.info(f"guard:mode_change {prev_mode}->caution reason={reason} limits={limits}")

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
            self._last_place_ts_ms = now_ms  # 新規注文が通る直前に“最後に出した時刻”を更新（Cautionのレート制御に使う）
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

    def _gate_key_from_tag(self, tag: str | None) -> str:
        """コード内でcorr付きタグを基底キーに畳み込む"""
        if not tag:
            return ""
        parts = [p for p in str(tag).split("|") if p and not str(p).startswith("corr:")]
        return parts[0] if parts else ""

    def _inflight_snapshot(
        self,
        *,
        side: str | None = None,
        ignore_reduce_only: bool = True,
    ) -> tuple[int, float, dict[str, int]]:
        """未約定注文の本数・数量・キー別本数を集計する（同一サイドのみ）"""
        orders = getattr(self, "sim", None)
        opens = getattr(orders, "open", []) if orders is not None else []
        total_count = 0
        total_qty = 0.0
        per_key: dict[str, int] = {}
        side_norm = self._normalize_side(side) if side is not None else None
        for o in opens or []:
            if o is None:
                continue
            if side_norm is not None and self._normalize_side(getattr(o, "side", None)) != side_norm:
                continue
            try:
                qty = float(getattr(o, "remaining", getattr(o, "size", 0.0)) or 0.0)
            except Exception:
                qty = 0.0
            if qty <= 0:
                continue
            try:
                reduces_pos = self.would_reduce_inventory(self.Q, getattr(o, "side", None), qty)
            except Exception:
                reduces_pos = False
            is_reduce = bool(getattr(o, "reduce_only", False) or getattr(o, "close_only", False) or reduces_pos)
            if ignore_reduce_only and is_reduce:
                continue
            total_count += 1
            total_qty += abs(qty)
            key = self._gate_key_from_tag(getattr(o, "tag", None))
            per_key[key] = per_key.get(key, 0) + 1
        return total_count, total_qty, per_key

    def now_ms(self) -> int:
        """【関数】戦略から利用する単調増加の現在時刻(ms)。"""
        return monotonic_ms()

    def _attach_strategy_context(self, strategy) -> None:
        if strategy is None:
            return
        try:
            setattr(strategy, "engine", self)
        except Exception:
            pass
        time_source = getattr(self, "now_ms", None)
        setter = getattr(strategy, "set_time_source", None)
        if callable(time_source) and callable(setter):
            try:
                setter(time_source)
            except Exception:
                pass
        children = getattr(strategy, "children", None)
        if children:
            for child in children:
                self._attach_strategy_context(child)

    def _queue_eta_on_execution(self, exe: dict) -> None:
        """[関数] executions(約定)1件をQueueETAへ流し込み、orderbook.queue_eta_msを更新する。"""
        if not isinstance(exe, dict) or not hasattr(self, "_queue_eta"):
            return

        # 何をするか：代表的なキーからQueueETAへ渡す（未対応実装ならdictで再試行）
        side_raw = exe.get("side") or exe.get("taker_side") or exe.get("exec_side") or exe.get("order_side")  # 約定のsideキー表記ゆれを吸収する
        price_raw = exe.get("price") or exe.get("exec_price") or exe.get("execPrice")  # 約定のpriceキー表記ゆれを吸収する
        size_raw = exe.get("size") or exe.get("exec_size") or exe.get("execSize") or exe.get("amount")  # 約定のsizeキー表記ゆれを吸収する
        if side_raw is None or price_raw is None or size_raw is None:
            return  # 必須キーが欠けている約定はETA推定に使えないので捨てる（推定器を壊さない）

        try:
            side = str(side_raw).upper()  # QueueETAが期待する "BUY"/"SELL" へ寄せる
            price = float(price_raw)  # 数値化してQueueETAへ渡す
            size = float(size_raw)  # 数値化してQueueETAへ渡す
        except Exception:
            return  # 変換不能な約定は捨てる（推定器を壊さない）
        ts_raw = exe.get("exec_date") or exe.get("timestamp") or exe.get("ts")
        ts = ts_raw
        if isinstance(ts_raw, str):
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                ts = None

        try:
            self._queue_eta.on_execution(side=side, price=price, size=size, ts=ts)
        except TypeError:
            try:
                self._queue_eta.on_execution(exe)
            except Exception:
                return
        except Exception:
            return

        # 何をするか：QueueETAの推定結果（ms）を取り出して、戦略が参照できる場所へ載せる
        eta_ms = None
        if hasattr(self._queue_eta, "queue_eta_ms"):
            eta_ms = getattr(self._queue_eta, "queue_eta_ms")
        elif hasattr(self._queue_eta, "eta_ms"):
            eta_ms = getattr(self._queue_eta, "eta_ms")

        # 何をするか：未初期化(None)の間は「無限大」扱いにして、ETAゲートが常に通らないようにする
        if eta_ms is None:
            eta_ms = float("inf")

        ob = getattr(self, "orderbook", None) or getattr(self, "ob", None)
        if ob is not None:
            setattr(ob, "queue_eta_ms", float(eta_ms))  # 何をするか：stall_then_strikeが読む値を更新する

    def _calc_queue_eta_ms(self, *, ob, now) -> float:
        """
        何をする関数：
          現在の板（bid, bid_size）とQueueETAから、「BUYをbidに置いたときのETA(ms)」を見積もる。
          流量不足などでETAが無限大（inf）になる場合は、ログや判定が壊れないように巨大な有限msへ落とす。
        """
        qeta = getattr(self, "queue_eta", None) or getattr(self, "_queue_eta", None)
        if qeta is None:
            self._queue_eta_last = None
            return 10_000_000.0  # 何をするか：推定器未配線時は「TTLより長い」扱いで安全側に倒す

        # 何をするコード：OrderBookから「最良Bid/Askの価格・サイズ」を安全に取り出す（実装差を吸収する）
        bid = getattr(ob, "bid", None)
        bid_size = getattr(ob, "bid_size", None)
        ask = getattr(ob, "ask", None)
        ask_size = getattr(ob, "ask_size", None)

        # 何をするコード：OrderBookが best_bid_price / best_ask_price 形式を持つ場合のフォールバック
        if bid is None:
            bid = getattr(ob, "best_bid_price", None)
        if bid_size is None:
            bid_size = getattr(ob, "best_bid_size", None)
        if ask is None:
            ask = getattr(ob, "best_ask_price", None)
        if ask_size is None:
            ask_size = getattr(ob, "best_ask_size", None)

        # 何をするコード：OrderBookが best_bid / best_ask （オブジェクト）形式を持つ場合のフォールバック
        best_bid = getattr(ob, "best_bid", None)
        if (bid is None or bid_size is None) and best_bid is not None:
            if bid is None:
                bid = getattr(best_bid, "price", None)
                if bid is None:
                    bid = getattr(best_bid, "px", None)
            if bid_size is None:
                bid_size = getattr(best_bid, "size", None)
                if bid_size is None:
                    bid_size = getattr(best_bid, "qty", None)

        best_ask = getattr(ob, "best_ask", None)
        if (ask is None or ask_size is None) and best_ask is not None:
            if ask is None:
                ask = getattr(best_ask, "price", None)
                if ask is None:
                    ask = getattr(best_ask, "px", None)
            if ask_size is None:
                ask_size = getattr(best_ask, "size", None)
                if ask_size is None:
                    ask_size = getattr(best_ask, "qty", None)
        if bid is None or bid_size is None:
            self._queue_eta_last = None
            return 10_000_000.0  # 何をするか：板がまだ無い/欠損なら「置かない」判定に寄せる

        qty = float(getattr(ob, "dust", None) or 0.001)  # 何をするか：代表数量（最小ロット想定）でETAを出す
        side = "BUY"
        price = float(bid) if bid is not None else None

        level_size = None  # 何をするか：この注文価格レベルに既に乗っている板数量（自分の前の行列）を推定する
        if price is not None:  # 何をするか：指値のときだけ、板厚（level_size）を推定できる
            if side == "BUY":  # 何をするか：BUY指値は bid 側の同値レベルに並ぶので best_bid.size を使う
                bb = best_bid
                if bb is not None and getattr(bb, "price", None) is not None and abs(float(price) - float(bb.price)) < 1e-9:
                    level_size = getattr(bb, "size", None)
            else:  # 何をするか：SELL指値は ask 側の同値レベルに並ぶので best_ask.size を使う
                ba = best_ask
                if ba is not None and getattr(ba, "price", None) is not None and abs(float(price) - float(ba.price)) < 1e-9:
                    level_size = getattr(ba, "size", None)

        # 何をするか：QueueETAに渡したlevel_sizeを、debug用の「ahead_qty」として必ず残す（ob.bid_size未配線でも0固定にしない）
        ahead_used_qty = 0.0
        try:
            ahead_used_qty = float(level_size) if level_size is not None else 0.0
        except Exception:
            ahead_used_qty = 0.0

        eta_sec = qeta.estimate_eta_sec(
            order_side=side,
            price=price,
            qty=qty,
            level_size=level_size,
            ts=now,
            price_first=False,  # 何をするか：価格別が薄いとinfになりやすいので、まずはside合計レート優先で安定化
        )
        self._queue_eta_last = {
            "qty": float(qty),
            "ahead_qty": float(max(0.0, ahead_used_qty)),
            "eta_sec": eta_sec,
        }

        if not math.isfinite(eta_sec):
            return 10_000_000.0  # 何をするか：inf/NaNは巨大msへ（TTLゲートで確実に落ちる）
        return eta_sec * 1000.0  # 何をするか：秒→ミリ秒へ変換して返す

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

    def _inv_brake_filter(self, orders: list["Order"], pos: float) -> list["Order"]:
        # 【関数】在庫ブレーキ（executor側）：在庫が閾値以上のとき、在庫が増える側の新規注文だけ落として偏りを戻す
        max_inv_raw = _cfg_pick(getattr(self, "cfg", None), "risk.max_inventory")
        if max_inv_raw is None:
            max_inv_raw = getattr(self, "max_inv", None)
        try:
            max_inv = float(max_inv_raw)
        except Exception:
            return orders
        brake_ratio = 0.25  # max_inventoryの25%から「増える側」を止めて、canary終了時のpos_beforeを小さく寄せる
        thr = max_inv * brake_ratio

        # 閾値が無効 or 在庫が小さいうちは何もしない
        try:
            pos_val = float(pos)
        except Exception:
            pos_val = 0.0
        if thr <= 0.0 or abs(pos_val) < thr:
            return orders

        # ロングなら BUY を落とす、ショートなら SELL を落とす（＝増える側だけ止める）
        blocked = "BUY" if pos_val > 0.0 else "SELL"

        kept: list["Order"] = []
        for o in orders or []:
            # side を持たない（cancel等の）オブジェクトが混ざっても壊れないように守る
            side = getattr(o, "side", None)
            if side is None:
                kept.append(o)
                continue

            if str(side).upper() == blocked:
                continue

            kept.append(o)

        return kept

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
        # 日次ロール: JST日付タグの変化に合わせてNDJSONファイルを切り替える
        try:
            jst = timezone(timedelta(hours=9))
            tag = now.astimezone(jst).strftime("%Y%m%d")
        except Exception:
            tag = None
        prev_tag = getattr(self, "_date_tag", None)
        if tag and tag != prev_tag:
            self._date_tag = tag
            self._hb_path = Path(f"logs/runtime/{tag}heartbeat.ndjson")
            self._hb_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                if getattr(self, "order_log", None) is not None and getattr(self.order_log, "_mirror", None) is not None:
                    self.order_log._mirror = Path(f"logs/orders/{tag}order_log.ndjson")
                    self.order_log._mirror.parent.mkdir(parents=True, exist_ok=True)
                if getattr(self, "trade_log", None) is not None and getattr(self.trade_log, "_mirror", None) is not None:
                    self.trade_log._mirror = Path(f"logs/trades/{tag}trade_log.ndjson")
                    self.trade_log._mirror.parent.mkdir(parents=True, exist_ok=True)
                if getattr(self, "decision_log", None) is not None and getattr(self.decision_log, "_mirror", None) is not None:
                    self.decision_log._mirror = Path(f"logs/analytics/{tag}decision_log.ndjson")
                    self.decision_log._mirror.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.warning(f"ndjson rollover failed: {e}")
        jst = now.astimezone(self._JST)
        jst_mid = jst.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_utc = jst_mid.astimezone(timezone.utc)
        if day_start_utc != self._day_start_utc:
            self._day_start_utc = day_start_utc
            self._daily_R, self._R_HWM = 0.0, 0.0  # 新しい一日としてリセット

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
        """【関数】意思決定ログへ記録：featuresと結論を1行で追加"""
        try:
            logger.info(f"decision record path={__file__} features_in={features}")
        except Exception:
            pass
        feats_win = getattr(getattr(self.cfg, 'features', None), 'ca_ratio_win_ms', 500)
        # ベースの特徴量をまず埋める
        feats = {
            'best_age_ms': self.ob.best_age_ms(now),
            'ca_ratio': self.ob.ca_ratio(now, window_ms=feats_win),
            'spread_tick': self.ob.spread_ticks(),
        }
        # 呼び出し元から渡された特徴量を上書き
        if isinstance(features, Mapping):
            for k, v in features.items():
                feats[k] = v
        # 戦略側の特徴量をマージ
        extra = self._consume_strategy_features()
        if isinstance(extra, Mapping):
            for key, value in extra.items():
                if key not in feats:
                    feats[key] = value

        if not actions:
            decision = 'none'
        else:
            places = [a for a in actions if a.get('type') == 'place']
            cancels = [a for a in actions if a.get('type') == 'cancel_tag']
            if places:
                sides = {p['order'].side for p in places if 'order' in p}
                decision = 'place_both' if sides == {'buy', 'sell'} else f"place_{list(sides)[0]}"
            elif cancels:
                decision = 'cancel'
            else:
                decision = 'none'

        _features = dict(feats or {})
        orders = [
            a.get('order')
            for a in (actions or [])
            if isinstance(a, dict) and a.get('type') == 'place' and a.get('order') is not None
        ]
        tags = sorted({
            str(getattr(o, 'tag', getattr(o, '_strategy', None)))
            for o in orders
            if getattr(o, 'tag', None) or getattr(o, '_strategy', None)
        })
        if tags:
            _features['tags'] = tags

        corr_hint = current_corr_ctx.get()
        if not corr_hint:
            corr_hint = next((getattr(o, '_corr_id', None) for o in orders if getattr(o, '_corr_id', None)), None)
        corr_id = corr_hint or uuid.uuid4().hex
        _features['corr_id'] = corr_id

        try:
            cfg_obj = getattr(self, 'cfg', None)
            risk_node = getattr(cfg_obj, 'risk', None) if cfg_obj is not None else None
            inv_node = getattr(risk_node, 'inv_capping', None) if risk_node is not None else None
            size_node = getattr(cfg_obj, 'size', None) if cfg_obj is not None else None

            max_inv = getattr(risk_node, 'max_inventory', None) if risk_node is not None else None
            inv_enabled = True
            target_ratio = 0.90
            eff_margin = 0.99
            try:
                from collections.abc import Mapping as _Mapping
                if inv_node is not None:
                    if isinstance(inv_node, _Mapping):
                        inv_enabled = bool(inv_node.get('enabled', True))
                        if inv_node.get('target_ratio') is not None:
                            target_ratio = float(inv_node.get('target_ratio'))
                        if inv_node.get('eff_limit_margin') is not None:
                            eff_margin = float(inv_node.get('eff_limit_margin'))
                    else:
                        inv_enabled = bool(getattr(inv_node, 'enabled', True))
                        tr = getattr(inv_node, 'target_ratio', None)
                        if isinstance(tr, (int, float)):
                            target_ratio = float(tr)
                        mg = getattr(inv_node, 'eff_limit_margin', None)
                        if isinstance(mg, (int, float)):
                            eff_margin = float(mg)
            except Exception:
                pass
            try:
                min_lot = float(getattr(size_node, 'min', None)) if size_node is not None else None
            except Exception:
                min_lot = None
            eff_limit = self.effective_inventory_limit()

            _features['inv_capping'] = {
                'enabled': bool(inv_enabled),
                'target_ratio': float(target_ratio) if isinstance(target_ratio, (int, float)) else target_ratio,
                'eff_limit_margin': float(eff_margin) if isinstance(eff_margin, (int, float)) else eff_margin,
                'max_inventory': float(max_inv) if isinstance(max_inv, (int, float)) else max_inv,
                'eff_limit': float(eff_limit) if eff_limit is not None else None,
                'min_lot': min_lot,
                'abs_q': abs(getattr(self, 'Q', 0.0)),
            }
        except Exception:
            pass

        decision_record = {
            'ts': self._ts_jst(now),
            'strategy': (current_strategy_ctx.get() or self.strat.name),
            'decision': decision,
            'features': _features,
            'expected_edge_bp': None,
            'eta_ms': None,
            'ca_ratio': feats.get('ca_ratio'),
            'best_age_ms': feats.get('best_age_ms'),
            'spread_state': ('zero' if feats.get('spread_tick') == 0 else 'ge1'),
        }
        decision_record.update({
            'expected_edge_bp': (
                feats.get('expected_edge_bp')
                or feats.get('mp_edge_bp')
                or (
                    (feats['microprice'] - feats['mid']) / feats['mid'] * 1e4
                    if 'microprice' in feats and 'mid' in feats and feats.get('mid')
                    else None
                )
            ),
            'eta_ms': (feats.get('queue_eta_ms') if feats.get('queue_eta_ms') is not None else feats.get('eta_ms')),  # 本命はqueue_eta_ms、互換としてeta_msにフォールバック
        })
        self.decision_log.add(**decision_record)


    def _consume_strategy_features(self) -> dict | None:
        getter = getattr(self.strat, "consume_decision_features", None)
        if callable(getter):
            extra = getter()
            if isinstance(extra, Mapping):
                return dict(extra)
        return None

    @_ar_non_reentrant  # 再入防止：同時に2回以上走らせない
    def _maybe_auto_reduce(self, now: float) -> None:
        """これは「inventory_guard/Caution/Halted 中は在庫を減らす向きに reduce-only+IOC を最小ロットで出す」関数です。"""
        # DEBUG: 何度呼ばれているか・どこで弾かれているかを軽量に可視化する
        log = getattr(self, "logger", logger)
        try:
            log.debug("auto_reduce: enter")
        except Exception:
            pass

        def _ar_dbg(reason: str) -> None:
            """DEBUG: auto_reduceの早期return理由を1行で記録する（数だけ見れば十分な最小情報）。"""
            try:
                log.debug(f"auto_reduce: skip {reason}")
            except Exception:
                pass

        cfg = getattr(self, "cfg", {}) or {}
        # cfg.risk.auto_reduce を「辞書でもオブジェクトでも」安全に取り出して辞書化する
        risk_obj = cfg.get("risk") if isinstance(cfg, dict) else getattr(cfg, "risk", None)
        auto_reduce_obj = (
            (risk_obj or {}).get("auto_reduce")
            if isinstance(risk_obj, dict)
            else getattr(risk_obj, "auto_reduce", None)
        )
        # Pydantic(BaseModel) や SimpleNamespace の場合も .model_dump() / vars() で辞書化する
        if isinstance(auto_reduce_obj, dict):
            ar = auto_reduce_obj
        elif hasattr(auto_reduce_obj, "model_dump"):
            try:
                ar = auto_reduce_obj.model_dump()
            except Exception:
                ar = {}
        elif auto_reduce_obj is not None:
            try:
                ar = vars(auto_reduce_obj)
            except Exception:
                ar = {}
        else:
            ar = {}
        # 設定から enabled / profit_only / cooldown_ms をドットパスで安全に取得する
        enabled = bool(_cfg_pick(cfg, "risk.auto_reduce.enabled", False))
        profit_only = bool(_cfg_pick(cfg, "risk.auto_reduce.profit_only", True))
        cooldown_ms = int(_cfg_pick(cfg, "risk.auto_reduce.cooldown_ms", 0))

        if not enabled:
            _ar_dbg(f"skip disabled (cfg resolved: enabled={enabled} profit_only={profit_only} cooldown_ms={cooldown_ms})")
            return

        _ar_dbg(f"cfg resolved: enabled={enabled} profit_only={profit_only} cooldown_ms={cooldown_ms}")
        try:
            override_ms = float(ar.get("board_stale_override_ms", 0) or 0.0)
        except Exception:
            override_ms = 0.0

        # クールダウン（連打防止）
        # 設定に cooldown_ms があればそれを尊重（<=0 ならクールダウン無し）
        cd_ms = int(cooldown_ms) if isinstance(cooldown_ms, (int, float)) else 0
        last_ts = getattr(self, "_ar_last_ts", 0.0) or 0.0
        if cd_ms > 0 and (now - last_ts) * 1000.0 < cd_ms:
            _ar_dbg("cooldown")
            return
        # 1秒あたりの発火回数を制限する
        try:
            recent = getattr(self, "_ar_recent", None)
            if recent is None:
                recent = deque(maxlen=32)
                self._ar_recent = recent
        except Exception:
            recent = deque(maxlen=32)
            self._ar_recent = recent
        now_ms = now * 1000.0
        try:
            while recent and (now_ms - recent[0]) > 1000.0:
                recent.popleft()
        except Exception:
            recent.clear()
        try:
            max_per_sec = float(_cfg_pick(cfg, "risk.auto_reduce.max_per_sec", 1.0))
        except Exception:
            max_per_sec = 1.0
        if max_per_sec > 0 and len(recent) >= max_per_sec:
            _ar_dbg("rate_limit")
            return
        # 分/時間あたりの発火回数・累積サイズリミット
        try:
            max_per_min = float(_cfg_pick(cfg, "risk.auto_reduce.max_triggers_per_min", 0) or 0)
        except Exception:
            max_per_min = 0.0
        try:
            max_qty_hour = float(_cfg_pick(cfg, "risk.auto_reduce.max_qty_per_hour", 0) or 0)
        except Exception:
            max_qty_hour = 0.0
        wall_ms = time.time() * 1000.0
        # minute bucket
        if self._ar_min_bucket_ms is None or (wall_ms - self._ar_min_bucket_ms) >= 60_000.0:
            self._ar_min_bucket_ms = wall_ms
            self._ar_min_count = 0
        # hour bucket
        if self._ar_hour_bucket_ms is None or (wall_ms - self._ar_hour_bucket_ms) >= 3_600_000.0:
            self._ar_hour_bucket_ms = wall_ms
            self._ar_hour_qty = 0.0
        if max_per_min > 0 and self._ar_min_count >= max_per_min:
            _ar_dbg("per_min_limit")
            return
        if max_qty_hour > 0 and self._ar_hour_qty >= max_qty_hour:
            _ar_dbg("hour_qty_limit")
            return

        # 在庫を読み、position が無ければ Q/A を使う
        def _as_float(val) -> float:
            try:
                return float(val)
            except Exception:
                return 0.0

        pos = getattr(self, "position", None)
        if pos is not None:
            q = _as_float(getattr(pos, "q", 0.0))
            a = _as_float(getattr(pos, "a", 0.0))
        else:
            q = _as_float(getattr(self, "Q", 0.0))
            a = _as_float(getattr(self, "A", 0.0))

        if q == 0.0:
            _ar_dbg("size_zero")
            return
        best_age_ms = None
        hb_gap_sec = None
        ob = getattr(self, "ob", None)
        last_rx_dt = getattr(ob, "_last_ts", None) if ob is not None else None
        if ob is not None:
            ba_attr = getattr(ob, "best_age_ms", None)
            if callable(ba_attr):
                try:
                    best_age_ms = float(ba_attr())
                except Exception:
                    best_age_ms = None
            elif isinstance(ba_attr, (int, float)):
                best_age_ms = float(ba_attr)
        if last_rx_dt is not None:
            try:
                hb_gap_sec = max(0.0, (datetime.now(timezone.utc) - last_rx_dt).total_seconds())
            except Exception:
                hb_gap_sec = None
        elif isinstance(getattr(self, "_last_heartbeat_ms", None), (int, float)):
            hb_gap_sec = max(0.0, (time.time() * 1000.0 - float(self._last_heartbeat_ms)) / 1000.0)
        feed_mode, feed_reason = _eval_feed_health(cfg, best_age_ms, hb_gap_sec)
        try:
            self._feed_mode = feed_mode
            self._last_feed_reason = feed_reason
        except Exception:
            pass
        if feed_mode in ("caution", "halted"):
            _ar_dbg(f"feed_unhealthy:{feed_mode}:{feed_reason}")
            return

        # target_eff 超過の判定（inv_capping と同等の考え方）
        # target_eff = target_ratio * eff_limit（eff_limit は effective_inventory_limit を採用）
        try:
            cfg_obj = getattr(self, "cfg", None)
            risk_node = getattr(cfg_obj, "risk", None) if cfg_obj is not None else None
            inv_node = getattr(risk_node, "inv_capping", None) if risk_node is not None else None
            tr = None
            if inv_node is not None:
                from collections.abc import Mapping as _Mapping
                if isinstance(inv_node, _Mapping):
                    tr = inv_node.get("target_ratio")
                else:
                    tr = getattr(inv_node, "target_ratio", None)
            target_ratio = float(tr) if isinstance(tr, (int, float)) else 0.90
        except Exception:
            target_ratio = 0.90
        eff_limit_val = self.effective_inventory_limit()
        try:
            eff_limit_f = float(eff_limit_val) if eff_limit_val is not None else 0.0
        except Exception:
            eff_limit_f = 0.0
        abs_q = abs(q)
        target_eff = (target_ratio * eff_limit_f) if eff_limit_f > 0.0 else 0.0
        over_eff = (abs_q > target_eff) if target_eff > 0.0 else False
        # target_eff を超えている間は、在庫縮小を優先させるために一時的に Close-Only と等価のフラグをON
        if over_eff and not getattr(self.risk, "inventory_guard_active", False):
            try:
                self.risk.inventory_guard_active = True
            except Exception:
                pass
        elif (not over_eff) and getattr(self.risk, "inventory_guard_active", False):
            # target_eff を下回っており、かつハード上限(=eff_limit)も未到達なら一時フラグを解除
            try:
                eff_lim_eff = float(self.effective_inventory_limit() or 0.0)
            except Exception:
                eff_lim_eff = 0.0
            hard_guard = (eff_lim_eff > 0.0 and abs_q >= eff_lim_eff)
            if not hard_guard:
                try:
                    self.risk.inventory_guard_active = False
                except Exception:
                    pass

        # guard/モード判定
        mode = str(getattr(self, "market_mode", getattr(self.risk, "market_mode", "healthy"))).lower()
        guard_on = bool(getattr(self.risk, "inventory_guard", False) or getattr(self.risk, "inventory_guard_active", False))
        if eff_limit_f > 0.0 and abs_q >= eff_limit_f:
            guard_on = True  # eff_limitを超えたら強制でガードON
        force_on_risk = bool(ar.get("force_on_risk", True))
        if not ((guard_on or over_eff) or (force_on_risk and mode in ("caution", "halted"))):  # over_eff時はHealthyでもreduce-onlyを許可して在庫を軽くする（market_mode_blockで止めない）
            _ar_dbg("market_mode_block")
            return  # 平常時は出さない

        side = "SELL" if q > 0.0 else "BUY"
        try:
            min_step_cfg = float(ar.get("min_step_qty", 0.001))
        except Exception:
            min_step_cfg = 0.001

        # --- auto_reduce の発注量を決める（在庫の絶対値を確実に減らすだけ） ---
        base_step = max(min_step_cfg, 0.0)
        qty_step = float(getattr(self, "qty_step", base_step or 0.001)) if base_step > 0.0 else float(getattr(self, "qty_step", 0.001))
        if qty_step <= 0.0:
            qty_step = 0.001
        try:
            max_step_cfg = ar.get("max_step_qty")
            max_step_cfg = float(max_step_cfg) if max_step_cfg is not None else None
        except Exception:
            max_step_cfg = None
        try:
            limit_ratio = float(ar.get("max_step_ratio_of_limit", 1.0))
            if limit_ratio <= 0.0:
                limit_ratio = 1.0
        except Exception:
            limit_ratio = 1.0
        caps = [abs_q, qty_step]
        if eff_limit_f > 0.0:
            caps.append(eff_limit_f * limit_ratio)
        if max_step_cfg is not None and max_step_cfg > 0.0:
            caps.append(max_step_cfg)
        step_cap = min(c for c in caps if isinstance(c, (int, float)) and c > 0.0)
        dust_eps = float(getattr(self, "dust_eps", qty_step * 0.1))
        if abs_q <= dust_eps:
            step = abs_q
        else:
            step = max(0.0, min(step_cap, abs_q))
            if step < dust_eps:
                _ar_dbg("size_zero")
                return
        # Guard中は利益条件を自動無効化（必ず薄めを試す）。監査用にフラグ保持。
        profit_overridden = False
        if guard_on and profit_only:
            profit_only = False
            profit_overridden = True

        # 板からmidを概算しておく
        ob = getattr(self, "orderbook", None) or getattr(self, "ob", None)
        stale_age_ms = None
        if profit_only and guard_on and override_ms > 0 and ob is not None:
            try:
                age_fn = getattr(ob, "best_age_ms", None)
                raw_age = age_fn() if callable(age_fn) else None
                if raw_age is None:
                    raw_age = getattr(ob, "_best_age_ms", None)
                if raw_age is not None:
                    stale_age_ms = float(raw_age)
            except Exception:
                stale_age_ms = None
            if stale_age_ms is not None and stale_age_ms >= override_ms:
                profit_only = False
                self.logger.debug(
                    "auto_reduce: profit_only overridden due to stale board (age_ms=%s >= %s)",
                    stale_age_ms,
                    override_ms,
                )

        def _best_px(book, attr: str):
            node = getattr(book, attr, None) if book is not None else None
            px = getattr(node, "price", None)
            try:
                val = float(px)
            except Exception:
                return None
            return val if val > 0 else None

        bid_px = _best_px(ob, "best_bid")
        ask_px = _best_px(ob, "best_ask")
        mid = 0.0
        if bid_px is not None and ask_px is not None:
            mid = (bid_px + ask_px) / 2.0
        elif bid_px is not None:
            mid = bid_px
        elif ask_px is not None:
            mid = ask_px

        # profit_only の簡易判定
        if profit_only:
            if mid <= 0.0 or a <= 0.0:
                _ar_dbg("board_not_suitable")
                return
            if (q > 0.0 and mid < a) or (q < 0.0 and mid > a):
                _ar_dbg("profit_only_gate")
                return

        tif = str(ar.get("tif", "IOC")).upper()
        px_for_sim = None
        if side == "SELL":
            px_for_sim = bid_px or ask_px or (mid if mid > 0 else None) or (a if a > 0 else None)
        else:
            px_for_sim = ask_px or bid_px or (mid if mid > 0 else None) or (a if a > 0 else None)

        # 発火理由を明示（Guard中か／profit_onlyの自動解除有無）
        try:
            self.logger.debug(
                "auto_reduce: fire reason=%s (profit_only_overridden=%s)",
                ("inventory_guard" if guard_on else "normal"),
                profit_overridden,
            )
        except Exception:
            pass

        try:
            ex = getattr(self, "exchange", None)
            if hasattr(ex, "sendchildorder"):
                self.logger.info(f"auto_reduce: try place (REST) side={side} qty={step:.6f} tif={tif} tag=auto_reduce")
                ex.sendchildorder(
                    side=side,
                    size=step,
                    price=None,
                    time_in_force=tif,
                    reduce_only=True,
                    tag="auto_reduce",
                )
            elif hasattr(self, "_place"):
                self.logger.info(f"auto_reduce: try place (engine) side={side} qty={step:.6f} tif={tif} tag=auto_reduce")
                self._place(
                    side=side,
                    qty=step,
                    price=None,
                    tif=tif,
                    reduce_only=True,
                    tag="auto_reduce",
                )
            elif hasattr(getattr(self, "sim", None), "place") and px_for_sim is not None:
                order = Order(
                    side=side.lower(),
                    price=float(px_for_sim),
                    size=step,
                    tif=tif,
                    ttl_ms=None,
                    tag="auto_reduce",
                )
                setattr(order, "reduce_only", True)
                ts_dt = datetime.now(timezone.utc)
                self.logger.info(f"auto_reduce: try place (sim) side={side} qty={step:.6f} tif={tif} tag=auto_reduce")
                self.sim.place(order, ts_dt)
                self.order_log.add(
                    ts=self._ts_jst(ts_dt),
                    action="place",
                    tif=order.tif,
                    ttl_ms=order.ttl_ms,
                    px=order.price,
                    sz=order.size,
                    reason=order.tag,
                )
                self._heartbeat(ts_dt, "place", reason=order.tag)
            else:
                self.logger.info(f"auto_reduce: skip (no place path) side={side} qty={step:.6f}")
                return
            self._ar_last_ts = now  # 発火時刻を更新
            try:
                self._ar_min_count += 1
            except Exception:
                pass
            try:
                self._ar_hour_qty += float(step)
            except Exception:
                pass
            try:
                self._ar_recent.append(now_ms)
            except Exception:
                pass
        except Exception as e:
            self.logger.warning(f"auto_reduce: place failed: {e}")
    def _heartbeat(self, now: datetime, event: str, reason: str | None = None) -> None:
        """【関数】ハートビート：Q/A/R・日次R・各ガード/窓の状態を1行JSONで追記する"""
        eff_limit = self.effective_inventory_limit()
        inventory_guard = eff_limit is not None and abs(self.Q) >= float(eff_limit)
        payload = {
            "ts": self._ts_jst(now),
            "event": event,           # "place" / "fill" / "pause" など直近イベント
            "reason": reason,         # "inventory_guard" / "midmove_guard" / "maintenance" / "funding" など
            "strategy": getattr(self.strat, "strategy_name", None) or self.strat.name,
            "Q": self.Q,              # 現在在庫（+ロング/−ショート）
            "A": self.A,              # 平均建値
            "R": self.R,              # 累計実現PnL
            "R_day": self._daily_R,   # 日次実現PnL
            "guard": {                # ガードのON/OFF（Trueで“新規停止中”）
                "inventory": inventory_guard,
                "midmove": self._midguard_paused,
            },
            "window": {
                "maint": self._in_maintenance(now),
                "funding": self._in_funding_calc(now) or self._in_funding_transfer(now),
            },
        }
        child_names = ([
            getattr(child, "strategy_name", None) or getattr(child, "name", "unknown")
            for child in getattr(self.strat, "children", [])
        ] or [payload["strategy"]])
        lines = []
        for child_name in child_names:
            entry = dict(payload)
            entry["strategy"] = child_name
            lines.append(orjson.dumps(entry).decode("utf-8"))
        if not lines:
            return
        # JST日付タグ(YYYYMMDD)で心拍ファイル名を自動決定
        try:
            jst = timezone(timedelta(hours=9))
            tag = now.astimezone(jst).strftime("%Y%m%d")
            hb_path = Path(f"logs/runtime/{tag}heartbeat.ndjson")
        except Exception:
            hb_path = self._hb_path
        hb_path.parent.mkdir(parents=True, exist_ok=True)
        with hb_path.open("a", encoding="utf-8") as fh:
            for line in lines:
                fh.write(line + "\n")
        # HBごとにreduce-onlyを1回だけ試行（板が止まっても在庫解放のチャンスを作る）
        self._maybe_auto_reduce(now=monotonic())

    # ─────────────────────────────────────────────────────────────
    def _apply_fill_and_log(
        self,
        ts_iso: str,
        side: str,
        px: float,
        sz: float,
        tag: str,
        *,
        order=None,
    ) -> None:
        """【関数】Fillを在庫Q/A/Rに適用し、orders/tradesへ記録（最小PnL）"""
        # 1) orders：fill行
        reason_tag = tag if tag is not None else "-"
        order_tag = getattr(order, "tag", getattr(order, "_strategy", "-"))
        corr_for_log = _coid_to_corr.get(
            getattr(order, "client_order_id", "") if order is not None else "",
            (getattr(order, "_corr_id", None) if order is not None else None)
            or current_corr_ctx.get()
            or "-",
        )
        if (
            order is not None
            and getattr(order, "client_order_id", None)
            and getattr(order, "_corr_id", None)
        ):
            _coid_to_corr[order.client_order_id] = order._corr_id
            if len(_coid_to_corr) > _CORR_MAP_MAX:
                _coid_to_corr.popitem(last=False)
        self.order_log.add(
            ts=ts_iso,
            action="fill",
            tif="GTC",
            ttl_ms=None,
            px=px,
            sz=sz,
            reason=f"{reason_tag}; tag={order_tag}; corr={corr_for_log}",
        )
        try:
            self._last_fill_ms = time.time() * 1000.0
        except Exception:
            pass
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
        order_strategy = None
        if order is not None:
            order_strategy = getattr(order, "_strategy", None)
        strategy_name = (
            current_strategy_ctx.get()
            or order_strategy
            or getattr(self.strat, "strategy_name", None)
            or self.strat.name
        )

        corr_for_trade = corr_for_log if corr_for_log != "-" else _coid_to_corr.get(
            getattr(order, "client_order_id", "") if order is not None else "",
            current_corr_ctx.get() or "-",
        )
        tag_payload = "" if tag is None else str(tag)
        if corr_for_trade and corr_for_trade != "-":
            if tag_payload:
                if f"corr:{corr_for_trade}" not in tag_payload:
                    tag_payload = f"{tag_payload}|corr:{corr_for_trade}"
            else:
                tag_payload = f"corr:{corr_for_trade}"
        final_tag = tag_payload or ("" if tag is None else str(tag))
        self.trade_log.add(
            ts=ts_iso, side=side, px=px, sz=sz, pnl=realized,
            strategy=strategy_name, tag=final_tag,
            inventory_after=self.Q,
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
        logger.info(
            f"paper start: product={self.product} strategy={self.strat.name} strategies={self.strategies}"
        )
        # JST日付タグでNDJSONログ名を自動付与
        _date_tag = _now_utc().astimezone(timezone(timedelta(hours=9))).strftime("%Y%m%d")
        self._date_tag = _date_tag
        # Heartbeatファイル
        self._hb_path = Path(f"logs/runtime/{_date_tag}heartbeat.ndjson")
        self._hb_path.parent.mkdir(parents=True, exist_ok=True)
        # NDJSONミラー（日付付き）
        try:
            if getattr(self, "order_log", None) is not None and getattr(self.order_log, "_mirror", None) is not None:
                self.order_log._mirror = Path(f"logs/orders/{_date_tag}order_log.ndjson")
                self.order_log._mirror.parent.mkdir(parents=True, exist_ok=True)
            if getattr(self, "trade_log", None) is not None and getattr(self.trade_log, "_mirror", None) is not None:
                self.trade_log._mirror = Path(f"logs/trades/{_date_tag}trade_log.ndjson")
                self.trade_log._mirror.parent.mkdir(parents=True, exist_ok=True)
            if getattr(self, "decision_log", None) is not None and getattr(self.decision_log, "_mirror", None) is not None:
                self.decision_log._mirror = Path(f"logs/analytics/{_date_tag}decision_log.ndjson")
                self.decision_log._mirror.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        paper_meta_path = Path(f"logs/runtime/{_date_tag}paper_start.ndjson")
        paper_meta_path.parent.mkdir(parents=True, exist_ok=True)
        features_obj = getattr(self.cfg, "features", None)
        if features_obj is None:
            _features_common = {}
        elif isinstance(features_obj, Mapping):
            _features_common = dict(features_obj)
        elif hasattr(features_obj, "model_dump"):
            _features_common = features_obj.model_dump()
        else:
            try:
                _features_common = dict(vars(features_obj))
            except TypeError:
                _features_common = {}
        features_overrides = {}
        if features_obj is not None:
            for name in self.strategies:
                if isinstance(features_obj, Mapping):
                    override_obj = features_obj.get(name)
                else:
                    override_obj = getattr(features_obj, name, None)
                if override_obj is None:
                    continue
                if isinstance(override_obj, Mapping):
                    override_payload = dict(override_obj)
                elif hasattr(override_obj, "model_dump"):
                    override_payload = override_obj.model_dump()
                else:
                    try:
                        override_payload = dict(vars(override_obj))
                    except TypeError:
                        continue
                if isinstance(_features_common, dict):
                    _features_common.pop(name, None)
                features_overrides[name] = override_payload
        try:
            with paper_meta_path.open("a", encoding="utf-8") as fh:
                fh.write(
                    orjson.dumps(
                        {
                            "ts": self._ts_jst(_now_utc()),
                            "event": "start",
                            "mode": "paper",
                            "product": self.product,
                            "strategy": getattr(self.strat, "strategy_name", None) or self.strat.name,
                            "strategies": list(self.strategies),
                            "features_common": _features_common,
                            "features_overrides": features_overrides,
                        }
                    ).decode("utf-8")
                    + "\n"
                )
        except Exception:
            logger.exception("paper start ndjson write failed")
        try:
            async for ev in event_stream(product_code=self.product):
                now = _parse_iso(ev["ts"])
                ch = ev.get("channel", "")

                if ch.startswith("lightning_board_"):
                    # ローカル板更新
                    self._last_heartbeat_ms = int(now.timestamp() * 1000)  # 何をするか：最新board受信時刻を記録しHB間隔を測る
                    # 日次境界（JST）を跨いだら R_day/HWM をリセット
                    self._roll_daily(now)

                    # Kill-Switch 判定（trueで全キャンセル→停止）
                    hit, why, rday, dd = self._maybe_trigger_kill()
                    if hit:
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"kill; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"kill; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                        self.halted = True
                        logger.error(f"Kill-Switch({why}) fired: R_day={rday:.2f}, DD={dd:.2f} → stopping")
                        return  # 安全停止（finallyでログflush）  # 文書の“Kill到達で停止”に準拠

                    self.ob.update_from_event(ev)
                    # 板更新ごとに target_eff(= eff_limit * target_ratio) 超なら reduce-only の自動解放を試行
                    try:
                        inv_node = getattr(getattr(self.cfg, "risk", None), "inv_capping", None)
                        tr = None
                        if inv_node is not None:
                            from collections.abc import Mapping as _Mapping
                            tr = inv_node.get("target_ratio") if isinstance(inv_node, _Mapping) else getattr(inv_node, "target_ratio", None)
                        target_ratio = float(tr) if isinstance(tr, (int, float)) else 0.90
                    except Exception:
                        target_ratio = 0.90
                    _eff_lim = self.effective_inventory_limit()
                    _over_eff = (_eff_lim is not None and _eff_lim > 0.0 and abs(self.Q) > target_ratio * float(_eff_lim))
                    # 在庫が“目標有効上限”を超えたら、決済だけの自動縮小（reduce-only）を必ず試す
                    if _over_eff:
                        self._maybe_auto_reduce(now=monotonic())
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
                        # Guard中でも target_eff 超過なら在庫解放（reduce-only）を試行
                        try:
                            inv_node = getattr(getattr(self.cfg, "risk", None), "inv_capping", None)
                            tr = None
                            if inv_node is not None:
                                from collections.abc import Mapping as _Mapping
                                tr = inv_node.get("target_ratio") if isinstance(inv_node, _Mapping) else getattr(inv_node, "target_ratio", None)
                            target_ratio = float(tr) if isinstance(tr, (int, float)) else 0.90
                        except Exception:
                            target_ratio = 0.90
                        _eff_lim = self.effective_inventory_limit()
                        _over_eff = (_eff_lim is not None and _eff_lim > 0.0 and abs(self.Q) > target_ratio * float(_eff_lim))
                        if _over_eff:
                            self._maybe_auto_reduce(now=monotonic())
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"window; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"window; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                            self._heartbeat(now, "pause", reason="maintenance")
                        continue  # このboardイベントでは新規Placeを行わない
                    
                    # Funding窓（計算 or 授受）：新規禁止＋同タグ一括Cancel（reason="funding"）
                    if self._in_funding_calc(now) or self._in_funding_transfer(now):
                        # Guard中でも target_eff 超過なら在庫解放（reduce-only）を試行
                        try:
                            inv_node = getattr(getattr(self.cfg, "risk", None), "inv_capping", None)
                            tr = None
                            if inv_node is not None:
                                from collections.abc import Mapping as _Mapping
                                tr = inv_node.get("target_ratio") if isinstance(inv_node, _Mapping) else getattr(inv_node, "target_ratio", None)
                            target_ratio = float(tr) if isinstance(tr, (int, float)) else 0.90
                        except Exception:
                            target_ratio = 0.90
                        _eff_lim = self.effective_inventory_limit()
                        _over_eff = (_eff_lim is not None and _eff_lim > 0.0 and abs(self.Q) > target_ratio * float(_eff_lim))
                        if _over_eff:
                            self._maybe_auto_reduce(now=monotonic())
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"funding; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )  # 【関数】Funding窓で停止
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"funding; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )  # 【関数】Funding窓で停止
                            self._heartbeat(now, "pause", reason="funding")
                        continue  # このboardイベントでは新規Placeを行わない

                    # TTL失効を処理（取消ログ）
                    for o in self.sim.on_time(now):
                        self.order_log.add(
                            ts=self._ts_jst(now),
                            action="cancel",
                            tif=o.tif,
                            ttl_ms=o.ttl_ms,
                            px=o.price,
                            sz=o.remaining,
                            reason=(
                                f"ttl; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                            ),
                        )

                    # ガード（速すぎるときは新規停止＋全取消）
                    paused = self._guard_midmove_bp(now)
                    self._midguard_paused = paused  # 直近のミッド移動ガード状態を保持（ハートビートに載せる）

                    if paused:
                        # Guard中でも target_eff 超過なら在庫解放（reduce-only）を試行
                        try:
                            inv_node = getattr(getattr(self.cfg, "risk", None), "inv_capping", None)
                            tr = None
                            if inv_node is not None:
                                from collections.abc import Mapping as _Mapping
                                tr = inv_node.get("target_ratio") if isinstance(inv_node, _Mapping) else getattr(inv_node, "target_ratio", None)
                            target_ratio = float(tr) if isinstance(tr, (int, float)) else 0.90
                        except Exception:
                            target_ratio = 0.90
                        _eff_lim = self.effective_inventory_limit()
                        _over_eff = (_eff_lim is not None and _eff_lim > 0.0 and abs(self.Q) > target_ratio * float(_eff_lim))
                        if _over_eff:
                            self._maybe_auto_reduce(now=monotonic())
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"guard; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"guard; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                            self._heartbeat(now, "pause", reason="midmove_guard")  # 直近イベントを要約（ミッド変化ガードで停止）
                        continue  # 新規は出さない
                    

                    # 在庫ガード: 警告/縮小/停止の段階制を適用
                    eff_limit = self.effective_inventory_limit()
                    close_only_mode = False
                    inv_state = "healthy"
                    inv_limits = {}
                    shrink_factor = 1.0
                    inv_cfg = getattr(getattr(self.cfg, "risk", None), "inventory_guard", None)

                    def _inv_val(key, default=None):
                        val = None
                        if isinstance(inv_cfg, Mapping):
                            val = inv_cfg.get(key)
                        elif inv_cfg is not None:
                            val = getattr(inv_cfg, key, None)
                        try:
                            return float(val) if val is not None else default
                        except Exception:
                            return default

                    warning_lim = _inv_val("warning", eff_limit * 0.7 if eff_limit is not None else None)
                    caution_lim = _inv_val("caution", eff_limit)
                    hard_lim = _inv_val("hard", (eff_limit * 1.5) if eff_limit is not None else None)
                    shrink_factor = _inv_val("shrink_factor", 0.5) or 0.5
                    if shrink_factor <= 0 or shrink_factor >= 1:
                        shrink_factor = 0.5

                    abs_q = abs(self.Q)
                    if hard_lim is not None and abs_q >= hard_lim:
                        inv_state = "hard"
                    elif caution_lim is not None and abs_q >= caution_lim:
                        inv_state = "caution"
                    elif warning_lim is not None and abs_q >= warning_lim:
                        inv_state = "warning"

                    inv_limits = {"warning": warning_lim, "caution": caution_lim, "hard": hard_lim}
                    if inv_state != getattr(self, "_inv_guard_state", "healthy"):
                        logger.info(f"inventory_guard: state_change {getattr(self, '_inv_guard_state', 'na')}->{inv_state} |Q|={abs_q:.6f} limits={inv_limits}")
                    self._inv_guard_state = inv_state

                    if inv_state == "hard":
                        close_only_mode = True
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"risk; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(
                                ts=self._ts_jst(now),
                                action="cancel",
                                tif=o.tif,
                                ttl_ms=o.ttl_ms,
                                px=o.price,
                                sz=o.remaining,
                                reason=(
                                    f"risk; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                    f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                                ),
                            )
                        logger.warning(f"risk guard: |Q|>={hard_lim} -> close-only (inventory hard guard)")
                        self._heartbeat(now, "pause", reason="inventory_guard")

                    now_ms_gate = int(now.timestamp() * 1000)
                    if inv_state in ("caution", "hard"):
                        mode = "halted" if inv_state == "hard" else "caution"
                        try:
                            self._last_gate_status = {"mode": mode, "reason": f"inventory_{inv_state}", "limits": inv_limits, "ts_ms": now_ms_gate}
                        except Exception:
                            pass

                    if hasattr(self, "risk"):
                        self.risk.inventory_guard_active = inv_state in ("caution", "hard")
                    if close_only_mode:
                        self._maybe_auto_reduce(now=monotonic())

                    actions = self.strat.evaluate(self.ob, now, self.cfg)
                    try:
                        logger.debug(f"engine.debug_actions: total_actions={len(actions) if actions is not None else 0}")
                    except Exception:
                        pass
                    ob = self.ob
                    queue_eta_ms = self._calc_queue_eta_ms(ob=ob, now=now)  # 何をするか：このサイクルのQueueETA(ms)を算出
                    setattr(ob, "queue_eta_ms", queue_eta_ms)  # 何をするか：strategy側（stall_then_strike等）が参照できるように板へ載せる
                    feats_win = getattr(getattr(self.cfg, "features", None), "ca_ratio_win_ms", 500)  # 何をする行か：CA比率集計窓(ms)を取得
                    features = {
                        "best_age_ms": self.ob.best_age_ms(now),  # 何をする行か：Best静止時間(ms)を記録
                        "ca_ratio": self.ob.ca_ratio(now, window_ms=feats_win),  # 何をする行か：C/A比率を記録
                        "spread_tick": self.ob.spread_ticks(),  # 何をする行か：現在スプレッド(tick)を記録
                        "queue_eta_ms": queue_eta_ms,  # 何をするか：decision_log/analyticsにETA(ms)を残す
                    }
                    side = "BUY"
                    price = getattr(ob, "bid", None)
                    qty = float(getattr(ob, "dust", None) or 0.001)
                    level_size = getattr(ob, "bid_size", None)
                    qeta_dbg = self._queue_eta.debug_estimate(
                        order_side=side,          # 何をするか：このETAを見たい注文side（既存の変数を使う）
                        price=price,              # 何をするか：このETAを見たい指値価格（既存の変数を使う）
                        qty=qty,                  # 何をするか：このETAを見たい注文サイズ（既存の変数を使う）
                        level_size=level_size,    # 何をするか：その価格レベルの板厚み（持っていれば）
                        ts=now,                   # 何をするか：QueueETAがexpireに使う時刻（datetime/数値で渡す）
                        price_first=False,        # 何をするか：現状の見積もり設定と同じに合わせる
                    )

                    features["queue_eta_dbg_samples"] = qeta_dbg["samples"]                 # 何をするか：サンプルが溜まっているか確認
                    features["queue_eta_dbg_hit_side"] = qeta_dbg["hit_side"]               # 何をするか：どっちの約定流量で見積もっているか確認
                    qeta_last = getattr(self, "_queue_eta_last", None) or {}
                    level_size = qeta_last.get("ahead_qty")
                    features["queue_eta_dbg_ahead_qty"] = float(level_size or 0.0)  # 何をするか：QueueETA.estimate_eta_secへ渡す「板の行列量(level_size)」と同じ値をdebugに残し、ahead=0張り付きを防ぐ
                    features["queue_eta_dbg_needed_qty"] = qeta_dbg["needed_qty"]           # 何をするか：必要流量（ahead+qty）が巨大でないか確認
                    features["queue_eta_dbg_rate_price_per_sec"] = qeta_dbg["rate_price_per_sec"]  # 何をするか：価格別レートが取れているか確認
                    features["queue_eta_dbg_rate_side_per_sec"] = qeta_dbg["rate_side_per_sec"]    # 何をするか：片側合計レートが取れているか確認
                    features["queue_eta_dbg_rate_used_per_sec"] = qeta_dbg["rate_used_per_sec"]    # 何をするか：実際に採用したレートが0扱いでないか確認
                    features["queue_eta_dbg_is_inf"] = qeta_dbg["is_inf"]                   # 何をするか：inf→キャップの発生有無を確認
                    features["queue_eta_dbg_raw_ms"] = qeta_dbg["raw_ms"]                   # 何をするか：キャップ前の生ETA(ms)を確認（infならNone）
                    qty = qeta_last.get("qty")
                    ahead_qty = qeta_last.get("ahead_qty")
                    eta_sec = qeta_last.get("eta_sec")
                    if qty is not None and ahead_qty is not None and eta_sec is not None:
                        # 何をするか：QueueETA が“常に天井(10,000,000ms)”になる原因（qtyの単位ミス or eta=inf）を切り分けるため、入力と生ETAを特徴量へ同梱する
                        features["queue_eta_dbg_qty_in"] = float(qty)
                        features["queue_eta_dbg_needed_qty"] = float(ahead_qty) + float(qty)

                        qe_is_inf = (eta_sec == float("inf")) or (eta_sec != eta_sec)  # 何をするか：inf / NaN を安全に検出して orjson で壊れない形にする
                        features["queue_eta_dbg_eta_sec_raw"] = None if qe_is_inf else float(eta_sec)  # 何をするか：生のETA(秒)を残す（inf/NaNはNone）
                        features["queue_eta_dbg_eta_ms_raw"] = None if qe_is_inf else float(eta_sec) * 1000.0  # 何をするか：生のETA(ms)を残す（inf/NaNはNone）
                    self._record_decision(now, actions, features=features)
                    try:
                        cfg_size = getattr(self.cfg, 'size', None)
                        sz_min_val = cfg_size.get('min') if isinstance(cfg_size, Mapping) else getattr(cfg_size, 'min', None)
                        size_min_guard = float(sz_min_val) if sz_min_val is not None else 0.001
                    except Exception:
                        size_min_guard = 0.001
                    for act in actions:
                        corr_from_action = act.get("_corr_id") if isinstance(act, dict) else None
                        corr_token = None
                        if corr_from_action is not None:
                            corr_token = current_corr_ctx.set(corr_from_action)
                        try:
                            if act.get("type") == "place":
                                allow_multi = bool(act.get("allow_multiple"))
                                # 同タグの重複を最小抑止 (allow_multiple=True なら使う)
                                if self.sim.has_open_tag(act["order"].tag) and not allow_multi:
                                    continue
                                o = act["order"]
                                reduce_only = bool(getattr(o, 'reduce_only', False) or getattr(o, 'close_only', False))
                                if close_only_mode and not reduce_only:
                                    continue
                                if inv_state == 'caution' and not reduce_only:
                                    try:
                                        orig_sz = getattr(o, 'size', getattr(o, 'sz', None))
                                        if orig_sz is not None:
                                            new_sz = max(size_min_guard, float(orig_sz) * shrink_factor)
                                            if new_sz < float(orig_sz) - 1e-12:
                                                logger.warning(f"inventory_guard:shrink size={orig_sz}->{new_sz} state=caution")
                                                if hasattr(o, 'size'):
                                                    o.size = new_sz
                                                if hasattr(o, 'sz'):
                                                    o.sz = new_sz
                                    except Exception:
                                        pass
                                corr_value = (
                                    corr_from_action
                                    or getattr(o, "_corr_id", None)
                                    or current_corr_ctx.get()
                                )
                                if corr_token is None and corr_value is not None:
                                    corr_token = current_corr_ctx.set(corr_value)
                                if corr_value is not None:
                                    try:
                                        setattr(o, "_corr_id", corr_value)
                                    except Exception:
                                        pass
                                if not getattr(o, "client_order_id", None):
                                    o.client_order_id = f"BFM-{uuid.uuid4().hex[:12]}"
                                if corr_value:
                                    _coid_to_corr[o.client_order_id] = corr_value
                                    if len(_coid_to_corr) > _CORR_MAP_MAX:
                                        _coid_to_corr.popitem(last=False)
                                    tag_current = getattr(o, "tag", "")
                                    tag_str = str(tag_current) if tag_current is not None else ""
                                    if not tag_str:
                                        tag_str = f"corr:{corr_value}"
                                    elif f"corr:{corr_value}" not in tag_str:
                                        tag_str = f"{tag_str}|corr:{corr_value}"
                                    try:
                                        o.tag = tag_str
                                    except Exception:
                                        pass
                                age = None  # 何をするか：best_age_msをfeatures/decisionから拾う準備
                                if isinstance(locals().get("features"), dict):  # 何をするか：戦略特徴量が存在する場合
                                    age = features.get("best_age_ms")
                                elif isinstance(locals().get("decision"), dict):  # 何をするか：決定ペイロードに含まれる場合
                                    age = locals().get("decision", {}).get("best_age_ms")  # 未定義NameErrorを避ける
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
                                # 在庫ガードの前に、安全側ロットへ切り下げる（Reduce-Only は素通し）
                                try:
                                    inv_node = None
                                    risk_node = getattr(self.cfg, "risk", None)
                                    if risk_node is not None:
                                        inv_node = getattr(risk_node, "inv_capping", None)
                                        if inv_node is None:
                                            extra = getattr(risk_node, "model_extra", None)
                                            if isinstance(extra, dict):
                                                inv_node = extra.get("inv_capping")
                                    # フォールバック：features.inv_capping も参照し、どちらから読んだかを記録
                                    cfg_path = "risk.inv_capping"
                                    try:
                                        feat_node = getattr(self.cfg, "features", None)
                                        inv_feat = None
                                        if feat_node is not None:
                                            inv_feat = getattr(feat_node, "inv_capping", None)
                                            if inv_feat is None:
                                                extra_f = getattr(feat_node, "model_extra", None)
                                                if isinstance(extra_f, dict):
                                                    inv_feat = extra_f.get("inv_capping")
                                        if inv_node is None and inv_feat is not None:
                                            inv_node = inv_feat
                                            cfg_path = "features.inv_capping"
                                        elif inv_node is None:
                                            cfg_path = "undefined"
                                    except Exception:
                                        cfg_path = "undefined"
                                    # inv_capping.enabled は dict/Pydantic 両対応。未設定や欠落時は True（既定で有効）。
                                    inv_enabled = True
                                    try:
                                        from collections.abc import Mapping as _Mapping
                                        if inv_node is not None:
                                            if isinstance(inv_node, _Mapping):
                                                inv_enabled = bool(inv_node.get("enabled", True))
                                            else:
                                                inv_enabled = bool(getattr(inv_node, "enabled", True))
                                    except Exception:
                                        inv_enabled = True
                                    # 在庫を減らす向き（reduce-only 相当）かを判定し、フラグを明示
                                    is_reduce = bool(reduce_only or self.would_reduce_inventory(self.Q, side_val, req_qty))
                                    try:
                                        setattr(o, "reduce_only", getattr(o, "reduce_only", False) or is_reduce)
                                    except Exception:
                                        pass
                                    if not reduce_only and inv_enabled:
                                        try:
                                            max_inv = float(getattr(risk_node, "max_inventory", None)) if risk_node is not None else None
                                        except Exception:
                                            max_inv = None
                                        try:
                                            margin = float(getattr(inv_node, "eff_limit_margin", 0.99)) if inv_node is not None else 0.99
                                        except Exception:
                                            margin = 0.99
                                        eff_for_cap = (max_inv * margin) if (max_inv is not None) else eff_limit
                                        if eff_for_cap is not None and eff_for_cap > 0:
                                            abs_q = abs(self.Q)
                                            r_pre = (abs_q + req_qty) / float(eff_for_cap)
                                            size_node = getattr(self.cfg, "size", None)
                                            try:
                                                min_lot = float(getattr(size_node, "min", 0.001)) if size_node is not None else 0.001
                                            except Exception:
                                                min_lot = 0.001
                                            try:
                                                tgt = float(getattr(inv_node, "target_ratio", 0.90)) if inv_node is not None else 0.90
                                            except Exception:
                                                tgt = 0.90
                                            # 在庫前処理は core に委譲（ログも一元化）
                                            safe_size, should_skip = inv_capping_preflight(
                                                req_raw=req_qty,
                                                abs_q=abs_q,
                                                eff_limit=float(eff_for_cap),
                                                min_lot=min_lot,
                                                target_ratio=tgt,
                                                cfg_path=cfg_path,
                                                reduces_position=is_reduce,
                                            )
                                            if should_skip and not is_reduce:
                                                self._heartbeat(now, "pause", reason="inventory_guard")
                                                continue
                                            # 実際に縮小が発生した場合のみサイズを更新
                                            # 余地があれば安全側サイズで上書きし、どれだけ縮めたかを記録
                                            # 在庫を減らす注文はそのまま通し、増える方向だけheadroomに合わせて縮める
                                            if (safe_size < (req_qty - 1e-12)) and (not is_reduce):
                                                try:
                                                    o.size = safe_size
                                                except Exception:
                                                    pass
                                                req_qty = safe_size
                                except Exception:
                                    pass
                                if eff_limit is not None:
                                            # 在庫ガード直前の“在庫連動ロット”前処理：
                                            # 余地ゼロなら固定文言でスキップし、SKIP_CAP を必ず計上
                                            risk_node = getattr(self.cfg, "risk", None)
                                            inv_node = None
                                            if risk_node is not None:
                                                inv_node = getattr(risk_node, "inv_capping", None)
                                                if inv_node is None:
                                                    extra = getattr(risk_node, "model_extra", None)
                                                    if isinstance(extra, dict):
                                                        inv_node = extra.get("inv_capping")

                                            # enabled/ratio/margin は既定値（enabled=True, ratio=0.90, margin=0.99）で防御
                                            inv_enabled = True
                                            target_ratio = 0.90
                                            eff_margin = 0.99
                                            try:
                                                from collections.abc import Mapping as _Mapping
                                                if inv_node is not None:
                                                    if isinstance(inv_node, _Mapping):
                                                        inv_enabled = bool(inv_node.get("enabled", True))
                                                        if inv_node.get("target_ratio") is not None:
                                                            target_ratio = float(inv_node.get("target_ratio"))
                                                        if inv_node.get("eff_limit_margin") is not None:
                                                            eff_margin = float(inv_node.get("eff_limit_margin"))
                                                    else:
                                                        inv_enabled = bool(getattr(inv_node, "enabled", True))
                                                        tr = getattr(inv_node, "target_ratio", None)
                                                        if isinstance(tr, (int, float)):
                                                            target_ratio = float(tr)
                                                        mg = getattr(inv_node, "eff_limit_margin", None)
                                                        if isinstance(mg, (int, float)):
                                                            eff_margin = float(mg)
                                            except Exception:
                                                pass

                                            # 参照元の可視化（DEBUG）：どの設定から読み取ったか
                                            try:
                                                logger.debug(
                                                    "inv_capping.cfg_path={} enabled={} target_ratio={:.2f} eff_limit_margin={:.2f}",
                                                    cfg_path, inv_enabled, target_ratio, eff_margin,
                                                )
                                            except Exception:
                                                pass

                                            try:
                                                max_inv = float(getattr(risk_node, "max_inventory", None)) if risk_node is not None else None
                                            except Exception:
                                                max_inv = None
                                            eff_for_cap = (max_inv * eff_margin) if (max_inv is not None) else eff_limit

                                            abs_q = abs(self.Q)
                                            req_raw = float(req_qty)
                                            size_node = getattr(self.cfg, "size", None)
                                            try:
                                                min_lot = float(getattr(size_node, "min", 0.001)) if size_node is not None else 0.001
                                            except Exception:
                                                min_lot = 0.001

                                            if inv_enabled and (eff_for_cap is not None) and (eff_for_cap > 0):
                                                # 在庫前処理は core に委譲（ログも一元化）
                                                safe_size, should_skip = inv_capping_preflight(
                                                    req_raw=req_raw,
                                                    abs_q=abs_q,
                                                    eff_limit=float(eff_for_cap),
                                                    min_lot=min_lot,
                                                    target_ratio=target_ratio,
                                                    cfg_path=cfg_path,
                                                    reduces_position=is_reduce,
                                                )
                                                if should_skip and not is_reduce:
                                                    self._heartbeat(now, "pause", reason="inventory_guard")
                                                    continue
                                                # 最終ガード: eff_limit に対しても再チェック（ログは core に統一）
                                                if (eff_limit is not None) and (not is_reduce) and ((abs(self.Q) + req_qty) > float(eff_limit)):
                                                    _s, _skip = inv_capping_preflight(
                                                        req_raw=req_raw,
                                                        abs_q=abs_q,
                                                        eff_limit=float(eff_limit),
                                                        min_lot=min_lot,
                                                        target_ratio=target_ratio,
                                                        cfg_path=cfg_path,
                                                    )
                                                    if _skip and not is_reduce:
                                                        self._heartbeat(now, "pause", reason="inventory_guard")
                                                        continue
                                elif close_only_mode:
                                    allows = reduce_only or self.would_reduce_inventory(self.Q, side_val, req_qty)
                                    if hasattr(self.risk, "inventory_guard_allows"):
                                        allows = self.risk.inventory_guard_allows(side=side_val, qty=req_qty)
                                    if not allows:
                                        logger.debug("skip place: close_only_mode (inventory_guard)")
                                        self._heartbeat(now, "pause", reason="inventory_guard")
                                        continue
                                # 実際にシミュレータへ発注し、placeログを記録
                                if hasattr(self, "risk") and hasattr(self.risk, "market_mode_allows"):
                                    # この注文が在庫を減らすなら True（ロングなら売り、ショートなら買い）
                                    reduces = self.would_reduce_inventory(self.Q, side_val, req_qty)
                                    # 市場モードが新規停止でも、reduce-only かつ reduces ならブロックしない（決済だけは通す）
                                    if (not self.risk.market_mode_allows(side=side_val, qty=req_qty)) and not (reduce_only and reduces):
                                        logger.debug("skip place: market_mode close_only")
                                        self._heartbeat(now, "pause", reason="market_mode")
                                        continue
                                inflight_limit = _cfg_pick(self.cfg, "orders.max_inflight")
                                if inflight_limit is None:
                                    inflight_limit = _cfg_pick(self.cfg, "risk.max_active_orders")
                                per_key_limit = _cfg_pick(self.cfg, "gate.max_inflight_per_key")
                                limit_qty_cfg = _cfg_pick(self.cfg, "risk.limit_qty")
                                try:
                                    limit_qty_val = float(limit_qty_cfg) if limit_qty_cfg is not None else None
                                except Exception:
                                    limit_qty_val = None
                                inflight_count, inflight_qty, inflight_by_key = self._inflight_snapshot(side=side_val)
                                gate_key = self._gate_key_from_tag(getattr(o, "tag", None))
                                is_reduce_order = bool(reduce_only or self.would_reduce_inventory(self.Q, side_val, req_qty))
                                if inflight_limit is not None and not is_reduce_order:
                                    try:
                                        if inflight_count >= int(inflight_limit):
                                            logger.debug(f"skip place: inflight_guard count={inflight_count} limit={int(inflight_limit)}")
                                            self._heartbeat(now, "pause", reason="inflight_guard")
                                            continue
                                    except Exception:
                                        pass
                                if per_key_limit is not None and gate_key and not is_reduce_order:
                                    try:
                                        if inflight_by_key.get(gate_key, 0) >= int(per_key_limit):
                                            logger.debug(f"skip place: inflight_guard key={gate_key} count={inflight_by_key.get(gate_key, 0)} limit={int(per_key_limit)}")
                                            self._heartbeat(now, "pause", reason="inflight_guard")
                                            continue
                                    except Exception:
                                        pass
                                if limit_qty_val is not None and not is_reduce_order:
                                    if (inflight_qty + abs(req_qty)) > limit_qty_val:
                                        logger.debug(f"skip place: inflight_guard qty={inflight_qty + abs(req_qty):.6f} limit={limit_qty_val}")
                                        self._heartbeat(now, "pause", reason="inflight_guard")
                                        continue
                                if not self._inv_brake_filter([o], pos=self.Q):
                                    logger.debug("skip place: inv_brake")
                                    self._heartbeat(now, "pause", reason="inv_brake")
                                    continue
                                self.sim.place(o, now)
                                corr_for_log = _coid_to_corr.get(
                                    getattr(o, "client_order_id", ""),
                                    (getattr(o, "_corr_id", None) or current_corr_ctx.get() or "-"),
                                )
                                self.order_log.add(
                                    ts=self._ts_jst(now),
                                    action="place",
                                    tif=o.tif,
                                    ttl_ms=o.ttl_ms,
                                    px=o.price,
                                    sz=o.size,
                                    reason=(
                                        f"{o.tag}; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; corr={corr_for_log}"
                                    ),
                                )  # placeでも“注文タグ”（stall / ca_gate）を記録する

                                self._heartbeat(now, "place", reason=o.tag)  # ハートビート：発注を要約

                            elif act.get("type") == "cancel_tag":
                                tag_val = act.get("tag")
                                if not tag_val:
                                    continue
                                for o in self.sim.cancel_by_tag(tag_val):
                                    corr_val = getattr(o, "_corr_id", None)
                                    if corr_token is None and corr_val is not None:
                                        corr_token = current_corr_ctx.set(corr_val)
                                    corr_for_log = _coid_to_corr.get(
                                        getattr(o, "client_order_id", ""),
                                        (getattr(o, "_corr_id", None) or current_corr_ctx.get() or "-"),
                                    )
                                    self.order_log.add(
                                        ts=self._ts_jst(now),
                                        action="cancel",
                                        tif=o.tif,
                                        ttl_ms=o.ttl_ms,
                                        px=o.price,
                                        sz=o.remaining,
                                        reason=(
                                            f"strategy; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; corr={corr_for_log}"
                                        ),
                                    )
                        finally:
                            if corr_token is not None:
                                current_corr_ctx.reset(corr_token)

                elif ch.startswith("lightning_executions_"):
                    self._maybe_auto_reduce(now=monotonic())  # 在庫とモードだけで“薄め”を1口（Reduce‑Only+IOC）
                    # 約定でシミュを進め、Fill明細を受け取る→PnL/ログ反映
                    executions = ev.get("message") or []
                    if not isinstance(executions, (list, tuple)):
                        executions = [executions]
                    for exe in executions:
                        self._queue_eta_on_execution(exe)  # 何をするか：この約定をQueueETAへ反映してETA(ms)を更新する
                    fills = self.sim.on_executions(executions, now)
                    for f in fills:
                        self._apply_fill_and_log(
                            ts_iso=f["ts"], side=f["side"], px=float(f["price"]),
                            sz=float(f["size"]), tag=f["tag"],
                            order=f.get("order"),
                        )
                    # TTLチェックをもう一度（成約後の期限切れ）
                    for o in self.sim.on_time(now):
                        self.order_log.add(
                            ts=self._ts_jst(now),
                            action="cancel",
                            tif=o.tif,
                            ttl_ms=o.ttl_ms,
                            px=o.price,
                            sz=o.remaining,
                            reason=(
                                f"ttl; tag={getattr(o, 'tag', getattr(o, '_strategy', '-'))}; "
                                f"corr={_coid_to_corr.get(getattr(o, 'client_order_id', ''), (getattr(o, '_corr_id', None) or current_corr_ctx.get() or '-'))}"
                            ),
                        )

        except asyncio.CancelledError:
            logger.info("paper cancelled")
            raise
        except KeyboardInterrupt:
            logger.info("Ctrl+C - stopping paper")
        finally:
            try:
                sim = getattr(self, "sim", None)
                if sim is not None:
                    for _ in range(10):
                        open_len = len(getattr(sim, "open", []))
                        if open_len == 0:
                            break
                        canceller = getattr(sim, "cancel_all", None)
                        if callable(canceller):
                            try:
                                canceller()
                            except Exception as e:
                                logger.warning(f"paper: final cancel_all failed: {e}")
                        else:
                            try:
                                getattr(sim, "open", []).clear()
                            except Exception:
                                pass
                        time.sleep(0.05)
                    residual = len(getattr(sim, "open", []))
                    if residual > 0:
                        logger.warning(f"paper: open orders remain after final cancel (count={residual})")
            except Exception as e:
                logger.warning(f"paper: final cancel failed: {e}")
            # ログの確定保存
            self.order_log.flush()
            self.trade_log.flush()
            self.decision_log.flush()
            logger.info(f"paper end: realized_pnl={self.R}, open_orders={len(self.sim.open)}")
