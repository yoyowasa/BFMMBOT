# src/runtime/live.py
# これは live（本番）起動の最小導線です。exchange adapter との疎通だけ行い、危険がないように dry-run（発注なし）にします。

from __future__ import annotations

import os  # 何をするか：APIキー/シークレットを環境変数から読む
import asyncio  # 何をするか：HTTPリプレイの一括実行に使う
from typing import Any, Sequence, Callable  # 何をするか：cfg の型ヒント用
from collections.abc import Mapping  # 何をするか：dict/Mapping を扱う
from loguru import logger  # 何をするか：進行ログを出す
from src.strategy.base import build_strategy_from_cfg  # 何をするか：cfg['strategies'] 配列から戦略群を構築する
import json  # 何をするか：heartbeatをndjsonで書くためにJSONへ直す
from zoneinfo import ZoneInfo  # 何をするか：JST（Asia/Tokyo）へのタイムゾーン変換に使う
from decimal import Decimal  # 何をするか：auto_reduce の計算で桁落ちを防ぐ

from src.core.exchange import RateLimitError  # 何をするか：取引所のレート制限例外を型で捕捉する
from src.core.risk import auto_reduce_should_fire  # 何をするか：Reduce-Only IOC を出すべきか判定する

from datetime import datetime  # 何をするか：heartbeatのts(ISO)を日時に直してレート制限に使う

import math  # 何をするか：サイズの刻み丸め（floor）で使う

import csv  # 何をするか：窓イベント（enter/exit）をCSVに書くために使う
import atexit  # 何をするか：終了時に1回だけ処理するフックを登録する
import sys  # 何をするか：未捕捉例外のフック(sys.excepthook)を差し替えるために使う

from types import SimpleNamespace  # 何をするか：起動時シード用の簡易な注文オブジェクトを作る
import time  # 何をするか：WS再接続の待ち時間（バックオフ）に使う
import signal  # 何をするか：Ctrl+C/SIGTERM を捕まえて安全停止する
from threading import Event  # 何をするか：停止フラグを扱う

from pathlib import Path  # 何をするか：ハートビートの出力ファイルを扱う
import orjson  # 何をするか：1行JSON(NDJSON)を書き出す
from datetime import datetime, timezone, timedelta  # 何をするか：UTC現在時刻とTTL計算
from collections import deque  # 何をするか：ミッド変化ガード用の履歴
from src.core.orderbook import OrderBook  # 何をするか：ローカル板（戦略の入力）
from src.core.orders import Order  # 何をするか：戦略が返す注文モデル（tif/ttl_ms/price/size/tag）
from src.core.realtime import stream_events  # 何をするか：WSのboard/executionsストリーム
from src.strategy.base import MultiStrategy  # 何をするか：複数戦略をまとめるラッパー
from src.core.logs import OrderLog, TradeLog  # 何をするか：orders/trades を Parquet＋NDJSON に記録する
from src.core.analytics import DecisionLog  # 何をするか：戦略の意思決定ログ（Parquet＋NDJSONミラー）を扱う

from src.core.exchange import BitflyerExchange, ExchangeError, ServerError, NetworkError, AuthError  # 何をするか：認証/権限エラー(AuthError)を検知して安全停止する
from src.core.position_replay import http_replay_for_position  # HTTP経由で約定履歴を取り寄せてポジションに適用する共通関数を使う
from src.core.position_replay import http_replay_for_position  # HTTP約定リプレイでQ/A/R/Fの欠損を埋める関数
def _normalize_strategy_names(
    primary: str,
    strategies: Sequence[str] | str | None,
) -> list[str]:
    names: list[str]
    if strategies is None:
        names = [primary]
    elif isinstance(strategies, str):
        names = [strategies]
    else:
        names = list(strategies)
    return names or [primary]


def _now_utc() -> datetime:
    """何をするか：JST現在時刻を返す（ログ/TTL計算の基準）"""
    return datetime.now(ZoneInfo("Asia/Tokyo"))

def _ttl_deadline(now: datetime, ttl_ms: int | None) -> datetime | None:
    """何をするか：TTLミリ秒から締切（UTC）を作る（Noneは無期限）"""
    return None if ttl_ms is None else now + timedelta(milliseconds=ttl_ms)

def _in_maintenance(now: datetime, cfg) -> bool:
    """何をする関数か：現在時刻がメンテ時間帯か（JST）判定する"""
    m = getattr(cfg, "maintenance", None)  # 設定から maintenance を安全に取得（dict/属性の両対応）
    if not m: return False  # メンテ設定が無いなら常に稼働OK
    start_s = getattr(m, "start", None)  # 開始時刻 "HH:MM"
    end_s = getattr(m, "end", None)      # 終了時刻 "HH:MM"
    if not start_s or not end_s: return False  # どちらか欠けたらメンテとはみなさない
    jst = ZoneInfo("Asia/Tokyo")               # JST に変換して判断
    t = now.astimezone(jst).time()             # 現在の JST 時刻（時刻型）
    s = datetime.strptime(start_s, "%H:%M").time()  # 開始時刻を時刻型へ
    e = datetime.strptime(end_s, "%H:%M").time()    # 終了時刻を時刻型へ
    return (s <= t <= e) if s <= e else (t >= s or t <= e)  # 日跨ぎ（例 23:00-02:00）も対応


def _in_funding_calc(now: datetime, cfg) -> bool:
    """何をするか：JSTのFunding計算タイミング ±60s に入っていれば True"""
    ms = getattr(cfg, "mode_switch", None)
    times = getattr(ms, "funding_calc_jst", None) or []
    jst = now.astimezone(timezone(timedelta(hours=9)))
    for tstr in times:
        hh, mm, ss = map(int, tstr.split(":"))
        target = jst.replace(hour=hh, minute=mm, second=ss, microsecond=0)
        if abs((jst - target).total_seconds()) <= 60:
            return True
    return False

def _in_funding_transfer(now: datetime, cfg) -> bool:
    """何をするか：Funding授受の推定1h窓（calc + lag_hours から1時間）に入っていれば True"""
    ms = getattr(cfg, "mode_switch", None)
    times = getattr(ms, "funding_calc_jst", None) or []
    lag_h = getattr(ms, "funding_transfer_lag_hours", 8)
    jst = now.astimezone(timezone(timedelta(hours=9)))
    for tstr in times:
        hh, mm, ss = map(int, tstr.split(":"))
        calc = jst.replace(hour=hh, minute=mm, second=ss, microsecond=0)
        start = calc + timedelta(hours=lag_h)
        end = start + timedelta(hours=1)
        if start <= jst <= end:
            return True
    return False

def _cfg_pick(root, dotted: str, default=None):
    """
    何をする関数か：
    - ドット区切りパス（例: "risk.auto_reduce.enabled"）で、dict / Pydantic モデル / SimpleNamespace を安全にたどる。
    - Pydantic v2 の model_extra も見ることで、schema 外の設定（auto_reduce など）も拾う。
    """
    cur = root
    for key in dotted.split("."):
        if cur is None:
            return default
        if isinstance(cur, Mapping):
            cur = cur.get(key)
        else:
            extra = getattr(cur, "model_extra", None)
            if isinstance(extra, dict) and key in extra:
                cur = extra[key]
            else:
                cur = getattr(cur, key, None)
    return default if cur is None else cur


def _mid_from_ob(ob: OrderBook) -> float | None:
    """何をするか：ローカル板からミッド価格を取り出す（実装差異を吸収）"""
    mid = getattr(ob, "mid", None)
    if callable(mid):
        return mid()
    mid_val = getattr(ob, "mid_price", None)
    return float(mid_val) if mid_val is not None else None

def _round_to_tick(px: float, tick: float) -> float:
    """何をするか：価格をtick単位に丸める（誤差や端数を防ぐ）"""
    return float(round(px / tick) * tick)

def _round_size(sz: float, step: float) -> float:
    """何をするか：サイズを最小刻み(step)に丸めて端数を防ぐ（0に潰れないよう四捨五入）"""
    if step <= 0.0:
        return float(sz)
    steps = round(float(sz) / step)
    rounded = float(steps * step)
    return rounded


def _maybe_auto_reduce_live(
    cfg,
    ex: BitflyerExchange,
    ob: OrderBook,
    now: datetime,
    pnl_state: dict,
    eff_inv_limit: float | None,
    last_ts: datetime | None,
    order_log: OrderLog,
    hb_path: Path,
) -> datetime | None:
    """
    何をする関数か：
    - live 環境で在庫が重いときに、Reduce-Only+IOC の自動決済を 1 回だけ試す。
    - paper 側の auto_reduce と同等の設定（risk.auto_reduce.*）を使うが、発注は REST(send_child_order) 経由。
    """
    try:
        q = float(pnl_state.get("pos", 0.0) or 0.0)
        a = float(pnl_state.get("avg_px", pnl_state.get("avg", 0.0)) or 0.0)
    except Exception:
        logger.debug("auto_reduce: skip invalid_pos")
        return last_ts

    enabled = bool(_cfg_pick(cfg, "risk.auto_reduce.enabled", False))
    profit_only = bool(_cfg_pick(cfg, "risk.auto_reduce.profit_only", True))
    force_on_risk = bool(_cfg_pick(cfg, "risk.auto_reduce.force_on_risk", True))
    try:
        cooldown_ms = int(_cfg_pick(cfg, "risk.auto_reduce.cooldown_ms", 1500))
    except Exception:
        cooldown_ms = 1500
    try:
        min_step_cfg = float(_cfg_pick(cfg, "risk.auto_reduce.min_step_qty", 0.001))
    except Exception:
        min_step_cfg = 0.001
    tif = str(_cfg_pick(cfg, "risk.auto_reduce.tif", "IOC") or "IOC").upper()

    if not enabled or eff_inv_limit is None:
        return last_ts

    if q == 0.0:
        logger.debug("auto_reduce: skip size_zero (flat)")
        return last_ts

    # クールダウン（連打防止）
    if cooldown_ms > 0 and last_ts is not None:
        elapsed_ms = (now - last_ts).total_seconds() * 1000.0
        if elapsed_ms < float(cooldown_ms):
            logger.debug("auto_reduce: skip cooldown")
            return last_ts

    # inv_capping.target_ratio を考慮して「どれだけ重いか」を見る
    try:
        target_ratio = float(_cfg_pick(cfg, "risk.inv_capping.target_ratio", 0.90))
    except Exception:
        target_ratio = 0.90
    eff_limit = float(eff_inv_limit or 0.0)
    target_eff = (target_ratio * eff_limit) if eff_limit > 0.0 else 0.0
    abs_q = abs(q)
    over_eff = (abs_q > target_eff) if target_eff > 0.0 else False
    if not over_eff:
        logger.debug("auto_reduce: skip not_over_eff")
        return last_ts

    # mark（現在価格）を best_bid/best_ask の mid として取得
    try:
        mid = _mid_from_ob(ob)
        mark_dec = Decimal(str(mid)) if mid is not None else None
    except Exception:
        mark_dec = None

    # auto_reduce_should_fire で「やるべきか」を判定（理由もログに残す）
    try:
        decision = auto_reduce_should_fire(
            enabled=True,
            profit_only=profit_only,
            force_on_risk=force_on_risk,
            q=Decimal(str(q)),
            a=Decimal(str(a)),
            mark=mark_dec,
            mode="halted",  # 在庫ガード発動中なので事実上 Close-Only/Halted 扱い
            recent_errors=[],
            eff_limit=Decimal(str(eff_limit)),
        )
    except Exception as e:
        logger.debug(f"auto_reduce: skip decision_error={e}")
        return last_ts

    if not decision.fire:
        logger.debug(f"auto_reduce: skip {decision.reason}")
        return last_ts

    side = (decision.side or ("SELL" if q > 0.0 else "BUY")).upper()

    # overshoot（target_eff 超過分）をロット刻みに揃えて減らす量に変換
    base_step = max(min_step_cfg, 0.0)
    qty_step = float(getattr(cfg, "qty_step", base_step or 0.001) or 0.001)
    if qty_step <= 0.0:
        qty_step = 0.001
    dust_eps = qty_step * 0.5

    overshoot = abs_q - target_eff
    if abs_q < qty_step or overshoot <= 0.0:
        logger.debug("auto_reduce: skip size_zero (no meaningful overshoot)")
        return last_ts

    try:
        lots = max(int(overshoot / qty_step), 1)
    except Exception:
        lots = 1
    step = min(abs_q, lots * qty_step)

    if step <= 0.0 or step < dust_eps:
        logger.debug("auto_reduce: skip size_zero (dust)")
        return last_ts

    step, reason = _calc_reduce_order_size(
        q,
        step,
        size_cfg=getattr(cfg, "size", None),
        step=qty_step,
        min_size=min_step_cfg,
    )
    if step is None:
        logger.debug(f"auto_reduce: skip reduce_size_{reason}")
        return last_ts

    try:
        logger.info(
            f"auto_reduce: try place (REST) side={side} qty={step:.6f} tif={tif} tag=auto_reduce"
        )
        acc = ex.place_ioc_reduce_only(side=side, size=step, tag="auto_reduce")
        # IOC なので TTL/price などは固定でよいが、orders/trades/heartbeat には記録しておく
        now_iso = now.isoformat()
        order_log.add(
            ts=now_iso,
            action="place",
            tif=tif,
            ttl_ms=None,
            px=None,
            sz=step,
            reason="auto_reduce",
        )
        _hb_write(
            hb_path,
            event="place",
            ts=now_iso,
            acc=str(acc),
            reason="auto_reduce",
            tif=tif,
            ttl_ms=None,
            px=None,
            sz=step,
        )
        return now
    except RateLimitError as e:
        logger.error(f"auto_reduce: RateLimit �� skip: {e}")
        return now  # 何をするか：レート制限時はクールダウンを進めて連打を止める
    except Exception as e:
        logger.warning(f"auto_reduce: place failed: {e}")
        return now  # 何をするか：失敗時もクールダウンを進めて暴走を避ける
def _net_inventory_btc(ex: BitflyerExchange) -> float:
    """何をするか：現在の建玉（BTC）を +BUY/-SELL で合算して返す（在庫ガード用）"""
    try:
        positions = ex.get_positions()
    except Exception:
        return 0.0
    q = 0.0
    for p in positions or []:
        side = str(p.get("side", "")).upper()
        sz = float(p.get("size", 0.0))
        q += sz if side == "BUY" else -sz
    return q

def _seed_live_orders_from_active(ex: BitflyerExchange, live_orders: dict[str, dict]) -> None:
    """取引所に残っている未約定(ACTIVE)注文を監視辞書へ投入する"""
    try:
        items = ex.list_active_child_orders(count=100)  # ACTIVEな子注文を最大100件取得
    except ExchangeError:
        return  # 一時失敗は何もしない（次回に回す）
    for it in items or []:
        acc = str(it.get("child_order_acceptance_id") or "")
        if not acc or acc in live_orders:
            continue  # IDなし/すでに監視中ならスキップ
        side = str(it.get("side", "")).upper()
        px = float((it.get("price") or it.get("average_price") or 0.0) or 0.0)
        sz = float(it.get("size", 0.0) or 0.0)
        executed = float(it.get("executed_size", 0.0) or 0.0)
        avg = float(it.get("average_price", 0.0) or 0.0)
        tif = str(it.get("time_in_force", "GTC") or "GTC")
        deadline = None
        order = SimpleNamespace(side=side, price=px, size=sz, tag="seed", tif=tif, ttl_ms=None)
        live_orders[acc] = {
            "deadline": deadline,
            "order": order,
            "executed": executed,
            "avg_price": avg,
            "reduce_only": False,
            "reduces_inventory": False,
        }

def _seed_inventory_and_avg_px(ex: BitflyerExchange) -> tuple[float | None, float, bool]:
    """何をするか：取引所の建玉一覧から“平均コスト（参考）”と“ネット建玉(BTC)”を取得して初期状態に入れる
    戻り値: (avg_px, net, success)
    """
    try:
        positions = ex.get_positions()  # 何をするか：現在保有している建玉一覧を取得
    except ExchangeError:
        return None, 0.0, False  # 何をするか：取れない時は成功フラグFalseで返す

    long_sz = long_not = 0.0
    short_sz = short_not = 0.0
    for p in positions or []:
        sz = float(p.get("size", 0.0) or 0.0)
        px = float(p.get("price", 0.0) or 0.0)
        side = str(p.get("side", "")).upper()
        if sz <= 0.0 or px <= 0.0:
            continue
        if side == "BUY":
            long_sz += sz
            long_not += sz * px
        elif side == "SELL":
            short_sz += sz
            short_not += sz * px

    net = long_sz - short_sz  # 何をするか：ロング合計−ショート合計＝ネット建玉（+ロング/−ショート）
    if net > 0.0 and long_sz > 0.0:
        avg = long_not / long_sz  # 何をするか：ロング側の平均建値
    elif net < 0.0 and short_sz > 0.0:
        avg = short_not / short_sz  # 何をするか：ショート側の平均建値
    else:
        avg, net = None, 0.0  # 何をするか：ネットがゼロなら平均は不要

    return avg, net, True

__last_pause_hb_at: dict[str, datetime] = {}  # 何をするか：pause理由ごとの直近送信時刻（静音化のためのメモ）

def _hb_write(hb_path, **fields):
    """何をする関数か：ハートビート1行を ndjson で追記する。pauseは同じreasonを1秒に1回だけ書く（静音化）"""
    # 何をするか：pause心拍の静音化（同じreasonを1秒に1回まで）
    if fields.get("event") == "pause":
        reason = fields.get("reason", "unknown")
        ts = fields.get("ts")
        try:
            now_dt = datetime.fromisoformat(ts) if isinstance(ts, str) else ts  # 何をするか：ISO文字列→datetime
        except Exception:
            now_dt = _now_utc()  # 何をするか：壊れていたら現在時刻で代用
        last = __last_pause_hb_at.get(reason)
        if last and (now_dt - last).total_seconds() < 1.0:
            return  # 何をするか：1秒未満なら今回は書かない
        __last_pause_hb_at[reason] = now_dt  # 何をするか：直近送信時刻を更新

    # 何をするか：ndjsonとして1行追記
    try:
        ts_val = fields.get("ts")
        if isinstance(ts_val, str):
            try:
                now_dt = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
            except Exception:
                now_dt = _now_utc()
        elif isinstance(ts_val, datetime):
            now_dt = ts_val
        else:
            now_dt = _now_utc()
        # JST日付タグ(YYYYMMDD)で心拍ファイル名を自動決定
        jst = timezone(timedelta(hours=9))
        tag = now_dt.astimezone(jst).strftime("%Y%m%d")
        p = Path(f"logs/runtime/{tag}heartbeat.ndjson")
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(fields, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.exception(f"heartbeat write failed: {e}")


def _safe_config_dict(source) -> dict[str, Any]:
    """何をするか：Mapping/モデル/属性オブジェクトを安全にdictへ落とす"""
    if source is None:
        return {}
    if isinstance(source, Mapping):
        return dict(source)
    model_dump = getattr(source, "model_dump", None)
    if callable(model_dump):
        try:
            dumped = model_dump()
        except Exception:
            dumped = None
        else:
            if isinstance(dumped, Mapping):
                return dict(dumped)
    try:
        return dict(vars(source))
    except Exception:
        return {}


def _strategy_cfg_overrides(strategy_cfg, strategies: Sequence[str] | None) -> dict[str, dict[str, Any]]:
    """何をするか：strategy_cfg から有効戦略ごとの上書きを dict 化する"""
    overrides: dict[str, dict[str, Any]] = {}
    if not strategy_cfg or not strategies:
        return overrides

    mapping_view: Mapping[str, Any] | None
    if isinstance(strategy_cfg, Mapping):
        mapping_view = strategy_cfg
    else:
        dumped = None
        model_dump = getattr(strategy_cfg, "model_dump", None)
        if callable(model_dump):
            try:
                dumped = model_dump()
            except Exception:
                dumped = None
        if isinstance(dumped, Mapping):
            mapping_view = dumped
        else:
            mapping_view = getattr(strategy_cfg, "__dict__", None)

    for name in strategies:
        cfg_node = None
        if isinstance(strategy_cfg, Mapping):
            cfg_node = strategy_cfg.get(name)
        else:
            cfg_node = getattr(strategy_cfg, name, None)
            if cfg_node is None and mapping_view:
                cfg_node = mapping_view.get(name)
        node_dict = _safe_config_dict(cfg_node)
        if node_dict:
            overrides[name] = node_dict

    return overrides


def _best_px(side) -> float | None:
    """何をするか：best_bid/best_ask に入るオブジェクト/辞書/数値から“価格(float)”だけを取り出す"""
    if side is None:
        return None
    if isinstance(side, (int, float)):
        return float(side)
    for attr in ("price", "px", "p"):  # 何をするか：代表的な属性名を順に試す
        v = getattr(side, attr, None)
        if isinstance(v, (int, float)):
            return float(v)
    if isinstance(side, dict):  # 何をするか：辞書形式にも対応
        for key in ("price", "px", "p"):
            v = side.get(key)
            if isinstance(v, (int, float)):
                return float(v)
    return None  # 何をするか：どれにも当てはまらなければ未取得（None）

def _act(o, key: str, default=None):
    """何をする関数か：戦略アクションoから key（'price','size','side','tif','tag' など）を属性/辞書どちらでも安全に取り出す"""
    return getattr(o, key, (o.get(key, default) if isinstance(o, dict) else default))

def _side_norm(v: str | int | None) -> str:
    """何をする関数か：side を取引所仕様の 'BUY' / 'SELL' に正規化（小文字/略称/数値も受け付ける）"""
    if v is None:
        return "BUY"  # 何をするか：デフォルトはBUY（保守的に片方に寄せる）
    s = str(v).strip().upper()
    if s in ("B", "BUY", "1", "+1"):
        return "BUY"
    if s in ("S", "SELL", "-1"):
        return "SELL"
    return "BUY"  # 何をするか：未知値はBUYへフォールバック（必要なら後続のガードで弾く）

def _would_reduce_inventory(current_inventory: float, side, request_qty: float) -> bool:
    """何をする関数か：この注文が在庫|Q|を減らす（決済）ならTrueを返す"""
    if side is None:
        return False
    try:
        side_norm = str(side).strip().lower()
    except Exception:
        return False
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

def _normalize_px_sz(cfg, px: float, sz: float) -> tuple[float | None, float | None]:
    """何をする関数か：価格をtick、サイズをstepへ丸め、最小サイズ未満は(None, None)を返して発注を止める"""
    tick = getattr(cfg, "tick_size", None)
    step = getattr(getattr(cfg, "size", None), "step", None)
    minsz = getattr(getattr(cfg, "size", None), "min", None)

    if (tick is not None) and (tick > 0):
        px = round(px / float(tick)) * float(tick)  # 何をするか：価格を最寄りのtickへ丸める

    if (step is not None) and (step > 0):
        sz = math.floor(sz / float(step)) * float(step)  # 何をするか：サイズは超過しないよう“切り捨て”で刻みに合わせる

    if (minsz is not None) and (sz < float(minsz)):
        return None, None  # 何をするか：最小サイズ未満は発注を止める

    return px, sz

def _calc_reduce_order_size(
    current_pos: float,
    request_size: float,
    *,
    size_cfg=None,
    step: float | None = None,
    min_size: float | None = None,
) -> tuple[float | None, str | None]:
    """何をするか：reduce-only用に「在庫以内へクリップ」「刻みで切り下げ」「最小0.001BTC未満や残尾が最小未満なら発注しない」を返す"""
    try:
        pos_abs = abs(float(current_pos or 0.0))
    except Exception:
        return None, "invalid_pos"
    if pos_abs <= 0.0:
        return None, "flat"
    try:
        req = float(request_size or 0.0)
    except Exception:
        return None, "invalid_size"
    if req <= 0.0:
        return None, "non_positive"

    try:
        cfg_step = float(getattr(size_cfg, "step", 0.0) or 0.0)
    except Exception:
        cfg_step = 0.0
    eff_step = float(step) if step is not None else cfg_step

    eff_min = 0.001
    try:
        cfg_min = float(getattr(size_cfg, "min", 0.0) or 0.0)
    except Exception:
        cfg_min = 0.0
    try:
        extra_min = float(min_size or 0.0)
    except Exception:
        extra_min = 0.0
    eff_min = max(eff_min, cfg_min, extra_min)

    capped = min(req, pos_abs)
    if eff_step > 0.0:
        capped = math.floor(capped / eff_step) * eff_step

    if capped < eff_min - 1e-12:
        return None, "below_min"

    residual = pos_abs - capped
    if 0.0 < residual < eff_min:
        return None, "dust_residual"

    return capped, None

def _csv_event_write(path: Path, row: dict) -> None:
    """何をするか：イベントCSV（enter/exit）を1行追記（初回はヘッダも書く）"""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)

def cancel_all_with_retry(
    ex: BitflyerExchange,
    product: str,
    *,
    max_retry: int = 3,
    sleep_sec: float = 1.0,
    timeout: float | None = None,
) -> None:
    """何をするか：cancel_all_child_orders を複数回リトライして確実に流すヘルパ"""
    try:
        attempts = int(max_retry)
    except Exception:
        attempts = 3
    attempts = max(1, attempts)
    orig_prod = getattr(ex, "product_code", None)
    orig_timeout = getattr(getattr(ex, "_client", None), "timeout", None)
    last_exc: Exception | None = None
    try:
        if product:
            try:
                ex.product_code = product
            except Exception:
                pass
        if timeout is not None:
            try:
                ex._client.timeout = timeout  # type: ignore[attr-defined]
            except Exception:
                pass
        for i in range(1, attempts + 1):
            try:
                ex.cancel_all_child_orders()
                return
            except Exception as e:
                last_exc = e
                logger.warning(f"cancel_all retry {i}/{attempts} failed: {e}")
                if i >= attempts:
                    break
                time.sleep(max(0.0, sleep_sec))
        if last_exc:
            raise last_exc
    finally:
        if orig_prod is not None:
            try:
                ex.product_code = orig_prod
            except Exception:
                pass
        if timeout is not None and orig_timeout is not None:
            try:
                ex._client.timeout = orig_timeout  # type: ignore[attr-defined]
            except Exception:
                pass

def _mk_atexit(
    ex: BitflyerExchange,
    hb_path: Path,
    product_code: str,
    live_orders: dict[str, dict],
    *,
    dry_run: bool,
    max_retry: int = 3,
    sleep_sec: float = 1.0,
    timeout: float | None = None,
    size_cfg=None,
) -> Callable[[str], None]:
    """何をするか：終了時に cancel_all＋reduce-onlyでのフラット＋stop心拍 を1回だけ通すハンドラを作る"""
    done = False
    def _on_exit(reason: str = "exit") -> None:
        nonlocal done
        if done:
            return
        done = True
        if not dry_run:
            try:
                cancel_all_with_retry(
                    ex,
                    product_code,
                    max_retry=max_retry,
                    sleep_sec=sleep_sec,
                    timeout=timeout,
                )
            except Exception as e:
                logger.warning(f"final cancel_all failed: {e}")
            try:
                q = _net_inventory_btc(ex)
            except Exception as e:
                logger.warning(f"final flatten: inventory fetch failed: {e}")
            else:
                attempts = 0
                while attempts < 3 and abs(q) >= 0.001:
                    size_try, clip_reason = _calc_reduce_order_size(
                        q,
                        abs(q),
                        size_cfg=size_cfg,
                    )
                    if size_try is None:
                        logger.warning(f"final flatten skipped: reason={clip_reason} pos={q:.6f}")
                        break
                    side = "SELL" if q > 0 else "BUY"
                    try:
                        ex.place_ioc_reduce_only(side=side, size=size_try, product_code=product_code, tag="final_flat")
                        _hb_write(hb_path, event="reduce_only", ts=_now_utc().isoformat(), side=side, sz=size_try, reason="final_flat")
                    except Exception as e:
                        logger.warning(f"final flatten attempt failed: {e}")
                    attempts += 1
                    try:
                        q = _net_inventory_btc(ex)
                    except Exception as e:
                        logger.warning(f"final flatten: inventory refresh failed: {e}")
                        break
                if abs(q) >= 0.001:
                    logger.warning(f"final flatten: residual position remains pos={q:.6f} BTC")
        else:
            logger.info("live(dry-run): final cancel_all skipped")
        try:
            live_orders.clear()
        except Exception:
            pass
        try:
            _hb_write(hb_path, event="stop", ts=_now_utc().isoformat(), reason=reason)  # 何をするか：終了の合図を記録
        except Exception:
            pass  # 何をするか：終了間際のエラーは握って静かに終わる
    return _on_exit

def _mk_excepthook(exit_handler: Callable[[str], None], stop_event: Event, orig_hook):
    """何をするか：未捕捉例外で停止フラグを立てつつ終了シーケンスへ誘導する excepthook を作って返す"""
    def _hook(exc_type, exc, tb):
        logger.exception(f"live: unexpected error → halt requested: {exc}")  # 何をするか：原因をrun.logに記録（スタック付き）
        try:
            stop_event.set()  # 何をするか：シグナルと同様に停止要求だけ伝える
        except Exception:
            pass
        try:
            exit_handler("exception")  # 何をするか：最終キャンセルを単一路で実施
        except Exception:
            pass
        try:
            orig_hook(exc_type, exc, tb)  # 何をするか：元のフックにも渡して正常終了パスへ
        except Exception:
            pass
    return _hook

def _stream_with_reconnect(product_code: str, hb_path: Path, *, max_backoff_s: int = 10, stop_event: Event | None = None):
    """何をするか：WSが切れたら心拍にpauseを書き、待ってから自動再接続してイベントを流し続ける（停止フラグも監視）"""
    backoff = 1
    while True:
        if stop_event is not None and stop_event.is_set():
            break
        try:
            for ev in stream_events(product_code):
                if stop_event is not None and stop_event.is_set():
                    return
                backoff = 1  # 何をするか：イベントを受け取れたらバックオフを初期化
                yield ev
        except Exception as e:
            if stop_event is not None and stop_event.is_set():
                break
            logger.warning(f"ws reconnect: {e}")  # 何をするか：再接続の理由をrun.logに残す
            try:
                _hb_write(hb_path, event="pause", ts=_now_utc().isoformat(), reason="ws_reconnect")  # 何をするか：心拍に“再接続”を記録
            except Exception:
                pass
            # 何をするか：上位が不足分をバックフィルできるように通知イベントを挟む
            yield {"channel": "__ws_status__", "event": "ws_reconnect"}
            time.sleep(backoff)  # 何をするか：少し待ってから再接続（バックオフ）
            backoff = min(max_backoff_s, backoff * 2 if backoff < max_backoff_s else max_backoff_s)
            continue

def _log_window_event(events_dir: Path, kind: str, action: str, ts: datetime) -> None:
    """何をするか：窓（maintenance / funding）の入退をCSVに1行追記して記録する"""
    events_dir.mkdir(parents=True, exist_ok=True)  # 何をするか：フォルダを事前作成
    fname = "maintenance.csv" if kind == "maintenance" else "funding_schedule.csv"
    line = f"{ts.isoformat()},{action}\n"  # 何をするか：列は ts,action の2列（シンプルに固定）
    (events_dir / fname).open("a", encoding="utf-8").write(line)


def _act(o, key, default=None):
    """何をする関数か：oがdictでも属性でも同じ書き方で値を取り出す"""
    return (o.get(key, default) if isinstance(o, dict) else getattr(o, key, default))



def _gate_key(tag: str | None) -> str:
    """corr付きなどを外したタグ基底"""
    if not tag:
        return ""
    parts = [p for p in str(tag).split("|") if p and not str(p).startswith("corr:")]
    return parts[0] if parts else ""

def _tag_matches(order_tag: str | None, query: str | None) -> bool:
    """何をする関数か：タグ一致を緩く判定（base と base|corr:xxx のような付加情報付きも拾う）。"""
    if not query:
        return False
    if order_tag == query:
        return True
    try:
        return str(order_tag).startswith(f"{query}|")
    except Exception:
        return False

def _inflight_state(live_orders: dict[str, dict], *, side: str | None = None) -> tuple[int, float, dict[str, int]]:
    """未約定のエントリー注文だけを本数/数量で集計（同一サイドのみ）"""
    total_count = 0
    total_qty = 0.0
    per_key: dict[str, int] = {}
    side_norm = _side_norm(side) if side else None
    for meta in live_orders.values():
        order = meta.get("order") if isinstance(meta, dict) else None
        if order is None:
            continue
        if side_norm and _side_norm(_act(order, "side")) != side_norm:
            continue
        reduce_flag = bool(meta.get("reduce_only") or meta.get("reduces_inventory") or getattr(order, "reduce_only", False) or getattr(order, "close_only", False))
        if reduce_flag:
            continue
        try:
            base_sz = float(_act(order, "size", 0.0) or 0.0)
        except Exception:
            base_sz = 0.0
        executed = float(meta.get("executed", 0.0) or 0.0) if isinstance(meta, dict) else 0.0
        remaining = max(base_sz - executed, 0.0)
        if remaining <= 0:
            continue
        total_count += 1
        total_qty += remaining
        key = _gate_key(_act(order, "tag", ""))
        per_key[key] = per_key.get(key, 0) + 1
    return total_count, total_qty, per_key

def _pull_fill_deltas(ex: BitflyerExchange, live_orders: dict[str, dict]) -> list[dict[str, Any]]:  # 何をするか：“増分約定”に finalかどうかの旗(done)や元注文を添える
    """何をするか：受理IDごとに“今回ぶんの増分約定”だけを取り出して属性付き辞書で返す"""
    fills: list[dict[str, Any]] = []
    for acc_id, meta in list(live_orders.items()):
        try:
            info_list = ex.get_child_order_by_acceptance_id(acc_id)  # 何をするか：受理IDで照会
        except ExchangeError:
            continue
        if not info_list:
            continue
        info = info_list[0]
        executed = float(info.get("executed_size", 0.0) or 0.0)
        avg_new = float(info.get("average_price", 0.0) or 0.0)  # 何をするか：これまでの平均約定単価
        outstanding = float(info.get("outstanding_size", 0.0) or 0.0)
        state = str(info.get("child_order_state", "")).upper()

        prev_exec = float(meta.get("executed", 0.0) or 0.0)
        prev_avg = float(meta.get("avg_price", 0.0) or 0.0)
        delta = max(executed - prev_exec, 0.0)
        if delta > 0:
            # 何をするか：“総額の差”から今回ぶんの約定単価を推定（平均×数量の差をdeltaで割る）
            try:
                px = (avg_new * executed - prev_avg * prev_exec) / delta
            except ZeroDivisionError:
                px = avg_new or meta["order"].price
            fills.append(
                {
                    "side": _side_norm(_act(meta["order"], "side")),
                    "price": float(px),
                    "size": float(delta),
                    "tag": str(_act(meta["order"], "tag", "")),
                    "done": state == "COMPLETED" or (outstanding <= 1e-12 and executed > 0.0),
                    "order": meta.get("order"),
                    "acceptance_id": acc_id,
                }
            )  # 何をするか：dict/属性両対応でside/tagを取得する


        # 何をするか：ローカル状態を更新（次回差分計算のため）
        meta["executed"] = executed
        meta["avg_price"] = avg_new

        # 何をするか：完了注文は監視から外す
        if state == "COMPLETED" or (outstanding <= 1e-12 and executed > 0.0):
            del live_orders[acc_id]
    return fills

def _apply_fill_and_pnl(state: dict, side: str, px: float, sz: float) -> float:
    """何をするか：建玉と平均コストを更新し、今回ぶんの実現PnL(JPY)を返す（部分/反転対応）"""
    pos = float(state.get("pos", 0.0))
    avg = state.get("avg_px", None)  # None は建玉ゼロ
    realized = 0.0
    if side.upper() == "BUY":
        # 何をするか：ショートを閉じる→残りがあればロングを作る
        if pos < 0.0:
            close = min(sz, -pos)
            realized += (avg - px) * close  # short: entry(avg) - exit(px)
            pos += close
            sz -= close
            if pos == 0.0:
                avg = None
        if sz > 0.0:
            if pos <= 0.0:
                avg = px
                pos += sz
            else:
                avg = (avg * pos + px * sz) / (pos + sz)
                pos += sz
    else:  # SELL
        if pos > 0.0:
            close = min(sz, pos)
            realized += (px - avg) * close  # long: exit(px) - entry(avg)
            pos -= close
            sz -= close
            if pos == 0.0:
                avg = None
        if sz > 0.0:
            if pos >= 0.0:
                avg = px
                pos -= sz  # 何をするか：ショート開始（負の建玉）
            else:
                avg = (avg * (-pos) + px * sz) / ((-pos) + sz)
                pos -= sz
    state["pos"], state["avg_px"] = pos, avg
    return realized

def _check_kill(daily_R: float, R_HWM: float, kill_cfg) -> bool:
    """何をするか：日次PnLとDDのしきい値を判定して Kill 到達なら True を返す"""
    if not kill_cfg:
        return False
    daily_lim = getattr(kill_cfg, "daily_pnl_jpy", None)
    dd_lim = getattr(kill_cfg, "max_dd_jpy", None)
    # 何をするか：日次PnL（損失が閾値以下）判定
    if daily_lim is not None and daily_R <= float(daily_lim):
        return True
    # 何をするか：DD（HWMからの落ち幅）判定
    if dd_lim is not None:
        dd = float(R_HWM - daily_R)
        if dd >= float(dd_lim):
            return True
    return False

def _refresh_fills(ex: BitflyerExchange, live_orders: dict[str, dict]) -> None:
    """何をするか：RESTの me/getchildorders で各注文の約定状況を取り込み、完了注文を監視対象から外す"""
    for acc_id, meta in list(live_orders.items()):
        try:
            info_list = ex.get_child_order_by_acceptance_id(acc_id)  # 何をするか：受理IDで照会
        except ExchangeError:
            continue  # 何をするか：一時失敗は無視して次回に回す
        if not info_list:
            continue
        info = info_list[0]
        executed = float(info.get("executed_size", 0.0) or 0.0)
        outstanding = float(info.get("outstanding_size", 0.0) or 0.0)
        state = str(info.get("child_order_state", "")).upper()
        live_orders[acc_id]["executed"] = executed  # 何をするか：累計約定量をローカルに反映
        if state == "COMPLETED" or (outstanding <= 1e-12 and executed > 0.0):
            del live_orders[acc_id]  # 何をするか：完了注文は監視から除去


def _rate_limit_strike(hits: deque[datetime], window_s: int, limit: int) -> bool:
    """何をするか：429の連発をカウントし、一定以上ならTrueで知らせる"""
    now = _now_utc()
    hits.append(now)
    while hits and (now - hits[0]).total_seconds() > window_s:
        hits.popleft()
    return len(hits) >= limit


def _prime_execution_cursor(ex: BitflyerExchange) -> int | None:
    """何をするか：getexecutionsの最新idだけを読み、カーソルを初期化する（重い追跡はしない）"""
    try:
        latest = ex.get_executions(count=1)
    except Exception:
        return None
    if not latest:
        return None
    try:
        return int(latest[0].get("id"))
    except Exception:
        return None


def _backfill_executions(
    ex: BitflyerExchange,
    *,
    last_exec_id: int | None,
    seen_exec_ids: set[int],
    pnl_state: dict,
    trade_log: TradeLog,
    order_log: OrderLog,
    live_orders: dict[str, dict],
    fee_bps: float,
    summary_name: str,
    fund_now: bool,
    maint_now: bool,
    hb_path: Path,
    on_fill_seen=None,
) -> int | None:
    """何をするか：WS途切れ時などに getexecutions で欠損を補う"""
    try:
        execs = ex.get_executions(after=last_exec_id, count=200)
    except RateLimitError as e:
        logger.warning(f"backfill executions rate limited: {e}")
        return last_exec_id
    except (ServerError, NetworkError, ExchangeError) as e:
        logger.warning(f"backfill executions failed: {e}")
        return last_exec_id

    if not execs:
        return last_exec_id

    got_fill = False
    new_last = last_exec_id
    for ev in execs:
        try:
            eid = int(ev.get("id"))
        except Exception:
            eid = None
        if eid is not None:
            if eid in seen_exec_ids:
                continue
            seen_exec_ids.add(eid)
            new_last = eid if (new_last is None or eid > new_last) else new_last

        side = str(ev.get("side", "")).upper()
        px = float(ev.get("price", 0.0) or 0.0)
        sz = float(ev.get("size", 0.0) or 0.0)
        if not side or sz <= 0.0 or px <= 0.0:
            continue
        got_fill = True
        acc_id = ev.get("child_order_acceptance_id") or ""
        tag = ""
        meta = live_orders.get(acc_id) if acc_id else None
        if meta:
            try:
                tag = str(_act(meta.get("order"), "tag", "") or "")
            except Exception:
                tag = ""
            try:
                meta["executed"] = float(meta.get("executed", 0.0) or 0.0) + sz
                base_sz = float(_act(meta.get("order"), "size", 0.0) or 0.0)
                if base_sz > 0 and meta["executed"] >= base_sz - 1e-12:
                    del live_orders[acc_id]
            except Exception:
                pass

        realized = _apply_fill_and_pnl(pnl_state, side, px, sz)
        fee = px * sz * (fee_bps / 10000.0)
        realized -= fee
        trade_log.add(
            ts=_now_utc().isoformat(),
            side=side,
            px=px,
            sz=sz,
            fee=fee,
            pnl=realized,
            strategy=summary_name,
            tag=tag,
            inventory_after=pnl_state.get("pos", 0.0),
            window_funding=fund_now,
            window_maint=maint_now,
        )
        order_log.add(ts=_now_utc().isoformat(), action="fill", tif=None, ttl_ms=None, px=px, sz=sz, reason=tag)
        _hb_write(hb_path, event="fill", ts=_now_utc().isoformat(), side=side, px=px, sz=sz, pnl=realized, tag=tag)
    if got_fill and callable(on_fill_seen):
        try:
            on_fill_seen()
        except Exception:
            pass
    return new_last


class HttpReplayPositionAdapter:
    # このクラスは、HTTPリプレイ関数が使いやすい形で既存のポジションをラップするアダプタクラスです

    def __init__(self, position):
        # このコンストラクタは、既存のポジションオブジェクトを受け取って中に覚えておく関数です
        self._position = position

    @property
    def last_fill_id(self):
        # このプロパティは、最後に処理した約定IDを取得する関数です
        return self._position.last_fill_id

    @last_fill_id.setter
    def last_fill_id(self, value):
        # このセッターは、最後に処理した約定IDを更新する関数です
        self._position.last_fill_id = value

    def apply_fill(self, fill):
        # このメソッドは、1件の約定イベントを既存ポジションに適用する関数です
        self._position.apply_fill(fill)


async def run_http_replay_for_live_position(
    *,
    exchange: BitflyerExchange,
    product_code: str,
    position,
    page_size: int = 500,
    logger=None,
) -> int:
    # この関数はlive起動前にHTTPで約定履歴を取り寄せてポジション(Q/A/R/F)の抜けをまとめて埋める係です

    # ここで既存のpositionオブジェクトをアダプタで包んで、last_fill_idとapply_fillだけをhttp_replay_for_positionに見せる
    adapter = HttpReplayPositionAdapter(position)

    # engine側と同じhttp_replay_for_positionを呼び出して、バリア以降の約定を順番に適用してもらう
    applied = await http_replay_for_position(
        exchange=exchange,
        product_code=product_code,
        position=adapter,
        page_size=page_size,
    )
    if logger:
        try:
            logger.info(f"http_replay: applied {applied} fills (product={product_code})")
        except Exception:
            pass
    return applied


async def _http_replay_position_live(
    *,
    exchange: BitflyerExchange,
    product_code: str,
    position,
    page_size: int = 500,
) -> int:
    # この関数は「HTTPでlast_fill_id以降の約定を取り寄せて、position.apply_fillに古い順で流す」係です。
    # live起動時に一度だけ呼び出し、WS停止中の欠損約定をまとめて埋める用途を想定しています。
    adapter = HttpReplayPositionAdapter(position)
    return await http_replay_for_position(
        exchange=exchange,
        product_code=product_code,
        position=adapter,
        page_size=page_size,
    )

def run_live(
    cfg: Any,
    strategy_name: str,
    dry_run: bool = True,
    *,
    strategies: Sequence[str] | str | None = None,
    strategy_cfg=None,
) -> None:
    """
    live（本番）を起動する関数（最小版・導線）。
    - 何をするか：API鍵の取得→ exchange adapter で疎通確認（未発注）
    - ねらい：鍵/署名/権限/ネットワークの不備を先に見つける（小ロット本番の前段）
    - 次ステップ：dry_run=False とイベントループ/TTL取消/戦略呼び出しをこのファイルに追記
    """
    # 何をするか：.env から API キーを読む（.env 運用はワークフロー文書に準拠）
    api_key = os.getenv("BF_API_KEY")
    api_secret = os.getenv("BF_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("BF_API_KEY / BF_API_SECRET が .env から読めません（.env を確認してください）")

    # 何をするか：CFD前提の既定銘柄。cfg に product_code があればそれを使う
    product_code = getattr(cfg, "product_code", "FX_BTC_JPY")
    tick = float(getattr(cfg, "tick_size", 1))  # 何をするか：価格をこの最小刻みに丸める（例：JPYなら1）


    # 何をするか：exchange adapter で「未約定一覧」を1件だけ取得し、疎通を確かめる
    strategy_list = _normalize_strategy_names(strategy_name, strategies)
    if len(strategy_list) > 1:
        primary_strategy = strategy_list[0]
        summary_name = MultiStrategy._compose_name(strategy_list)
    else:
        primary_strategy = strategy_list[0]
        summary_name = primary_strategy

    with BitflyerExchange(api_key, api_secret, product_code=product_code) as ex:
        live_orders: dict[str, dict] = {}
        exit_reason = "exit"
        cancel_retry = 3
        cancel_sleep_sec = 1.0
        try:
            cancel_timeout = float(getattr(getattr(cfg, "tx", None), "cancel_all_timeout_sec", 10.0))
        except Exception:
            cancel_timeout = 10.0
        exit_handler: Callable[[str], None] | None = None
        try:
            _ = ex.list_active_child_orders(count=1)
            if dry_run:
                logger.info(
                    f"live(dry-run): exchange OK product={product_code} strategy={summary_name} strategies={strategy_list}"
                )
                logger.info("live(dry-run): ここでは発注しません（導線の疎通確認だけ）")
            else:
                # 次ステップで：ここにイベントループ＋戦略呼び出し＋TTL取消などを実装
                pass
            if not bool(getattr(cfg, "cancel_all_on_start", True)):  # 何をするか：起動時に全取消しない運用なら、残っている注文を監視にシード
                _seed_live_orders_from_active(ex, live_orders)

            mid_hist = deque(maxlen=2048)  # 何をするか：ミッド価格の履歴（30秒変化ガード用）
            max_bp = getattr(getattr(cfg, "guard", None), "max_mid_move_bp_30s", None)  # 何をするか：ミッド変化ガードの閾値
            inv_limit = getattr(getattr(cfg, "risk", None), "max_inventory", None)  # 何をするか：在庫上限
            inv_eps_default = 0.0 if inv_limit is None else max(0.0, float(inv_limit) * 0.01)
            inventory_eps = float(getattr(getattr(cfg, "risk", None), "inventory_eps", inv_eps_default))
            eff_inv_limit = None if inv_limit is None else max(0.0, float(inv_limit) - float(inventory_eps))

            canary_m = getattr(cfg, "canary_minutes", None)  # 何をするか：実運転の時間制限（分）。None/0なら無効
            fee_bps = float(getattr(getattr(cfg, "fees", None), "bps", 0.0) or 0.0)  # 何をするか：手数料(bps)を設定から取得（無ければ0.0）
            dry_limit_s = getattr(cfg, "dry_run_max_sec", None)  # 何をするか：dry-runの自動停止（秒）。Noneなら無効

            orders_cfg = getattr(cfg, "orders", None)
            max_inflight = getattr(orders_cfg, "max_inflight", None)
            gate_cfg = getattr(cfg, "gate", None)
            max_inflight_per_key = getattr(gate_cfg, "max_inflight_per_key", None)
            limit_qty = getattr(getattr(cfg, "risk", None), "limit_qty", None)
            if max_inflight is None:
                max_inflight = getattr(getattr(cfg, "risk", None), "max_active_orders", None)
            max_spread_bp = getattr(getattr(cfg, "guard", None), "max_spread_bp", None)  # 何をするか：スプレッドが広すぎる時の停止しきい値(bp)
            stale_ms = int(getattr(getattr(cfg, "guard", None), "max_stale_ms", 3000))  # 何をするか：WS/板の鮮度しきい値(ms)。超えたら新規を止める
            board_reconnect_after_s = float(getattr(getattr(cfg, "guard", None), "board_reconnect_after_s", 5.0) or 0.0)  # 何をするか：bad_book/stale_dataがこの秒数以上続いたら板ストリームを再接続する
            book_warmup_s = float(getattr(getattr(cfg, "guard", None), "book_warmup_s", 2.0) or 0.0)  # 何をするか：接続直後に板の充填を待つ猶予秒数
            fills_stale_after_s = float(getattr(getattr(cfg, "guard", None), "fills_stale_after_s", 15.0) or 0.0)  # 何をするか：この秒数 fill が見えなければ欠損疑いとして扱う
            fills_replay_cooldown_s = float(getattr(getattr(cfg, "guard", None), "fills_replay_cooldown_s", 30.0) or 0.0)  # 何をするか：HTTPバックフィル連打のクールダウン
            last_ev_at = _now_utc()  # 何をするか：直近イベントの時刻（鮮度ガードの基準）
            book_warmup_until: datetime | None = None  # 何をするか：板ウォームアップ猶予の期限
            connection_started_at: datetime | None = None  # 何をするか：この接続の開始時刻（判定リセット用）
            warmup_guard_s = max(book_warmup_s, 5.0)  # 何をするか：初期のbad_book/stale判定を必ず緩める最低猶予秒数
            snapshot_seen = False  # 何をするか：板スナップショットを受信済みかのフラグ
            hb_path = Path("logs/runtime/heartbeat.ndjson")  # 何をするか：ハートビートの出力先
            maint_prev = None  # 何をするか：メンテ窓の前回状態（enter/exit検知用）
            fund_prev = None   # 何をするか：Funding窓（計算or授受）の前回状態（enter/exit検知用）
            maint_csv = Path("logs/events/maintenance.csv")  # 何をするか：メンテ窓のイベントCSVのパス
            fund_csv = Path("logs/events/funding.csv")       # 何をするか：Funding窓のイベントCSVのパス
            maint_now = False  # 何をするか：参照前の未定義を避けるための初期値（後で毎周回の判定で上書き）
            fund_now = False   # 何をするか：同上（Funding窓も先にFalseで用意しておく）
            margin_leverage = float(getattr(getattr(cfg, "risk", None), "margin_leverage", 2.0) or 2.0)  # 何をするか：証拠金必要額の概算レバレッジ
            margin_buffer = float(getattr(getattr(cfg, "risk", None), "margin_buffer", 0.9) or 0.9)  # 何をするか：余力に掛ける安全係数
            margin_precheck_safety = float(getattr(getattr(cfg, "risk", None), "margin_precheck_safety", 1.0) or 1.0)  # 証拠金試算の安全倍率
            collateral_refresh_s = float(getattr(getattr(cfg, "risk", None), "collateral_refresh_sec", 5.0) or 5.0)  # 何をするか：証拠金情報を再取得する間隔
            collateral_cache: dict[str, Any] | None = None
            collateral_last_fetch: datetime | None = None
            margin_last_avail: float | None = None  # 直近の余剰証拠金（プリチェック用メモ）
            margin_last_required: float | None = None  # 直近の必要証拠金（プリチェック用メモ）

            hb_interval_s = int(getattr(getattr(cfg, "logging", None), "heartbeat_status_sec", 5))  # 何をするか：ステータス心拍の間隔（秒）
            hb_next = _now_utc() + timedelta(seconds=hb_interval_s)  # 何をするか：次に出す時刻

            order_log = OrderLog("logs/orders/order_log.parquet", mirror_ndjson="logs/orders/order_log.ndjson")  # 何をするか：発注/取消イベントを記録
            trade_log = TradeLog("logs/trades/trade_log.parquet", mirror_ndjson="logs/trades/trade_log.ndjson")  # 何をするか：約定明細とPnLを記録
            decision_log = DecisionLog("logs/analytics/decision_log.parquet", mirror_ndjson="logs/analytics/decision_log.ndjson")  # 何をするか：意思決定（features/decision）を記録する
            events_dir = Path("logs/events")  # 何をするか：窓イベントCSVの保存先
            events_dir.mkdir(parents=True, exist_ok=True)  # 何をするか：フォルダを作成
            (events_dir / "maintenance.csv").touch(exist_ok=True)  # 何をするか：ファイルを事前作成
            (events_dir / "funding_schedule.csv").touch(exist_ok=True)  # 何をするか：ファイルを事前作成
            # 初期は None のまま（変化だけ検出）

            hb_path.parent.mkdir(parents=True, exist_ok=True)  # 何をするか：保存先フォルダを作る

            exit_handler = _mk_atexit(
                ex,
                hb_path,
                product_code,
                live_orders,
                dry_run=dry_run,
                max_retry=cancel_retry,
                sleep_sec=cancel_sleep_sec,
                timeout=cancel_timeout,
                size_cfg=getattr(cfg, "size", None),
            )  # 何をするか：最終キャンセルとstop心拍を単一路に集約
            atexit.register(exit_handler)  # 何をするか：プロセス終了時も同じ入口を通す
            stop_event = Event()  # 何をするか：シグナルや例外で立てる停止フラグ

            halted = False  # 何をするか：Kill 到達後は新規を出さない
            avg0, net0, ok0 = _seed_inventory_and_avg_px(ex)  # 何をするか：起動時の建玉(数量/平均建値)を反映してPnL状態を初期化
            pnl_state = {"pos": float(net0 or 0.0) if ok0 else 0.0, "avg_px": avg0 if ok0 else None}
            last_exchange_pos = float(net0 or 0.0) if ok0 else None
            inv_sync_ok = bool(ok0)
            daily_R, R_HWM = 0.0, 0.0  # 何をするか：日次実現PnLとその高値（HWM）
            _jst = timezone(timedelta(hours=9))  # 何をするか：JST（Killの日次境界に使用）
            jst_day = _now_utc().astimezone(_jst).date()  # 何をするか：当日のJST日付
            kill_cfg = getattr(getattr(cfg, "risk", None), "kill", None)  # 何をするか：Killしきい値
            min_tx_ms = int(getattr(getattr(cfg, "tx", None), "min_interval_ms", 100))  # 何をするか：新規注文を連続で送らない最小間隔（ms）
            place_dedup_ms = int(getattr(getattr(cfg, "tx", None), "place_dedup_ms", 300))  # 何をするか：同一(side×price×tag)の連打をこのms以内ならスキップ
            last_place: dict[str, datetime] = {}  # 何をするか：直近に出した(side|price|tag)→時刻 を覚える
            _last_tx_at = _now_utc() - timedelta(milliseconds=min_tx_ms)  # 何をするか：直近の送信時刻（初期は「今−間隔」で即送れる状態）
            fee_bps = float(getattr(getattr(cfg, "fees", None), "bps", 0.0))  # 何をするか：手数料のbps設定（未指定は0.0）
            canary_min = (10**9 if dry_run else int(getattr(cfg, "canary_minutes", 0) or 0))  # DRYは無効（分）
            throttle_until: datetime | None = None  # 何をするか：レート制限に当たった時のクールダウン期限
            auto_reduce_last_ts: datetime | None = None  # 何をするか：auto_reduceのクールダウン共有用
            inv_resync_ms = int(getattr(getattr(getattr(cfg, "risk", None), "auto_reduce", None), "resync_ms", 5000))  # 何をするか：定期的に取引所の建玉とPnL状態を同期する間隔(ms)
            last_inv_sync_at: datetime | None = _now_utc()  # 何をするか：直近の建玉再同期時刻
            margin_err_window_s = 10  # 何をするか：-205（証拠金不足）をカウントする時間窓（秒）
            margin_err_limit = 3      # 何をするか：時間窓内にこの回数 -205 が出たら新規を一時停止する
            margin_cooldown_s = 60    # 何をするか：-205 連発時に新規を止める時間（秒）
            margin_block_until: datetime | None = None  # 何をするか：-205 連発により新規を止める期限
            margin_err_log: deque[datetime] = deque()  # 何をするか：直近の -205 発生時刻を覚える
            reduce_retry_until: datetime | None = None  # 何をするか：自動決済IOCの連続送信を抑制する
            reduce_fail_count = 0  # 何をするか：reduce-onlyが連続で失敗した回数（決済不能の検知用）
            reduce_fail_limit = 3   # 何をするか：この回数連続で失敗したらHaltして手動介入を促す
            rate_limit_hits: deque[datetime] = deque()
            rate_limit_window_s = 60
            rate_limit_limit = 5
            seen_exec_ids: set[int] = set()
            bad_book_since: datetime | None = None
            stale_since: datetime | None = None
            last_exec_id: int | None = _prime_execution_cursor(ex)
            last_exchange_pos: float | None = None
            inv_sync_ok: bool = False
            fills_last_seen: datetime | None = _now_utc()  # 何をするか：直近で“見えている”約定の時刻（WS/HTTP問わず）
            fills_unhealthy_since: datetime | None = None  # 何をするか：欠損疑いに落ちた開始時刻
            fills_last_replay: datetime | None = None  # 何をするか：HTTPバックフィルを最後に走らせた時刻

            try:
                _ = ex.list_active_child_orders(count=1)  # 何をするか：認証/権限・疎通の最小チェック（実発注なし）
            except AuthError as e:
                logger.error(f"live: auth failed → halt: {e}")  # 何をするか：理由をrun.logへ
                _hb_write(hb_path, event="kill", ts=_now_utc().isoformat(), reason="auth")  # 何をするか：心拍に“auth停止”を記録
                exit_reason = "auth"
                return  # 何をするか：安全に終了（実運転に入らない）

            sys.excepthook = _mk_excepthook(exit_handler, stop_event, sys.excepthook)  # 何をするか：未捕捉例外で停止フラグを立てて終了シーケンスへ流す
            if getattr(cfg, "cancel_all_on_start", True):  # 何をするか：起動時の安全装置（全取消）設定を確認
                if dry_run:  # 何をするか：dry-run中は実際に取消しを実行しない（安全にスキップ）
                    logger.info("live(dry-run): startup safety — skip cancel_all")  # 何をするか：スキップした事実をrun.logへ記録
                else:
                    cancel_all_with_retry(
                        ex,
                        product_code,
                        max_retry=cancel_retry,
                        sleep_sec=cancel_sleep_sec,
                        timeout=cancel_timeout,
                    )  # 何をするか：本運転のみ、残っている全ての子注文をリトライ付きで取消
                    logger.info("live: startup safety — cancel_all issued")  # 何をするか：実行した事実をrun.logへ記録


            started_at = _now_utc()  # 何をするか：Canary の開始時刻
            _JST = timezone(timedelta(hours=9))  # 何をするか：JSTのタイムゾーン
            day_start_utc = _now_utc().astimezone(_JST).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)  # 何をするか：当日のJST=00:00（UTCに直した時刻）

            def _ensure_collateral(now: datetime, force: bool = False) -> tuple[dict[str, Any] | None, datetime | None]:
                """何をするか：必要に応じて get_collateral を呼び、キャッシュと取得時刻を返す"""
                nonlocal collateral_cache, collateral_last_fetch
                try:
                    if force or collateral_cache is None or collateral_last_fetch is None or (now - collateral_last_fetch).total_seconds() >= collateral_refresh_s:
                        collateral_cache = ex.get_collateral()
                        collateral_last_fetch = now
                except Exception as e:
                    logger.warning(f"collateral fetch failed: {e}")
                return collateral_cache, collateral_last_fetch

            def _on_signal(signum, frame) -> None:
                logger.warning(f"signal received: {signum} → halt requested")  # 何をするか：受信をログ
                stop_event.set()  # 何をするか：イベントループに停止を伝える

            def _aggressive_reduce_inventory(now: datetime) -> None:
                """何をするか：close-only / margin_guard 時に在庫を削るための reduce-only IOC を複数回トライ"""
                nonlocal reduce_retry_until, reduce_fail_count
                if dry_run or throttled:
                    return
                if reduce_retry_until is not None and now < reduce_retry_until:
                    return
                q_abs = abs(float(pnl_state.get("pos", 0.0) or 0.0))
                if q_abs <= 0.0:
                    return
                side = "SELL" if float(pnl_state.get("pos", 0.0)) > 0 else "BUY"
                size_cfg = getattr(cfg, "size", None)
                try:
                    base_sz = float(getattr(size_cfg, "default", q_abs) or q_abs)
                except Exception:
                    base_sz = q_abs
                base_sz = min(q_abs, base_sz) if base_sz > 0 else q_abs
                attempts: list[float] = []
                cur = base_sz
                for _ in range(3):
                    s = cur if cur > 0 else q_abs
                    attempts.append(max(s, 0.0))
                    cur *= 0.5
                attempted = False
                for sz_try in attempts:
                    if sz_try <= 0.0:
                        continue
                    attempted = True
                    norm_sz, reason = _calc_reduce_order_size(
                        pnl_state.get("pos", 0.0),
                        sz_try,
                        size_cfg=size_cfg,
                    )
                    if norm_sz is None:
                        logger.debug(f"auto reduce-only skip size={sz_try:.6f} reason={reason}")
                        continue
                    try:
                        ex.place_ioc_reduce_only(side=side, size=norm_sz, product_code=product_code, tag="auto_close_only")
                        logger.info(f"auto reduce-only IOC sent side={side} sz={norm_sz}")
                        _hb_write(hb_path, event="reduce_only", ts=now.isoformat(), side=side, sz=norm_sz, reason="auto_close_only")
                        # 失敗しても次の試行へ進む（約定確認は次ループで反映）
                        reduce_fail_count = 0
                    except Exception as e:
                        logger.warning(f"auto reduce-only IOC failed: {e}")
                        reduce_fail_count += 1
                if attempted:
                    reduce_retry_until = now + timedelta(seconds=5)
                if reduce_fail_count >= reduce_fail_limit:
                    logger.error(f"reduce-only flatten failed {reduce_fail_count} times → halt for manual action")
                    _hb_write(hb_path, event="kill", ts=_now_utc().isoformat(), reason="reduce_only_failed", attempts=reduce_fail_count)
                    stop_event.set()
                    return

            def _mark_fill_seen(ts: datetime | None = None) -> None:
                """何をするか：fill を観測した時刻を記録し、欠損疑いフラグをクリアする"""
                nonlocal fills_last_seen, fills_unhealthy_since
                fills_last_seen = ts or _now_utc()
                fills_unhealthy_since = None

            signal.signal(signal.SIGINT, _on_signal)   # 何をするか：Ctrl+C（SIGINT）で停止
            signal.signal(signal.SIGTERM, _on_signal)  # 何をするか：SIGTERM（停止要求）で停止

            ob = OrderBook()  # 何をするか：ローカル板（戦略の入力）を用意
            if book_warmup_s > 0.0:
                book_warmup_until = _now_utc() + timedelta(seconds=book_warmup_s)  # 何をするか：接続直後は板未充填を許容する猶予を持つ
            cfg_payload = _safe_config_dict(cfg)
            if not cfg_payload and isinstance(cfg, Mapping):
                cfg_payload = dict(cfg)
            if strategy_list:
                cfg_payload["strategies"] = list(strategy_list)
            cfg_features = getattr(cfg, "features", None)
            effective_strategy_cfg = strategy_cfg
            if effective_strategy_cfg is None:
                if isinstance(cfg, Mapping):
                    effective_strategy_cfg = cfg.get("strategy_cfg")
                else:
                    effective_strategy_cfg = getattr(cfg, "strategy_cfg", None)
            if effective_strategy_cfg is None:
                effective_strategy_cfg = cfg_payload.get("strategy_cfg")
            strat = build_strategy_from_cfg(
                cfg_payload,
                strategy_cfg=effective_strategy_cfg,
            )  # 何をするか：本番起動でも複数戦略を1プロセスで束ねて回す
            summary_name = strat.name

            strategy_names = [
                getattr(child, "strategy_name", getattr(child, "name", "unknown"))
                for child in getattr(strat, "children", [])
            ] or [getattr(strat, "strategy_name", getattr(strat, "name", "unknown"))]
            strategy_list = strategy_names
            summary_name = getattr(strat, "strategy_name", summary_name)
            logger.info(
                f"live: starting loop product={product_code} strategy={summary_name} strategies={strategy_list}"
            )  # 何をするか：起動ログ
            try:
                analytics_dir = Path("logs/analytics")
                analytics_dir.mkdir(parents=True, exist_ok=True)
                env_value = getattr(cfg, "env", None)
                if env_value is None and isinstance(cfg, Mapping):
                    env_value = cfg.get("env")
                if env_value is None:
                    env_value = cfg_payload.get("env")
                entry: dict[str, Any] = {
                    "ts": _now_utc().isoformat(),
                    "env": env_value,
                    "product": product_code,
                    "strategies": list(strategy_list),
                }
                features_payload = _safe_config_dict(cfg_features)
                if features_payload:
                    entry["features"] = features_payload
                strategy_overrides = _strategy_cfg_overrides(effective_strategy_cfg, strategy_list)
                if strategy_overrides:
                    entry["strategy_cfg"] = strategy_overrides
                cfg_log_path = analytics_dir / "strategy_cfg.ndjson"
                with cfg_log_path.open("ab") as f:
                    f.write(orjson.dumps(entry))
                    f.write(b"\n")
            except Exception as e:
                logger.warning(f"live: strategy_cfg analytics append failed: {e}")
            else:
                logger.info("live: strategy_cfg analytics appended")
            _hb_write(
                hb_path,
                event="start",
                ts=_now_utc().isoformat(),
                reason="launch",
                product=product_code,
                strategy=summary_name,
                strategies=strategy_list,
            )  # 何をするか：起動の合図をrun.logと揃えて心拍に記録
            # 起動直後にHTTPで約定履歴を取り寄せ、ポジション(Q/A/R/F)の抜けを埋める
            class _ReplayPosition:
                def __init__(self, last_id: int | None):
                    self.last_fill_id = last_id

                def apply_fill(self, fill: dict) -> None:
                    nonlocal last_exec_id, pnl_state, fund_now, maint_now
                    try:
                        eid = int(fill.get("id"))
                        if eid is not None:
                            last_exec_id = eid if last_exec_id is None or eid > last_exec_id else last_exec_id
                            self.last_fill_id = last_exec_id
                    except Exception:
                        pass
                    side = str(fill.get("side", "")).upper()
                    px = float(fill.get("price", 0.0) or 0.0)
                    sz = float(fill.get("size", 0.0) or 0.0)
                    if not side or px <= 0.0 or sz <= 0.0:
                        return
                    tag = str(fill.get("child_order_acceptance_id") or fill.get("tag") or "")
                    realized = _apply_fill_and_pnl(pnl_state, side, px, sz)
                    fee = px * sz * (fee_bps / 10000.0)
                    realized -= fee
                    trade_log.add(
                        ts=_now_utc().isoformat(),
                        side=side,
                        px=px,
                        sz=sz,
                        fee=fee,
                        pnl=realized,
                        strategy=summary_name,
                        tag=tag,
                        inventory_after=pnl_state.get("pos", 0.0),
                        window_funding=fund_now,
                        window_maint=maint_now,
                    )
                    order_log.add(ts=_now_utc().isoformat(), action="fill", tif=None, ttl_ms=None, px=px, sz=sz, reason=tag)
                    _hb_write(hb_path, event="fill", ts=_now_utc().isoformat(), side=side, px=px, sz=sz, pnl=realized, tag=tag)

            # このブロックは、起動時にHTTP約定履歴をリプレイしてポジション(Q/A/R/F)の抜けを埋める処理です
            try:
                replay_now = _now_utc()
                maint_now = _in_maintenance(replay_now, cfg)
                fund_now = (_in_funding_calc(replay_now, cfg) or _in_funding_transfer(replay_now, cfg))
                replay_position = _ReplayPosition(last_exec_id)
                applied = asyncio.run(
                    run_http_replay_for_live_position(
                        exchange=ex,
                        product_code=product_code,
                        position=replay_position,
                        page_size=500,
                        logger=logger,
                    )
                )
                if replay_position.last_fill_id is not None:
                    last_exec_id = replay_position.last_fill_id
                if applied > 0:
                    _mark_fill_seen(replay_now)
                # このログは、起動時のHTTPリプレイが正常終了したことをrun.logに残して「リプレイ完了」を一目で確認するための行です
                logger.info(f"live: HTTP replay for position completed successfully (applied={applied})")
            except Exception:
                # HTTPリプレイに失敗したらログを残して起動を中止する
                logger.exception("live: HTTP replay for position failed; aborting startup")
                raise

            dbg_event_count = 0  # 何をするか：接続後の受信イベントを最初の数件だけログに出すためのカウンタ
            while True:
                if stop_event.is_set():
                    exit_reason = "signal"
                    break
                force_reconnect = False
                for ev in _stream_with_reconnect(product_code, hb_path, stop_event=stop_event):  # 何をするか：WSが切れても自動再接続しながらイベントを処理（停止フラグも監視）
                    try:
                        logger.info(f"live_loop recv ev channel={ev.get('channel')}")
                    except Exception:
                        logger.info(f"live_loop recv ev={ev}")
                    now = _now_utc()  # 何をするか：UTCの現在時刻
                    try:
                        logger.info(
                            f"trace: loop head force_reconnect={force_reconnect} throttle_until={throttle_until} "
                            f"throttled={(throttle_until is not None and now < throttle_until)}"
                        )
                    except Exception:
                        pass
                    if throttle_until and now >= throttle_until:
                        throttle_until = None  # 何をするか：クールダウンが明けたら解除

                    throttled = throttle_until is not None and now < throttle_until  # 何をするか：現在クールダウン中か判定
                    if (not dry_run) and canary_m and (now - started_at).total_seconds() >= float(canary_m) * 60.0:  # 何をするか：実運転のみ時間超過で停止
                        logger.info("live: canary time limit reached → halt")  # 何をするか：停止理由をrun.logへ
                        _hb_write(hb_path, event="kill", ts=now.isoformat(), reason="canary", runtime_sec=int((now - started_at).total_seconds()))  # 何をするか：心拍に“canary停止”を記録
                        exit_reason = "canary"
                        break  # 何をするか：ループを抜けて終了シーケンスへ

                    if dry_run and dry_limit_s and (now - started_at).total_seconds() >= float(dry_limit_s):  # 何をするか：dry-runの時間制限を超えたら終了
                        logger.info("live(dry-run): time limit reached → halt")  # 何をするか：終了理由をrun.logに記録
                        _hb_write(hb_path, event="kill", ts=now.isoformat(), reason="dryrun_done", runtime_sec=int((now - started_at).total_seconds()))  # 何をするか：心拍に終了理由と経過秒を記録
                        exit_reason = "dryrun_done"
                        break  # 何をするか：ループを抜けて終了シーケンスへ

                    # 何をするか：WS接続ステータスイベントを受けたらリセット＋欠損約定をRESTで追う
                    if ev.get("channel") == "__ws_status__" and ev.get("event") in ("ws_reconnect", "ws_connected", "ws_error"):
                        # 何をするか：接続イベントごとに判定カウンタをリセットし、ウォームアップ猶予を再設定
                        if book_warmup_s > 0.0:
                            book_warmup_until = now + timedelta(seconds=book_warmup_s)
                        else:
                            book_warmup_until = None
                        connection_started_at = now
                        bad_book_since = None
                        stale_since = None
                        last_ev_at = now
                        snapshot_seen = False
                        if ev.get("event") == "ws_error":
                            # ws_error は欠損バックフィル不要、次のイベントまで待つ
                            continue
                        maint_now = _in_maintenance(now, cfg)
                        fund_now = (_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg))
                        last_exec_id = _backfill_executions(
                            ex,
                            last_exec_id=last_exec_id,
                            seen_exec_ids=seen_exec_ids,
                            pnl_state=pnl_state,
                            trade_log=trade_log,
                            order_log=order_log,
                            live_orders=live_orders,
                            fee_bps=fee_bps,
                            summary_name=summary_name,
                            fund_now=fund_now,
                            maint_now=maint_now,
                            hb_path=hb_path,
                            on_fill_seen=lambda: _mark_fill_seen(_now_utc()),
                        )
                        continue

                    # まずイベント内容をローカル板に反映（板が空のまま no_snapshot 判定になるのを防ぐ）
                    if dbg_event_count < 5:
                        logger.info(f"recv_event channel={ev.get('channel')} keys={list(ev.keys())}")
                        dbg_event_count += 1

                    ob.update_from_event(ev)  # 何をするか：ローカル板にイベントを反映（例外はそのまま吐く）
                    try:
                        logger.info(f"after_update channel={ev.get('channel')} best_bid={getattr(ob,'best_bid',None)} best_ask={getattr(ob,'best_ask',None)}")
                    except Exception:
                        pass
                    last_ev_at = now  # 何をするか：イベント受信時刻を必ず更新（stale判定の基準）

                    ch = str(ev.get("channel", "")).lower()
                    if "board_snapshot" in ch:
                        snapshot_seen = True

                    bid = _best_px(getattr(ob, "best_bid", None))  # 何をするか：オブジェクト/辞書/数値を価格(float)に正規化
                    ask = _best_px(getattr(ob, "best_ask", None))  # 何をするか：同上
                    has_book = (bid is not None) and (ask is not None) and (ask > bid)

                    # 受信した値を毎回ログ（診断優先）
                    if ch.startswith("lightning_board"):
                        logger.info(f"board_event channel={ch} bid={bid} ask={ask} has_book={has_book} snapshot_seen={snapshot_seen}")
                        try:
                            logger.info(
                                f"trace: after_update state bid={bid} ask={ask} has_book={has_book} "
                                f"snapshot_seen={snapshot_seen} maint_prev={maint_prev} fund_prev={fund_prev}"
                            )
                        except Exception:
                            pass

                    try:
                        dbg_seen = getattr(ob, "_dbg_seen", 0)
                    except Exception:
                        dbg_seen = 0
                    if dbg_seen < 3:
                        logger.debug(f"board_event channel={ch} bid={bid} ask={ask} has_book={has_book} snapshot_seen={snapshot_seen}")
                        try:
                            setattr(ob, "_dbg_seen", dbg_seen + 1)
                        except Exception:
                            pass

                    # 以降どのガードで止まるかを追跡するためのトレース
                    try:
                        logger.info(
                            f"trace: pre_guard warmup_guard_s={warmup_guard_s} "
                            f"connection_started_at={connection_started_at} now={now} "
                            f"snapshot_seen={snapshot_seen} has_book={has_book}"
                        )
                    except Exception:
                            pass

                    # 何をするか：bid>=ask の逆転板はこのイベントを無視しつつ bad_book_since を進める（一定時間で再接続）
                    bid_sz = getattr(getattr(ob, "best_bid", None), "size", 0.0) or 0.0
                    ask_sz = getattr(getattr(ob, "best_ask", None), "size", 0.0) or 0.0
                    if bid is not None and ask is not None and ask <= bid:
                        # サイズ0や欠損の場合は逆転扱いにしない（実質的に板欠損として待機）
                        if bid_sz <= 0 or ask_sz <= 0:
                            logger.info(
                                f"skip: inverted_book_zero_size bid={bid} bid_sz={bid_sz} "
                                f"ask={ask} ask_sz={ask_sz} → wait"
                            )
                            stale_since = None
                            bad_book_since = None
                            continue
                        bad_book_since = bad_book_since or now
                        elapsed_bad = int((now - bad_book_since).total_seconds()) if bad_book_since else 0
                        logger.info(f"skip: inverted_book bid={bid} ask={ask} elapsed_bad={elapsed_bad}s")
                        if board_reconnect_after_s > 0.0 and (now - bad_book_since).total_seconds() >= board_reconnect_after_s:
                            logger.warning(
                                f"live: inverted_book persisted {int((now - bad_book_since).total_seconds())}s "
                                f"→ reconnect board stream"
                            )
                            _hb_write(
                                hb_path,
                                event="pause",
                                ts=now.isoformat(),
                                reason="board_reconnect_inverted_book",
                                duration_s=int((now - bad_book_since).total_seconds()),
                            )
                            force_reconnect = True
                            break
                        continue

                    # 何をするか：現在の窓状態を判定（メンテ／Funding計算・授受のどれかでもTrue）
                    maint_now = _in_maintenance(now, cfg)
                    try:
                        logger.info(f"trace: guard_check maint_now={maint_now} fund_now_pending=?")
                    except Exception:
                        pass
                    if maint_now:  # 何をするか：メンテ窓の間は新規発注を止める
                        logger.info("guard: maintenance window → skip evaluate")
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="maintenance")
                        continue  # 何をするか：この周回は新規パートへ進まない
                    fund_now = (_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg))  # 何をするか：Funding窓の“現在”を先に計算
                    if fund_now:  # 何をするか：Funding（計算/授受）窓の間は新規発注を止める
                        logger.info("guard: funding window → skip evaluate")
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="funding")
                        continue  # 何をするか：この周回は新規パートへ進まない
                    else:
                        try:
                            logger.info(
                                f"trace: guard_pass maint_now={maint_now} fund_now={fund_now} snapshot_seen={snapshot_seen} has_book={has_book}"
                            )
                        except Exception:
                            pass
                    try:
                        logger.info("trace: before_fills_entry (guard passed)")
                    except Exception:
                        pass

                    # 何をするか：窓の“出入り”を検知してCSVに1行追記（enter/exit）
                    if (maint_prev is not None) and (maint_now != maint_prev):
                        _csv_event_write(maint_csv, {"ts": now.isoformat(), "event": ("enter" if maint_now else "exit")})
                    if (fund_prev is not None) and (fund_now != fund_prev):
                        _csv_event_write(fund_csv, {"ts": now.isoformat(), "event": ("enter" if fund_now else "exit")})

                    # 何をするか：次回の比較用に前回状態を更新
                    maint_prev, fund_prev = maint_now, fund_now

                    # 何をするか：接続直後の一定期間はstale/bad_bookを判定しない（ただし板は更新済みなので状態は進む）
                    if connection_started_at and (now - connection_started_at).total_seconds() < warmup_guard_s:
                        bad_book_since = None
                        stale_since = None
                        logger.info(
                            f"skip: warmup_guard secs={(now - connection_started_at).total_seconds():.2f} "
                            f"warmup={warmup_guard_s:.2f} snapshot={snapshot_seen}"
                        )
                        continue
                    else:
                        try:
                            logger.info(
                                f"trace: warmup_passed secs={(now - (connection_started_at or now)).total_seconds():.2f} "
                                f"warmup={warmup_guard_s:.2f} snapshot={snapshot_seen}"
                            )
                        except Exception:
                            pass

                    # 何をするか：スナップショット未取得の間は bad_book/stale を判定しない（一定時間で強制再接続）
                    has_bid = _best_px(getattr(ob, "best_bid", None)) is not None
                    has_ask = _best_px(getattr(ob, "best_ask", None)) is not None
                    if not snapshot_seen:
                        bid_tmp = _best_px(getattr(ob, "best_bid", None))
                        ask_tmp = _best_px(getattr(ob, "best_ask", None))
                        if has_bid and has_ask and ask_tmp is not None and bid_tmp is not None and ask_tmp > bid_tmp:
                            snapshot_seen = True
                        else:
                            bad_book_since = None
                            stale_since = None
                            if board_reconnect_after_s > 0.0 and connection_started_at and (now - connection_started_at).total_seconds() >= board_reconnect_after_s:
                                logger.warning(f"live: no_snapshot_yet persisted {int((now - connection_started_at).total_seconds())}s → reconnect board stream")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="board_reconnect_no_snapshot", duration_s=int((now - connection_started_at).total_seconds()))
                                force_reconnect = True
                                break
                            logger.info(
                                f"skip: no_snapshot_yet has_bid={has_bid} has_ask={has_ask} "
                                f"bid_tmp={bid_tmp} ask_tmp={ask_tmp}"
                            )
                            continue

                    # 何をするか：板が逆転している・欠落している間は評価をスキップし、一定時間続いたら再接続
                    if not has_book:
                        if bad_book_since is None:
                            bad_book_since = now
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="bad_book")
                        if board_reconnect_after_s > 0.0 and (now - bad_book_since).total_seconds() >= board_reconnect_after_s:
                            logger.warning(
                                f"live: bad_book persisted {int((now - bad_book_since).total_seconds())}s "
                                f"→ reconnect board stream bid={bid} ask={ask}"
                            )
                            _hb_write(
                                hb_path,
                                event="pause",
                                ts=now.isoformat(),
                                reason="board_reconnect_bad_book",
                                duration_s=int((now - bad_book_since).total_seconds()),
                            )
                            force_reconnect = True
                            break
                        logger.info(
                            f"skip: bad_book has_book={has_book} bid={bid} ask={ask} "
                            f"elapsed={int((now - bad_book_since).total_seconds())}s"
                        )
                        continue
                    else:
                        bad_book_since = None

                    # stale_data 判定：スナップショット取得前は判定しない
                    if stale_ms and (now - last_ev_at).total_seconds() * 1000.0 >= stale_ms:
                        if stale_since is None:
                            stale_since = now
                        logger.info(f"guard: stale_data gap={int((now - last_ev_at).total_seconds()*1000)}ms ? {stale_ms}ms → skip evaluate")
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="stale_data")
                        if board_reconnect_after_s > 0.0 and (now - stale_since).total_seconds() >= board_reconnect_after_s:
                            logger.warning(f"live: stale_data persisted {int((now - stale_since).total_seconds())}s → reconnect board stream")
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="board_reconnect_stale", gap_ms=int((now - last_ev_at).total_seconds() * 1000), duration_s=int((now - stale_since).total_seconds()))
                            force_reconnect = True
                            break
                        continue
                    else:
                        stale_since = None

                    if now >= day_start_utc + timedelta(days=1):  # 何をするか：JSTで新しい日になったか？
                        day_start_utc = now.astimezone(_JST).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)  # 何をするか：新しい“今日”の起点をセット
                        daily_R, R_HWM = 0.0, 0.0  # 何をするか：日次PnLとその日HWMをリセット
                        logger.info("live: JST day rollover → reset daily PnL/HWM")  # 何をするか：run.logに書く
                        _hb_write(hb_path, event="start", ts=now.isoformat(), reason="day_reset")  # 何をするか：心拍にも“日次リセット”を記録

                    if stop_event.is_set():  # 何をするか：停止フラグが立っていたら安全停止
                        exit_reason = "signal"
                        _hb_write(hb_path, event="kill", ts=now.isoformat(), daily_pnl_jpy=daily_R, dd_jpy=R_HWM - daily_R, reason="signal")  # 何をするか：停止を1行JSONで記録
                        break  # 何をするか：イベントループを終了

                    # Canary 停止は (not dry_run) and canary_m の判定に統一

                    # 何をするか：30秒間のミッド価格変化(bp)がしきい値以上なら、その周回は新規発注を止める
                    if max_bp is not None:
                        mid = (bid + ask) / 2.0
                        mid_hist.append((now, mid))  # 何をするか：時刻とミッドを履歴に追加

                        cutoff = now - timedelta(seconds=30)  # 何をするか：30秒窓の下限
                        while mid_hist and mid_hist[0][0] < cutoff:
                            mid_hist.popleft()  # 何をするか：窓から外れた古いデータを捨てる

                        base = mid_hist[0][1] if mid_hist else None  # 何をするか：窓の最古のミッド
                        if (base is not None) and (base > 0.0):
                            move_bp = abs((mid - base) / base) * 10000.0
                            if move_bp >= float(max_bp):
                                logger.info(f"guard: mid_move {move_bp:.1f}bp ? {float(max_bp)}bp (30s) → skip evaluate")  # 何をするか：理由をrun.logに記録
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="mid_move", move_bp=move_bp)  # 何をするか：心拍にも停止を記録
                                continue  # 何をするか：この周回は新規発注パートに進まない

                    last_ev_at = now  # 何をするか：イベントを受け取れたので鮮度の基準時刻を更新
                    mid = _mid_from_ob(ob)  # 何をするか：最新のミッド価格を得る
                    bp_30s = None  # 何をするか：30秒変化の大きさ（bp）を一時的に保持
                    if mid is not None:
                        mid_hist.append((now, mid))  # 何をするか：ミッドの履歴を追加
                        older_than = now - timedelta(seconds=35)  # 何をするか：古すぎる履歴（35秒超）は捨てる
                        while mid_hist and mid_hist[0][0] < older_than:
                            mid_hist.popleft()
                        cutoff = now - timedelta(seconds=30)  # 何をするか：30秒前の基準点を探す
                        ref = None
                        for t, m in mid_hist:
                            if t <= cutoff:
                                ref = m
                            else:
                                break
                        paused_mid = False
                        if (max_bp is not None) and (ref is not None) and (ref > 0):
                            bp_30s = abs((mid - ref) / ref) * 10000.0
                            paused_mid = bp_30s >= float(max_bp)
                    else:
                        paused_mid = False

                    if paused_mid:  # 何をするか：ミッド変化が大きい間は新規発注を止める
                        logger.info(f"guard: midmove_guard Δ30s={bp_30s:.1f}bp ? {float(max_bp)}bp → skip evaluate")  # 何をするか：理由をrun.logに記録
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="midmove_guard")  # 何をするか：心拍にも停止を記録
                        continue  # 何をするか：この周回は新規発注パートに進まない

                    if now >= hb_next:  # 何をするか：定期ステータスの時刻になったら
                        _hb_write(hb_path, event="status", ts=now.isoformat(),
                                Q=float(pnl_state.get("pos", 0.0)),  # 何をするか：建玉（BTC）
                                A=len(live_orders),                  # 何をするか：生きている注文の数
                                R=daily_R,                           # 何をするか：当日実現PnL(JPY)
                                maint=_in_maintenance(now, cfg),      # 何をするか：メンテ窓フラグ
                                funding=(_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)))  # 何をするか：Funding窓フラグ
                        # これは「status を run.log にも INFO で1行残す」ための補助ログ（監視容易化のため）。
                        try:
                            logger.info(f"hb status Q={float(pnl_state.get('pos', 0.0)):.3f} A={len(live_orders)} R={daily_R:.0f} maint={_in_maintenance(now, cfg)} funding={_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)}")
                        except Exception:
                            pass  # ログ出力失敗時は無視（安全側）
                        hb_next = now + timedelta(seconds=hb_interval_s)  # 何をするか：次回の予定を更新

                    maint_now = _in_maintenance(now, cfg)  # 何をするか：いまメンテ窓の中かを判定
                    fund_now = _in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)  # 何をするか：Funding計算 or 授受窓か
                    if maint_now != maint_prev:
                        _log_window_event(events_dir, "maintenance", "enter" if maint_now else "exit", now)  # 何をするか：入退の瞬間だけ記録
                        maint_prev = maint_now
                    if fund_now != fund_prev:
                        _log_window_event(events_dir, "funding", "enter" if fund_now else "exit", now)  # 何をするか：入退の瞬間だけ記録
                        fund_prev = fund_now

                    jst_now = now.astimezone(_jst)  # 何をするか：JSTの現在日付
                    if jst_now.date() != jst_day:
                        daily_R, R_HWM = 0.0, 0.0  # 何をするか：日付が変わったらPnLとHWMをリセット
                        jst_day = jst_now.date()

                    # 何をするか：ミッド変化ガード（過去30秒比のbp変化が大きい時は一時停止）
                    mid = _mid_from_ob(ob)
                    if mid is not None:
                        mid_hist.append((now, mid))
                    paused_mid = False
                    if max_bp and len(mid_hist) >= 2 and mid is not None:
                        oldest_mid = None
                        for ts, m in mid_hist:
                            if (now - ts).total_seconds() >= 30:
                                oldest_mid = m
                                break
                        if oldest_mid is not None:
                            try:
                                move_bp = abs((mid - oldest_mid) / oldest_mid) * 10000.0
                            except Exception:
                                move_bp = None
                            else:
                                paused_mid = move_bp >= float(max_bp)
                                if paused_mid:
                                    logger.info(f"guard: midmove_guard Δ30s={move_bp:.1f}bp >= {max_bp}bp → skip evaluate")

                    try:
                        logger.info(
                            f"trace: pre_eval paused_mid={paused_mid} maint_now={_in_maintenance(now, cfg)} "
                            f"fund_now={_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)} "
                            f"stop={stop_event.is_set()} live_orders={len(live_orders)}"
                        )
                    except Exception:
                        pass

                    # 何をするか：メンテ/ファンディングの“窓”やガード中は新規を出さず整理だけ
                    if paused_mid or _in_maintenance(now, cfg) or _in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg):
                        try:
                            logger.info(
                                f"trace: skip_before_fill reason={'midmove_guard' if paused_mid else ('maintenance' if _in_maintenance(now, cfg) else ('funding' if (_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)) else 'pause'))}"
                            )
                        except Exception:
                            pass
                        reason = "midmove_guard" if paused_mid else ("maintenance" if _in_maintenance(now, cfg) else ("funding" if (_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)) else "pause"))  # 何をするか：停止理由を決める
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason=reason)  # 何をするか：停止を記録

                        if live_orders:
                            ex.cancel_all_child_orders()
                            live_orders.clear()
                        continue  # 何をするか：次のイベントまで待つ

                    try:
                        logger.info("trace: enter_fill_pipeline")
                    except Exception:
                        pass

                    try:
                        logger.info(f"trace: enter_fill_pipeline_pre_stop stop_event={stop_event.is_set()}")
                    except Exception:
                        pass

                    try:
                        logger.info("[version-check] live.py build mark=ENTER_FILL_PIPELINE")
                    except Exception:
                        pass

                    if stop_event.is_set():
                        exit_reason = "signal"
                        break

                # 何をするか：fill パイプラインの健全性を監視し、欠損疑いならHTTPバックフィルを試みて在庫系ガードを強める
                try:
                    import sys as _sys  # ローカルでstderr出力に使う
                    _sys.stderr.write("MARK_A\n")
                    logger.info("trace: PIPELINE_START")
                    logger.info("trace: MARK_A_LOG")
                    logger.info("trace: after_enter_fill_pipeline")
                    logger.info(
                        f"trace: fills_guard_entry last_seen={fills_last_seen} pos={pnl_state.get('pos', 0.0)} "
                        f"live_orders={len(live_orders)} throttled={throttled}"
                    )
                    logger.info("trace: after_enter_fill_pipeline (post-logging guard)")
                    logger.info("trace: before_fill_guard_logic")
                    logger.info("trace: PIPELINE_MARK_LOG")

                    fills_unhealthy = False
                    if fills_stale_after_s > 0.0:
                        gap_s = None if fills_last_seen is None else (now - fills_last_seen).total_seconds()
                        has_risk = (abs(float(pnl_state.get("pos", 0.0) or 0.0)) >= float(inventory_eps or 0.0)) or bool(live_orders)
                        if has_risk and (fills_last_seen is None or (gap_s is not None and gap_s >= fills_stale_after_s)):
                            fills_unhealthy = True
                            if fills_unhealthy_since is None:
                                fills_unhealthy_since = now
                                _hb_write(
                                    hb_path,
                                    event="pause",
                                    ts=now.isoformat(),
                                    reason="fills_unhealthy",
                                    gap_ms=(None if gap_s is None else int(gap_s * 1000)),
                                    q=float(pnl_state.get("pos", 0.0) or 0.0),
                                    live=len(live_orders),
                                )
                                logger.warning("live: fills stream looks stale → switch to caution (skip auto_reduce/new orders)")
                                try:
                                    logger.info(
                                        f"trace: fills_unhealthy start gap_s={gap_s} has_risk={has_risk} pos={pnl_state.get('pos', 0.0)} live={len(live_orders)}"
                                    )
                                except Exception:
                                    pass
                            # 欠損疑いのときだけHTTPバックフィルを一定間隔で試みる
                            if (not dry_run) and (
                                fills_replay_cooldown_s <= 0.0
                                or fills_last_replay is None
                                or (now - fills_last_replay).total_seconds() >= fills_replay_cooldown_s
                            ):
                                logger.info("trace: before_backfill_attempt")
                                fills_last_replay = now
                                maint_now = _in_maintenance(now, cfg)
                                fund_now = (_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg))
                                logger.info("trace: BEFORE_FETCH_PULL_DELTAS")
                                last_exec_id = _backfill_executions(
                                    ex,
                                    last_exec_id=last_exec_id,
                                    seen_exec_ids=seen_exec_ids,
                                    pnl_state=pnl_state,
                                    trade_log=trade_log,
                                    order_log=order_log,
                                    live_orders=live_orders,
                                    fee_bps=fee_bps,
                                    summary_name=summary_name,
                                    fund_now=fund_now,
                                    maint_now=maint_now,
                                    hb_path=hb_path,
                                    on_fill_seen=lambda: _mark_fill_seen(_now_utc()),
                                )
                                logger.info("trace: AFTER_FETCH_PULL_DELTAS")
                                logger.info("trace: after_backfill_attempt")
                        else:
                            fills_unhealthy_since = None
                    else:
                        fills_unhealthy_since = None
                    logger.info("trace: fills_guard_done")
                    logger.info("trace: PIPELINE_BEFORE_FETCH")
                except Exception as e:
                    logger.exception(f"fill_pipeline error: {e}")
                    continue
                try:
                    logger.info(
                        f"trace: fills_guard_exit fills_unhealthy={fills_unhealthy} since={fills_unhealthy_since} throttled={throttled}"
                    )
                except Exception:
                    pass

                # 何をするか：一定間隔で取引所の建玉（get_positions）とローカル状態を再同期し、ズレを減らす
                resync_failed = False
                if inv_resync_ms > 0 and (last_inv_sync_at is None or (now - last_inv_sync_at).total_seconds() * 1000.0 >= inv_resync_ms):
                    net_sync = None
                    avg_sync = None
                    ok_sync = False
                    try:
                        avg_sync, net_sync, ok_sync = _seed_inventory_and_avg_px(ex)
                        if ok_sync:
                            inv_sync_ok = True
                            last_exchange_pos = float(net_sync or 0.0)
                            # 何をするか：実ポジ0返りでもローカルをリセットしない（保持）仕様
                            if abs(float(net_sync or 0.0)) > 0.0 or abs(float(pnl_state.get("pos", 0.0) or 0.0)) <= 0.0:
                                pnl_state["pos"] = float(net_sync or 0.0)
                            if avg_sync is not None and abs(float(net_sync or 0.0)) > 0.0:
                                pnl_state["avg_px"] = avg_sync
                        else:
                            inv_sync_ok = False
                    except RateLimitError as e:
                        resync_failed = True
                        inv_sync_ok = False
                        logger.warning(f"inventory resync rate limited: {e}")
                        if _rate_limit_strike(rate_limit_hits, rate_limit_window_s, rate_limit_limit):
                            exit_reason = "rate_limit"
                        throttle_until = _now_utc() + timedelta(seconds=10)
                        throttled = True
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="throttle")
                    except Exception as e:
                        resync_failed = True
                        inv_sync_ok = False
                        logger.warning(f"inventory resync failed: {e}")
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inventory_resync_failed")
                    finally:
                        last_inv_sync_at = now
                    if (not resync_failed) and inv_sync_ok:
                        diff = abs(float(pnl_state.get("pos", 0.0)) - float(last_exchange_pos or 0.0))
                        if diff >= max(1e-4, float(inventory_eps or 0.0)):
                            logger.error(f"inventory desync detected: local={pnl_state.get('pos')} exchange={last_exchange_pos}")
                            close_only_mode = True
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="desync", local=float(pnl_state.get("pos", 0.0)), exchange=float(last_exchange_pos or 0.0))
                    elif not inv_sync_ok:
                        close_only_mode = True
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inv_sync_unavailable")
                if exit_reason == "rate_limit":
                    break
                if exit_reason == "rate_limit":
                    break

                # 何をするか：在庫上限ガード（建玉 |Q| が上限以上なら新規を止める）

                close_only_mode = bool(fills_unhealthy)


                if eff_inv_limit is not None:
                    local_Q = float(pnl_state.get("pos", 0.0) or 0.0)
                    exch_Q = float(last_exchange_pos or local_Q or 0.0)
                    Q = exch_Q if abs(exch_Q) >= abs(local_Q) else local_Q
                    if fills_unhealthy:
                        close_only_mode = True
                        _hb_write(
                            hb_path,
                            event="pause",
                            ts=now.isoformat(),
                            reason="fills_unhealthy",
                            q=float(Q),
                            live=len(live_orders),
                        )
                        if live_orders:
                            ex.cancel_all_child_orders()
                            live_orders.clear()
                        logger.debug("pause inventory_guard: fills_unhealthy → auto_reduce禁止")
                        continue
                    if abs(Q) >= eff_inv_limit:
                        close_only_mode = True
                        if (not dry_run) and (not throttled):
                            auto_reduce_last_ts = _maybe_auto_reduce_live(
                                cfg,
                                ex,
                                ob,
                                now,
                                pnl_state,
                                eff_inv_limit,
                                auto_reduce_last_ts,
                                order_log,
                                hb_path,
                            )  # 何をするか：在庫が厚いときに Reduce-Only IOC を投げて軽くする
                        if not dry_run and not throttled:
                            _aggressive_reduce_inventory(now)
                        if live_orders:
                            ex.cancel_all_child_orders()
                            live_orders.clear()
                        logger.debug(f"pause inventory_guard: |Q|={abs(Q)} ≥ {eff_inv_limit}")

                        continue





                # 何をするか：TTL 超過の注文を自動キャンセル（レート制限中は呼ばない）
                if not throttled:
                    for acc_id, meta in list(live_orders.items()):
                        if (meta.get("deadline") is not None) and (now >= meta["deadline"]):  # 何をするか：締切のある注文だけTTL取消の対象にする
                            o = meta["order"]  # 何をするか：元注文情報（tif/ttl/px/sz）を参照
                            try:
                                ex.cancel_child_order(child_order_acceptance_id=acc_id)  # 何をするか：TTL超過の注文を取消
                            except (RateLimitError, ServerError, NetworkError, ExchangeError) as e:
                                logger.warning(f"ttl cancel failed for {acc_id}: {e}")  # 何をするか：失敗は記録して今回は見送り（次周回で再試行）
                                if isinstance(e, RateLimitError) and _rate_limit_strike(rate_limit_hits, rate_limit_window_s, rate_limit_limit):
                                    exit_reason = "rate_limit"
                                    _hb_write(hb_path, event="kill", ts=_now_utc().isoformat(), reason="rate_limit")
                                    break
                                throttle_until = _now_utc() + timedelta(seconds=10)
                                throttled = True
                                _hb_write(hb_path, event="pause", ts=_now_utc().isoformat(), reason="throttle")
                                break
                            del live_orders[acc_id]  # 何をするか：成功したら監視リストから外す
                            order_log.add(ts=now.isoformat(), action="cancel", tif=getattr(o, "tif", "GTC"), ttl_ms=getattr(o, "ttl_ms", None), px=getattr(o, "price", None), sz=getattr(o, "size", None), reason="ttl")  # 何をするか：ordersログにTTL取消を記録
                            _hb_write(hb_path, event="cancel", ts=now.isoformat(), acc=acc_id, reason="ttl", px=getattr(o, "price", None), sz=getattr(o, "size", None))  # 何をするか：ハートビートにもTTL取消を1行記録

                if exit_reason == "rate_limit":
                    break

                fill_actions: list[dict[str, Any]] = []
                try:
                    logger.info(f"trace: before_fills connection_started_at={connection_started_at} now={now}")
                    logger.info(f"trace: pre_fills throttled={throttled} live={len(live_orders)}")
                except Exception:
                    pass
                try:
                    logger.info("trace: BEFORE_FETCH_PULL_DELTAS")
                except Exception:
                    pass
                try:
                    fills = [] if throttled else _pull_fill_deltas(ex, live_orders)  # 何をするか：レート制限中はRESTを呼ばない
                except RateLimitError as e:
                    logger.error(f"live: exchange RateLimit → cooldown: {e}")  # 何をするか：停止せずクールダウンへ切替
                    if _rate_limit_strike(rate_limit_hits, rate_limit_window_s, rate_limit_limit):
                        exit_reason = "rate_limit"
                        _hb_write(hb_path, event="kill", ts=_now_utc().isoformat(), reason="rate_limit")
                        break
                    throttle_until = _now_utc() + timedelta(seconds=10)        # 何をするか：10秒は新規/取消を止める
                    throttled = True
                    _hb_write(hb_path, event="pause", ts=_now_utc().isoformat(), reason="throttle")  # 何をするか：心拍に“throttle”を記録
                    continue  # 何をするか：haltせず次周回へ
                try:
                    logger.info(f"trace: AFTER_FETCH_PULL_DELTAS count={len(fills)} throttled={throttled}")
                except Exception:
                    pass
                for fill in fills:  # 何をするか：done=True なら完了（fill）、False なら部分約定（partial）
                    side = str(fill.get("side", "BUY")).upper()
                    px = float(fill.get("price", 0.0))
                    sz = float(fill.get("size", 0.0))
                    tag = str(fill.get("tag", ""))
                    done = bool(fill.get("done", False))
                    realized = _apply_fill_and_pnl(pnl_state, side, px, sz)  # 何をするか：建玉を更新し実現PnLを積算
                    fee = px * sz * (fee_bps / 10000.0)  # 何をするか：約定金額×bpsで手数料（正=コスト/負=リベート）
                    realized -= fee  # 何をするか：PnLは手数料込み（ネット）で積算する

                    trade_log.add(ts=now.isoformat(), side=side, px=px, sz=sz, fee=fee, pnl=realized, strategy=summary_name, tag=tag, inventory_after=pnl_state["pos"], window_funding=fund_now, window_maint=maint_now)  # 何をするか：手数料込みで trades を記録
                    order_log.add(ts=now.isoformat(), action=("fill" if done else "partial"), tif=None, ttl_ms=None, px=px, sz=sz, reason=tag)  # 何をするか：ordersログにも fill/partial を記録する
                    _hb_write(hb_path, event=("fill" if done else "partial"), ts=now.isoformat(), side=side, px=px, sz=sz, pnl=realized, tag=tag)  # 何をするか：約定イベントを心拍へ（部分約定はpartialとして記録）
                    daily_R += realized  # 何をするか：当日実現PnL(JPY)を更新（手数料込みの realized を積算）
                    R_HWM = max(R_HWM, daily_R)  # 何をするか：当日の最高益(HWM)を更新
                    if (not dry_run) and _check_kill(daily_R, R_HWM, kill_cfg):  # 何をするか：dry-run時はKillを発火させない（疎通運転で止まらない）
                        logger.warning(f"kill-switch: daily_pnl={daily_R:.0f} JPY, dd={R_HWM - daily_R:.0f} JPY → halt")  # 何をするか：停止理由をrun.logへ
                        _hb_write(hb_path, event="kill", ts=now.isoformat(), daily_pnl_jpy=daily_R, dd_jpy=R_HWM - daily_R)  # 何をするか：心拍にKillを記録
                        exit_reason = "kill"
                        return  # 何をするか：run_live を終了（安全停止）

                    for _acc_id, _meta in list(live_orders.items()):  # 何をするか：全部さばけた注文を監視から外す（TTLや二重取消を防ぐ）
                        _o = _meta.get("order")
                        if _o is None:
                            continue  # 何をするか：保険（order情報が無い場合は何もしない）
                        if float(_meta.get("executed", 0.0)) >= float(getattr(_o, "size", 0.0)) - 1e-12:
                            del live_orders[_acc_id]  # 何をするか：完了注文を片付ける

                    if hasattr(strat, "on_fill") and fills:
                        for fill in fills:
                            fill_event = SimpleNamespace(
                                side=fill.get("side"),
                                price=fill.get("price"),
                                size=fill.get("size"),
                                tag=fill.get("tag"),
                                done=fill.get("done"),
                                order=fill.get("order"),
                                acceptance_id=fill.get("acceptance_id"),
                            )
                            try:
                                new_actions = strat.on_fill(ob, fill_event)
                            except Exception as e:
                                logger.exception(f"strategy on_fill error: {e}")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="strategy_on_fill_error")
                                continue
                            for act in new_actions or []:
                                if act:
                                    fill_actions.append(act)

                if fills:
                    _mark_fill_seen(now)



                actions_to_process: list[dict[str, Any]] = list(fill_actions)
                skip_new_orders = False
                evaluate_actions: list[dict[str, Any]] = []

                # decision ブロックに入ったことを明示（どこで止まっているかの診断用）
                try:
                    logger.info("decision reach: about to check guards and evaluate")
                except Exception:
                    pass

                try:
                    logger.info(
                        f"decision checkpoint: enter guards q={float(pnl_state.get('pos', 0.0) or 0.0):.4f} "
                        f"live={len(live_orders)} throttled={throttled} fills_unhealthy={fills_unhealthy}"
                    )
                    if resync_failed:
                        logger.debug("pause: inventory_resync_failed")
                        skip_new_orders = True
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inventory_resync_failed")

                    # 何をするか：-205（証拠金不足）が短時間に連発した場合は一定時間 close-only にする
                    margin_guard = (margin_block_until is not None) and (now < margin_block_until)

                    inv_paused = (eff_inv_limit is not None) and (abs(float(pnl_state.get("pos", 0.0))) >= eff_inv_limit)  # 何をするか：在庫上限に達しているかを判定
                    if inv_paused:
                        logger.info(f"guard: inventory |Q|={abs(pnl_state.get('pos', 0.0)):.3f} >= {eff_inv_limit} → skip new orders")  # 何をするか：理由をrun.logに記録
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inventory_guard")  # 何をするか：ハートビートに停止を記録
                        skip_new_orders = True
                    elif margin_guard:
                        logger.info("guard: margin_guard (recent -205 exceeded limit) → skip new orders")  # 何をするか：run.logに停止理由を記録
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="margin_guard", cooldown_s=margin_cooldown_s)  # 何をするか：心拍にも margin_guard を記録
                        # 可能なら Reduce-Only IOC で少しでも在庫を削る
                        if (not dry_run) and (not throttled) and (not fills_unhealthy):
                            if eff_inv_limit is not None:
                                auto_reduce_last_ts = _maybe_auto_reduce_live(
                                    cfg,
                                    ex,
                                    ob,
                                    now,
                                    pnl_state,
                                    eff_inv_limit,
                                    auto_reduce_last_ts,
                                    order_log,
                                    hb_path,
                                )
                            _aggressive_reduce_inventory(now)
                        skip_new_orders = True
                    elif _in_maintenance(now, cfg):
                        logger.debug("pause: maintenance window")  # 何をするか：理由をrun.logに記録
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="maintenance")  # 何をするか：心拍に停止を記録
                        skip_new_orders = True
                    elif _in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg):
                        logger.debug("pause: funding window")  # 何をするか：理由をrun.logに記録
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="funding")  # 何をするか：心拍に停止を記録
                        skip_new_orders = True
                    else:
                        feats_win = getattr(getattr(cfg, "features", None), "ca_ratio_win_ms", 500)
                        try:
                            logger.info(f"decision checkpoint: evaluate start summary={summary_name}")
                            t0 = time.perf_counter()  # 何をするか：戦略評価の開始時刻（ms測定）
                            evaluate_actions = strat.evaluate(ob, now, cfg) or []  # 何をするか：None安全化
                        except Exception as e:
                            logger.exception(f"strategy error: {e}")  # 何をするか：原因をrun.logに記録（スタック付き）
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="strategy_error")  # 何をするか：心拍に“戦略エラー”を記録
                            feats = {
                                'best_age_ms': ob.best_age_ms(now),
                                'ca_ratio': ob.ca_ratio(now, window_ms=feats_win),
                                'spread_tick': ob.spread_ticks(),
                            }
                            try:
                                extra = getattr(strat, 'consume_decision_features', None)
                                extra = extra() if callable(extra) else None
                                if isinstance(extra, Mapping):
                                    for k, v in extra.items():
                                        if k not in feats:
                                            feats[k] = v
                            except Exception:
                                pass
                            spread_state = ('zero' if feats.get('spread_tick') == 0 else 'ge1') if feats.get('spread_tick') is not None else None
                            logger.info(f"decision record (error) path={__file__} features_in={feats}")
                            decision_log.add(ts=now.isoformat(), strategy=summary_name, decision='error', features=feats, expected_edge_bp=None, eta_ms=int((time.perf_counter() - t0) * 1000), ca_ratio=feats.get('ca_ratio'), best_age_ms=feats.get('best_age_ms'), spread_state=spread_state)  # 何をするか：必須のKW引数をすべて埋めて“error”を記録
                            skip_new_orders = True
                        else:
                            feats = {
                                'best_age_ms': ob.best_age_ms(now),
                                'ca_ratio': ob.ca_ratio(now, window_ms=feats_win),
                                'spread_tick': ob.spread_ticks(),
                            }
                            try:
                                extra = getattr(strat, 'consume_decision_features', None)
                                extra = extra() if callable(extra) else None
                                if isinstance(extra, Mapping):
                                    for k, v in extra.items():
                                        if k not in feats:
                                            feats[k] = v
                            except Exception:
                                pass
                            spread_state = 'zero' if feats.get('spread_tick') == 0 else 'ge1'
                            logger.info(f"decision record path={__file__} features_in={feats}")
                            decision_log.add(ts=now.isoformat(), strategy=summary_name, decision=('place' if evaluate_actions else 'hold'), features=feats, expected_edge_bp=None, eta_ms=int((time.perf_counter() - t0) * 1000), ca_ratio=feats.get('ca_ratio'), best_age_ms=feats.get('best_age_ms'), spread_state=spread_state)  # 何をするか：必須のKW引数をすべて埋めて“place/hold”を記録

                except Exception as e:
                    logger.error(f"strategy evaluate failed: {e}")
                    skip_new_orders = True

                # ここで「なぜこの周回で新規を出さないのか」を明示する診断ログを出す
                try:
                    logger.info(
                        f"decision summary skip_new_orders={skip_new_orders} throttled={throttled} "
                        f"close_only={close_only_mode} margin_guard={margin_guard} inv_paused={inv_paused} "
                        f"fills_unhealthy={fills_unhealthy} actions={(len(evaluate_actions) if evaluate_actions is not None else 0)}"
                    )
                except Exception:
                    pass

                if not skip_new_orders and evaluate_actions:
                    try:
                        logger.debug(f"engine.debug_actions: total_actions={len(evaluate_actions) if evaluate_actions is not None else 0}")
                    except Exception:
                        pass
                    actions_to_process.extend(evaluate_actions)

                if throttled:
                    continue

                guard_close_only = close_only_mode or margin_guard  # 何をするか：在庫超過や証拠金ガード中はクローズ専用に寄せる

                margin_guard_triggered = False  # 何をするか：-205検知でこの周回の新規発注を止めるフラグ

                for o in actions_to_process or []:
                    act_type = str(_act(o, "type", "place") or "place").lower()  # 何をするか：action.type を拾う（無ければplace扱い）
                    if act_type == "cancel_tag":
                        tag_query = str(_act(o, "tag", "") or "")
                        if dry_run:
                            logger.info(f"live[dry_run]: skip cancel_tag tag={tag_query}")  # 何をするか：dry-runでは取消しは実行しない
                            continue
                        if throttled:
                            logger.debug("pause: throttle (skip cancel_tag)")  # 何をするか：レート制限クールダウン中は取消しもしない
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="throttle")
                            continue
                        if not tag_query:
                            continue
                        cancelled = 0
                        for acc_id, meta in list(live_orders.items()):
                            order_meta = meta.get("order")
                            if not _tag_matches(_act(order_meta, "tag", None), tag_query):
                                continue
                            try:
                                ex.cancel_child_order(child_order_acceptance_id=acc_id)  # 何をするか：タグ一致の注文を個別にキャンセル
                            except (RateLimitError, ServerError, NetworkError, ExchangeError) as e:
                                logger.warning(f"cancel_tag failed for {acc_id}: {e}")
                                if isinstance(e, RateLimitError) and _rate_limit_strike(rate_limit_hits, rate_limit_window_s, rate_limit_limit):
                                    exit_reason = "rate_limit"
                                    _hb_write(hb_path, event="kill", ts=_now_utc().isoformat(), reason="rate_limit")
                                    stop_event.set()  # 何をするか：レート制限多発時は安全停止
                                    break
                                throttle_until = _now_utc() + timedelta(seconds=10)
                                throttled = True
                                _hb_write(hb_path, event="pause", ts=_now_utc().isoformat(), reason="throttle")
                                break
                            del live_orders[acc_id]
                            cancelled += 1
                            order_log.add(
                                ts=now.isoformat(),
                                action="cancel",
                                tif=_act(order_meta, "tif", "GTC"),
                                ttl_ms=_act(order_meta, "ttl_ms", None),
                                px=_act(order_meta, "price", None),
                                sz=_act(order_meta, "size", None),
                                reason=tag_query,
                            )
                            _hb_write(hb_path, event="cancel", ts=now.isoformat(), acc=acc_id, reason="cancel_tag", tag=tag_query)
                        if cancelled:
                            logger.info(f"cancel_tag applied tag={tag_query} cancelled={cancelled}")
                        if exit_reason == "rate_limit" or throttled:
                            break
                        continue
                    if act_type != "place":
                        continue

                    order_obj = _act(o, "order", o)  # 何をするか：action内にorderがあれば優先的に読む
                    sz = float(_act(order_obj, "size", getattr(getattr(cfg, "size", None), "default", 0.0)) or 0.0)  # 何をするか：dict/object両対応でサイズ取得（未指定ならconfigのdefault）
                    if sz <= 0.0:  # 何をするか：サイズが無い/0のときは発注しない
                        logger.debug("pause: size_missing_or_zero")  # 何をするか：理由をrun.logに残す
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="size_missing_or_zero")  # 何をするか：心拍にも残す
                        continue  # 何をするか：この周回の発注パートはスキップ

                    px_raw = _act(order_obj, "price", None)  # 何をするか：dict/object両対応で価格を取得（未指定ならNone）
                    if px_raw is None:
                        # 何をするか：price未指定のときは板の最良気配から自動補完（実稼働向けの安全デフォルト）
                        side_norm = _side_norm(_act(order_obj, "side"))  # 何をするか：'BUY'/'SELL'へ正規化
                        bid = _best_px(getattr(ob, "best_bid", None))  # 何をするか：最良買いの価格(float)を取り出す
                        ask = _best_px(getattr(ob, "best_ask", None))  # 何をするか：最良売りの価格(float)を取り出す
                        px = (bid if side_norm == "BUY" else ask)  # 何をするか：向きに応じて使う価格を選ぶ
                        if px is None:  # 何をするか：板が欠落していて価格が出せない場合だけスキップ
                            logger.debug("pause: price_fallback_unavailable")  # 何をするか：理由をrun.logへ
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="price_fallback_unavailable")  # 何をするか：心拍にも残す
                            continue  # 何をするか：この周回は発注パートへ進まない
                    else:
                        try:
                            px = float(px_raw)  # 何をするか：指定されていれば数値化して採用
                        except Exception:
                            logger.debug("pause: price_invalid")  # 何をするか：価格が数値化できないときは安全にスキップ
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="price_invalid")
                            continue


                    elapsed_ms = (now - _last_tx_at).total_seconds() * 1000.0  # 何をするか：前回送信からの経過ms
                    if elapsed_ms < min_tx_ms:  # 何をするか：まだ最小間隔に達していなければ送らない
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="throttle", elapsed_ms=int(elapsed_ms), min_ms=min_tx_ms)  # 何をするか：スロットリングで見送ったことを心拍に記録
                        logger.debug(f"throttle tx: {elapsed_ms:.0f}ms < {min_tx_ms}ms")  # 何をするか：スロットリングしたことを記録
                        continue

                    sz = getattr(o, "size", None) or float(getattr(getattr(cfg, "size", None), "default", 0.01))  # 何をするか：サイズ未指定なら config の size.default を使う
                    min_sz = float(getattr(getattr(cfg, "size", None), "min", 0.0))  # 何をするか：設定の最小ロットを読む
                    size_step = float(getattr(getattr(cfg, "size", None), "step", min_sz))  # 何をするか：サイズ刻み（未指定は min を刻みとして使う）
                    sz = _round_size(sz, size_step)  # 何をするか：サイズを刻みに丸める（約定拒否を防ぐ）
                    if sz < min_sz: sz = min_sz  # 何をするか：丸めた結果が下限未満なら下限に引き上げる

                    px = float(px)  # 何をするか：上流で決定済みの価格(px)をそのまま使う（_normalize_px_szでtick丸め済みのため二重丸めしない）
                    tag = getattr(order_obj, "tag", getattr(o, "tag", ""))  # 何をするか：発注理由（タグ）
                    dedup_key = f"{_side_norm(_act(order_obj, 'side'))}|{px}|{_act(order_obj, 'tag', '')}"  # 何をするか：実発注と同じ'BUY'/'SELL'でキー化し二重発注を防ぐ


                    if dedup_key in last_place and (now - last_place[dedup_key]).total_seconds() * 1000.0 < place_dedup_ms:
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="dedup", key=dedup_key, within_ms=place_dedup_ms)  # 何をするか：短時間の同一発注を見送ったことを心拍に記録
                        logger.debug(f"dedup skip: {dedup_key} within {place_dedup_ms}ms")  # 何をするか：短時間の同一発注は見送る
                        continue


                    try:
                        if dry_run:  # 何をするか：dry-run時は実発注せずスキップ（ログはrun.logにだけ残す）
                            logger.info(f"live[dry_run]: skip place side={_act(order_obj, 'side')} px={px} sz={sz} tag={_act(order_obj, 'tag', '')}")  # 何をするか：dict対応のtagを表示
                            continue

                        px, sz = _normalize_px_sz(cfg, px, sz)  # 何をするか：価格/サイズを取引所の刻みに正規化（最小サイズ未満はNone）
                        try:
                            setattr(order_obj, "size", sz)
                            setattr(order_obj, "price", px)
                        except Exception:
                            pass
                        dedup_key = f"{_side_norm(_act(order_obj, 'side'))}|{px}|{_act(order_obj, 'tag', '')}"  # 何をするか：正規化後の価格でデデュープキーを作る

                        gap_ms = getattr(getattr(cfg, "tx", None), "place_dedup_ms", None)  # 何をするか：デデュープ間隔（ms）。None/0なら無効
                        cool_ms = getattr(getattr(cfg, "tx", None), "min_interval_ms", None)  # 何をするか：最小発注間隔(ms)。None/0なら無効
                        if cool_ms and _last_tx_at and ((now - _last_tx_at).total_seconds() * 1000.0) < float(cool_ms):
                            logger.debug("pause: throttle (min tx interval)")  # 何をするか：間隔未満なので今回は見送り
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="throttle", wait_ms=float(cool_ms))  # 何をするか：心拍にスキップ理由を記録
                            continue  # 何をするか：このアクションの発注はスキップ

                        if gap_ms:
                            last_ts = last_place.get(dedup_key)  # 何をするか：このキーで前回いつ出したかを見る
                            if last_ts and ((now - last_ts).total_seconds() * 1000.0) < float(gap_ms):
                                logger.debug("pause: dedup (recently placed same order)")  # 何をするか：連打防止でスキップ
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="dedup", key=dedup_key, gap_ms=float(gap_ms))
                                continue  # 何をするか：この発注は見送り

                        if (px is None) or (sz is None):
                            logger.debug("pause: size_too_small after normalize")  # 何をするか：小さすぎるので今回は出さない
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="size_too_small")  # 何をするか：心拍にスキップ理由を記録
                            continue  # 何をするか：この周回は発注を行わない

                        side_norm = _side_norm(_act(order_obj, "side"))  # 何をするか：sideを'BUY'/'SELL'に正規化
                        try:
                            current_pos = float(pnl_state["pos"])
                        except Exception:
                            current_pos = 0.0
                        reduce_only = bool(_act(order_obj, "reduce_only", False))
                        reduces_inventory = _would_reduce_inventory(current_pos, side_norm, sz)
                        try:
                            setattr(order_obj, "reduce_only", reduce_only)
                        except Exception:
                            pass
                        inflight_count, inflight_qty, inflight_by_key = _inflight_state(live_orders, side=side_norm)
                        if (eff_inv_limit is not None) and (not inv_sync_ok) and (not reduce_only) and (not reduces_inventory):
                            logger.debug("pause: inv_sync_unavailable (close-only enforced)")
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inv_sync_unavailable", side=side_norm, sz=sz)
                            continue
                        if (max_inflight is not None) and (not reduce_only) and (not reduces_inventory):
                            try:
                                if inflight_count >= int(max_inflight):
                                    logger.debug(f"pause: inflight_guard count={inflight_count} limit={int(max_inflight)}")
                                    _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inflight_guard", A=inflight_count, limit=int(max_inflight))
                                    continue
                            except Exception:
                                pass
                        key_for_gate = _gate_key(tag)
                        if (max_inflight_per_key is not None) and key_for_gate and (not reduce_only) and (not reduces_inventory):
                            try:
                                if inflight_by_key.get(key_for_gate, 0) >= int(max_inflight_per_key):
                                    logger.debug(f"pause: inflight_guard key={key_for_gate} count={inflight_by_key.get(key_for_gate, 0)} limit={int(max_inflight_per_key)}")
                                    _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inflight_guard", key=key_for_gate, A=inflight_by_key.get(key_for_gate, 0), limit=int(max_inflight_per_key))
                                    continue
                            except Exception:
                                pass
                        if (limit_qty is not None) and (not reduce_only) and (not reduces_inventory):
                            try:
                                limit_qty_val = float(limit_qty)
                            except Exception:
                                limit_qty_val = None
                            if (limit_qty_val is not None) and ((inflight_qty + abs(sz)) > limit_qty_val):
                                logger.debug(f"pause: inflight_guard qty={inflight_qty + abs(sz):.6f} limit={limit_qty_val}")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inflight_guard", qty=inflight_qty + abs(sz), limit=limit_qty_val)
                                continue
                        if guard_close_only and not reduces_inventory:
                            logger.debug("pause: close_only_mode (margin/inventory)")  # 何をするか：クローズ専用モード中は在庫を増やさない
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="close_only", side=side_norm, sz=sz)  # 何をするか：心拍に理由を記録
                            continue
                        if reduce_only or guard_close_only:
                            clipped_sz, clip_reason = _calc_reduce_order_size(
                                current_pos,
                                sz,
                                size_cfg=getattr(cfg, "size", None),
                                step=size_step,
                            )
                            if clipped_sz is None:
                                logger.debug(f"pause: reduce_clip_{clip_reason}")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="reduce_clip", side=side_norm, sz=sz, detail=clip_reason)
                                continue
                            sz = clipped_sz
                            reduce_only = True
                            reduces_inventory = _would_reduce_inventory(current_pos, side_norm, sz)
                            if not reduces_inventory:
                                logger.debug("pause: reduce_clip_not_reducing")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="close_only", side=side_norm, sz=sz)
                                continue
                        if eff_inv_limit is not None:  # 何をするか：在庫上限ガード
                            guard_base = float(last_exchange_pos if last_exchange_pos is not None else current_pos)
                            guard_abs = max(abs(current_pos), abs(guard_base))
                            pos_after = guard_base + (sz if side_norm == "BUY" else -sz)  # 何をするか：この発注が通った後の建玉を試算（取引所ポジを優先）
                            if abs(pos_after) > eff_inv_limit or guard_abs > eff_inv_limit:
                                if reduce_only or reduces_inventory:
                                    pass  # 何をするか：在庫を減らす注文なので通す
                                else:
                                    logger.debug("pause: inventory_guard")  # 何をするか：上限超過のため止める
                                    _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inventory_guard", pos_before=current_pos, pos_after=pos_after, limit=eff_inv_limit, side=side_norm, sz=sz)  # 何をするか：心拍に理由を記録
                                    continue  # 何をするか：このアクションは見送り
                            elif close_only_mode and not (reduce_only or reduces_inventory):
                                logger.debug("pause: inventory_guard_close_only")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inventory_guard", pos_before=current_pos, pos_after=pos_after, limit=eff_inv_limit, side=side_norm, sz=sz)
                                continue

                        # 何をするか：証拠金のざっくりチェック（reduce-only/在庫減少は除外）
                        if not reduce_only and not reduces_inventory:
                            coll, coll_ts = _ensure_collateral(now)
                            if coll is None:
                                logger.debug("pause: margin_precheck collateral_unavailable")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="margin_precheck_unavailable")
                                continue
                            try:
                                avail = float(coll.get("collateral", 0.0)) - float(coll.get("require_collateral", 0.0))
                                required_raw = (float(px) * float(sz)) / max(margin_leverage, 1e-9)
                                required = required_raw * margin_precheck_safety
                                margin_last_avail = avail
                                margin_last_required = required
                                if required > avail * margin_buffer:
                                    logger.debug(f"pause: margin_precheck required={required:.0f} avail={avail:.0f} raw={required_raw:.0f} safety={margin_precheck_safety}")
                                    _hb_write(
                                        hb_path,
                                        event="pause",
                                        ts=now.isoformat(),
                                        reason="margin_precheck",
                                        required_jpy=int(required),
                                        required_raw_jpy=int(required_raw),
                                        safety=margin_precheck_safety,
                                        avail_jpy=int(avail),
                                        leverage=margin_leverage,
                                        collateral_ts=(coll_ts.isoformat() if coll_ts else None),
                                    )
                                    continue
                            except Exception as e:
                                logger.debug(f"margin_precheck skipped: {e}")

                        acc = ex.send_child_order(
                            side=side_norm, size=sz, price=px, time_in_force=_act(order_obj, "tif", "GTC"), reduce_only=reduce_only
                        )  # 何をするか：正規化後・ガード通過後にだけ実発注する
                        if reduce_only:
                            reduce_fail_count = 0

                        if not acc or (isinstance(acc, str) and acc.strip() == ""):
                            logger.warning("send order did not return acceptance id → skip")  # 何をするか：受理IDが無いのでこの発注は見送る
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="no_acceptance_id", side=side_norm, px=px, sz=sz)  # 何をするか：心拍にも“受理IDなし”を記録
                            continue  # 何をするか：live_ordersへは何も登録せず次のアクションへ
 # 何をするか：この周回は発注を行わない

                        last_place[dedup_key] = now  # 何をするか：この(side×price×tag)は今出した、と記録
                        _last_tx_at = now  # 何をするか：送信できたので直近送信時刻を更新

                        deadline = _ttl_deadline(now, _act(order_obj, "ttl_ms", getattr(getattr(cfg, "features", None), "ttl_ms", None)))  # 何をするか：ttl_ms を dict/object両対応で取得
                        live_orders[acc] = {"deadline": deadline, "order": order_obj, "executed": 0.0, "avg_price": 0.0, "reduce_only": reduce_only, "reduces_inventory": reduces_inventory}  # TTL管理と約定進捗を保持
                        order_log.add(ts=now.isoformat(), action="place", tif=_act(order_obj, "tif", "GTC"), ttl_ms=_act(order_obj, "ttl_ms", None), px=px, sz=sz, reason=_act(order_obj, "tag", ""))  # 何をするか：発注イベントをordersログへ記録
                        _hb_write(hb_path, event="place", ts=now.isoformat(), acc=acc, reason=_act(order_obj, "tag", ""), tif=_act(order_obj, "tif", "GTC"), ttl_ms=_act(order_obj, "ttl_ms", None), px=px, sz=sz)  # 何をするか：発注イベントを心拍に記録


                    except RateLimitError as e:
                        logger.error(f"live: exchange RateLimit → cooldown: {e}")  # 何をするか：停止せずクールダウンへ切替
                        if _rate_limit_strike(rate_limit_hits, rate_limit_window_s, rate_limit_limit):
                            exit_reason = "rate_limit"
                            _hb_write(hb_path, event="kill", ts=_now_utc().isoformat(), reason="rate_limit")
                            break
                        throttle_until = _now_utc() + timedelta(seconds=10)        # 何をするか：10秒は新規/取消を止める
                        throttled = True
                        _hb_write(hb_path, event="pause", ts=_now_utc().isoformat(), reason="throttle")  # 何をするか：心拍に“throttle”を記録
                        break
                    except (ServerError, NetworkError, ExchangeError) as e:
                        msg = str(e)
                        if "-205" in msg and margin_last_avail is not None and margin_last_required is not None:
                            logger.warning(
                                f"margin_guard snapshot: required={margin_last_required:.0f} avail={margin_last_avail:.0f} buffer={margin_buffer} leverage={margin_leverage} safety={margin_precheck_safety}"
                            )
                        logger.warning(f"send order rejected: {msg}")
                        is_reduce = reduce_only or reduces_inventory
                        is_margin205 = ("-205" in msg) or ("Margin amount is insufficient" in msg)
                        if is_margin205:
                            margin_err_log.append(now)
                            cutoff = now - timedelta(seconds=margin_err_window_s)
                            while margin_err_log and margin_err_log[0] < cutoff:
                                margin_err_log.popleft()
                            margin_block_until = now + timedelta(seconds=margin_cooldown_s)
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="margin_guard", cooldown_s=margin_cooldown_s)
                            logger.warning(f"margin_guard: -205 detected → close-only for {margin_cooldown_s}s (count={len(margin_err_log)}/{margin_err_limit} window={margin_err_window_s}s)")
                            margin_guard_triggered = True
                            if is_reduce:
                                reduce_fail_count += 1
                                fallback_sz, clip_reason = _calc_reduce_order_size(
                                    current_pos,
                                    sz * 0.5,
                                    size_cfg=getattr(cfg, "size", None),
                                    step=size_step,
                                )
                                if fallback_sz is not None and fallback_sz < sz - 1e-12:
                                    try:
                                        acc_fb = ex.send_child_order(
                                            side=side_norm,
                                            size=fallback_sz,
                                            price=px,
                                            time_in_force=_act(order_obj, "tif", "GTC"),
                                            reduce_only=True,
                                        )
                                        reduce_fail_count = 0
                                        last_place[dedup_key] = now
                                        _last_tx_at = now
                                        deadline = _ttl_deadline(now, _act(order_obj, "ttl_ms", getattr(getattr(cfg, "features", None), "ttl_ms", None)))
                                        live_orders[acc_fb] = {"deadline": deadline, "order": order_obj, "executed": 0.0, "avg_price": 0.0}
                                        order_log.add(ts=now.isoformat(), action="place", tif=_act(order_obj, "tif", "GTC"), ttl_ms=_act(order_obj, "ttl_ms", None), px=px, sz=fallback_sz, reason=_act(order_obj, "tag", ""))
                                        _hb_write(hb_path, event="place", ts=now.isoformat(), acc=acc_fb, reason=_act(order_obj, "tag", ""), tif=_act(order_obj, "tif", "GTC"), ttl_ms=_act(order_obj, "ttl_ms", None), px=px, sz=fallback_sz)
                                        continue
                                    except Exception as fb_exc:
                                        logger.warning(f"reduce-only retry failed: {fb_exc}")
                                else:
                                    logger.debug(f"reduce-only retry skipped size_reason={clip_reason}")
                                if reduce_fail_count >= reduce_fail_limit:
                                    logger.error(f"reduce-only rejected repeatedly ({reduce_fail_count}) → halt for manual action")
                                    _hb_write(hb_path, event="kill", ts=_now_utc().isoformat(), reason="reduce_only_failed", attempts=reduce_fail_count)
                                    stop_event.set()
                                    exit_reason = "reduce_only_failed"
                                    break
                            # 可能ならReduce-Only IOCで在庫を減らす
                            if (not dry_run) and (not fills_unhealthy):
                                if eff_inv_limit is not None:
                                    auto_reduce_last_ts = _maybe_auto_reduce_live(
                                        cfg,
                                        ex,
                                        ob,
                                        now,
                                        pnl_state,
                                        eff_inv_limit,
                                        auto_reduce_last_ts,
                                        order_log,
                                        hb_path,
                                    )
                                _aggressive_reduce_inventory(now)
                        continue

                if throttled or margin_guard_triggered:
                    continue

                if force_reconnect:
                    try:
                        logger.warning("live: board stream unhealthy -> force reconnect")
                        _hb_write(hb_path, event="pause", ts=_now_utc().isoformat(), reason="board_reconnect")
                    except Exception:
                        pass
                    ob = OrderBook(tick)
                    mid_hist.clear()
                    bad_book_since = None
                    stale_since = None
                    last_ev_at = _now_utc()
                    if book_warmup_s > 0.0:
                        book_warmup_until = last_ev_at + timedelta(seconds=book_warmup_s)
                    continue

                break

        except (AuthError, RateLimitError, ServerError, NetworkError, ExchangeError) as e:
            exit_reason = "exception"
            logger.error(f"live: exchange 疎通に失敗しました: {e}")
            raise
        finally:
            if exit_handler is not None:
                try:
                    exit_handler(exit_reason)
                except Exception:
                    logger.warning("live: 終了処理で例外が発生しましたが握りつぶします", exc_info=True)
