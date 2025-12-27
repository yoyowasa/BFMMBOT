# 何をするファイルか：疑似発注（ペーパー運転）の最小ランナー
# - 戦略が出した action を「place」として心拍に記録
# - 板の最良気配に当たれば「fill」として記録（即時・全量）
# - TTL 超過は「cancel」
# - 実際の取引所 REST は呼ばない（完全オフラインな紙運転）

from pathlib import Path  # 何をするか：ログファイルの場所を扱う
from datetime import datetime, timezone, timedelta  # 何をするか：UTCの現在時刻・経過の計算
from zoneinfo import ZoneInfo  # 何をするか：JSTタイムゾーンを扱う
import json  # 何をするか：心拍を ndjson(1行1JSON) で書く
import uuid  # 何をするか：疑似の受理ID(acc)を作る
import math  # 何をするか：サイズ刻みの丸めに使う
import time  # 何をするか：ログ間引き用に単調増加時計を使う
from typing import Any, Dict, List, Optional  # 何をするか：型ヒント（辞書など）
from collections.abc import Mapping
import csv  # 何をするか：orders.csv / trades.csv に追記するために使う
from types import SimpleNamespace  # 何をするか：戦略 on_fill に渡す簡易オブジェクトを作る

from loguru import logger  # 何をするか：run.log へ可読ログを出す
from src.strategy.base import build_strategy_from_cfg  # 何をするか：cfg['strategies'] 配列から戦略群を構築する
from src.core.realtime import stream_events  # 何をするか：WSイベントの同期ジェネレーター
from src.core.orderbook import OrderBook  # 何をするか：ローカル板（best_bid/best_ask を持つ）
from src.core.logs import OrderLog, TradeLog  # 何をするか：Parquet出力（orders/trades）用のヘルパ
from src.core.utils import monotonic_ms

# ---- 小さなユーティリティ（live.pyに依存せず単体で動く最小セット） ----

def _now_utc() -> datetime:
    """何をする関数か：JSTの現在時刻を返す"""
    return datetime.now(ZoneInfo("Asia/Tokyo"))

__hb_ctx: dict[str, Any] = {}  # 何をするか：bad_book切り分け用に、直近の板/WS状態を保持して_hb_writeから参照する
_pause_last_key = None  # 直近に出した pause のキー(reason/min_ms)
_pause_last_emit_mono = 0.0  # 直近 pause を出した monotonic 時刻
_pause_suppressed = 0  # 間引きで捨てた pause の回数（次にまとめて出す）

def _apply_inventory_brake(orders, *, pos, max_pos, brake_ratio):
    # 在庫ブレーキ: 在庫が偏ったら「偏り方向に増える side」の新規注文を落として片寄りを止める
    if not orders:
        return orders, 0  # 何も無ければそのまま返す
    if max_pos <= 0:
        return orders, 0  # 上限が無効ならブレーキもしない

    r = brake_ratio
    if r < 0:
        r = 0  # brake_ratio は 0〜1 に丸める（安全側）
    if r > 1:
        r = 1  # brake_ratio は 0〜1 に丸める（安全側）

    start = max_pos * r  # ブレーキ開始しきい値（max_pos の r 倍）
    if abs(pos) < start:
        return orders, 0  # まだ安全域なら何もしない

    block_side = "BUY" if pos > 0 else "SELL"  # ロングなら BUY を抑制、ショートなら SELL を抑制

    kept = []
    dropped = 0
    for o in orders:
        # orders が dict/オブジェクトどちらでも動くように side を読む
        side = o.get("side") if isinstance(o, dict) else getattr(o, "side", None)
        if side == block_side:
            dropped += 1  # 偏り方向に増える注文は落とす
            continue
        kept.append(o)  # 反対側（在庫を減らす側）は通す

    return kept, dropped  # フィルタ後の注文と、落とした件数を返す

def _hb_write(hb_path: Path, **fields):
    """何をする関数か：心拍を ndjson で1行追記する（paper用の簡易版）"""
    global _pause_last_key, _pause_last_emit_mono, _pause_suppressed
    try:
        rec = dict(fields)

        if rec.get("event") == "pause":
            pause_key = (rec.get("reason"), rec.get("min_ms"))
            now_mono = time.monotonic()

            if pause_key == _pause_last_key and (now_mono - _pause_last_emit_mono) < 1.0:
                _pause_suppressed += 1
                return

            rec = dict(rec)
            rec["suppressed"] = _pause_suppressed if pause_key == _pause_last_key else 0

            _pause_last_key = pause_key
            _pause_last_emit_mono = now_mono
            _pause_suppressed = 0

        # bad_book のときだけ、切り分けに必要な「板＋WSの状態」をheartbeatへ同梱する（後方互換：既存キーは残す）
        event = rec.get("event")
        reason = rec.get("reason")
        ts = rec.get("ts")
        if event == "pause" and reason == "bad_book" and ("bad_book_detail" not in rec):
            # --- 銘柄 ---
            product_code = __hb_ctx.get("product_code") or rec.get("product_code") or rec.get("product")

            # --- ローカル板スナップ（best/サイズ/更新時刻/シーケンス）を安全に抜く ---
            ob = __hb_ctx.get("orderbook") or __hb_ctx.get("ob")

            def _pick_px(x):
                # bestが dict / object / 数値 のどれでも「価格」だけ抜く
                if x is None:
                    return None
                if isinstance(x, dict):
                    return x.get("price", x.get("px"))
                return getattr(x, "price", x)

            def _pick_sz(x):
                # bestが dict / object / 数値 のどれでも「数量」だけ抜く（無ければNone）
                if x is None:
                    return None
                if isinstance(x, dict):
                    return x.get("size") if "size" in x else x.get("qty")
                return getattr(x, "size", getattr(x, "qty", None))

            bid_obj = getattr(ob, "best_bid", None) if ob is not None else None
            ask_obj = getattr(ob, "best_ask", None) if ob is not None else None

            bid_px = _pick_px(getattr(ob, "best_bid_px", None) or getattr(ob, "bid_px", None) or bid_obj) if ob is not None else None
            ask_px = _pick_px(getattr(ob, "best_ask_px", None) or getattr(ob, "ask_px", None) or ask_obj) if ob is not None else None
            bid_sz = _pick_sz(getattr(ob, "best_bid_sz", None) or getattr(ob, "bid_sz", None) or bid_obj) if ob is not None else None
            ask_sz = _pick_sz(getattr(ob, "best_ask_sz", None) or getattr(ob, "ask_sz", None) or ask_obj) if ob is not None else None

            book_last_ts = (getattr(ob, "_last_ts", None) or getattr(ob, "last_update_ts", None) or getattr(ob, "last_ts", None) or getattr(ob, "updated_at", None)) if ob is not None else None
            book_seq = (getattr(ob, "seq", None) or getattr(ob, "sequence", None) or getattr(ob, "snapshot_seq", None)) if ob is not None else None

            # --- 派生（spread/mid/cross/空/古さ）を例外なく計算（失敗したらNoneのまま） ---
            spread_px = None
            mid_px = None
            crossed = None
            try:
                if bid_px is not None and ask_px is not None:
                    spread_px = float(ask_px) - float(bid_px)
                    mid_px = (float(ask_px) + float(bid_px)) / 2.0
                    crossed = bool(float(bid_px) >= float(ask_px))
            except Exception:
                pass

            empty_bid = bool(bid_px is None or bid_sz in (None, 0, 0.0))
            empty_ask = bool(ask_px is None or ask_sz in (None, 0, 0.0))

            book_stale_ms = None
            try:
                ts_dt = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
                if isinstance(ts_dt, datetime) and isinstance(book_last_ts, datetime):
                    book_stale_ms = int((ts_dt - book_last_ts).total_seconds() * 1000.0)
            except Exception:
                pass

            # --- WS受信状況（取れていれば）を同梱：チャンネル名＋最終受信時刻＋古さ ---
            ch_board = f"lightning_board_{product_code}" if product_code else None
            ch_exec = f"lightning_executions_{product_code}" if product_code else None

            last_rx_map = __hb_ctx.get("last_rx_ts_by_channel")
            board_rx_ts = last_rx_map.get(ch_board) if isinstance(last_rx_map, dict) and ch_board in last_rx_map else None
            exec_rx_ts = last_rx_map.get(ch_exec) if isinstance(last_rx_map, dict) and ch_exec in last_rx_map else None

            ws_board_stale_ms = None
            ws_exec_stale_ms = None
            try:
                ts_dt = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
                if isinstance(ts_dt, datetime) and isinstance(board_rx_ts, datetime):
                    ws_board_stale_ms = int((ts_dt - board_rx_ts).total_seconds() * 1000.0)
                if isinstance(ts_dt, datetime) and isinstance(exec_rx_ts, datetime):
                    ws_exec_stale_ms = int((ts_dt - exec_rx_ts).total_seconds() * 1000.0)
            except Exception:
                pass

            rec["bad_book_detail"] = {
                # 何の銘柄・何のチャンネルか（後から照合できるように）
                "product_code": product_code,
                "channels": {"board": ch_board, "executions": ch_exec},

                # 板の最小スナップ（stale/クロス/欠損 を即判定できる）
                "bid_px": bid_px,
                "ask_px": ask_px,
                "bid_sz": bid_sz,
                "ask_sz": ask_sz,
                "spread_px": spread_px,
                "mid_px": mid_px,
                "crossed": crossed,
                "empty_bid": empty_bid,
                "empty_ask": empty_ask,

                # 板更新の情報（stale判定の根拠）
                "book_seq": book_seq,
                "book_last_ts": (book_last_ts.isoformat() if hasattr(book_last_ts, "isoformat") else book_last_ts),
                "book_stale_ms": book_stale_ms,

                # WS受信の情報（遅延/断線っぽさの根拠。取れないならNone）
                "ws_board_last_ts": (board_rx_ts.isoformat() if hasattr(board_rx_ts, "isoformat") else board_rx_ts),
                "ws_exec_last_ts": (exec_rx_ts.isoformat() if hasattr(exec_rx_ts, "isoformat") else exec_rx_ts),
                "ws_board_stale_ms": ws_board_stale_ms,
                "ws_exec_stale_ms": ws_exec_stale_ms,
            }

        hb_path.parent.mkdir(parents=True, exist_ok=True)
        with hb_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.exception(f"heartbeat write failed: {e}")

ORDERS_CSV = Path("logs/orders.csv")  # 何をするか：疑似発注の履歴（place/partial/fill/cancel）を書き出す先
TRADES_CSV = Path("logs/trades.csv")  # 何をするか：疑似約定（partial/fill）の履歴を書き出す先
TRADE_LOG_CSV = Path("logs/trades/trade_log.csv")  # 何をするか：既存ツールが参照しやすい場所にもミラー出力する

def _csv_append(path: Path, header: list[str], row: dict):
    """何をする関数か：CSVファイルに1行追記（初回はヘッダを書いてから追記）"""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()
        with path.open("a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            if write_header:
                w.writeheader()
            w.writerow(row)
    except Exception as e:
        logger.exception(f"csv write failed: {path}: {e}")

def _apply_fill_and_pnl(state: dict, side: str, px: float, sz: float) -> float:
    """何をする関数か：加重平均法で建玉(state['pos'], state['avg'])を更新し、手数料控除前の実現PnL(JPY)を返す"""
    pos = state.get("pos", 0.0)
    avg = state.get("avg", 0.0)
    realized = 0.0
    signed = sz if side == "BUY" else -sz  # BUYを＋、SELLを−で扱う

    if pos == 0.0 or (pos > 0 and signed > 0) or (pos < 0 and signed < 0):
        # 何をするか：同方向に増やす（平均単価を更新）
        new_pos = pos + signed
        new_avg = px if pos == 0.0 else (avg * abs(pos) + px * abs(signed)) / (abs(pos) + abs(signed))
        state["pos"], state["avg"] = new_pos, new_avg
    else:
        # 何をするか：反対売買でポジションを減らす（実現PnL発生）
        close_qty = min(abs(pos), abs(signed))
        if pos > 0:   # ロング縮小：売値−平均買値
            realized += (px - avg) * close_qty
        else:         # ショート縮小：平均売値−買戻し値
            realized += (avg - px) * close_qty
        new_pos = pos + signed
        if new_pos == 0.0:
            state["pos"], state["avg"] = 0.0, 0.0
        elif (pos > 0 and new_pos > 0) or (pos < 0 and new_pos < 0):
            state["pos"], state["avg"] = new_pos, avg
        else:
            # 何をするか：反転した残量は新規玉として現値を平均値に採用
            state["pos"], state["avg"] = new_pos, px

    state["realized"] = state.get("realized", 0.0) + realized
    return realized

def _orders_add(ts: str, action: str, tif, ttl_ms, px: float, sz: float, reason: str):
    """何をする関数か：orders.csv に place/partial/fill/cancel を1行追記する"""
    _csv_append(ORDERS_CSV, ["ts", "action", "tif", "ttl_ms", "px", "sz", "reason"],
                {"ts": ts, "action": action, "tif": tif, "ttl_ms": ttl_ms, "px": px, "sz": sz, "reason": reason})

def _trades_add(ts: str, side: str, px: float, sz: float, fee: float, pnl: float, strategy: str, tag: str, inventory_after: float):
    """何をする関数か：trades を2か所へCSV追記し、手数料と実現PnL、約定後在庫も記録する"""
    row = {"ts": ts, "side": side, "px": px, "sz": sz, "fee": fee, "pnl": pnl, "strategy": strategy, "tag": tag, "inventory_after": inventory_after}
    header = ["ts", "side", "px", "sz", "fee", "pnl", "strategy", "tag", "inventory_after"]
    _csv_append(TRADES_CSV, header, row)
    _csv_append(TRADE_LOG_CSV, header, row)



def _act(o: Any, key: str, default=None):
    """何をする関数か：action が dict でもオブジェクトでも同じ取り方で値を取り出す"""
    if isinstance(o, dict):
        return o.get(key, default)
    return getattr(o, key, default)

def _side_norm(v: Any) -> str:
    """何をする関数か：side を 'BUY' / 'SELL' に正規化（略記/小文字/数値にも対応）"""
    s = str(v).strip().upper() if v is not None else "BUY"
    if s in ("B", "BUY", "1", "+1"):
        return "BUY"
    if s in ("S", "SELL", "-1"):
        return "SELL"
    return "BUY"

def _best_px(x: Any):
    """何をする関数か：best_bid/best_ask から float 価格を安全に取り出す"""
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, dict):
            v = x.get("price", x.get("px"))
            return float(v) if v is not None else None
        if hasattr(x, "price"):
            return float(getattr(x, "price"))
        if isinstance(x, (list, tuple)) and len(x) >= 1:
            return float(x[0])
    except Exception:
        return None
    return None

def _round_to_tick(px: float, tick: float) -> float:
    """何をする関数か：価格を取引所の刻みに丸める"""
    if not tick:
        return float(px)
    return float(round(float(px) / float(tick)) * float(tick))

def _round_size(sz: float, step: float) -> float:
    """何をする関数か：サイズを刻みに丸める（step<=0ならそのまま返す）"""
    try:
        step_val = float(step)
    except Exception:
        step_val = 0.0
    if step_val <= 0.0:
        return float(sz)
    try:
        return float(math.floor(float(sz) / step_val) * step_val)
    except Exception:
        return float(sz)

def _gate_key(tag: Optional[str]) -> str:
    """何をする関数か：corr付きなどを外したタグ基底（base|corr:xxx → base）"""
    if not tag:
        return ""
    parts = [p for p in str(tag).split("|") if p and not str(p).startswith("corr:")]
    return parts[0] if parts else ""

def _tag_matches(order_tag: Optional[str], query: Optional[str]) -> bool:
    """何をする関数か：タグ一致を緩く判定（base と base|corr:xxx のような付加情報付きも拾う）"""
    if not query:
        return False
    if order_tag == query:
        return True
    try:
        return str(order_tag).startswith(f"{query}|")
    except Exception:
        return False


def cancel_all_open_orders(
    *,
    live_orders: Dict[str, Dict[str, Any]],
    hb_path: Path,
    now: datetime,
    order_log: OrderLog,
    reason: str = "halt",
) -> int:
    """何をする関数か：未約定を全キャンセルして、フラット化の邪魔を消す（paper用）"""
    canceled = 0
    for acc, meta in list(live_orders.items()):
        try:
            _hb_write(hb_path, event="cancel", ts=now.isoformat(), acc=acc, reason=reason)
            _orders_add(
                ts=now.isoformat(),
                action="cancel",
                tif=None,
                ttl_ms=meta.get("ttl_ms"),
                px=meta.get("px"),
                sz=meta.get("rem_sz", 0.0),
                reason=reason,
            )
            order_log.add(
                ts=now.isoformat(),
                action="cancel",
                tif=None,
                ttl_ms=meta.get("ttl_ms"),
                px=meta.get("px"),
                sz=meta.get("rem_sz", 0.0),
                reason=reason,
            )
            canceled += 1
        except Exception:
            pass
        try:
            del live_orders[acc]
        except Exception:
            pass
    return canceled


def place_close_order(
    *,
    hb_path: Path,
    now: datetime,
    order_log: OrderLog,
    trade_log: TradeLog,
    strategy_name: str,
    pnl_state: dict,
    fee_total: float,
    fee_bps: float,
    tick: float,
    close_side: str,
    close_size: float,
    ref_bid: float | None,
    ref_ask: float | None,
    last_mark: float | None,
    attempt: int,
    order_type: str = "IOC",
    reason: str = "halt",
) -> tuple[float, float | None]:
    """何をする関数か：現在のposと逆方向に、確実に約定するクローズ注文を出す（paper用・即時fill）"""
    close_side = _side_norm(close_side)
    try:
        close_size = float(close_size)
    except Exception:
        close_size = 0.0
    if close_size <= 0.0:
        return fee_total, last_mark

    # 何をするか：クローズ側の参照価格（BUYはask、SELLはbid）。無ければ last_mark を使う
    px_ref = ref_ask if close_side == "BUY" else ref_bid
    if px_ref is None:
        px_ref = last_mark
    if px_ref is None:
        # 価格が無いと何もできない
        return fee_total, last_mark
    fill_px = _round_to_tick(float(px_ref), float(tick or 1.0))

    # 何をするか：flattenの試行を心拍へ（後から原因追跡できるように）
    _hb_write(
        hb_path,
        event="flatten_try",
        ts=now.isoformat(),
        reason=reason,
        attempt=int(attempt),
        close_side=close_side,
        close_size=float(close_size),
        order_type=str(order_type),
        ref_bid=ref_bid,
        ref_ask=ref_ask,
        fill_px=float(fill_px),
    )

    # 何をするか：orders/tradesにも「place→fill」を残す（実運転に近いログ形）
    acc = f"paper-flat-{uuid.uuid4().hex[:12]}"
    try:
        _hb_write(
            hb_path,
            event="place",
            ts=now.isoformat(),
            acc=acc,
            side=close_side,
            px=fill_px,
            sz=close_size,
            tif=order_type,
            ttl_ms=None,
            reason="flatten",
        )
    except Exception:
        pass
    try:
        _orders_add(ts=now.isoformat(), action="place", tif=order_type, ttl_ms=None, px=fill_px, sz=close_size, reason="flatten")
        order_log.add(ts=now.isoformat(), action="place", tif=order_type, ttl_ms=None, px=fill_px, sz=close_size, reason="flatten")
    except Exception:
        pass

    realized_gross = _apply_fill_and_pnl(pnl_state, close_side, float(fill_px), float(close_size))
    fee_jpy = float(fill_px) * float(close_size) * (float(fee_bps or 0.0) / 10000.0)
    fee_total = float(fee_total or 0.0) + float(fee_jpy or 0.0)
    realized_net = realized_gross - fee_jpy

    pos_after = float(pnl_state.get("pos", 0.0) or 0.0)
    ts_jst = now.astimezone(timezone(timedelta(hours=9))).isoformat()
    tag = f"flatten|{reason}"
    try:
        _trades_add(ts=ts_jst, side=close_side, px=float(fill_px), sz=float(close_size), fee=float(fee_jpy), pnl=float(realized_net), strategy=strategy_name, tag=tag, inventory_after=pos_after)
        trade_log.add(ts=ts_jst, side=close_side, px=float(fill_px), sz=float(close_size), fee=float(fee_jpy), pnl=float(realized_net), strategy=strategy_name, tag=tag, inventory_after=pos_after, window_funding=False, window_maint=False)
    except Exception:
        pass

    try:
        _hb_write(hb_path, event="fill", ts=now.isoformat(), acc=acc, side=close_side, px=float(fill_px), sz=float(close_size), reason=tag)
    except Exception:
        pass
    try:
        _orders_add(ts=now.isoformat(), action="fill", tif=None, ttl_ms=None, px=float(fill_px), sz=float(close_size), reason=tag)
        order_log.add(ts=now.isoformat(), action="fill", tif=None, ttl_ms=None, px=float(fill_px), sz=float(close_size), reason=tag)
    except Exception:
        pass

    return fee_total, float(fill_px)


def wait_position_update(*, pnl_state: dict) -> float:
    """何をする関数か：ポジション更新を待つ（paperは即時反映なので、値を読むだけ）"""
    try:
        return float(pnl_state.get("pos", 0.0) or 0.0)
    except Exception:
        return 0.0


def flatten_on_halt(
    *,
    hb_path: Path,
    order_log: OrderLog,
    trade_log: TradeLog,
    strategy_name: str,
    pnl_state: dict,
    fee_total: float,
    fee_bps: float,
    tick: float,
    size_step: float,
    min_sz: float,
    ref_bid: float | None,
    ref_ask: float | None,
    last_mark: float | None,
    reason: str,
    max_attempts: int = 5,
) -> tuple[float, float | None, bool, str, float | None]:
    """何をする関数か：停止理由が来たら、在庫を0へ寄せ切ってから終了させる（paper用）"""
    try:
        pos0 = float(pnl_state.get("pos", 0.0) or 0.0)
    except Exception:
        pos0 = 0.0

    flatten_action = "n/a"
    summary_dust = None
    dust = float(min_sz or 0.001)
    if abs(pos0) < dust:
        # 最小ロット未満（dust未満）はクローズ注文を出せないため、flattenをスキップした事実だけheartbeatに残す
        _hb_write(
            hb_path,
            event="flatten_skip",
            ts=_now_utc().isoformat(),
            reason=reason,
            pos_before=pos0,
            dust=dust,
        )
        # pos_before < dust なのでフラット化はスキップ、という印を summary に残す
        flatten_action = "skip"
        summary_dust = dust
        return fee_total, last_mark, True, flatten_action, summary_dust

    _hb_write(hb_path, event="flatten_start", ts=_now_utc().isoformat(), reason=reason, pos_before=pos0, dust=dust, max_attempts=int(max_attempts))

    attempts = 0
    success = False
    last_px = last_mark
    while attempts < int(max_attempts):
        pos = wait_position_update(pnl_state=pnl_state)
        if abs(pos) < dust:
            success = True
            break

        close_side = "SELL" if pos > 0 else "BUY"
        close_need = abs(pos)

        # 何をするか：サイズ刻みで“減らすだけ”の数量を作る（超過して反転しない）
        try:
            step = float(size_step or 0.0)
        except Exception:
            step = 0.0
        if step > 0.0:
            close_sz = _round_size(close_need, step)
        else:
            close_sz = close_need

        if close_sz <= 0.0:
            break
        if close_sz < dust:
            break

        attempts += 1
        now = _now_utc()
        fee_total, last_px = place_close_order(
            hb_path=hb_path,
            now=now,
            order_log=order_log,
            trade_log=trade_log,
            strategy_name=strategy_name,
            pnl_state=pnl_state,
            fee_total=fee_total,
            fee_bps=fee_bps,
            tick=tick,
            close_side=close_side,
            close_size=close_sz,
            ref_bid=ref_bid,
            ref_ask=ref_ask,
            last_mark=last_px,
            attempt=attempts,
            order_type="IOC",
            reason=reason,
        )

    pos_after = wait_position_update(pnl_state=pnl_state)
    if abs(pos_after) < dust:
        success = True
        # 何をするか：浮動小数の誤差で「ほぼゼロ」が残ると評価が歪むので、dust未満は0扱いに寄せる
        try:
            pnl_state["pos"] = 0.0
            pnl_state["avg"] = 0.0
            pos_after = 0.0
        except Exception:
            pass

    _hb_write(hb_path, event="flatten_done", ts=_now_utc().isoformat(), reason=reason, pos_after=pos_after, attempts=int(attempts), success=bool(success))
    # IOC フラット化の結果（成功/失敗）を summary に残す
    flatten_action = "done" if success else "fail"
    summary_dust = dust
    return fee_total, last_px, bool(success), flatten_action, summary_dust


def _attach_strategy_time_source(strategy, time_source) -> None:
    if strategy is None:
        return
    setter = getattr(strategy, "set_time_source", None)
    if callable(time_source) and callable(setter):
        try:
            setter(time_source)
        except Exception:
            pass
    children = getattr(strategy, "children", None)
    if children:
        for child in children:
            _attach_strategy_time_source(child, time_source)

# ---- メイン：疑似発注ランナー ----

def run_paper(
    cfg,
    strategy_name: str,
    *,
    strategies: Optional[List[str]] = None,
    strategy_cfg=None,
):
    """何をする関数か：疑似発注（place/ fill / cancel）を心拍に記録しながら回す最小ランナー"""
    product = getattr(cfg, "product_code", "FX_BTC_JPY")
    tick = float(getattr(cfg, "tick_size", 1))
    default_sz = float(getattr(getattr(cfg, "size", None), "default", 0.0) or 0.0)
    ttl_cfg = getattr(getattr(cfg, "features", None), "ttl_ms", None)  # 何をするか：TTLの既定値（ms）

    hb_path = Path("logs/runtime/heartbeat.ndjson")

    if strategies:
        strategy_list = list(strategies)
    else:
        cfg_strategies = getattr(cfg, "strategies", None)
        if cfg_strategies is None:
            strategy_list = [strategy_name] if strategy_name else []
        elif isinstance(cfg_strategies, (list, tuple)):
            strategy_list = [str(s) for s in cfg_strategies if s]
        else:
            strategy_list = [str(cfg_strategies)]
    primary_strategy = strategy_list[0] if strategy_list else strategy_name
    order_log = OrderLog(path=Path("logs/orders/order_log.parquet"))  # 何をするか：ordersのParquet出力先を明示
    trade_log = TradeLog(path=Path("logs/trades/trade_log.parquet"))  # 何をするか：tradesのParquet出力先を明示
    fee_bps = float(getattr(getattr(cfg, "fees", None), "bps", 0.0))  # 何をするか：手数料(bps)。設定が無ければ0bps
    pnl_state = {"pos": 0.0, "avg": 0.0, "realized": 0.0}  # 何をするか：建玉（数量・平均価格）と累計実現PnL
    fee_total = 0.0  # 何をするか：累計手数料（JPY）
    paper_pos = 0.0  # 何をするか：紙運転の在庫（買いで＋・売りで−）を持つ


    ob = OrderBook()  # 何をするか：ローカル板
    # 何をするか：bad_book切り分け用コンテキスト（板/WSの直近状態）を初期化
    __hb_ctx["product_code"] = product
    __hb_ctx["orderbook"] = ob
    __hb_ctx["last_rx_ts_by_channel"] = {}
    if hasattr(cfg, "model_dump"):
        cfg_payload = cfg.model_dump()
    elif isinstance(cfg, dict):
        cfg_payload = dict(cfg)
    else:
        cfg_payload = dict(getattr(cfg, "__dict__", {}))
    if strategy_list:
        cfg_payload["strategies"] = strategy_list
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
    )  # 何をするか：--strategy省略時でも config[strategies] をそのまま束ねて起動する
    _attach_strategy_time_source(strat, monotonic_ms)
    strategy_names = [
        getattr(child, "strategy_name", getattr(child, "name", "unknown"))
        for child in getattr(strat, "children", [])
    ] or [getattr(strat, "strategy_name", getattr(strat, "name", "unknown"))]
    primary_strategy = strategy_names[0]
    logger.info(
        f"paper start: product={product} strategies={strategy_names}"
    )
    # run.log の summary に「dust と flatten_action」を出すための箱（初期値）
    flatten_action = "n/a"  # skip/done/fail など、停止時フラット化の結果を入れる
    summary_dust = None     # 判定に使った dust（最小サイズ）を入れる
    _hb_write(
        hb_path,
        event="start",
        ts=_now_utc().isoformat(),
        mode="paper",
        product=product,
        strategy=primary_strategy,
        strategies=strategy_names,
    )
    start_at = _now_utc()
    canary_sec = int(getattr(cfg, "dry_run_max_sec", 3600))  # 何をするか：安全のため紙運転も1時間で停止（必要に応じて調整）

    # 何をするか：受理ID(acc) → メタ情報（side/px/sz/ttl/置いた時刻/tag）
    live_orders: Dict[str, Dict[str, Any]] = {}
    tx_cfg = _act(cfg, "tx", None)
    min_ms = 600  # throttle の最小間隔(ms)。TTL(650/900ms)より短くして「常時 throttle」を減らす
    min_tx_ms = min_ms  # 何をするか：新規発注の最小間隔(ms)
    place_dedup_ms = int(_act(tx_cfg, "place_dedup_ms", 0) or 0)  # 何をするか：同一発注の短時間重複抑止(ms)
    last_place: Dict[str, datetime] = {}  # 何をするか：直近に出した(side|px|tag)→時刻
    _last_tx_at: datetime = _now_utc() - timedelta(milliseconds=(min_tx_ms or 0))  # 何をするか：直近の送信時刻（初期は即送れる状態）
    orders_cfg = _act(cfg, "orders", None)
    max_inflight = _act(orders_cfg, "max_inflight", None)  # 何をするか：同時インフライト上限（Noneなら無効）
    gate_cfg = _act(cfg, "gate", None)
    max_inflight_per_key = _act(gate_cfg, "max_inflight_per_key", None)  # 何をするか：タグ基底ごとの上限
    size_cfg = _act(cfg, "size", None)
    min_sz = float(_act(size_cfg, "min", 0.0) or 0.0)  # 何をするか：最小ロット
    size_step = float(_act(size_cfg, "step", min_sz) or min_sz)  # 何をするか：サイズ刻み（未指定はminを採用）
    risk_cfg = _act(cfg, "risk", None)
    try:
        max_pos = float(_act(risk_cfg, "max_inventory", 0.0) or 0.0)
    except Exception:
        max_pos = 0.0
    try:
        brake_ratio = float(_act(risk_cfg, "inv_brake_ratio", 0.25) or 0.25)
    except Exception:
        brake_ratio = 0.25
    exit_reason = "exit"  # 何をするか：終了理由（heartbeatに載せる）
    last_mark: Optional[float] = None  # 何をするか：最終の評価価格（mid）

    try:
        for ev in stream_events(product):
            now = _now_utc()

            # 何をするか：bad_book切り分け用に、直近のWS受信時刻をチャンネル別に覚える
            try:
                ch_rx = str((ev or {}).get("channel") or "")
                ts_rx = (ev or {}).get("ts")
                if isinstance(ts_rx, str):
                    try:
                        ts_rx_dt = datetime.fromisoformat(ts_rx.replace("Z", "+00:00"))
                    except Exception:
                        ts_rx_dt = None
                elif isinstance(ts_rx, datetime):
                    ts_rx_dt = ts_rx
                else:
                    ts_rx_dt = None
                rx_map = __hb_ctx.get("last_rx_ts_by_channel")
                if isinstance(rx_map, dict) and ch_rx and isinstance(ts_rx_dt, datetime):
                    rx_map[ch_rx] = ts_rx_dt
            except Exception:
                pass

            # 何をするか：カナリア（安全タイマー）
            if canary_sec and (now - start_at).total_seconds() >= canary_sec:
                exit_reason = "canary"
                logger.info("paper: canary reached → stop")
                break

            # 何をするか：イベントをローカル板へ反映
            try:
                ob.update_from_event(ev)
            except Exception:
                pass  # 何をするか：壊れたイベントは捨てる

            # 何をするか：最良気配の取得と異常板ガード
            bid = _best_px(getattr(ob, "best_bid", None))
            ask = _best_px(getattr(ob, "best_ask", None))
            if (bid is None) or (ask is None) or (ask <= bid):
                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="bad_book")
                continue
            last_mark = (float(bid) + float(ask)) / 2.0  # 何をするか：最終の評価価格（mid）を更新

            # 何をするか：TTL超過の疑似 cancel
            for acc, meta in list(live_orders.items()):
                ttl_ms = meta.get("ttl_ms")
                placed_at = meta.get("placed_at")
                if ttl_ms and placed_at and ((now - placed_at).total_seconds() * 1000.0 >= float(ttl_ms)):
                    _hb_write(hb_path, event="cancel", ts=now.isoformat(), acc=acc, reason="ttl")
                    _orders_add(ts=now.isoformat(), action="cancel", tif=None, ttl_ms=ttl_ms, px=meta.get("px"), sz=meta.get("rem_sz", 0.0), reason="ttl")  # 何をするか：取消をorders.csvへ
                    order_log.add(ts=now.isoformat(), action="cancel", tif=None, ttl_ms=ttl_ms, px=meta.get("px"), sz=meta.get("rem_sz", 0.0), reason="ttl")  # 何をするか：Parquetにもcancelを記録
                    del live_orders[acc]

            def _register_actions(action_list: Optional[List[Dict[str, Any]]]) -> None:
                nonlocal _last_tx_at
                for act in action_list or []:
                    if not act:
                        continue
                    act_type = str(_act(act, "type", "place") or "place").lower()
                    if act_type == "cancel_tag":
                        tag_val = str(_act(act, "tag", ""))
                        for acc_key, meta in list(live_orders.items()):
                            if tag_val and (not _tag_matches(str(meta.get("tag", "")), tag_val)):
                                continue
                            _hb_write(hb_path, event="cancel", ts=now.isoformat(), acc=acc_key, reason="cancel_tag", tag=tag_val)
                            _orders_add(ts=now.isoformat(), action="cancel", tif=None, ttl_ms=meta.get("ttl_ms"), px=meta.get("px"), sz=meta.get("rem_sz", 0.0), reason=tag_val)
                            order_log.add(ts=now.isoformat(), action="cancel", tif=None, ttl_ms=meta.get("ttl_ms"), px=meta.get("px"), sz=meta.get("rem_sz", 0.0), reason=tag_val)
                            del live_orders[acc_key]
                        continue

                    if act_type != "place":
                        continue

                    payload = _act(act, "order", None)
                    target = payload or act
                    sz = float(_act(target, "size", default_sz) or 0.0)
                    if sz <= 0.0:
                        continue
                    sz = _round_size(sz, size_step)
                    if min_sz > 0.0 and sz < min_sz:
                        sz = min_sz  # 何をするか：最小ロット未満は下限に引き上げ（約定拒否を防ぐ）

                    side = _side_norm(_act(target, "side"))
                    px_raw = _act(target, "price", None)
                    if px_raw is None:
                        px_val = bid if side == "BUY" else ask
                    else:
                        try:
                            px_val = float(px_raw)
                        except Exception:
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="price_invalid")
                            continue
                    if px_val is None:
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="price_fallback_unavailable")
                        continue

                    px_val = _round_to_tick(px_val, tick)
                    tag_val = _act(target, "tag", "")
                    tag_val_s = "" if tag_val is None else str(tag_val)
                    elapsed_ms = (now - _last_tx_at).total_seconds() * 1000.0  # 何をするか：前回送信からの経過ms
                    if min_tx_ms and (elapsed_ms < float(min_tx_ms)):
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="throttle", elapsed_ms=int(elapsed_ms), min_ms=int(min_tx_ms))
                        continue
                    dedup_key = f"{side}|{px_val}|{tag_val_s}"
                    if place_dedup_ms and (dedup_key in last_place) and ((now - last_place[dedup_key]).total_seconds() * 1000.0 < float(place_dedup_ms)):
                        _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="dedup", key=dedup_key, within_ms=int(place_dedup_ms))
                        continue

                    # 何をするか：インフライト上限（建玉を増やす注文だけに適用）
                    inflight_count = sum(1 for v in live_orders.values() if float(v.get("rem_sz", 0.0) or 0.0) > 1e-12)
                    inflight_by_key: Dict[str, int] = {}
                    for v in live_orders.values():
                        if float(v.get("rem_sz", 0.0) or 0.0) <= 1e-12:
                            continue
                        k = _gate_key(str(v.get("tag", "")))
                        if not k:
                            continue
                        inflight_by_key[k] = inflight_by_key.get(k, 0) + 1
                    reduces_inventory = (paper_pos > 0 and side == "SELL") or (paper_pos < 0 and side == "BUY")
                    try:
                        if (max_inflight is not None) and (not reduces_inventory) and inflight_count >= int(max_inflight):
                            _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inflight_guard", A=inflight_count, limit=int(max_inflight))
                            continue
                    except Exception:
                        pass
                    key_for_gate = _gate_key(tag_val_s)
                    try:
                        if (max_inflight_per_key is not None) and key_for_gate and (not reduces_inventory):
                            if inflight_by_key.get(key_for_gate, 0) >= int(max_inflight_per_key):
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="inflight_guard", key=key_for_gate, A=inflight_by_key.get(key_for_gate, 0), limit=int(max_inflight_per_key))
                                continue
                    except Exception:
                        pass

                    orders, dropped = _apply_inventory_brake(
                        [{"side": side}],
                        pos=paper_pos,
                        max_pos=max_pos,
                        brake_ratio=brake_ratio,
                    )  # 発注直前で在庫ブレーキを掛ける
                    if dropped:
                        _hb_write(
                            hb_path,
                            event="pause",
                            ts=now.isoformat(),
                            reason="inv_brake",
                            pos=float(paper_pos),
                            dropped=int(dropped),
                            max_pos=float(max_pos),
                            brake_ratio=float(brake_ratio),
                        )
                        continue

                    acc_new = f"paper-{uuid.uuid4().hex[:12]}"
                    ttl_ms = _act(target, "ttl_ms", ttl_cfg)
                    tif = _act(target, "tif", "GTC")
                    tag_val = tag_val_s
                    live_orders[acc_new] = {
                        "side": side,
                        "px": px_val,
                        "sz": sz,
                        "rem_sz": sz,
                        "ttl_ms": ttl_ms,
                        "placed_at": now,
                        "tag": tag_val,
                        "order": payload if payload is not None else SimpleNamespace(side=side, price=px_val, size=sz, tif=tif, ttl_ms=ttl_ms, tag=tag_val),
                    }
                    _hb_write(hb_path, event="place", ts=now.isoformat(), acc=acc_new, side=side, px=px_val, sz=sz, tif=tif, ttl_ms=ttl_ms, reason=tag_val)
                    _orders_add(ts=now.isoformat(), action="place", tif=tif, ttl_ms=ttl_ms, px=px_val, sz=sz, reason=tag_val)
                    order_log.add(ts=now.isoformat(), action="place", tif=tif, ttl_ms=ttl_ms, px=px_val, sz=sz, reason=tag_val)
                    _last_tx_at = now
                    if place_dedup_ms:
                        last_place[dedup_key] = now

            ch = str(ev.get("channel") or "")  # 何をするか：stream_events() は channel/message を直で返す

            # 何をするか：戦略評価（boardイベント時だけ）。executionsで評価すると発注が過多になりやすい
            if ch.startswith("lightning_board_"):
                try:
                    actions = strat.evaluate(ob, now, cfg)
                except Exception as e:
                    logger.exception(f"paper: strategy error: {e}")
                    _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="strategy_error")
                    continue
                _register_actions(actions)

            execs = []  # 何をするか：この周回で観測した出来高（price/sizeの配列）をためる
            try:
                msg = ev.get("message")
                if "executions" in ch and isinstance(msg, list):
                    for m in msg:
                        pr = m.get("price") if isinstance(m, dict) else None
                        sz = m.get("size") if isinstance(m, dict) else None
                        sd = m.get("side") if isinstance(m, dict) else None
                        if (pr is not None) and (sz is not None):
                            execs.append({"price": float(pr), "size": float(sz), "side": _side_norm(sd)})  # 何をするか：後段のFIFO配分用に整形
            except Exception:
                execs = []


            if execs:
                # FIFO（置いた順）で出来高を割り当てる
                q = sorted(live_orders.items(), key=lambda kv: kv[1].get("placed_at", now))
                for exi in execs:
                    ex_price = exi["price"]
                    ex_side = exi.get("side")
                    ex_rem = float(exi["size"])
                    if ex_rem <= 0:
                        continue
                    for acc, meta in q:
                        if ex_rem <= 0:
                            break
                        side, px, rem_sz, tag = meta["side"], meta["px"], meta.get("rem_sz", 0.0), meta["tag"]
                        if rem_sz <= 0.0:
                            continue
                        # 何をするか：実運用寄せ（約定サイドの整合）
                        # - BUY指値が当たるのは SELLの約定（売り成行が板を叩く）
                        # - SELL指値が当たるのは BUYの約定（買い成行が板を叩く）
                        if (side == "BUY" and ex_side != "SELL") or (side == "SELL" and ex_side != "BUY"):
                            continue
                        # 何をするか：価格条件（約定価格が指値を跨いだら当たりとみなす）
                        hit = (side == "BUY" and ex_price <= px) or (side == "SELL" and ex_price >= px)
                        if not hit:
                            continue
                        fill_sz = float(min(rem_sz, ex_rem))
                        meta["rem_sz"] = rem_sz - fill_sz
                        ex_rem -= fill_sz
                    

                        # 何をするか：trades.csv と heartbeat に fill/partial を記録
                        # - 価格は「自分の指値」で約定したものとして扱う（楽観的な改善約定を避ける）
                        fill_px = float(px)
                        realized_gross = _apply_fill_and_pnl(pnl_state, side, fill_px, fill_sz)  # 何をするか：建玉更新＆実現PnL(手数料前)
                        fee_jpy = fill_px * fill_sz * (fee_bps / 10000.0)  # 何をするか：約定金額×bpsで手数料
                        fee_total += fee_jpy  # 何をするか：累計手数料
                        realized = realized_gross - fee_jpy  # 何をするか：PnLは手数料控除後（ネット）
                        paper_pos = pnl_state["pos"]  # 何をするか：CSV/Parquet出力用の在庫を同期
                        ts_jst = now.astimezone(timezone(timedelta(hours=9))).isoformat()
                        _trades_add(ts=ts_jst, side=side, px=fill_px, sz=fill_sz, fee=fee_jpy, pnl=realized, strategy=strategy_name, tag=tag, inventory_after=paper_pos)  # 何をするか：CSVへ約定を記録（JST）
                        trade_log.add(ts=ts_jst, side=side, px=fill_px, sz=fill_sz, fee=fee_jpy, pnl=realized, strategy=strategy_name, tag=tag, inventory_after=paper_pos, window_funding=False, window_maint=False)  # 何をするか：Parquetへ約定を記録（JST）

                        done_local = meta.get("rem_sz", 0.0) <= 1e-12  # 何をするか：この注文が完了したか（残量ゼロか）を安全に再判定
                        order_log.add(ts=now.isoformat(), action=("fill" if done_local else "partial"), tif=None, ttl_ms=None, px=fill_px, sz=fill_sz, reason=tag)  # 何をするか：Parquetにもfill/partialを記録


                        done = meta["rem_sz"] <= 1e-12
                        _hb_write(hb_path, event=("fill" if done else "partial"), ts=now.isoformat(), acc=acc, side=side, px=fill_px, sz=fill_sz, reason=tag)
                        _orders_add(ts=now.isoformat(), action=("fill" if done else "partial"), tif=None, ttl_ms=None, px=fill_px, sz=fill_sz, reason=tag)

                        if hasattr(strat, "on_fill"):
                            fill_event = SimpleNamespace(
                                side=side,
                                price=fill_px,
                                size=fill_sz,
                                pnl=realized,
                                tag=tag,
                                done=done,
                                order=meta.get("order"),
                                acceptance_id=acc,
                            )
                            try:
                                follow_actions = strat.on_fill(ob, fill_event)
                            except Exception as e:
                                logger.exception(f"paper: strategy on_fill error: {e}")
                                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="strategy_on_fill_error")
                            else:
                                _register_actions(follow_actions)

                # 何をするか：残量ゼロになった注文を片付ける
                for acc in [k for k, v in live_orders.items() if v.get("rem_sz", 0.0) <= 1e-12]:
                    del live_orders[acc]


    except KeyboardInterrupt:
        exit_reason = "interrupt"
        logger.warning("paper: interrupted by user")
    finally:
        # 何をするか：停止時は「新規発注なし」＋「全キャンセル」＋「フラット化」を通してから終了する
        now = _now_utc()
        cancel_all_open_orders(live_orders=live_orders, hb_path=hb_path, now=now, order_log=order_log, reason="halt")

        # 何をするか：posが残っていれば在庫を0へ寄せ切る（canaryでもinterruptでも同様）
        bid = _best_px(getattr(ob, "best_bid", None))
        ask = _best_px(getattr(ob, "best_ask", None))
        fee_total, flat_px, flat_ok, flatten_action, summary_dust = flatten_on_halt(
            hb_path=hb_path,
            order_log=order_log,
            trade_log=trade_log,
            strategy_name=strategy_name,
            pnl_state=pnl_state,
            fee_total=fee_total,
            fee_bps=fee_bps,
            tick=tick,
            size_step=size_step,
            min_sz=min_sz,
            ref_bid=bid,
            ref_ask=ask,
            last_mark=last_mark,
            reason=str(exit_reason or "halt"),
            max_attempts=5,
        )
        if last_mark is None and flat_px is not None:
            last_mark = float(flat_px)
        if not flat_ok:
            try:
                logger.warning(f"paper: flatten failed pos={float(pnl_state.get('pos', 0.0) or 0.0):.6f}")
            except Exception:
                logger.warning("paper: flatten failed")

        # 何をするか：flatten後の時刻で summary/kill を打つ（tsの前後関係が崩れないようにする）
        now = _now_utc()

        # 何をするか：最終サマリ（実現/未実現/建玉）を心拍とログに残す
        pos = float(pnl_state.get("pos", 0.0) or 0.0)
        avg_px = float(pnl_state.get("avg", 0.0) or 0.0)
        realized_gross = float(pnl_state.get("realized", 0.0) or 0.0)
        realized_net = realized_gross - float(fee_total or 0.0)
        if last_mark is not None and abs(pos) > 1e-12:
            unreal = (float(last_mark) - avg_px) * pos
        else:
            unreal = 0.0
        total = realized_net + unreal
        mark_px = f"{float(last_mark):.1f}" if last_mark is not None else "None"
        logger.info(f"paper summary: reason={exit_reason} realized_net={realized_net:.3f} fee={fee_total:.3f} unreal={unreal:.3f} total={total:.3f} pos={pos:.6f} avg={avg_px:.1f} mark={mark_px} dust={summary_dust} flatten_action={flatten_action}")
        _hb_write(
            hb_path,
            event="summary",
            ts=now.isoformat(),
            reason=exit_reason,
            realized_net_jpy=realized_net,
            fee_jpy=float(fee_total or 0.0),
            unreal_jpy=unreal,
            total_jpy=total,
            pos=pos,
            avg_px=avg_px,
            mark_px=float(last_mark) if last_mark is not None else None,
            dust=summary_dust,  # summaryにもdustを入れて「posが残って見えるのがダストか」を即判別できるようにする
            flatten_action=flatten_action,  # summaryにもflatten結果(skip/done/fail)を入れて終了時の状態を1イベントで追えるようにする
        )
        if exit_reason == "canary":
            _hb_write(hb_path, event="kill", ts=now.isoformat(), reason="canary", daily_pnl_jpy=total, dd_jpy=0.0)
        order_log.flush()  # 何をするか：orders.parquetへ反映
        trade_log.flush()  # 何をするか：trades.parquetへ反映

        _hb_write(hb_path, event="stop", ts=_now_utc().isoformat(), reason=exit_reason)
        logger.info(f"paper end: reason={exit_reason}")
