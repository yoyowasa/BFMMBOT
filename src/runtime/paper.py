# 何をするファイルか：疑似発注（ペーパー運転）の最小ランナー
# - 戦略が出した action を「place」として心拍に記録
# - 板の最良気配に当たれば「fill」として記録（即時・全量）
# - TTL 超過は「cancel」
# - 実際の取引所 REST は呼ばない（完全オフラインな紙運転）

from pathlib import Path  # 何をするか：ログファイルの場所を扱う
from datetime import datetime, timezone  # 何をするか：UTCの現在時刻・経過の計算
import json  # 何をするか：心拍を ndjson(1行1JSON) で書く
import uuid  # 何をするか：疑似の受理ID(acc)を作る
from typing import Any, Dict, List, Optional  # 何をするか：型ヒント（辞書など）
import csv  # 何をするか：orders.csv / trades.csv に追記するために使う
from types import SimpleNamespace  # 何をするか：戦略 on_fill に渡す簡易オブジェクトを作る

from loguru import logger  # 何をするか：run.log へ可読ログを出す
from src.core.realtime import stream_events  # 何をするか：WSイベントの同期ジェネレーター
from src.core.orderbook import OrderBook  # 何をするか：ローカル板（best_bid/best_ask を持つ）
from src.strategy.stall_then_strike import StallThenStrike  # 何をするか：例の戦略（必要なら増やす）
from src.core.logs import OrderLog, TradeLog  # 何をするか：Parquet出力（orders/trades）用のヘルパ

# ---- 小さなユーティリティ（live.pyに依存せず単体で動く最小セット） ----

def _now_utc() -> datetime:
    """何をする関数か：UTCの現在時刻を返す"""
    return datetime.now(timezone.utc)

def _hb_write(hb_path: Path, **fields):
    """何をする関数か：心拍を ndjson で1行追記する（paper用の簡易版）"""
    try:
        hb_path.parent.mkdir(parents=True, exist_ok=True)
        with hb_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(fields, ensure_ascii=False) + "\n")
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

def _select_strategy(strategy_name: str, cfg, strategy_cfg=None):
    """何をする関数か：live側の選択ロジックを再利用して戦略インスタンスを生成する"""
    # 何をするか：関数内だけで使う遅延importで依存ループを避ける
    from src.runtime.live import _select_strategy as _select_strategy_live
    return _select_strategy_live(strategy_name, cfg, strategy_cfg=strategy_cfg)


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

    strategy_list = strategies or [strategy_name]
    primary_strategy = strategy_list[0] if strategy_list else strategy_name
    order_log = OrderLog(path=Path("logs/orders/order_log.parquet"))  # 何をするか：ordersのParquet出力先を明示
    trade_log = TradeLog(path=Path("logs/trades/trade_log.parquet"))  # 何をするか：tradesのParquet出力先を明示
    fee_bps = float(getattr(getattr(cfg, "fees", None), "bps", 0.0))  # 何をするか：手数料(bps)。設定が無ければ0bps
    pnl_state = {"pos": 0.0, "avg": 0.0, "realized": 0.0}  # 何をするか：建玉（数量・平均価格）と累計実現PnL
    paper_pos = 0.0  # 何をするか：紙運転の在庫（買いで＋・売りで−）を持つ


    ob = OrderBook()  # 何をするか：ローカル板
    strat = _select_strategy(primary_strategy, cfg, strategy_cfg=strategy_cfg)  # 何をするか：戦略インスタンス
    strategy_names = [
        getattr(child, "strategy_name", getattr(child, "name", "unknown"))
        for child in getattr(strat, "children", [])
    ] or [getattr(strat, "strategy_name", getattr(strat, "name", "unknown"))]
    primary_strategy = strategy_names[0]
    logger.info(
        f"paper start: product={product} strategies={strategy_names}"
    )
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

    try:
        for ev in stream_events(product):
            now = _now_utc()

            # 何をするか：カナリア（安全タイマー）
            if canary_sec and (now - start_at).total_seconds() >= canary_sec:
                _hb_write(hb_path, event="kill", ts=now.isoformat(), reason="canary", daily_pnl_jpy=0.0, dd_jpy=0.0)
                _hb_write(hb_path, event="stop", ts=_now_utc().isoformat(), reason="exit")
                logger.info("paper end: canary reached")
                return

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
                for act in action_list or []:
                    if not act:
                        continue
                    act_type = str(_act(act, "type", "place") or "place").lower()
                    if act_type == "cancel_tag":
                        tag_val = str(_act(act, "tag", ""))
                        for acc_key, meta in list(live_orders.items()):
                            if tag_val and str(meta.get("tag", "")) != tag_val:
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
                    acc_new = f"paper-{uuid.uuid4().hex[:12]}"
                    ttl_ms = _act(target, "ttl_ms", ttl_cfg)
                    tif = _act(target, "tif", "GTC")
                    tag_val = _act(target, "tag", "")
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

            # 何をするか：戦略評価（place/hold の判断）
            try:
                actions = strat.evaluate(ob, now, cfg)
            except Exception as e:
                logger.exception(f"paper: strategy error: {e}")
                _hb_write(hb_path, event="pause", ts=now.isoformat(), reason="strategy_error")
                continue

            _register_actions(actions)


            execs = []  # 何をするか：この周回で観測した出来高（price/sizeの配列）をためる
            try:
                ch = str(ev.get("channel") or "")          # 何をするか：stream_events() は channel/message を直で返す
                msg = ev.get("message")
                if "executions" in ch and isinstance(msg, list):
                    for m in msg:
                        pr = m.get("price") if isinstance(m, dict) else None
                        sz = m.get("size") if isinstance(m, dict) else None
                        if (pr is not None) and (sz is not None):
                            execs.append({"price": float(pr), "size": float(sz)})  # 何をするか：後段のFIFO配分用に整形
            except Exception:
                execs = []


            if execs:
                # FIFO（置いた順）で出来高を割り当てる
                q = sorted(live_orders.items(), key=lambda kv: kv[1].get("placed_at", now))
                for exi in execs:
                    ex_price = exi["price"]
                    ex_rem = float(exi["size"])
                    if ex_rem <= 0:
                        continue
                    for acc, meta in q:
                        if ex_rem <= 0:
                            break
                        side, px, rem_sz, tag = meta["side"], meta["px"], meta.get("rem_sz", 0.0), meta["tag"]
                        if rem_sz <= 0.0:
                            continue
                        # 何をするか：BUYは exec_price <= 注文価格、SELLは exec_price >= 注文価格 で当たるとみなす
                        hit = (side == "BUY" and ex_price <= px) or (side == "SELL" and ex_price >= px)
                        if not hit:
                            continue
                        fill_sz = float(min(rem_sz, ex_rem))
                        meta["rem_sz"] = rem_sz - fill_sz
                        ex_rem -= fill_sz
                    

                        # 何をするか：trades.csv と heartbeat に fill/partial を記録
                        realized = _apply_fill_and_pnl(pnl_state, side, ex_price, fill_sz)  # 何をするか：建玉更新＆実現PnL(手数料前)
                        fee_jpy = ex_price * fill_sz * (fee_bps / 10000.0)  # 何をするか：約定金額×bpsで手数料
                        realized -= fee_jpy  # 何をするか：PnLは手数料控除後（ネット）
                        paper_pos = pnl_state["pos"]  # 何をするか：CSV/Parquet出力用の在庫を同期
                        _trades_add(ts=now.isoformat(), side=side, px=ex_price, sz=fill_sz, fee=fee_jpy, pnl=realized, strategy=strategy_name, tag=tag, inventory_after=paper_pos)  # 何をするか：CSVへ約定を記録
                        trade_log.add(ts=now.isoformat(), side=side, px=ex_price, sz=fill_sz, fee=fee_jpy, pnl=realized, strategy=strategy_name, tag=tag, inventory_after=paper_pos, window_funding=False, window_maint=False)  # 何をするか：Parquetへ約定を記録

                        done_local = meta.get("rem_sz", 0.0) <= 1e-12  # 何をするか：この注文が完了したか（残量ゼロか）を安全に再判定
                        order_log.add(ts=now.isoformat(), action=("fill" if done_local else "partial"), tif=None, ttl_ms=None, px=ex_price, sz=fill_sz, reason=tag)  # 何をするか：Parquetにもfill/partialを記録


                        done = meta["rem_sz"] <= 1e-12
                        _hb_write(hb_path, event=("fill" if done else "partial"), ts=now.isoformat(), acc=acc, side=side, px=ex_price, sz=fill_sz, reason=tag)
                        _orders_add(ts=now.isoformat(), action=("fill" if done else "partial"), tif=None, ttl_ms=None, px=ex_price, sz=fill_sz, reason=tag)

                        if hasattr(strat, "on_fill"):
                            fill_event = SimpleNamespace(
                                side=side,
                                price=ex_price,
                                size=fill_sz,
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
        logger.warning("paper: interrupted by user")
    finally:
        # 何をするか：残りがあれば cancel を書いてから終了
        now = _now_utc()
        for acc in list(live_orders.keys()):
            _hb_write(hb_path, event="cancel", ts=now.isoformat(), acc=acc, reason="halt")
        order_log.flush()  # 何をするか：orders.parquetへ反映
        trade_log.flush()  # 何をするか：trades.parquetへ反映

        _hb_write(hb_path, event="stop", ts=_now_utc().isoformat(), reason="exit")
        logger.info("paper end")
