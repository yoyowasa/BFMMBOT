# src/backtest/runner.py
# 役割：録画テープを再生→ローカル板更新→戦略評価→最小シミュ適用までを一気通貫で実行する
from __future__ import annotations

from typing import Dict, Any  # サマリ返却用
from datetime import datetime  # 受信時刻の解析
from loguru import logger  # サマリ出力

from src.backtest.loader import iter_tape  # テープ読み（前ステップ）
from src.core.orderbook import OrderBook  # ローカル板（前ステップ）
from src.core.analytics import DecisionLog  # 【関数】決定ログ（analytics/decision_log.parquet）

from src.core.logs import OrderLog, TradeLog  # 【関数】Parquet ログ出力

from src.core.simulator import MiniSimulator  # 最小シミュ（本ステップ）
from src.strategy.stall_then_strike import StallThenStrike  # 戦略#1（本ステップ）
from src.strategy.age_microprice import AgeMicroprice  # #3 エイジ×MPを選べるようにする
from src.strategy.cancel_add_gate import CancelAddGate  # #2 キャンセル比ゲートを選べるようにする

def _parse_iso(ts: str) -> datetime:
    """【関数】ISO文字列→datetime（'Z' を +00:00 に）"""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def run_backtest_min(cfg, tape_path: str, strategy_name: str = "stall_then_strike") -> Dict[str, Any]:
    """【関数】最小バックテストランナー
    - 役割: boardでローカル板を更新→戦略でPlace/Cancelを出す→executionsでFill判定
    - 返り値: 置いた/消した/埋まった件数などのサマリ
    """
    order_log = OrderLog("logs/orders/order_log.parquet")  # 【関数】発注ログの器
    decision_log = DecisionLog("logs/analytics/decision_log.parquet")  # 【関数】決定ログの器
    trade_log = TradeLog("logs/trades/trade_log.parquet")  # 【関数】約定ログの器
    Q = 0.0   # 【関数】在庫（最小）：+はロング、-はショート
    A = 0.0   # 【関数】平均建値（最小）
    R = 0.0   # 【関数】実現PnLの累計（最小）

    ob = OrderBook(tick_size=float(getattr(cfg, "tick_size", 1.0)))
    sim = MiniSimulator()
    # 【関数】戦略選択：引数で #1/#2/#3 を切替（既定は #1）
    if strategy_name == "cancel_add_gate":
        strat = CancelAddGate()
    elif strategy_name == "age_microprice":
        strat = AgeMicroprice()
    else:
        strat = StallThenStrike()



    start = cfg.period.start if getattr(cfg, "period", None) else None
    end = cfg.period.end if getattr(cfg, "period", None) else None

    total = 0
    for ev in iter_tape(tape_path, start=start, end=end):
        total += 1
        now = _parse_iso(ev["ts"])
        ch = ev.get("channel", "")

        if ch.startswith("lightning_board_"):
            # ローカル板を適用→TTLチェック→戦略評価→アクション適用
            ob.update_from_event(ev)
            # ── features を集計（best_age / ca_ratio / spread）
            ca_win = getattr(getattr(cfg, "features", None), "ca_ratio_win_ms", 500)
            _feats = {
                "best_age_ms": ob.best_age_ms(now),
                "ca_ratio": ob.ca_ratio(now, window_ms=ca_win),
                "spread_tick": ob.spread_ticks(),
            }
            _spread_state = "zero" if _feats["spread_tick"] == 0 else "ge1"

            # ── 戦略を評価し、decision を文字列で要約
            actions = strat.evaluate(ob, now, cfg)
            if not actions:
                _decision = "none"
            else:
                _places = [a for a in actions if a.get("type") == "place"]
                _cancels = [a for a in actions if a.get("type") == "cancel_tag"]
                if _places:
                    _sides = {p["order"].side for p in _places if "order" in p}
                    _decision = "place_both" if _sides == {"buy", "sell"} else f"place_{list(_sides)[0]}"
                elif _cancels:
                    _decision = "cancel"
                else:
                    _decision = "none"

            # ── 決定ログに1行追加（expected_edge_bp/eta_ms は最小版なので未算出）
            decision_log.add(
                ts=now.isoformat(),
                strategy=strat.name,
                decision=_decision,
                features=_feats,
                expected_edge_bp=None,
                eta_ms=None,
                ca_ratio=_feats["ca_ratio"],
                best_age_ms=_feats["best_age_ms"],
                spread_state=_spread_state,
            )

# ── 以降は、従来どおり actions を適用（下の for 置換に続く）

            expired = sim.on_time(now)  # 【関数】TTL失効の検出
            for o in expired:  # 失効は cancel として orders に記録
                order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                            px=o.price, sz=o.remaining, reason="ttl")

            sim.on_time(now)
            for act in actions:

                if act.get("type") == "place":
                    # 同タグの未約定が残っていれば重複発注を抑止（最小のゲート）
                    if sim.has_open_tag(act["order"].tag):
                        continue
                    sim.place(act["order"], now)
                    o = act["order"]
                    order_log.add(ts=now.isoformat(), action="place", tif=o.tif, ttl_ms=o.ttl_ms, px=o.price, sz=o.size, reason=o.tag)  # バックテストでも“タグ名”で統一


                elif act.get("type") == "cancel_tag":
                    for o in sim.cancel_by_tag(act["tag"]):  # 取消明細ごとに orders に記録
                        order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                    px=o.price, sz=o.remaining, reason="strategy")


        elif ch.startswith("lightning_executions_"):
            # 約定イベントでFill判定→TTLも進める
            fills = sim.on_executions(ev.get("message") or [], now)  # 明細を受け取る
            for f in fills:
                o = f["order"]; px = float(f["price"]); sz = float(f["size"])
                # 1) orders ログ（fill/partial）
                order_log.add(ts=f["ts"], action=("partial" if f["partial"] else "fill"),
                            tif=o.tif, ttl_ms=o.ttl_ms, px=px, sz=sz, reason=f["tag"])
                # 2) 最小PnL更新（FIFO近似）と在庫
                realized = 0.0
                if f["side"] == "sell":
                    if Q > 0:  # ロングの利確
                        matched = min(sz, Q)
                        realized += (px - A) * matched
                        Q -= matched
                        if sz > matched:  # 反転→ショート開始
                            A = px
                            Q -= (sz - matched)
                    else:  # 新規 or 追いショート
                        A = (A * abs(Q) + px * sz) / (abs(Q) + sz) if Q < 0 else px
                        Q -= sz
                else:  # buy
                    if Q < 0:  # ショートの手仕舞い
                        matched = min(sz, -Q)
                        realized += (A - px) * matched
                        Q += matched
                        if sz > matched:  # 反転→ロング開始
                            A = px
                            Q += (sz - matched)
                    else:  # 新規 or 追いロング
                        A = (A * Q + px * sz) / (Q + sz) if Q > 0 else px
                        Q += sz
                R += realized
                # 3) trades ログ（実現PnLを記録）
                trade_log.add(ts=f["ts"], side=f["side"], px=px, sz=sz, pnl=realized,
                            strategy=strat.name, tag=f["tag"], inventory_after=Q)


            sim.on_time(now)
            
    order_log.flush()  # 【関数】orders を Parquet へ
    trade_log.flush()  # 【関数】trades を Parquet へ
    decision_log.flush()  # 【関数】analytics/decision_log.parquet へ保存


    summary = {
        "events": total,
        "placed": sim.placed,
        "cancelled": sim.cancelled,
        "filled": sim.filled,
        "open_orders_end": len(sim.open),
        "strategy": strat.name,
        "realized_pnl": R,
    }
    logger.info(f"runner summary: {summary}")
    return summary
