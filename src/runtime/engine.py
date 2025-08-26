# src/runtime/engine.py
# 役割：リアルタイムの“paper実行”エンジン（WS→ローカル板→戦略→最小シミュ→ログ保存）
# - 【関数】run_paper：WSイベントを流し込み、#1/#2戦略を評価→発注/取消→Fill反映→ログ保存
# - 【関数】_guard_midmove_bp：30秒のミッド変化(bps)を監視し、閾値超なら新規停止＋全取消
# - ログは文書仕様どおり logs/orders・logs/trades・logs/analytics にParquetで出力する
from __future__ import annotations

import asyncio  # 非同期ループ/キャンセル
from collections import deque  # 30sミッド履歴でガード
from datetime import datetime, timezone  # ts解析と現在時刻
from typing import Deque, Tuple  # 型ヒント

from loguru import logger  # 実行ログ
from src.core.realtime import event_stream  # 【関数】WS購読（board/executions）:contentReference[oaicite:2]{index=2}
from src.core.orderbook import OrderBook  # 【関数】ローカル板（Best/Spread/C-A）:contentReference[oaicite:3]{index=3}
from src.core.simulator import MiniSimulator  # 【関数】最小約定シミュ（価格タッチ）:contentReference[oaicite:4]{index=4}
from src.core.logs import OrderLog, TradeLog  # 【関数】発注/約定ログ（Parquet）:contentReference[oaicite:5]{index=5}
from src.core.analytics import DecisionLog  # 【関数】意思決定ログ（Parquet）:contentReference[oaicite:6]{index=6}
from src.strategy.stall_then_strike import StallThenStrike  # #1 静止→一撃（ON）:contentReference[oaicite:7]{index=7}
from src.strategy.cancel_add_gate import CancelAddGate  # #2 キャンセル比ゲート（ON）:contentReference[oaicite:8]{index=8}

def _parse_iso(ts: str) -> datetime:
    """【関数】ISO→datetime（'Z'も+00:00に正規化）"""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _now_utc() -> datetime:
    """【関数】現在UTC（実行時刻の印）"""
    return datetime.now(timezone.utc)

class PaperEngine:
    """リアルタイム“paper”の最小エンジン"""

    def __init__(self, cfg, strategy_name: str) -> None:
        # 設定（製品コード/刻み/ガード閾値）
        self.cfg = cfg
        self.product = getattr(cfg, "product_code", "FX_BTC_JPY") or "FX_BTC_JPY"
        self.tick = float(getattr(cfg, "tick_size", 1.0))
        self.guard_bp = None
        if getattr(cfg, "guard", None) is not None:
            self.guard_bp = getattr(cfg.guard, "max_mid_move_bp_30s", None)

        # 戦略（#1/#2）を選択
        if strategy_name == "cancel_add_gate":
            self.strat = CancelAddGate()
        else:
            self.strat = StallThenStrike()

        # ローカル板・シミュ・ログ器
        self.ob = OrderBook(tick_size=self.tick)
        self.sim = MiniSimulator()
        self.order_log = OrderLog("logs/orders/order_log.parquet")
        self.trade_log = TradeLog("logs/trades/trade_log.parquet")
        self.decision_log = DecisionLog("logs/analytics/decision_log.parquet")

        # PnL最小モデルの内部状態（自炊Q/A/Rのミニ版）
        self.Q = 0.0  # 在庫（+ロング/−ショート）
        self.A = 0.0  # 平均建値
        self.R = 0.0  # 実現PnL累計

        # 30秒ミッド履歴（ガード用）：(epoch_sec, mid)
        self._midwin: Deque[Tuple[float, float]] = deque()

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

    # ─────────────────────────────────────────────────────────────
    def _record_decision(self, now: datetime, actions) -> None:
        """【関数】意思決定ログへ記録（featuresと結論の一行）"""
        # 特徴量を収集
        feats_win = getattr(getattr(self.cfg, "features", None), "ca_ratio_win_ms", 500)
        feats = {
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

    # ─────────────────────────────────────────────────────────────
    def _apply_fill_and_log(self, ts_iso: str, side: str, px: float, sz: float, tag: str) -> None:
        """【関数】Fillを在庫Q/A/Rに適用し、orders/tradesへ記録（最小PnL）"""
        # 1) orders：fill行
        self.order_log.add(ts=ts_iso, action="fill", tif="GTC", ttl_ms=None, px=px, sz=sz, reason=tag)
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
        # 3) trades：約定行
        self.trade_log.add(
            ts=ts_iso, side=side, px=px, sz=sz, pnl=realized,
            strategy=self.strat.name, tag=tag, inventory_after=self.Q
        )

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
                    self.ob.update_from_event(ev)

                    # TTL失効を処理（取消ログ）
                    for o in self.sim.on_time(now):
                        self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                           px=o.price, sz=o.remaining, reason="ttl")

                    # ガード（速すぎるときは新規停止＋全取消）
                    paused = self._guard_midmove_bp(now)
                    if paused:
                        for o in self.sim.cancel_by_tag("stall"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                               px=o.price, sz=o.remaining, reason="guard")
                        for o in self.sim.cancel_by_tag("ca_gate"):
                            self.order_log.add(ts=now.isoformat(), action="cancel", tif=o.tif, ttl_ms=o.ttl_ms,
                                               px=o.price, sz=o.remaining, reason="guard")
                        continue  # 新規は出さない

                    # 戦略評価→意思決定ログ→アクション適用
                    actions = self.strat.evaluate(self.ob, now, self.cfg)
                    self._record_decision(now, actions)
                    for act in actions:
                        if act.get("type") == "place":
                            # 同タグの重複を最小抑止
                            if self.sim.has_open_tag(act["order"].tag):
                                continue
                            self.sim.place(act["order"], now)
                            o = act["order"]
                            self.order_log.add(ts=now.isoformat(), action="place", tif=o.tif, ttl_ms=o.ttl_ms,
                                               px=o.price, sz=o.size, reason=self.strat.name)
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
