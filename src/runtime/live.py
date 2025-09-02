# src/runtime/live.py
# これは live（本番）起動の最小導線です。exchange adapter との疎通だけ行い、危険がないように dry-run（発注なし）にします。

from __future__ import annotations

import os  # 何をするか：APIキー/シークレットを環境変数から読む
from typing import Any  # 何をするか：cfg の型ヒント用
from loguru import logger  # 何をするか：進行ログを出す
import signal  # 何をするか：Ctrl+C/SIGTERM を捕まえて安全停止する
from threading import Event  # 何をするか：停止フラグを扱う

from pathlib import Path  # 何をするか：ハートビートの出力ファイルを扱う
import orjson  # 何をするか：1行JSON(NDJSON)を書き出す
from datetime import datetime, timezone, timedelta  # 何をするか：UTC現在時刻とTTL計算
from collections import deque  # 何をするか：ミッド変化ガード用の履歴
from src.core.orderbook import OrderBook  # 何をするか：ローカル板（戦略の入力）
from src.core.orders import Order  # 何をするか：戦略が返す注文モデル（tif/ttl_ms/price/size/tag）
from src.core.realtime import stream_events  # 何をするか：WSのboard/executionsストリーム
from src.strategy.stall_then_strike import StallThenStrike  # 何をするか：#1 戦略
from src.strategy.cancel_add_gate import CancelAddGate  # 何をするか：#2 戦略
from src.strategy.age_microprice import AgeMicroprice  # 何をするか：#3 戦略
from src.core.logs import OrderLog, TradeLog  # 何をするか：orders/trades を Parquet＋NDJSON に記録する
from src.core.analytics import DecisionLog  # 何をするか：戦略の意思決定ログ（Parquet＋NDJSONミラー）を扱う

# 何をするか：前ステップで作った exchange adapter を使う
from src.core.exchange import (
    BitflyerExchange,
    ExchangeError,
    AuthError,
    RateLimitError,
    ServerError,
    NetworkError,
)

def _select_strategy(name: str, cfg):
    """何をするか：戦略名から実体を生成（#1/#2/#3のいずれか）"""
    if name == "stall_then_strike":
        try: return StallThenStrike(cfg)
        except TypeError: return StallThenStrike()
    if name == "cancel_add_gate":
        try: return CancelAddGate(cfg)
        except TypeError: return CancelAddGate()
    if name == "age_microprice":
        try: return AgeMicroprice(cfg)
        except TypeError: return AgeMicroprice()
    raise ValueError(f"unknown strategy: {name}")

def _now_utc() -> datetime:
    """何をするか：UTC現在時刻を返す（ログ/TTL計算の基準）"""
    return datetime.now(timezone.utc)

def _ttl_deadline(now: datetime, ttl_ms: int | None) -> datetime | None:
    """何をするか：TTLミリ秒から締切（UTC）を作る（Noneは無期限）"""
    return None if ttl_ms is None else now + timedelta(milliseconds=ttl_ms)

def _in_maintenance(now: datetime, cfg) -> bool:
    """何をするか：JSTのメンテナンス窓に入っていれば True"""
    ms = getattr(cfg, "mode_switch", None)
    maint = getattr(ms, "maintenance", None)
    if not maint:
        return False
    start_s, end_s = maint.get("start"), maint.get("end")
    if not (start_s and end_s):
        return False
    jst = now.astimezone(timezone(timedelta(hours=9)))
    sh, sm, ss = map(int, start_s.split(":"))
    eh, em, es = map(int, end_s.split(":"))
    start = jst.replace(hour=sh, minute=sm, second=ss, microsecond=0)
    end = jst.replace(hour=eh, minute=em, second=es, microsecond=0)
    return start <= jst <= end  # 何をするか：窓の中なら停止

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

def _hb_write(path: Path, *, event: str, **fields) -> None:
    """何をするか：ハートビート(NDJSON)に1行追記して、運転状況をリアルタイム可視化する"""
    rec = {"event": event}
    rec.update(fields)
    path.parent.mkdir(parents=True, exist_ok=True)  # 何をするか：ディレクトリを事前作成
    with path.open("a", encoding="utf-8") as f:
        f.write(orjson.dumps(rec).decode("utf-8") + "\n")  # 何をするか：1行JSONを追記

def _log_window_event(events_dir: Path, kind: str, action: str, ts: datetime) -> None:
    """何をするか：窓（maintenance / funding）の入退をCSVに1行追記して記録する"""
    events_dir.mkdir(parents=True, exist_ok=True)  # 何をするか：フォルダを事前作成
    fname = "maintenance.csv" if kind == "maintenance" else "funding_schedule.csv"
    line = f"{ts.isoformat()},{action}\n"  # 何をするか：列は ts,action の2列（シンプルに固定）
    (events_dir / fname).open("a", encoding="utf-8").write(line)

def _pull_fill_deltas(ex: BitflyerExchange, live_orders: dict[str, dict]) -> list[tuple[str, float, float, str, bool]]:  # 何をするか：“増分約定”に finalかどうかの旗(done)を追加で返す
    """何をするか：受理IDごとに“今回ぶんの増分約定”だけを取り出して (side, price, size, tag) のリストで返す"""
    fills: list[tuple[str, float, float, str]] = []
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
            fills.append((meta["order"].side, float(px), float(delta), getattr(meta["order"], "tag", ""), state == "COMPLETED" or (outstanding <= 1e-12 and executed > 0.0)))  # 何をするか：(side, px, sz, tag, done) を積む

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

def run_live(cfg: Any, strategy_name: str, dry_run: bool = True) -> None:
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
    with BitflyerExchange(api_key, api_secret, product_code=product_code) as ex:
        try:
            _ = ex.list_active_child_orders(count=1)
            if dry_run:
                logger.info(f"live(dry-run): exchange OK product={product_code} strategy={strategy_name}")
                logger.info("live(dry-run): ここでは発注しません（導線の疎通確認だけ）")
            else:
                # 次ステップで：ここにイベントループ＋戦略呼び出し＋TTL取消などを実装
            # 何をするか：ここから live のイベントループ（WS→板→戦略→発注／TTL取消／ガード）を回す
                ob = OrderBook()  # 何をするか：ローカル板（戦略の入力）を用意
            strat = _select_strategy(strategy_name, cfg)  # 何をするか：戦略に設定（cfg）を渡す
            live_orders: dict[str, dict] = {}  # 何をするか：受理ID→TTLなどのメタ情報を保持
            mid_hist = deque(maxlen=2048)  # 何をするか：ミッド価格の履歴（30秒変化ガード用）
            max_bp = getattr(getattr(cfg, "guard", None), "max_mid_move_bp_30s", None)  # 何をするか：ミッド変化ガードの閾値
            inv_limit = getattr(getattr(cfg, "risk", None), "max_inventory", None)  # 何をするか：在庫上限
            hb_path = Path("logs/runtime/heartbeat.ndjson")  # 何をするか：ハートビートの出力先
            hb_interval_s = int(getattr(getattr(cfg, "logging", None), "heartbeat_status_sec", 5))  # 何をするか：ステータス心拍の間隔（秒）
            hb_next = _now_utc() + timedelta(seconds=hb_interval_s)  # 何をするか：次に出す時刻

            order_log = OrderLog("logs/orders/order_log.parquet", mirror_ndjson="logs/orders/order_log.ndjson")  # 何をするか：発注/取消イベントを記録
            trade_log = TradeLog("logs/trades/trade_log.parquet", mirror_ndjson="logs/trades/trade_log.ndjson")  # 何をするか：約定明細とPnLを記録
            decision_log = DecisionLog("logs/analytics/decision_log.parquet", mirror_ndjson="logs/analytics/decision_log.ndjson")  # 何をするか：意思決定（features/decision）を記録する
            events_dir = Path("logs/events")  # 何をするか：窓イベントCSVの保存先
            events_dir.mkdir(parents=True, exist_ok=True)  # 何をするか：フォルダを作成
            (events_dir / "maintenance.csv").touch(exist_ok=True)  # 何をするか：ファイルを事前作成
            (events_dir / "funding_schedule.csv").touch(exist_ok=True)  # 何をするか：ファイルを事前作成
            maint_prev, fund_prev = False, False  # 何をするか：直前の窓状態（入っていたか）を保持

            hb_path.parent.mkdir(parents=True, exist_ok=True)  # 何をするか：保存先フォルダを作る

            halted = False  # 何をするか：Kill 到達後は新規を出さない
            pnl_state = {"pos": 0.0, "avg_px": None}  # 何をするか：建玉と平均コスト（PnL計算用）
            daily_R, R_HWM = 0.0, 0.0  # 何をするか：日次実現PnLとその高値（HWM）
            _jst = timezone(timedelta(hours=9))  # 何をするか：JST（Killの日次境界に使用）
            jst_day = _now_utc().astimezone(_jst).date()  # 何をするか：当日のJST日付
            kill_cfg = getattr(getattr(cfg, "risk", None), "kill", None)  # 何をするか：Killしきい値
            min_tx_ms = int(getattr(getattr(cfg, "tx", None), "min_interval_ms", 100))  # 何をするか：新規注文を連続で送らない最小間隔（ms）
            _last_tx_at = _now_utc() - timedelta(milliseconds=min_tx_ms)  # 何をするか：直近の送信時刻（初期は「今−間隔」で即送れる状態）
            fee_bps = float(getattr(getattr(cfg, "fees", None), "bps", 0.0))  # 何をするか：手数料のbps設定（未指定は0.0）
            canary_min = int(getattr(cfg, "canary_minutes", 60))  # 何をするか：最長運転時間（分）。未指定は60分

            logger.info(f"live: starting loop product={product_code} strategy={strategy_name}")  # 何をするか：起動ログ
            
            if bool(getattr(cfg, "cancel_all_on_start", True)):  # 何をするか：起動時に既存注文を掃除（既定ON）
                try: ex.cancel_all_child_orders(); logger.info("live: startup safety — cancel_all issued")
                except ExchangeError as e: logger.warning(f"live: startup cancel_all failed: {e}")

            started_at = _now_utc()  # 何をするか：Canary の開始時刻

            stop_event = Event()  # 何をするか：停止フラグ（signal 受信で立てる）

            def _on_signal(signum, frame) -> None:
                logger.warning(f"signal received: {signum} → cancel all & halt")  # 何をするか：受信をログ
                stop_event.set()  # 何をするか：イベントループに停止を伝える

            signal.signal(signal.SIGINT, _on_signal)   # 何をするか：Ctrl+C（SIGINT）で停止
            signal.signal(signal.SIGTERM, _on_signal)  # 何をするか：SIGTERM（停止要求）で停止

            _hb_write(hb_path, event="start", ts=_now_utc().isoformat(), product=product_code, strategy=strategy_name)  # 何をするか：起動を記録

            for ev in stream_events(product_code):  # 何をするか：WSのboard/executionsイベントを受け取る
                now = _now_utc()  # 何をするか：UTCの現在時刻
                if stop_event.is_set():  # 何をするか：停止フラグが立っていたら安全停止
                    if live_orders:
                        ex.cancel_all_child_orders()  # 何をするか：生きている注文をすべて取消
                        live_orders.clear()
                    _hb_write(hb_path, event="kill", ts=now.isoformat(), daily_pnl_jpy=daily_R, dd_jpy=R_HWM - daily_R, reason="signal")  # 何をするか：停止を1行JSONで記録
                    break  # 何をするか：イベントループを終了
                
                if (now - started_at).total_seconds() >= canary_min * 60:  # 何をするか：Canaryの経過時間をチェック
                    if live_orders:
                        ex.cancel_all_child_orders()  # 何をするか：安全のため全て取消
                        live_orders.clear()
                    _hb_write(hb_path, event="kill", ts=now.isoformat(), daily_pnl_jpy=daily_R, dd_jpy=R_HWM - daily_R, reason="canary")  # 何をするか：停止理由を心拍に記録
                    break  # 何をするか：live を終了

                ob.update_from_event(ev)  # 何をするか：ローカル板にイベントを反映
                if now >= hb_next:  # 何をするか：定期ステータスの時刻になったら
                    _hb_write(hb_path, event="status", ts=now.isoformat(),
                            Q=float(pnl_state.get("pos", 0.0)),  # 何をするか：建玉（BTC）
                            A=len(live_orders),                  # 何をするか：生きている注文の数
                            R=daily_R,                           # 何をするか：当日実現PnL(JPY)
                            maint=_in_maintenance(now, cfg),      # 何をするか：メンテ窓フラグ
                            funding=(_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)))  # 何をするか：Funding窓フラグ
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
                if max_bp and len(mid_hist) >= 2:
                    oldest_mid = None
                    for ts, m in mid_hist:
                        if (now - ts).total_seconds() >= 30:
                            oldest_mid = m
                            break
                    if oldest_mid:
                        move_bp = abs((mid - oldest_mid) / oldest_mid) * 10000.0
                        paused_mid = move_bp >= float(max_bp)
                        if paused_mid:
                            logger.debug(f"pause midmove_guard: {move_bp:.1f}bp ≥ {max_bp}")

                # 何をするか：メンテ/ファンディングの“窓”やガード中は新規を出さず整理だけ
                if paused_mid or _in_maintenance(now, cfg) or _in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg):
                    reason = "midmove_guard" if paused_mid else ("maintenance" if _in_maintenance(now, cfg) else ("funding" if (_in_funding_calc(now, cfg) or _in_funding_transfer(now, cfg)) else "pause"))  # 何をするか：停止理由を決める
                    _hb_write(hb_path, event="pause", ts=now.isoformat(), reason=reason)  # 何をするか：停止を記録

                    if live_orders:
                        ex.cancel_all_child_orders()
                        live_orders.clear()
                    continue  # 何をするか：次のイベントまで待つ

                # 何をするか：在庫上限ガード（建玉 |Q| が上限以上なら新規を止める）
                if inv_limit is not None:
                    try:
                        Q = _net_inventory_btc(ex)  # 何をするか：現在の建玉（BTC）を取得して合算
                    except NameError:
                        Q = 0.0  # 何をするか：ヘルパ未追加でも落ちないように0扱い
                    if abs(Q) >= float(inv_limit):
                        if live_orders:
                            ex.cancel_all_child_orders()
                            live_orders.clear()
                        logger.debug(f"pause inventory_guard: |Q|={abs(Q)} ≥ {inv_limit}")
                        continue

                # 何をするか：TTL 超過の注文を自動キャンセル
                for acc_id, meta in list(live_orders.items()):
                    if now >= meta["deadline"]:
                        o = meta["order"]  # 何をするか：元注文情報（tif/ttl/px/sz）を取り出す
                        ex.cancel_child_order(child_order_acceptance_id=acc_id)  # 何をするか：TTL超過分を取消
                        del live_orders[acc_id]  # 何をするか：監視対象から外す
                        order_log.add(ts=now.isoformat(), action="cancel", tif=getattr(o, "tif", "GTC"), ttl_ms=getattr(o, "ttl_ms", None), px=getattr(o, "price", None), sz=getattr(o, "size", None), reason="ttl")  # 何をするか：TTL取消を記録

                fills = _pull_fill_deltas(ex, live_orders)  # 何をするか：今回ぶんの増分約定を取り出す
                for side, px, sz, tag, done in fills:  # 何をするか：done=True なら完了（fill）、False なら部分約定（partial）
                    realized = _apply_fill_and_pnl(pnl_state, side, px, sz)  # 何をするか：建玉を更新し実現PnLを積算
                    fee = px * sz * (fee_bps / 10000.0)  # 何をするか：約定金額×bpsで手数料（正=コスト/負=リベート）
                    realized -= fee  # 何をするか：PnLは手数料込み（ネット）で積算する

                    trade_log.add(ts=now.isoformat(), side=side, px=px, sz=sz, fee=fee, pnl=realized, strategy=strategy_name, tag=tag, inventory_after=pnl_state["pos"], window_funding=fund_now, window_maint=maint_now)  # 何をするか：手数料込みで trades を記録
                    order_log.add(ts=now.isoformat(), action=("fill" if done else "partial"), tif=None, ttl_ms=None, px=px, sz=sz, reason=tag)  # 何をするか：ordersログにも fill/partial を記録する
                    _hb_write(hb_path, event="fill", ts=now.isoformat(), side=side, px=px, sz=sz, pnl=realized, tag=tag)  # 何をするか：約定とPnLを記録

                    if realized != 0.0:
                        daily_R += realized
                        R_HWM = max(R_HWM, daily_R)
                if _check_kill(daily_R, R_HWM, kill_cfg):  # 何をするか：Kill到達なら停止
                    logger.warning(f"kill-switch: daily_pnl={daily_R:.0f} JPY, dd={R_HWM - daily_R:.0f} JPY → halt")
                    _hb_write(hb_path, event="kill", ts=now.isoformat(), daily_pnl_jpy=daily_R, dd_jpy=R_HWM - daily_R)  # 何をするか：Kill発火を記録

                    if live_orders:
                        ex.cancel_all_child_orders()
                        live_orders.clear()
                    break  # 何をするか：run_live を終了



                # 何をするか：戦略を評価して、必要なら注文（Order）を発行
                try:
                    actions = strat.evaluate(ob)
                    decision_log.add(ts=now.isoformat(), strategy=strategy_name, features_json={}, decision=("place" if actions else "hold"), expected_edge_bp=None, eta_ms=None, ca_ratio=None, best_age_ms=None, spread_state=None)  # 何をするか：この時点の判断を固定スキーマで記録（不足の特徴量は None で埋める）
                except Exception as e:
                    logger.error(f"strategy evaluate failed: {e}")
                    continue

                for o in actions or []:
                    elapsed_ms = (now - _last_tx_at).total_seconds() * 1000.0  # 何をするか：前回送信からの経過ms
                    if elapsed_ms < min_tx_ms:  # 何をするか：まだ最小間隔に達していなければ送らない
                        logger.debug(f"throttle tx: {elapsed_ms:.0f}ms < {min_tx_ms}ms")  # 何をするか：スロットリングしたことを記録
                        continue

                    sz = getattr(o, "size", None) or float(getattr(getattr(cfg, "size", None), "default", 0.01))  # 何をするか：サイズ未指定なら config の size.default を使う
                    min_sz = float(getattr(getattr(cfg, "size", None), "min", 0.0))  # 何をするか：設定の最小ロットを読む
                    size_step = float(getattr(getattr(cfg, "size", None), "step", min_sz))  # 何をするか：サイズ刻み（未指定は min を刻みとして使う）
                    sz = _round_size(sz, size_step)  # 何をするか：サイズを刻みに丸める（約定拒否を防ぐ）
                    if sz < min_sz: sz = min_sz  # 何をするか：丸めた結果が下限未満なら下限に引き上げる

                    px = _round_to_tick(o.price, tick)  # 何をするか：戦略の価格をtickに丸めてから使う

                    try:
                        acc = ex.send_child_order(
                            side=o.side, size=sz, price=px, time_in_force=getattr(o, "tif", "GTC")
                        )  # 何をするか：RESTで新規注文を送る
                        _last_tx_at = now  # 何をするか：送信できたので直近送信時刻を更新

                        deadline = _ttl_deadline(now, getattr(o, "ttl_ms", getattr(getattr(cfg, "features", None), "ttl_ms", None)))  # 何をするか：TTL未指定なら config の features.ttl_ms を使う
                        if deadline:
                            live_orders[acc] = {"deadline": deadline, "order": o, "executed": 0.0, "avg_price": 0.0}  # 何をするか：増分約定の単価推定に使う平均価格も保持
                            order_log.add(ts=now.isoformat(), action="place", tif=getattr(o, "tif", "GTC"), ttl_ms=getattr(o, "ttl_ms", None), px=px, sz=sz, reason=getattr(o, "tag", ""))  # 何をするか：記録も丸め後の価格で統一
                            _hb_write(hb_path, event="place", ts=now.isoformat(), reason=getattr(o, "tag", ""), tif=getattr(o, "tif", "GTC"), ttl_ms=getattr(o, "ttl_ms", None), px=px, sz=sz)  # 何をするか：心拍にも丸め後の価格を出す

                    except (RateLimitError, ServerError, ExchangeError) as e:
                        logger.warning(f"send order rejected: {e}")
                        continue

        except (AuthError, RateLimitError, ServerError, NetworkError, ExchangeError) as e:
            logger.error(f"live: exchange 疎通に失敗しました: {e}")
            raise
