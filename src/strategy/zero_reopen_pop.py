# 何をするモジュールか：
#   「スプレッドが一瞬0になった直後、1tick以上に再拡大した“その一拍”だけ」片面で最小ロットを置き、
#   当たったら即IOCで+1tick利確して退出する“イベント駆動ワンショットMM”の本体実装。

from dataclasses import dataclass, asdict  # 何をするか：設定の正規化結果をログ出力するため asdict を使う
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional

# 何をするimportか：戦略の骨組み・板の読み取り・注文生成・時刻取得（すべて既存の共通層を利用）
from src.strategy.base import StrategyBase
from src.core.orderbook import OrderBook  # best/中値/tick/スプレッド/健全性 health_ok() を提供
from src.core.orders import Order        # Limit/IOC と TTL/タグを付けて発注する型
from src.core.utils import now_ms        # クールダウンや“直後”判定に使うミリ秒時刻

import logging  # 何をするか：この戦略の意思決定ログを出すために使う
import random  # 何をするか：TTLに±ゆらぎ（jitter）を与えるための乱数を使う
from collections import deque  # 何をするか：レート制限用に“時刻のキュー”を使う


logger = logging.getLogger(__name__)  # 何をするか：戦略専用のロガーを用意（情報/デバッグを出す）


@dataclass
class ZeroReopenConfig:
    """何をする設定か：最小限の外だしパラメータ（YAML上書き前提）"""

    min_spread_tick: int = 1       # 再拡大の下限（1tick以上に開いていること）
    max_spread_tick: int = 2       # 何をする設定か：再拡大が広すぎる（毒性高い）ときは出さない上限tick
    max_speed_ticks_per_s: float = 12.0  # 何をする設定か：midの速さ（tick/秒）がこの上限を超えたら発注しない
    fee_maker_bp: float = 0.0      # 何をする設定か：メーカー手数料（bp, 例 0.0〜-1.0）。1bp=0.01%
    fee_taker_bp: float = 0.0      # 何をする設定か：テイカー手数料（bp, 例 10.0 は0.10%）
    edge_bp_min: float = 0.0       # 何をする設定か：手数料控除後に最低これだけの余裕(bps)がなければ発注しない
    flat_timeout_ms: int = 600         # 何をする設定か：利確IOCが通らない時の“時間でフラット”の締切ms
    ttl_ms: int = 800              # 指値の寿命（置きっぱなし防止・秒速撤退のため短め）
    size_min: float = 0.001        # 最小ロット（取引所の最小単位に合わせる）
    min_take_qty: float = 0.0    # 何をする設定か：+1tick利確の相手側Bestに最低この数量が無いと発注しない（0で無効）
    max_join_qty: float = 0.0   # 何をする設定か：自分が並ぶ側のBest数量がこの値を超えていたら出さない（0で無効）
    cooloff_ms: int = 250          # 連打禁止と毒性回避のための“息継ぎ”
    seen_zero_window_ms: int = 1000  # どれだけ“ゼロ直後”を有効とみなすか
    loss_cooloff_ms: int = 1500   # 何をする設定か：非常口フラット後に“お休み”する時間ms（連打で再被弾を防ぐ）
    stop_adverse_ticks: int = 2    # 何をする設定か：エントリーVWAPから不利にこのtick以上動いたら即フラットIOCで逃げる
    exact_one_tick_only: bool = True  # 何をする設定か：スプレッドが“ちょうど1tick”のときだけ出す（+1tick利確がその場で当たる）

    entries_window_ms: int = 10000   # 何をする設定か：この時間窓（ms）内のエントリー回数を数える
    max_entries_in_window: int = 6   # 何をする設定か：時間窓内に許可する最大エントリー回数
    ttl_jitter_ms: int = 80      # 何をする設定か：TTLに与える±ゆらぎ幅（ms）。同時発注の衝突を避ける
    reopen_stable_ms: int = 50   # 何をする設定か：再拡大してから“この時間だけ継続”したら発注を許可（瞬間ノイズで出さない）
    min_best_age_ms: int = 200   # 何をする設定か：Bestがこの時間（ms）以上変わらず“落ち着いて”いたら発注を許

def zero_reopen_config_from(cfg: Any) -> ZeroReopenConfig | None:
    """【関数】設定オブジェクトから zero_reopen_pop セクションを取り出し ZeroReopenConfig に変換する"""

    if cfg is None:
        return None

    features = getattr(cfg, "features", None)
    section: Any = None

    if isinstance(features, Mapping):
        section = features.get("zero_reopen_pop")
    elif features is not None:
        section = getattr(features, "zero_reopen_pop", None)
        if section is None:
            extra = getattr(features, "model_extra", None)
            if isinstance(extra, Mapping):
                section = extra.get("zero_reopen_pop")
            elif hasattr(features, "__dict__") and isinstance(features.__dict__, dict):
                section = features.__dict__.get("zero_reopen_pop")

    if not section:
        return None

    if isinstance(section, ZeroReopenConfig):
        return section

    if isinstance(section, Mapping):
        return ZeroReopenConfig(**section)

    raise TypeError("zero_reopen_pop config must be a mapping or ZeroReopenConfig instance")


class ZeroReopenPop(StrategyBase):
    """
    何をする戦略か：
      - 直近に spread==0（タッチ/クロス）を観測 → 記録
      - その直後（既定1秒以内）に spread >= 1tick に再拡大した“一拍だけ”片面を最小ロットで提示（GTC+TTL）
      - 約定したら即IOCで+1tick利確して退出
      - ガード（health_ok 等）が悪いときは何もしない
    """

    name: str = "zero_reopen_pop"

    def __init__(self, *, cfg: Optional[ZeroReopenConfig] = None) -> None:
        # 何をする関数か：設定と内部状態（直近ゼロ時刻／直近アクション時刻）の初期化
        super().__init__()
        self.cfg = cfg or ZeroReopenConfig()
        self._validate_config()  # 何をするか：設定値を安全な範囲に正規化し、矛盾を解消する
        self._last_spread_zero_ms: int = -10**9
        self._last_action_ms: int = -10**9
        self._last_entry_ttl_ms: int = self.cfg.ttl_ms  # 何をするか：直近エントリーの実TTL（jitter反映）を記録し、ロック時間と合わせる
        self._entry_ts_ms: Deque[int] = deque()  # 何をするか：最近のエントリー時刻を入れておく（レート制限判定に使用）
        self._fired_on_this_zero: bool = False  # 何をするか：同一“ゼロ”イベントにつき発注は1回だけにするフラグ
        self._lock_until_ms: int = -10**9  # 何をするか：同時に複数枚を出さない“発注ロック”の期限ms（TTL中は新規禁止）
        self._penalty_until_ms: int = -10**9  # 何をするか：罰ゲーム中はここまで新規発注を禁止（ロス・クールオフの期限ms）
        self._reopen_since_ms: int = -10**9  # 何をするか：再拡大が始まった“時刻”を記録して安定時間を測る
        self._reopen_start_bid: Optional[float] = None  # 何をするか：再拡大が始まった時のbest bid（方向判定の基準）
        self._reopen_start_ask: Optional[float] = None  # 何をするか：再拡大が始まった時のbest ask（方向判定の基準）
        self._last_best_change_ms: int = -10**9  # 何をするか：Bestが最後に変わった時刻（ms）を記録
        self._last_best_bid: Optional[float] = None  # 何をするか：前回のbest bid価格
        self._last_best_ask: Optional[float] = None  # 何をするか：前回のbest ask価格
        self._tp_pending: bool = False        # 何をするか：利確IOC待ち（まだ手仕舞えていない）かどうか
        self._tp_deadline_ms: int = -10**9    # 何をするか：この時刻を過ぎたら“フラットIOC”を出す締切
        self._open_side: Optional[str] = None # 何をするか：保有している方向（BUY/SELL）
        self._open_vwap_px: float = 0.0  # 何をするか：保有ポジションの平均価格（VWAP）を記録（部分約定に対応）
        self._open_size: float = 0.0          # 何をするか：保有しているサイズ
        self._last_mid_px: float = 0.0       # 何をするか：前回のmid価格を記録して速さ（速度）を測る
        self._last_mid_ts_ms: int = -10**9   # 何をするか：前回midの記録時刻（ms）
        self._entry_active: bool = False   # 何をするか：現在エントリー指値が生きているかを覚えて cancel_tag を制御する
        self._entry_tag: str = "zero_reopen"
        self._take_tag: str = "zero_reopen_take"

    # -------------------------
    # 内部ヘルパ（責務を明記）
    # -------------------------

    def _validate_config(self) -> None:
        """【関数】設定の正規化：何をするか：危険/矛盾のある値を安全な範囲に丸め、運用で困らない形に整える"""
        c = self.cfg

        # 時間系の最小値を確保
        if c.ttl_ms < 1: c.ttl_ms = 1
        if c.ttl_jitter_ms < 0: c.ttl_jitter_ms = 0
        if c.ttl_jitter_ms > c.ttl_ms: c.ttl_jitter_ms = c.ttl_ms
        if c.seen_zero_window_ms < 1: c.seen_zero_window_ms = 1
        if c.reopen_stable_ms < 0: c.reopen_stable_ms = 0
        if c.min_best_age_ms < 0: c.min_best_age_ms = 0
        if c.cooloff_ms < 0: c.cooloff_ms = 0
        if c.entries_window_ms < 1: c.entries_window_ms = 1
        if c.flat_timeout_ms < 1: c.flat_timeout_ms = 1
        if c.loss_cooloff_ms < 0: c.loss_cooloff_ms = 0

        # 量/カウントの下限
        if c.size_min <= 0: c.size_min = 0.001
        if c.max_entries_in_window < 1: c.max_entries_in_window = 1

        # 幅（tick）の整合
        if c.min_spread_tick < 1: c.min_spread_tick = 1
        if c.max_spread_tick < c.min_spread_tick: c.max_spread_tick = c.min_spread_tick
        if c.exact_one_tick_only:
            c.min_spread_tick = 1
            c.max_spread_tick = 1  # 何をするか：1tick限定モードでは幅を自動固定

        # 速度・撤退の下限
        if c.max_speed_ticks_per_s < 0: c.max_speed_ticks_per_s = 0.0
        if c.stop_adverse_ticks < 0: c.stop_adverse_ticks = 0

        # 参考：手数料/期待エッジ（bp）は負値も運用上あり得るため丸めない

        # 正規化結果をログへ
        try:
            logger.info("zr_cfg_normalized %s", asdict(c))  # 何をするか：最終的に使う設定を1行で記録
        except Exception:
            logger.exception("zr_cfg_log_error")  # 何をするか：ログ化に失敗しても戦略は継続

    def _log_decision(self, reason: str, **fields) -> None:
        """【関数】意思決定ログ：何をするか：判断理由と主要パラメータを1行で記録する"""
        try:
            payload = " ".join(f"{k}={v}" for k, v in fields.items())
            logger.info("zr_decision reason=%s %s", reason, payload)
        except Exception:
            logger.exception("zr_decision_log_error")

    def _get_best_prices(self, ob: OrderBook) -> tuple[float | None, float | None]:
        """【関数】best bid/ask を callable/属性/価格オブジェクトから float に正規化して返す"""

        def _extract(value: Any) -> float | None:
            if callable(value):
                try:
                    value = value()
                except Exception:
                    return None
            if value is None:
                return None
            price = getattr(value, "price", value)
            if callable(price):
                try:
                    price = price()
                except Exception:
                    return None
            if price is None:
                return None
            try:
                return float(price)
            except (TypeError, ValueError):
                return None

        bid_px = _extract(getattr(ob, "best_bid", None))
        ask_px = _extract(getattr(ob, "best_ask", None))
        return bid_px, ask_px

    def _to_ms(self, now: datetime | int | float | None) -> int:
        """【関数】datetime/整数/None を ms(int) に正規化"""
        if isinstance(now, datetime):
            return int(now.timestamp() * 1000)
        if now is None:
            return now_ms()
        try:
            return int(now)
        except Exception:
            return now_ms()

    def _mark_zero(self, ob: OrderBook, now_ms: int) -> None:
        """【関数】ゼロ記録：spread==0 を見た“時刻”を記録して、のちほど“直後”判定に使う"""
        if ob.spread_ticks() <= 0:  # 何をするか：スプレッドが“0以下”（ロック/クロス）もゼロ扱いにして合図を取りこぼさない
            self._last_spread_zero_ms = now_ms
            self._fired_on_this_zero = False  # 何をするか：新しい“ゼロ”を見たので再発注可にリセット
            self._reopen_since_ms = -10**9  # 何をするか：新しい“ゼロ”を見たので再拡大の起点をリセット
            self._reopen_start_bid = None  # 何をするか：新しいゼロなので起点bestをクリア
            self._reopen_start_ask = None  # 何をするか：新しいゼロなので起点bestをクリア

    def _is_reopen(self, ob: OrderBook, now_ms: int) -> bool:
        """【関数】再拡大判定：“直近ゼロあり かつ 現在は≥min_spread_tick”かどうか"""
        seen_zero_recently = (now_ms - self._last_spread_zero_ms) <= self.cfg.seen_zero_window_ms
        spread = ob.spread_ticks()  # 何をするか：現在のスプレッドtickを1回だけ読み取る
        if self.cfg.exact_one_tick_only:
            spread_ok = (spread == 1)  # 何をするか：1tickちょうどの時だけOK（+1tick利確IOCが即マッチ）
        else:
            spread_ok = (self.cfg.min_spread_tick <= spread <= self.cfg.max_spread_tick)  # 何をするか：通常の幅チェック
        if spread_ok and self._reopen_since_ms < 0:
            self._reopen_since_ms = now_ms  # 何をするか：再拡大を初めて確認した時刻を刻む
            bid_px, ask_px = self._get_best_prices(ob)
            self._reopen_start_bid = bid_px  # 何をするか：再拡大の起点となるbest bidを保存
            self._reopen_start_ask = ask_px  # 何をするか：再拡大の起点となるbest askを保存
        if not spread_ok:
            self._reopen_since_ms = -10**9  # 何をするか：幅が外れたら起点を破棄（やり直し）
            self._reopen_start_bid = None  # 何をするか：幅条件が外れたので起点bestを破棄（やり直し）
            self._reopen_start_ask = None  # 何をするか：幅条件が外れたので起点bestを破棄（やり直し）
        stable_ok = spread_ok and (now_ms - self._reopen_since_ms) >= self.cfg.reopen_stable_ms  # 何をするか：十分“開いたまま”続いたか
        return seen_zero_recently and stable_ok and (not self._fired_on_this_zero)  # 何をするか：ゼロ直後×幅OK×安定×未発射のときだけ許可

    def _pass_gates(self, ob: OrderBook, now_ms: int) -> bool:
        """【関数】安全ゲート：標準ガード（health_ok）・クールダウン・best存在チェックをまとめて判定"""
        health_check = getattr(ob, "health_ok", None)
        if callable(health_check) and not health_check():
            return False
        if (now_ms - self._last_action_ms) < self.cfg.cooloff_ms:
            return False
        if self._tp_pending:
            return False  # 何をするか：まだ手仕舞い（利確/フラット）待ちの在庫があるので新規は出さない
        if now_ms < self._lock_until_ms:
            return False  # 何をするか：まだTTL中＝前の注文が生きているので、新しい発注をロックして1枚運用を守る
        if now_ms < self._penalty_until_ms:
            self._log_decision("skip_penalty", until=self._penalty_until_ms, now=now_ms)  # 何をするか：“罰ゲーム中なので見送り”を記録
            return False  # 何をするか：ロス・クールオフ中は新規発注しない
        bid_px, ask_px = self._get_best_prices(ob)
        if bid_px is None or ask_px is None:
            return False  # 何をするか：どちらか欠けていたら発注不可

        if (
            self._last_best_bid is None
            or self._last_best_ask is None
            or self._last_best_bid != bid_px
            or self._last_best_ask != ask_px
        ):
            self._last_best_change_ms = now_ms  # 何をするか：Bestが変わったら“変化時刻”を更新して年齢リセット
            self._last_best_bid = bid_px
            self._last_best_ask = ask_px

        best_age_ms = now_ms - self._last_best_change_ms  # 何をするか：Bestが同じ値で続いている時間を計算
        if best_age_ms < self.cfg.min_best_age_ms:
            self._log_decision("skip_best_age", age_ms=best_age_ms, min_ms=self.cfg.min_best_age_ms)  # 何をするか：若すぎて見送りを記録
            return False  # 何をするか：Bestがまだ落ち着いていないので発注しない

        # 何をするか：midの“速さ”（tick/秒）を計算し、上限を超えると危険なので発注を見送る
        mid = ob.mid_price()
        tick = ob.tick_size()
        if self._last_mid_ts_ms > 0:
            dt_ms = now_ms - self._last_mid_ts_ms
            if dt_ms > 0:
                speed_ticks_per_s = abs(mid - self._last_mid_px) / tick * (1000.0 / dt_ms)
                if speed_ticks_per_s > self.cfg.max_speed_ticks_per_s:
                    self._log_decision("skip_speed", speed=f"{speed_ticks_per_s:.2f}", limit=self.cfg.max_speed_ticks_per_s)  # 何をするか：速すぎて見送りの理由を記録
                    return False  # 速すぎる＝トレンド急進中と判断し、今回は出さない
        # 記録を更新（次回の速度計算のため）
        self._last_mid_px = mid
        self._last_mid_ts_ms = now_ms
        # 何をするか：1tick利確の“期待エッジ（bps）”を計算し、手数料合計＋余裕未満なら危険なので発注しない
        edge_est_bp = (tick / max(mid, 1e-9)) * 10000.0 - (self.cfg.fee_maker_bp + self.cfg.fee_taker_bp)
        if edge_est_bp < self.cfg.edge_bp_min:
            self._log_decision("skip_edge", edge_bp=f"{edge_est_bp:.2f}", min_bp=self.cfg.edge_bp_min)  # 何をするか：採算不足で見送りの理由を記録
            return False

        if self.cfg.entries_window_ms > 0 and self.cfg.max_entries_in_window > 0:
            while self._entry_ts_ms and (now_ms - self._entry_ts_ms[0]) > self.cfg.entries_window_ms:
                self._entry_ts_ms.popleft()  # 何をするか：窓からはみ出た古い記録を捨てる
            if len(self._entry_ts_ms) >= self.cfg.max_entries_in_window:
                self._log_decision(
                    "skip_rate",
                    n=len(self._entry_ts_ms),
                    max=self.cfg.max_entries_in_window,
                )  # 何をするか：レート制限により見送りを記録
                return False
        return True

    def _choose_side(self, ob: OrderBook) -> str:
        """【関数】サイド決定：再拡大の“起点best”からの開き量で方向を判定"""
        bid_px, ask_px = self._get_best_prices(ob)
        if bid_px is None or ask_px is None:
            return "buy"

        start_bid = self._reopen_start_bid
        start_ask = self._reopen_start_ask
        try:
            start_bid_f = float(start_bid) if start_bid is not None else None
        except (TypeError, ValueError):
            start_bid_f = None
        try:
            start_ask_f = float(start_ask) if start_ask is not None else None
        except (TypeError, ValueError):
            start_ask_f = None

        if start_bid_f is not None and start_ask_f is not None:
            delta_ask = max(0.0, float(ask_px) - start_ask_f)  # 何をするか：上方向に開いた量
            delta_bid = max(0.0, start_bid_f - float(bid_px))  # 何をするか：下方向に開いた量
            side = "buy" if delta_ask >= delta_bid else "sell"  # 何をするか：上に開いた方が大きければBUY/下ならSELL
            self._log_decision(
                "choose_side",
                delta_ask=f"{delta_ask:.1f}",
                delta_bid=f"{delta_bid:.1f}",
                side=side.upper(),
            )
            return side

        mid = None
        microprice = getattr(ob, "microprice", None)
        if callable(microprice):
            try:
                mid = microprice()
            except Exception:
                mid = None
        if mid is None:
            mid = (bid_px + ask_px) / 2.0

        bid_offset = abs(float(mid) - float(bid_px))
        ask_offset = abs(float(ask_px) - float(mid))

        if ask_offset > bid_offset:
            self._log_decision("choose_side_fallback", rule="mid_distance", side="SELL")
            return "sell"
        self._log_decision("choose_side_fallback", rule="mid_distance", side="BUY")
        return "buy"

    def _build_entry(self, ob: OrderBook, side: str) -> Dict[str, Any]:
        """【関数】エントリー生成：片面1発の指値（GTC+TTL・最小ロット・戦略タグ付）を作る"""
        bid_px, ask_px = self._get_best_prices(ob)
        if bid_px is None or ask_px is None:
            raise ValueError("best bid/ask required for entry order")
        best_bid = bid_px  # 何をするか：BUY時のメイク価格（tick整合済みのbest）
        best_ask = ask_px  # 何をするか：SELL時のメイク価格（tick整合済みのbest）
        side_str = str(side).upper()
        px = best_bid if side_str == "BUY" else best_ask  # 何をするか：BUY→best_bid / SELL→best_ask に統一（ズレ防止）
        ttl = max(0, int(self.cfg.ttl_ms + random.randint(-self.cfg.ttl_jitter_ms, self.cfg.ttl_jitter_ms)))  # 何をするか：TTLに±ゆらぎを与える
        self._last_entry_ttl_ms = ttl  # 何をするか：この発注に使う実TTLを記録（のちのロック解除に使う）
        order = Order(
            side=side,
            price=px,
            size=self.cfg.size_min,
            tif="GTC",
            ttl_ms=self._last_entry_ttl_ms,
            tag=self._entry_tag,
        )
        return {"type": "place", "order": order}

    def _build_take_profit(self, ob: OrderBook, fill: Any) -> Dict[str, Any]:
        """【関数】利確生成：fillを受けたら反対側に+1tickのIOCを即返す（秒速撤退）"""
        tick = float(getattr(ob, "tick", 1.0))
        fill_side = getattr(fill, "side", None)
        fill_price = getattr(fill, "price", None)
        fill_size = getattr(fill, "size", None)
        if isinstance(fill, Mapping):
            fill_side = fill_side or str(fill.get("side", "")).lower()
            if fill_price is None:
                fill_price = fill.get("price")
            if fill_size is None:
                fill_size = fill.get("size")
            order_info = fill.get("order")
            if fill_price is None and order_info is not None:
                fill_price = getattr(order_info, "price", None)
            if fill_size is None and order_info is not None:
                fill_size = getattr(order_info, "size", None)
        else:
            if fill_side is not None:
                fill_side = str(fill_side).lower()

        if fill_price is None or fill_size is None:
            raise ValueError("fill price/size required for take profit")

        exit_side = "sell" if str(fill_side).lower() == "buy" else "buy"
        exit_price = (float(fill_price) + tick) if exit_side == "sell" else (float(fill_price) - tick)
        order = Order(
            side=exit_side,
            price=exit_price,
            size=float(fill_size),
            tif="IOC",
            ttl_ms=200,
            tag=self._take_tag,
        )
        return {"type": "place", "order": order}

    def _build_cancel(self) -> Dict[str, Any]:
        """【関数】エントリーの戦略タグを一括取消するアクション"""
        self._entry_active = False
        self._lock_until_ms = -10**9
        return {"type": "cancel_tag", "tag": self._entry_tag}

    # -------------------------
    # ランタイム・フック
    # -------------------------

    def evaluate(self, ob: OrderBook, now: datetime, cfg) -> List[Dict[str, Any]]:
        """【関数】板イベント：ゼロを記録 → “直後の再拡大 & ゲート合格”ならアクションを返す"""
        now_ms = self._to_ms(now)
        actions: List[Dict[str, Any]] = []

        self._mark_zero(ob, now_ms)

        if self._tp_pending:
            if now_ms >= self._tp_deadline_ms:
                bid_px, ask_px = self._get_best_prices(ob)
                if (
                    bid_px is not None
                    and ask_px is not None
                    and self._open_side in {"BUY", "SELL"}
                    and self._open_size > 0.0
                ):
                    self._log_decision("flat_timeout", side=self._open_side, size=self._open_size)  # 何をするか：締切超過で非常口フラットを記録
                    order_side = "sell" if self._open_side == "BUY" else "buy"
                    order_price = float(bid_px) if order_side == "sell" else float(ask_px)
                    order = Order(
                        side=order_side,
                        price=order_price,
                        size=self._open_size,
                        tif="IOC",
                        ttl_ms=200,
                        tag="zero_reopen_flat",
                    )
                    actions.append({"type": "place", "order": order})
                    self._tp_deadline_ms = now_ms + self.cfg.flat_timeout_ms
                    return actions
            return actions

        # 何をするか：逆行が規定tickを超えたら、時間を待たずに即フラットIOCで逃げる
        if self._tp_pending and self._open_size > 0.0 and self.cfg.stop_adverse_ticks > 0:
            best_bid, best_ask = self._get_best_prices(ob)
            if best_bid is not None and best_ask is not None and self._open_vwap_px > 0.0:
                try:
                    tick = float(ob.tick_size())
                except Exception:
                    tick = 0.0
                tick = max(tick, 1e-12)
                adverse_ticks = 0.0
                if self._open_side == "BUY":
                    adverse_ticks = max(0.0, (self._open_vwap_px - float(best_bid)) / tick)
                elif self._open_side == "SELL":
                    adverse_ticks = max(0.0, (float(best_ask) - self._open_vwap_px) / tick)
                if adverse_ticks >= self.cfg.stop_adverse_ticks:
                    self._log_decision(
                        "flat_adverse",
                        side=self._open_side,
                        adv_ticks=f"{adverse_ticks:.1f}",
                        stop=self.cfg.stop_adverse_ticks,
                    )
                    if self._open_side == "BUY":
                        order = Order(
                            side="sell",
                            price=float(best_bid),
                            size=self._open_size,
                            tif="IOC",
                            ttl_ms=200,
                            tag="zero_reopen_flat",
                        )
                    else:
                        order = Order(
                            side="buy",
                            price=float(best_ask),
                            size=self._open_size,
                            tif="IOC",
                            ttl_ms=200,
                            tag="zero_reopen_flat",
                        )
                    return [{"type": "place", "order": order}]

        if self._entry_active and (now_ms >= self._lock_until_ms or not self._is_reopen(ob, now_ms)):
            actions.append(self._build_cancel())

        if self._is_reopen(ob, now_ms) and self._pass_gates(ob, now_ms):
            try:
                side = self._choose_side(ob)
                # 何をするか：自分が並ぶ側のBestの“行列（数量）”が多すぎるなら、順番が回りにくいので今回は出さない
                if self.cfg.max_join_qty > 0:
                    maker_size_reader = (
                        getattr(ob, "best_bid_size", None) if side == "BUY" else getattr(ob, "best_ask_size", None)
                    )
                    maker_sz = maker_size_reader() if callable(maker_size_reader) else None  # 何をするか：板ビューが数量APIを持っていれば読む（無ければスキップ）
                    if (maker_sz is not None) and (maker_sz > self.cfg.max_join_qty):
                        self._log_decision("skip_maker_queue", have=maker_sz, max=self.cfg.max_join_qty, side=side)  # 何をするか：見送り理由を記録
                        return []
                # 何をするか：+1tick利確が通りやすいよう、相手側Bestの板厚が足りなければ今回は出さない
                if self.cfg.min_take_qty > 0:
                    size_reader = getattr(ob, "best_ask_size", None) if side == "BUY" else getattr(ob, "best_bid_size", None)
                    take_sz = size_reader() if callable(size_reader) else None  # 何をするか：OrderbookViewにsize参照APIがあれば使う（無ければNone）
                    if (take_sz is not None) and (take_sz < self.cfg.min_take_qty):
                        self._log_decision("skip_take_liq", need=self.cfg.min_take_qty, have=take_sz, side=side)  # 何をするか：見送り理由を記録
                        return []
                action = self._build_entry(ob, side)
            except ValueError:
                return actions
            order = action.get("order")
            order_px = getattr(order, "price", None) if order is not None else None
            self._log_decision("entry", spread=ob.spread_ticks(), side=side, px=order_px, ttl=self.cfg.ttl_ms)  # 何をするか：エントリー実行を記録
            actions.append(action)
            self._last_action_ms = now_ms
            self._lock_until_ms = now_ms + self._last_entry_ttl_ms  # 何をするか：ロックは“実際に使ったTTL分だけ”かける
            self._entry_ts_ms.append(now_ms)  # 何をするか：このエントリーの時刻を記録（レート制限判定で使用）
            self._entry_active = True
            self._fired_on_this_zero = True  # 何をするか：この“ゼロ”での発注を消費（次は新しいゼロが来るまで出さない）

        return actions

    def on_board(self, ob: OrderBook) -> List[Order]:
        """【関数】後方互換：旧IF（List[Order]）向けに place アクションだけを返す"""
        actions = self.evaluate(ob, datetime.now(timezone.utc), None)
        return [a["order"] for a in actions if a.get("type") == "place" and "order" in a]

    def on_fill(self, ob: OrderBook, my_fill: Any) -> List[Dict[str, Any]]:
        """【関数】約定イベント：+1tickのIOC利確を即返して“秒速で退出”する"""
        now = now_ms()  # 何をするか：フラット締切の計算に使う
        tag = getattr(my_fill, "tag", "")
        if tag is None:
            tag = ""
        tag_str = str(tag)

        if tag_str in ("zero_reopen_take", "zero_reopen_flat"):
            # 何をするか：決済で埋まった分だけ在庫を“差し引き”、ゼロになったら完了扱い
            fill_size = getattr(my_fill, "size", None)
            if isinstance(my_fill, Mapping) and fill_size is None:
                fill_size = my_fill.get("size")
            if fill_size is None:
                order_info = getattr(my_fill, "order", None)
                if order_info is not None:
                    fill_size = getattr(order_info, "size", None)
            size_val = float(fill_size) if fill_size is not None else 0.0
            self._entry_active = False
            self._open_size = max(0.0, self._open_size - size_val)
            if self._open_size <= 1e-12:
                self._open_size = 0.0
                self._tp_pending = False
                self._open_side = None
                self._open_vwap_px = 0.0  # 何をするか：在庫がゼロになったのでVWAPをリセット
                self._lock_until_ms = now - 1
            else:
                self._tp_pending = True

            if tag_str == "zero_reopen_flat":
                self._penalty_until_ms = now + self.cfg.loss_cooloff_ms  # 何をするか：非常口で逃げたら“お休み時間”をセット
            logger.info(
                "zero_reopen closed_by=%s side=%s px=%s remain=%s",
                tag_str,
                getattr(my_fill, "side", None),
                getattr(my_fill, "price", None),
                self._open_size,
            )  # 何をするか：残り在庫も記録
            return []  # 何をするか：決済側の約定時は新規注文を出さない（自己再送防止）

        self._entry_active = False
        self._lock_until_ms = -10**9

        # 何をするか：エントリー約定を受けたので、利確IOCの“締切”をセットして待機フラグON
        fill_side = getattr(my_fill, "side", None)
        fill_size = getattr(my_fill, "size", None)
        fill_price = getattr(my_fill, "price", None)
        if isinstance(my_fill, Mapping):
            if fill_side is None:
                fill_side = my_fill.get("side")
            if fill_size is None:
                fill_size = my_fill.get("size")
            if fill_price is None:
                fill_price = my_fill.get("price")
            order_info = my_fill.get("order")
            if order_info is not None:
                if fill_side is None:
                    fill_side = getattr(order_info, "side", None)
                if fill_size is None:
                    fill_size = getattr(order_info, "size", None)
                if fill_price is None:
                    fill_price = getattr(order_info, "price", None)
        side_str = str(fill_side).upper() if fill_side is not None else None
        size_val = float(fill_size) if fill_size is not None else 0.0
        if size_val <= 0.0:
            size_val = 0.0
        price_val = float(fill_price) if fill_price is not None else None
        self._tp_pending = True
        self._tp_deadline_ms = now + self.cfg.flat_timeout_ms
        if self._open_side is None:
            if side_str:
                self._open_side = side_str
            self._open_size = size_val
            self._open_vwap_px = price_val if price_val is not None else 0.0
        else:
            if size_val > 0.0:
                new_size = self._open_size + size_val
                if price_val is not None:
                    self._open_vwap_px = (
                        (self._open_vwap_px * self._open_size + price_val * size_val)
                        / max(new_size, 1e-12)
                    )
                self._open_size = new_size
        if self._open_size <= 1e-12:
            self._tp_pending = False
            self._open_side = None
            self._open_size = 0.0
            self._open_vwap_px = 0.0

        try:
            action = self._build_take_profit(ob, my_fill)
        except ValueError:
            if self._tp_pending:
                self._tp_pending = False
                self._open_side = None
                self._open_size = 0.0
            return []
        order = action.get("order") if isinstance(action, Mapping) else None
        if order is not None:
            logger.info(
                "zero_reopen take_send side=%s px=%s", str(getattr(order, "side", "")).upper(), getattr(order, "price", None)
            )  # 何をするか：利確IOCの送信を記録
        return [action]
