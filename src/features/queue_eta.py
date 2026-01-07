from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
import math
import time
from typing import Deque, Dict, Optional, Tuple


TsLike = datetime | int | float | None


@dataclass(frozen=True)
class ExecSample:
    """ローリング窓で約定出来高を数えるための1サンプル。"""
    ts_sec: float
    side: str  # 'BUY' or 'SELL'（bitFlyer executions の side を想定）
    price: float
    size: float


class QueueETA:
    """
    Queue-ETA 推定器。

    目的：
      - 直近 window_ms の executions を使って「流量（size/sec）」を推定し、
        指値の “前にいる行列量 + 自分の数量” を捌くのに何秒かかるか（ETA秒）を返す。

    注意：
      - order_side=BUY のとき、約定側は SELL 流量（売り成行が bid を叩く）を使う想定。
      - order_side=SELL のとき、約定側は BUY 流量（買い成行が ask を叩く）を使う想定。
    """

    def __init__(
        self,
        window_sec: float = 30.0,  # 何をするか：ローリング窓を長めにして「サンプル0→レート極小→ETA天井張り付き」を起こしにくくする
        min_rate: float = 0.0,
    ) -> None:
        # 何秒ぶんの出来高でレート（size/sec）を作るか
        self._window_sec = max(0.001, float(window_sec))

        # 0割りや“ほぼゼロ流量”で ETA が暴れるのを避けるための下限
        self._min_rate = float(min_rate)

        # 約定サンプル（古い順）
        self._samples: Deque[ExecSample] = deque()

        # (side, price) ごとのローリング出来高合計（size）
        self._sum_by_side_price: Dict[Tuple[str, float], float] = {}

        # side ごとのローリング出来高合計（size）
        self._sum_by_side_total: Dict[str, float] = {"BUY": 0.0, "SELL": 0.0}
        self._last_estimate = {}  # 何をするか：直近のETA計算の材料（rate/ahead/needed等）を保持して、異常値の原因を追えるようにする

    def on_execution(self, *, side: str, price: float, size: float, ts: TsLike = None) -> None:
        """
        何をする関数：
          executions（約定）1件を受け取り、ローリング窓の集計を更新する。
        """
        now = self._to_ts_sec(ts)
        side_n = self._norm_side(side)
        px = float(price)
        sz = float(size)

        # 変な値は捨てる（集計を壊さない）
        if sz <= 0.0 or not math.isfinite(sz):
            return

        # 先に古い約定を落としてから、新しい約定を足す
        self._expire(now)

        sample = ExecSample(ts_sec=now, side=side_n, price=px, size=sz)
        self._samples.append(sample)

        key = (side_n, px)
        self._sum_by_side_price[key] = self._sum_by_side_price.get(key, 0.0) + sz
        self._sum_by_side_total[side_n] = self._sum_by_side_total.get(side_n, 0.0) + sz

    def estimate_eta_sec(
        self,
        *,
        order_side: str,
        price: Optional[float],
        qty: float,
        level_size: Optional[float] = None,
        queue_ahead: Optional[float] = None,
        ts: TsLike = None,
        price_first: bool = True,
    ) -> float:
        """
        何をする関数：
          指値注文（order_side, price, qty）が “全部約定” するまでの ETA（秒）を見積もる。

        引数の考え方：
          - level_size: その価格レベルに今乗っている板数量（分かるなら渡す）
          - queue_ahead: 「自分より前にいる量」を直接渡せるならこちらが優先
          - price_first: price の出来高が取れているなら price別レート優先、無ければ side合計へフォールバック
        """
        now = self._to_ts_sec(ts)
        self._expire(now)

        ord_side = self._norm_side(order_side)
        hit_side = "SELL" if ord_side == "BUY" else "BUY"

        q = float(qty)
        if q <= 0.0:
            return 0.0

        # “前の行列”は queue_ahead があればそれを使い、無ければ level_size（板の厚み）を使う
        ahead = float(queue_ahead) if queue_ahead is not None else float(level_size or 0.0)
        ahead = max(0.0, ahead)

        # 全量約定を狙うなら「前の行列 + 自分の量」ぶんの流量が必要
        needed = ahead + q

        rate = self._rate_per_sec(
            hit_side=hit_side,
            price=float(price) if price is not None else None,
            price_first=price_first,
        )

        # 流量が無い（または小さすぎる）なら ETA は無限大扱い（ただしデバッグ情報は残す）
        if rate <= self._min_rate:
            eta_sec = math.inf
        else:
            eta_sec = needed / rate

        # 何をするか：後段（engine/strategy/log）から「なぜ inf/巨大になったか」を見れるように最後の計算材料を保存する
        self._last_estimate = {
            "ts_sec": now,
            "order_side": ord_side,
            "hit_side": hit_side,
            "price": float(price) if price is not None else None,
            "qty": q,
            "ahead": ahead,
            "needed": needed,
            "rate_per_sec": rate,
            "samples": len(self._samples),
            "eta_sec": eta_sec,
            "price_first": price_first,
        }

        return eta_sec

    def debug_estimate(
        self,
        *,
        order_side: str,
        price: Optional[float],
        qty: float,
        level_size: Optional[float] = None,
        queue_ahead: Optional[float] = None,
        ts: TsLike = None,
        price_first: bool = True,
    ) -> dict:
        """
        何をする関数：
          QueueETA の見積もりが「inf扱い→キャップ」になっていないかを切り分けるため、
          ETA計算に使った前提（ahead/needed）、サンプル数、rate（price別/side合計/採用）、
          そして生のETA（ms）をまとめて返す（観測専用・例外を投げない）。
        """
        try:
            now = self._to_ts_sec(ts)
            self._expire(now)

            ord_side = self._norm_side(order_side)
            hit_side = "SELL" if ord_side == "BUY" else "BUY"

            q = float(qty)
            if q <= 0.0 or not math.isfinite(q):
                return {
                    "samples": len(self._samples),
                    "hit_side": hit_side,
                    "ahead_qty": 0.0,
                    "needed_qty": 0.0,
                    "rate_price_per_sec": 0.0,
                    "rate_side_per_sec": 0.0,
                    "rate_used_per_sec": 0.0,
                    "is_inf": False,
                    "raw_ms": 0.0,
                }

            ahead = float(queue_ahead) if queue_ahead is not None else float(level_size or 0.0)
            ahead = max(0.0, ahead)
            needed = ahead + q

            px = float(price) if price is not None else None

            # 価格別レート（取れているかを見る用）
            rate_price = self._rate_per_sec(hit_side=hit_side, price=px, price_first=True)

            # 片側合計レート（フォールバックが効くかを見る用）
            rate_side = self._rate_per_sec(hit_side=hit_side, price=None, price_first=False)

            # 実際に採用されるレート（estimate_eta_sec と同じ指定）
            rate_used = self._rate_per_sec(hit_side=hit_side, price=px, price_first=price_first)

            if rate_used <= self._min_rate:
                return {
                    "samples": len(self._samples),
                    "hit_side": hit_side,
                    "ahead_qty": ahead,
                    "needed_qty": needed,
                    "rate_price_per_sec": float(rate_price),
                    "rate_side_per_sec": float(rate_side),
                    "rate_used_per_sec": float(rate_used),
                    "is_inf": True,
                    "raw_ms": None,
                }

            eta_sec = needed / rate_used
            raw_ms = eta_sec * 1000.0

            return {
                "samples": len(self._samples),
                "hit_side": hit_side,
                "ahead_qty": ahead,
                "needed_qty": needed,
                "rate_price_per_sec": float(rate_price),
                "rate_side_per_sec": float(rate_side),
                "rate_used_per_sec": float(rate_used),
                "is_inf": False,
                "raw_ms": float(raw_ms),
            }
        except Exception:
            # デバッグ関数は「絶対に落とさない」：落ちたら観測不能になるため
            return {
                "samples": 0,
                "hit_side": "UNKNOWN",
                "ahead_qty": 0.0,
                "needed_qty": 0.0,
                "rate_price_per_sec": 0.0,
                "rate_side_per_sec": 0.0,
                "rate_used_per_sec": 0.0,
                "is_inf": True,
                "raw_ms": None,
            }

    def get_eta_ms(
        self,
        price: float | None = None,
        queue_qty: float | None = None,
        side: str | None = None,
    ) -> int | None:
        # 何をするか：estimate_eta_sec の戻り（秒）をミリ秒へ変換して返す。inf/NaN/負値/計算不能は None にする。
        import math

        # 何をするか：estimate_eta_sec の引数の形が違っても動くように、複数パターンで呼び出しを試す。
        eta_sec = None

        kwargs = {}
        if price is not None:
            kwargs["price"] = price
        if queue_qty is not None:
            kwargs["queue_qty"] = queue_qty
        if side is not None:
            kwargs["side"] = side

        try:
            eta_sec = self.estimate_eta_sec(**kwargs) if kwargs else self.estimate_eta_sec()
        except TypeError:
            # 何をするか：キーワードが合わない/シグネチャが違う場合に、位置引数でも試す。
            try:
                if price is not None and queue_qty is not None:
                    try:
                        eta_sec = self.estimate_eta_sec(price, queue_qty)
                    except TypeError:
                        eta_sec = self.estimate_eta_sec(queue_qty, price)
                elif price is not None:
                    eta_sec = self.estimate_eta_sec(price)
                elif queue_qty is not None:
                    eta_sec = self.estimate_eta_sec(queue_qty)
            except Exception:
                eta_sec = None
        except Exception:
            eta_sec = None

        if eta_sec is None:
            return None

        try:
            eta = float(eta_sec)
        except Exception:
            return None

        if not math.isfinite(eta) or eta < 0.0:
            return None

        return int(eta * 1000)

    def _rate_per_sec(self, *, hit_side: str, price: Optional[float], price_first: bool) -> float:
        """
        何をする関数：
          ローリング窓の出来高から流量（size/sec）を返す（内部用）。
        """
        if price_first and price is not None:
            v = self._sum_by_side_price.get((hit_side, price), 0.0)
            if v > 0.0:
                return v / self._window_sec

        v_all = self._sum_by_side_total.get(hit_side, 0.0)
        return v_all / self._window_sec

    def _expire(self, now_sec: float) -> None:
        """
        何をする関数：
          ローリング窓（now - window_sec より古い）から外れた約定を捨て、集計を保つ（内部用）。
        """
        cutoff = now_sec - self._window_sec

        while self._samples and self._samples[0].ts_sec < cutoff:
            old = self._samples.popleft()

            # side合計から引く
            self._sum_by_side_total[old.side] = self._sum_by_side_total.get(old.side, 0.0) - old.size

            # (side, price) 合計から引く
            key = (old.side, old.price)
            cur = self._sum_by_side_price.get(key, 0.0) - old.size
            if cur <= 0.0:
                self._sum_by_side_price.pop(key, None)
            else:
                self._sum_by_side_price[key] = cur

        # float誤差でマイナスに沈んだらゼロへ戻す（集計を壊さない）
        for s in ("BUY", "SELL"):
            if self._sum_by_side_total.get(s, 0.0) < 0.0:
                self._sum_by_side_total[s] = 0.0

    @staticmethod
    def _norm_side(side: str) -> str:
        """
        何をする関数：
          side表記ゆれを 'BUY' / 'SELL' に正規化する（内部用）。
        """
        u = str(side).strip().upper()
        if u in {"BUY", "B", "LONG"}:
            return "BUY"
        if u in {"SELL", "S", "SHORT"}:
            return "SELL"
        # 想定外でも落とさない（呼び出し側で正しい値を渡すのが前提）
        return "BUY"

    @staticmethod
    def _to_ts_sec(ts: TsLike) -> float:
        """
        何をする関数：
          ts を epoch seconds (float) にそろえる（内部用）。
        """
        if ts is None:
            return time.time()

        if isinstance(ts, (int, float)):
            x = float(ts)
            # ざっくり判定：ms epoch は 1e12 付近、sec epoch は 1e9 付近
            if x > 1e12:
                return x / 1000.0
            return x

        return float(ts.timestamp())
