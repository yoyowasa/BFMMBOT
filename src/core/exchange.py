# src/core/exchange.py
# これは bitFlyer Lightning REST の送信口（exchange adapter）の最小実装です。
# 認証ヘッダ生成・新規注文・取消・未約定一覧・安全なリトライ（GET/取消のみ）を提供します。

from __future__ import annotations

import hashlib  # 署名（HMAC-SHA256）に使う
import hmac     # 署名（HMAC-SHA256）に使う
import httpx    # HTTPクライアント（Poetry依存に含まれる）
import json     # リクエストBodyのシリアライズ
import time     # 単調なタイムスタンプ生成（簡易ノンス）
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode  # GETのクエリを安全に構築
from loguru import logger           # リトライ時の警告出力

# ---- エラー型（何が起きたかを上位で判別しやすくする） ----
class ExchangeError(Exception):
    """bitFlyer REST呼び出しの一般的な失敗（4xx等）"""

class AuthError(ExchangeError):
    """認証失敗（鍵/署名/権限）"""

class RateLimitError(ExchangeError):
    """429 レート制限"""

class ServerError(ExchangeError):
    """5xx サーバ側エラー"""

class NetworkError(ExchangeError):
    """ネットワーク到達性などの httpx 例外"""

# ---- 本体：bitFlyer 送信口 ----
class BitflyerExchange:
    """
    bitFlyer Lightning REST の最小Adapter。
    - 認証：ACCESS-KEY / ACCESS-TIMESTAMP / ACCESS-SIGN を付与
    - 注文：sendchildorder（新規）、cancelchildorder / cancelallchildorders（取消）
    - 照会：getchildorders（未約定一覧・受け付けID検索）
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        product_code: str = "FX_BTC_JPY",  # CFDの既定銘柄
        base_url: str = "https://api.bitflyer.com",
        timeout: float = 10.0,
    ) -> None:
        """API鍵と銘柄を保持し、同期httpxクライアントを準備する"""
        self.api_key = api_key
        self.api_secret = api_secret
        self.product_code = product_code
        self._client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)
        self._last_ts = 0.0  # 単調増加の時刻を保ち、簡易ノンス衝突を避ける

    # ---- ライフサイクル管理 ----
    def close(self) -> None:
        """HTTP接続を閉じる"""
        self._client.close()

    def __enter__(self) -> "BitflyerExchange":
        """with 構文で使えるようにする"""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """with ブロック終了時に自動クローズする"""
        self.close()

    # ---- 内部ユーティリティ（署名/リクエスト共通） ----
    def _ts(self) -> str:
        """秒ベースのUNIX時刻を文字列で返す（単調性を保証して簡易ノンスにする）"""
        now = time.time()
        if now <= self._last_ts:
            now = self._last_ts + 0.001
        self._last_ts = now
        return str(now)

    def _sign(self, ts: str, method: str, path_with_query: str, body_str: str) -> str:
        """
        bitFlyerの署名を作成する関数。
        ルール：ACCESS-SIGN = HMAC_SHA256( secret, ts + method + path(+query) + body )
        """
        payload = f"{ts}{method}{path_with_query}{body_str}"
        return hmac.new(self.api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        idempotent: bool = True,
        max_retries: int = 3,
    ) -> Any:
        """
        認証ヘッダを付けてAPIを呼ぶ共通関数。
        - GET/取消など**副作用が小さい**呼び出しは429/5xxで指数バックオフ再試行。
        - **新規注文（POST /sendchildorder）は重複防止のため既定で自動再試行しない**。
        """
        method = method.upper()
        path_with_query = path if not params else f"{path}?{urlencode(params)}"
        body_str = json.dumps(json_body, ensure_ascii=False, separators=(",", ":")) if json_body else ""

        attempt = 0
        while True:
            ts = self._ts()
            sign = self._sign(ts, method, path_with_query, body_str)
            headers = {
                "ACCESS-KEY": self.api_key,
                "ACCESS-TIMESTAMP": ts,
                "ACCESS-SIGN": sign,
                "Content-Type": "application/json",
            }

            try:
                resp = self._client.request(
                    method,
                    path_with_query,
                    headers=headers,
                    content=body_str if body_str else None,
                )
            except httpx.HTTPError as e:
                # 通信到達性等の例外：idempotent時のみリトライ
                if idempotent and attempt < max_retries:
                    backoff = min(2 ** attempt, 8)
                    time.sleep(backoff)
                    attempt += 1
                    continue
                raise NetworkError(str(e)) from e

            # レート/サーバ障害：idempotent時のみ指数バックオフ
            if resp.status_code in (429, 500, 503):
                if idempotent and attempt < max_retries:
                    backoff = min(2 ** attempt, 8)
                    logger.warning(f"retry {attempt+1}/{max_retries} after {backoff}s: {method} {path}")
                    time.sleep(backoff)
                    attempt += 1
                    continue
                if resp.status_code == 429:
                    raise RateLimitError(resp.text)
                raise ServerError(resp.text)

            if resp.status_code in (401, 403):
                # 鍵/署名/権限の問題
                raise AuthError(resp.text)

            if resp.is_success:
                # 200/204 など：JSONなら辞書を返し、空なら None を返す
                ctype = resp.headers.get("Content-Type", "")
                return resp.json() if "application/json" in ctype else None

            # その他 4xx は注文拒否など
            raise ExchangeError(f"{resp.status_code} {resp.text}")

    # ---- パブリックメソッド（実際に使う呼び出し） ----
    def send_child_order(
        self,
        *,
        side: str,
        size: float,
        price: Optional[float] = None,
        time_in_force: str = "GTC",
        minute_to_expire: Optional[int] = None,
        child_order_type: str = "LIMIT",
    ) -> str:
        """
        新規注文（子注文）を出す関数。
        - LIMIT/IOC/FOK に対応（TIF は time_in_force で指定）
        - **重複発注を避けるため本呼び出しは自動リトライしません**
        戻り値：child_order_acceptance_id
        """
        body: Dict[str, Any] = {
            "product_code": self.product_code,
            "child_order_type": child_order_type,
            "side": side.upper(),
            "size": size,
        }
        if child_order_type == "LIMIT":
            if price is None:
                raise ValueError("LIMIT注文には price が必要です")
            body["price"] = price
        if minute_to_expire is not None:
            body["minute_to_expire"] = minute_to_expire
        if time_in_force:
            body["time_in_force"] = time_in_force

        data = self._request(
            "POST",
            "/v1/me/sendchildorder",
            json_body=body,
            idempotent=False,  # ここは自動リトライしない
            max_retries=0,
        )
        return data["child_order_acceptance_id"]

    def cancel_child_order(
        self,
        *,
        child_order_acceptance_id: Optional[str] = None,
        child_order_id: Optional[str] = None,
    ) -> None:
        """
        単一の子注文をキャンセルする関数。
        - acceptance_id か order_id のどちらかを指定（片方は省略）
        """
        if not child_order_acceptance_id and not child_order_id:
            raise ValueError("child_order_acceptance_id または child_order_id を指定してください")

        body: Dict[str, Any] = {"product_code": self.product_code}
        if child_order_acceptance_id:
            body["child_order_acceptance_id"] = child_order_acceptance_id
        else:
            body["child_order_id"] = child_order_id

        self._request(
            "POST",
            "/v1/me/cancelchildorder",
            json_body=body,
            idempotent=True,
            max_retries=3,
        )

    def cancel_all_child_orders(self) -> None:
        """
        対象銘柄の子注文をすべてキャンセルする関数。
        - 失敗時は指数バックオフで再試行
        """
        body = {"product_code": self.product_code}
        self._request(
            "POST",
            "/v1/me/cancelallchildorders",
            json_body=body,
            idempotent=True,
            max_retries=3,
        )

    def get_positions(self) -> List[Dict[str, Any]]:
        """現在の建玉一覧を返す関数（+BUY/-SELLの合算に使う）"""
        params: Dict[str, Any] = {"product_code": self.product_code}
        data = self._request("GET", "/v1/me/getpositions", params=params, idempotent=True, max_retries=3)
        return data

    def get_child_order_by_acceptance_id(self, acceptance_id: str) -> List[Dict[str, Any]]:
        """何をするか：受理IDで子注文の詳細（状態/約定量/残量/平均価格など）を照会して返す"""
        params: Dict[str, Any] = {
            "product_code": self.product_code,
            "child_order_acceptance_id": acceptance_id,
            "count": 1,  # 何をするか：対象は一意のため1件だけ取得
        }
        data = self._request("GET", "/v1/me/getchildorders", params=params, idempotent=True, max_retries=3)
        return data  # 何をするか：APIの素の配列（0件 or 1件）をそのまま返す

    def list_active_child_orders(self, *, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        未約定（ACTIVE）の子注文一覧を取得する関数。
        - child_order_state=ACTIVE を指定して取得
        """
        params: Dict[str, Any] = {"product_code": self.product_code, "child_order_state": "ACTIVE"}
        if count is not None:
            params["count"] = count
        data = self._request("GET", "/v1/me/getchildorders", params=params, idempotent=True, max_retries=3)
        return data  # APIはJSON配列を返す
