# src/core/realtime.py
# 役割：bitFlyer Lightning WSに接続し、board/executions(FX_BTC_JPY)を購読してイベントを流す
# - 【関数】WS接続管理（再接続/購読復元/指数バックオフ）
# - 【関数】イベント受信→単一路（async generator）
# - 【関数】重複排除（executionsのidで簡易に）※最小版
# 参照：文書「core/realtime.py：WS購読（board/executions）」の要件。:contentReference[oaicite:4]{index=4}

from __future__ import annotations

import asyncio  # 再接続の待ち・キャンセル制御
import json  # JSON-RPCの組み立て（送信のみは標準でOK）
from datetime import datetime, timezone  # 受信刻印(ts)の付与
import time  # 何をするか：dry-run用ダミーストリームで sleep に使う
import os
import ssl

from typing import AsyncIterator, Dict, Any, Iterable  # 型ヒント
import websockets  # WebSocket接続（poetryで導入済み）
from loguru import logger  # 見やすいログ
from queue import Queue  # 何をするか：非同期→同期ブリッジのキュー
import threading         # 何をするか：asyncストリームをバックグラウンドで回すために使う



_WS_URL = "wss://ws.lightstream.bitflyer.com/json-rpc"  # Lightning WS（文書の購読先）:contentReference[oaicite:5]{index=5}

def _subscribe_msg(channel: str) -> str:
    """【関数】購読メッセージ作成：JSON-RPC 2.0のsubscribeを作る（最小）"""
    return json.dumps({"method": "subscribe", "params": {"channel": channel}})

def _now_iso_utc() -> str:
    """【関数】UTCの現在時刻をISO文字列で返す（録画に使う）"""
    return datetime.now(timezone.utc).isoformat()

async def event_stream(
    product_code: str = "FX_BTC_JPY",
    channels: Iterable[str] | None = None,
) -> AsyncIterator[Dict[str, Any]]:
    """
    【関数】イベントストリーム（async generator）
    - 役割：board/executions を購読して、受けたメッセージを1件ずつ辞書でyieldする
    - 再接続：切れたら指数バックオフで接続し直し、購読を復元する
    - 重複排除：executionsのidで簡易除外（最低限）
    """
    if channels is None:
        channels = (
            f"lightning_board_{product_code}",
            f"lightning_executions_{product_code}",
        )

    backoff = 1.0  # 秒（指数的に増やすが上限あり）
    max_backoff = 30.0
    seen_exec_ids: set[int] = set()  # 重複排除（最小）

    while True:
        try:
            logger.info(f"WS connecting to {_WS_URL} ...")
            ssl_ctx = ssl.create_default_context()
            cafile = os.getenv("BF_SSL_CAFILE")
            if cafile:
                ssl_ctx.load_verify_locations(cafile=cafile)
            elif os.getenv("BF_SSL_INSECURE") in ("1", "true", "TRUE", "yes", "YES"):
                ssl_ctx = ssl._create_unverified_context()

            async with websockets.connect(
                _WS_URL,
                ping_interval=20,
                close_timeout=10,
                ssl=ssl_ctx,
            ) as ws:
                # 購読を復元
                for ch in channels:
                    await ws.send(_subscribe_msg(ch))
                    logger.info(f"subscribed: {ch}")

                backoff = 1.0  # 成功したらバックオフをリセット

                # 受信ループ
                async for raw in ws:
                    ts = _now_iso_utc()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("skip: invalid json")
                        continue

                    # JSON-RPC仕様：channelMessageのみ拾う
                    if msg.get("method") != "channelMessage":
                        continue

                    params = msg.get("params") or {}
                    ch = params.get("channel")
                    payload = params.get("message")

                    # executions は配列メッセージ（id重複の可能性あり）
                    if ch and ch.startswith("lightning_executions_") and isinstance(payload, list):
                        # 新規idだけを残す（最小の重複排除）
                        new_items = []
                        for e in payload:
                            eid = e.get("id")
                            if isinstance(eid, int) and eid not in seen_exec_ids:
                                seen_exec_ids.add(eid)
                                new_items.append(e)
                        if not new_items:
                            continue
                        payload = new_items

                    yield {
                        "ts": ts,              # 受信刻印（UTC ISO）
                        "channel": ch,         # チャンネル名
                        "message": payload,    # 生のメッセージ（board差分 or executions配列）
                        "source": "bitflyer_ws",
                    }

        except asyncio.CancelledError:
            logger.info("WS stream cancelled.")
            raise
        except Exception as e:
            logger.warning(f"WS error: {e!r} (reconnect in {backoff:.1f}s)")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, max_backoff)
            continue

def stream_events(product_code: str = "FX_BTC_JPY", channels: Iterable[str] | None = None):
    """何をする関数か：asyncな event_stream(...) をバックグラウンドで動かし、同期forで使えるようにするブリッジ"""
    q: Queue = Queue(maxsize=1024)  # 何をするか：イベント受け渡し用のキュー
    _STOP = object()                # 何をするか：終了の合図

    async def _runner():
        """何をするか：asyncのevent_streamから受けたイベントを順次キューへ入れる"""
        try:
            async for ev in event_stream(product_code=product_code, channels=channels):
                q.put(ev)  # 何をするか：受け取ったイベントを同期側へ渡す
        except Exception as e:
            logger.exception(f"realtime: runner error → stop: {e}")
        finally:
            q.put(_STOP)  # 何をするか：終了の合図を送る

    def _run_loop():
        """何をするか：専用イベントループで_runnerを回す"""
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_runner())
        finally:
            loop.close()

    th = threading.Thread(target=_run_loop, daemon=True)  # 何をするか：バックグラウンドでasyncを回す
    th.start()

    while True:
        item = q.get()  # 何をするか：キューから順に取り出して同期forの呼び出し側へ渡す
        if item is _STOP:
            break
        yield item
