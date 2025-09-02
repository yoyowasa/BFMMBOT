# src/cli/health.py
# これは live 本番前の「安全確認（ヘルスチェック）」を行うCLIです。実発注は一切せず、.envとREST疎通だけを確認します。

from __future__ import annotations

import os  # 何をするか：APIキーを .env から読む
import argparse  # 何をするか：CLI引数を扱う
from loguru import logger  # 何をするか：人間が読めるログを出す

# 何をするか：REST送信口（exchange adapter）を使って、未約定一覧/建玉照会で疎通確認
from src.core.exchange import BitflyerExchange, ExchangeError, AuthError, RateLimitError, ServerError, NetworkError


def check_env() -> tuple[str, str]:
    """何をするか：.env から API キー/シークレットを読み、無ければ例外にする"""
    key = os.getenv("BF_API_KEY")
    sec = os.getenv("BF_API_SECRET")
    if not key or not sec:
        raise RuntimeError("BF_API_KEY / BF_API_SECRET が見つかりません（.env を確認してください）")
    return key, sec


def check_exchange(product_code: str) -> None:
    """何をするか：list_active と get_positions を呼んで、REST疎通と権限を確認（実発注は行わない）"""
    key, sec = check_env()
    with BitflyerExchange(key, sec, product_code=product_code) as ex:
        try:
            _ = ex.list_active_child_orders(count=1)  # 何をするか：未約定一覧が取れるか確認
            _ = ex.get_positions()  # 何をするか：建玉一覧が取れるか確認（CFD前提）
        except (AuthError, RateLimitError, ServerError, NetworkError, ExchangeError) as e:
            logger.error(f"health NG: exchange error → {e}")
            raise
    logger.info(f"health OK: REST疎通＆権限OK product={product_code}")


def main() -> None:
    """何をするか：CLI入口。--product-code が無ければ FX_BTC_JPY（ワークフロー既定）で確認"""
    p = argparse.ArgumentParser(description="bitFlyer live ヘルスチェック（実発注なし）")
    p.add_argument("--product-code", default="FX_BTC_JPY", help="何をするか：確認する銘柄（既定=FX_BTC_JPY）")
    args = p.parse_args()

    try:
        check_exchange(args.product_code)
    except Exception as e:
        logger.error(f"health NG: {e}")
        raise SystemExit(2)
    logger.info("health OK: ready for canary (30–60min small size)")

if __name__ == "__main__":
    main()
