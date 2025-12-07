from typing import Any, Dict, List, Optional

from .exchange import BitflyerExchange


async def fetch_new_fills_since(
    exchange: BitflyerExchange,
    product_code: str,
    last_execution_id: Optional[int],
    page_size: int = 500,
) -> List[Dict[str, Any]]:
    # この関数は「バリアID(last_execution_id)より後の自分の約定(フィル)」を
    # HTTP API からまとめて取ってくる係です。
    # 後でポジション自炊の HTTP リプレイ(欠損埋め)で使います。

    # ここで集めるのは「HTTP側から見た生の約定リスト」です。
    all_execs: List[Dict[str, Any]] = []

    # bitFlyer の after パラメータには「このIDより後」を渡す想定です。
    # last_execution_id が None のときは「制限なし＝最新から一定件数」になります。
    after: Optional[int] = last_execution_id

    while True:
        # Exchange.fetch_my_executions を使って 1 ページ分の約定を取得します。
        executions: List[Dict[str, Any]] = await exchange.fetch_my_executions(
            product_code=product_code,
            after=after,
            count=page_size,
        )

        # 約定が 0 件なら、もうこれ以上は無いのでループ終了です。
        if not executions:
            break

        all_execs.extend(executions)

        # 次のページを取るための after を更新します。
        # API 仕様上 id が大きいほど新しい想定なので、その最大値を after にします。
        try:
            max_id = max(int(e["id"]) for e in executions)
        except (KeyError, ValueError, TypeError):
            # id が取れない・数値化できないような異常データが来た場合は
            # 無限ループを避けるため、ここでループを打ち切ります。
            break

        # 前回と同じ max_id しか返ってこなくなった場合も抜けます（安全弁）。
        if after is not None and max_id <= after:
            break

        after = max_id

        # 1 ページ分の件数が page_size より少ないときは、ここで取り切ったと判断して終了します。
        if len(executions) < page_size:
            break

    # last_execution_id よりも新しいものだけに絞り込み、id 昇順に並べて返します。
    filtered: List[Dict[str, Any]] = []

    for e in all_execs:
        try:
            exec_id = int(e["id"])
        except (KeyError, ValueError, TypeError):
            # id が変なときはスキップして、ポジション計算を壊さないようにします。
            continue

        # last_execution_id が None のときは全部、そうでなければ「より大きい id」だけを採用します。
        if last_execution_id is None or exec_id > last_execution_id:
            filtered.append(e)

    # Q/A/R/F に適用しやすいように、古いものから順番に並べて返します。
    filtered.sort(key=lambda e: int(e["id"]))

    return filtered


async def replay_missing_fills(
    exchange: BitflyerExchange,
    product_code: str,
    last_execution_id: Optional[int],
    apply_fill,
    page_size: int = 500,
) -> int:
    # この関数は「バリアID(last_execution_id)以降の自分の約定」を
    # HTTPで全部集めて、古い順に apply_fill(...) で Q/A/R/F に適用する係です。
    # 後で engine 側から呼び出して、WSで落ちたぶんの欠損をまとめて埋めるときに使います。

    # まずは Step2 で作った関数で、新しい約定だけを全部集めます。
    executions = await fetch_new_fills_since(
        exchange=exchange,
        product_code=product_code,
        last_execution_id=last_execution_id,
        page_size=page_size,
    )

    # executions は id 昇順（古い→新しい）に並んでいる想定なので、
    # そのままの順番で apply_fill に渡していきます。
    applied_count = 0

    for e in executions:
        # apply_fill は「約定イベント1件を Q/A/R/F に反映する」あなた側の関数を想定しています。
        # ここでは「dict をそのまま渡す」だけにしておきます。
        apply_fill(e)
        applied_count += 1

    # 何件リプレイしたかを返しておくと、ログや監査で状況を説明しやすくなります。
    return applied_count


async def http_replay_for_position(
    exchange: BitflyerExchange,
    product_code: str,
    position,
    page_size: int = 500,
) -> int:
    # この関数は「position.last_fill_id」をバリアとして、
    # HTTP経由でその後の自分の約定だけを取り寄せて、
    # position 側の Q/A/R/F に順番に反映する係です。
    # 後で engine の回復モードから呼び出して、
    # WS の取りこぼし（欠損）をまとめて埋めるときに使います。

    # position 側に「最後に処理した約定ID」を覚えているフィールドが
    # ある想定です（名前が違う場合はここを書き換えてください）。
    last_execution_id = getattr(position, "last_fill_id", None)

    def apply_fill_from_http(execution: Dict[str, Any]) -> None:
        # この内側の関数は「HTTPで取ってきた約定1件」を
        # position に反映する係です。
        # すでに WS 約定を処理する関数があるなら、
        # execution をその関数が受け取れる形に変換して呼び出してください。

        if hasattr(position, "apply_fill_from_http"):
            # 例：HTTP用の変換付きハンドラがある場合
            position.apply_fill_from_http(execution)
        elif hasattr(position, "apply_fill"):
            # 例：汎用の apply_fill(...) だけがある場合は、そのまま流用します。
            position.apply_fill(execution)
        else:
            # まだ position 側に「約定1件を適用する関数」が無い場合は、
            # NotImplementedError を投げて、あとで必ず実装するようにします。
            raise NotImplementedError(
                "position.apply_fill_from_http または position.apply_fill を実装してください"
            )

    # ここで Step3 で作った replay_missing_fills(...) を呼び出します。
    # last_execution_id より新しい約定だけを HTTP から集めて、
    # 古い順に apply_fill_from_http(execution) を呼びます。
    return await replay_missing_fills(
        exchange=exchange,
        product_code=product_code,
        last_execution_id=last_execution_id,
        apply_fill=apply_fill_from_http,
        page_size=page_size,
    )
