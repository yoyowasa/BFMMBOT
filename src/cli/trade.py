# src/cli/trade.py
# 役割：paper実行のCLI（設定読込→エンジン起動→Ctrl+Cで安全終了）
# 文書の 8.3 ペーパー運用に対応（--config / --strategy）。:contentReference[oaicite:11]{index=11}
from __future__ import annotations

import argparse  # 引数処理
import asyncio  # 非同期ランタイム
from collections.abc import Mapping, Sequence  # 何をするか：設定のdictアクセスを許可
from typing import List
from loguru import logger  # 実行ログ
try:
    from dotenv import load_dotenv, find_dotenv  # 何をするか：.env を読み込む（healthと同じ方式）
except Exception:
    load_dotenv = lambda *_, **__: None  # 何をするか：dotenv未導入でも壊れないダミー（渡された引数は捨てる）
    find_dotenv = lambda *_, **__: ""    # 何をするか：dotenv未導入でも壊れないダミー（空文字を返す）


from pathlib import Path  # run.log の保存先を扱う


def _cfg_get(obj, key: str, default):
    """何をする関数か：設定オブジェクトから属性/キーを安全に取得する"""
    if obj is None:
        return default
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _format_log_template(template: str | None, strategy: str) -> str | None:
    """何をする関数か：テンプレート文字列に strategy 名を埋め込み、失敗時は原文を返す"""
    if not template:
        return None
    try:
        return template.format(strategy=strategy)
    except KeyError:
        return template
    except Exception as exc:
        logger.warning(f"log_template_format_failed template={template} err={exc}")
        return template


def _setup_text_logs(cfg, strategy: str) -> list[int]:
    """何をする関数か：共通run.logと戦略別ログのシンクを初期化する"""
    log_cfg = getattr(cfg, "logging", None)
    rotate_mb = _cfg_get(log_cfg, "rotate_mb", 128)
    base_level = _cfg_get(log_cfg, "level", "INFO")
    run_template = _cfg_get(log_cfg, "run_log_template", None)
    strategy_template = _cfg_get(log_cfg, "strategy_log_template", None)
    strategy_level = _cfg_get(log_cfg, "strategy_level", None) or base_level

    sink_ids: list[int] = []
    seen_paths: set[str] = set()

    def _add_sink(path_str: str | None, level: str) -> None:
        if not path_str:
            return
        path = Path(path_str).expanduser()
        key = str(path)
        if key in seen_paths:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            logger.error(f"log_path_mkdir_failed path={path} err={exc}")
            return
        rotation = f"{int(rotate_mb)} MB"
        sink_ids.append(logger.add(path, level=level, rotation=rotation, enqueue=True))
        seen_paths.add(key)

    resolved_run = _format_log_template(run_template, strategy)
    if not resolved_run:
        resolved_run = "logs/runtime/run.log"
    _add_sink(resolved_run, base_level)

    resolved_strategy = _format_log_template(strategy_template, strategy)
    if resolved_strategy:
        _add_sink(resolved_strategy, strategy_level)

    return sink_ids

from src.core.utils import load_config  # 【関数】設定ローダー（base＋上書き）:contentReference[oaicite:12]{index=12}
from src.runtime.engine import PaperEngine  # 【関数】paperエンジン（本ステップ）
from src.runtime.live import run_live  # 何をするか：本番（live）の最小導線（疎通確認）を呼び出す
try:
    from src.runtime.engine import run_paper  # 何をするか：標準のペーパー入口（無い環境もあるのでtryで受ける）
except Exception:
    run_paper = None  # 何をするか：無いときは後段で動的に探すためのダミーにする


def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：--config（必須）/ --strategy（#1 既定 か #2）"""
    p = argparse.ArgumentParser(description="Run paper trading (real-time)")
    p.add_argument("--config", required=True, help="configs/paper.yml など")
    p.add_argument(
        "--strategy",
        nargs="*",
        default=None,
        choices=["stall_then_strike", "cancel_add_gate", "age_microprice", "zero_reopen_pop"],
        help="どの戦略で動かすか（省略時は config[strategies] を使用する）",
    )
    p.add_argument("--dry-run", action="store_true", help="何をするか：liveでも実発注せず疎通確認だけ行う（安全テスト）")
    p.add_argument("--paper", action="store_true", help="何をするか：取引所へ発注せず、板に当たれば fills をシミュレートする")

    return p.parse_args()

def main() -> None:
    """【関数】エントリ：設定を読み、paperエンジンを走らせる"""
    load_dotenv(find_dotenv())  # 何をするか：プロジェクト直下の .env を読み込んでから run_live を呼ぶ
    args = _parse_args()
    cfg = load_config(args.config)
    strategy_cfg = None

    cfg_strategies = getattr(cfg, "strategies", None)
    cli_strategies = args.strategy
    if cli_strategies is None:
        raw_strategies = cfg_strategies
    else:
        raw_strategies = cli_strategies

    normalized_strategies: List[str]
    if raw_strategies is None:
        normalized_strategies = []
    elif isinstance(raw_strategies, Sequence) and not isinstance(raw_strategies, (str, bytes)):
        normalized_strategies = [str(s) for s in raw_strategies if s]
    else:
        normalized_strategies = [str(raw_strategies)]

    if not normalized_strategies:
        logger.error("strategy_not_specified CLIまたはconfigのstrategiesが空です")
        raise SystemExit(1)

    # NOTE: マルチ戦略対応を見据えてリストは保持しつつ、現状の呼び出しは先頭のみ使用する
    selected_strategy = normalized_strategies[0]

    sink_ids = _setup_text_logs(cfg, selected_strategy)
    try:
        # 何をするか：設定の env を見て live/paper を切り替える（ワークフローの 8.3→8.4 切替）
        if getattr(cfg, "env", "paper") == "live":
            if args.paper:  # 何をするか：--paper 指定なら疑似発注（fillsまで再現）
                rp = run_paper  # 何をするか：まずは通常のrun_paperを候補にする
                if rp is None:
                    import importlib  # 何をするか：モジュールを動的に読み込んで関数を探す
                    try:
                        mod = importlib.import_module("src.runtime.engine")  # 何をするか：engine内の別名候補を探す
                        for name in ("run_paper", "paper_main", "run_paper_engine", "paper_run"):
                            fn = getattr(mod, name, None)
                            if callable(fn):
                                rp = fn
                                break
                    except Exception:
                        rp = None
                    if rp is None:
                        try:
                            mod2 = importlib.import_module("src.runtime.paper")  # 何をするか：paper専用モジュールがあれば使う
                            for name in ("run_paper", "main", "run"):
                                fn = getattr(mod2, name, None)
                                if callable(fn):
                                    rp = fn
                                    break
                        except Exception:
                            rp = None
                if rp is None:
                    raise RuntimeError("paper runner が見つかりません（engine.run_paper / engine.paper_main / runtime.paper.main などを確認）")  # 何をするか：どこにも無ければ分かりやすく停止
                try:
                    rp(cfg, selected_strategy, strategy_cfg=strategy_cfg)  # 何をするか：見つけた入口でペーパー運転を開始
                except TypeError:
                    rp(cfg, selected_strategy)  # 互換：旧シグネチャ（strategy_cfg未対応）の場合は従来呼び出し
            else:
                run_live(cfg, selected_strategy, dry_run=args.dry_run, strategy_cfg=strategy_cfg)  # 何をするか：従来どおりlive/dry-run
            return  # 何をするか：live 分岐ではここで終了（paper へは進まない）

        if cfg.env != "paper":
            logger.warning(f"env is '{cfg.env}' (expected 'paper') - 続行はします")
        engine = PaperEngine(cfg, selected_strategy, strategy_cfg=strategy_cfg)
        try:
            asyncio.run(engine.run_paper())
        except KeyboardInterrupt:
            pass
    finally:
        for sink_id in sink_ids:
            logger.remove(sink_id)

if __name__ == "__main__":
    main()
