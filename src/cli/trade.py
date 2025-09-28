# src/cli/trade.py
# 役割：paper実行のCLI（設定読込→エンジン起動→Ctrl+Cで安全終了）
# 文書の 8.3 ペーパー運用に対応（--config / --strategy）。:contentReference[oaicite:11]{index=11}
from __future__ import annotations

import argparse  # 引数処理
import asyncio  # 非同期ランタイム
from loguru import logger  # 実行ログ
try:
    from dotenv import load_dotenv, find_dotenv  # 何をするか：.env を読み込む（healthと同じ方式）
except Exception:
    load_dotenv = lambda *_, **__: None  # 何をするか：dotenv未導入でも壊れないダミー（渡された引数は捨てる）
    find_dotenv = lambda *_, **__: ""    # 何をするか：dotenv未導入でも壊れないダミー（空文字を返す）


from pathlib import Path  # run.log の保存先を扱う

from src.core.utils import load_config  # 【関数】設定ローダー（base＋上書き）:contentReference[oaicite:12]{index=12}
from src.runtime.engine import PaperEngine  # 【関数】paperエンジン（本ステップ）
from src.runtime.live import run_live  # 何をするか：本番（live）の最小導線（疎通確認）を呼び出す
from src.strategy.zero_reopen_pop import ZeroReopenConfig, zero_reopen_config_from  # 何をするか：Zero→Reopen Popの設定器を読み込む
try:
    from src.runtime.engine import run_paper  # 何をするか：標準のペーパー入口（無い環境もあるのでtryで受ける）
except Exception:
    run_paper = None  # 何をするか：無いときは後段で動的に探すためのダミーにする


def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：--config（必須）/ --strategy（#1 既定 か #2）"""
    p = argparse.ArgumentParser(description="Run paper trading (real-time)")
    p.add_argument("--config", required=True, help="configs/paper.yml など")
    p.add_argument("--strategy", default="stall_then_strike",
                choices=["stall_then_strike", "cancel_add_gate", "age_microprice", "zero_reopen_pop"],
                help="どの戦略で動かすか（#1/#2/#3/#4）")
    p.add_argument("--dry-run", action="store_true", help="何をするか：liveでも実発注せず疎通確認だけ行う（安全テスト）")
    p.add_argument("--paper", action="store_true", help="何をするか：取引所へ発注せず、板に当たれば fills をシミュレートする")

    return p.parse_args()

def main() -> None:
    """【関数】エントリ：設定を読み、paperエンジンを走らせる"""
    load_dotenv(find_dotenv())  # 何をするか：プロジェクト直下の .env を読み込んでから run_live を呼ぶ
    args = _parse_args()
    cfg = load_config(args.config)
    zero_reopen_cfg: ZeroReopenConfig | None = None
    if args.strategy == "zero_reopen_pop":
        zero_reopen_cfg = zero_reopen_config_from(cfg)
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
                rp(cfg, args.strategy, strategy_cfg=zero_reopen_cfg)  # 何をするか：見つけた入口でペーパー運転を開始
            except TypeError:
                rp(cfg, args.strategy)  # 互換：旧シグネチャ（strategy_cfg未対応）の場合は従来呼び出し
        else:
            run_live(cfg, args.strategy, dry_run=args.dry_run, strategy_cfg=zero_reopen_cfg)  # 何をするか：従来どおりlive/dry-run


        return  # 何をするか：live 分岐ではここで終了（paper へは進まない）

    log_path = Path("logs/runtime/run.log")  # 【関数】ログファイルの出力先
    log_path.parent.mkdir(parents=True, exist_ok=True)  # フォルダ作成
    rotate_mb = getattr(getattr(cfg, "logging", None), "rotate_mb", 128)  # 既定128MB
    level = getattr(getattr(cfg, "logging", None), "level", "INFO")  # 既定INFO
    logger.add(log_path, level=level, rotation=f"{int(rotate_mb)} MB", enqueue=True)  # ローテ付きで出力

    if cfg.env != "paper":
        logger.warning(f"env is '{cfg.env}' (expected 'paper') - 続行はします")
    engine = PaperEngine(cfg, args.strategy, strategy_cfg=zero_reopen_cfg)
    try:
        asyncio.run(engine.run_paper())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
