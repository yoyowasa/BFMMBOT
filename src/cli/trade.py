# src/cli/trade.py
# 役割：paper実行のCLI（設定読込→エンジン起動→Ctrl+Cで安全終了）
# 文書の 8.3 ペーパー運用に対応（--config / --strategy）。:contentReference[oaicite:11]{index=11}
from __future__ import annotations

import argparse  # 引数処理
import asyncio  # 非同期ランタイム
import os  # プロセスID取得やロックファイル用
import sys  # コマンドライン表示用
import hashlib  # ロック名ハッシュ化
from collections.abc import Mapping, Sequence  # 何をするか：設定のdictアクセスを許可
from datetime import datetime  # ロックファイルに時刻を書く
from typing import List
from loguru import logger  # 実行ログ
try:
    from dotenv import load_dotenv, find_dotenv  # 何をするか：.env を読み込む（healthと同じ方式）
except Exception:
    load_dotenv = lambda *_, **__: None  # 何をするか：dotenv未導入でも壊れないダミー（渡された引数は捨てる）
    find_dotenv = lambda *_, **__: ""    # 何をするか：dotenv未導入でも壊れないダミー（空文字を返す）


from pathlib import Path  # run.log の保存先を扱う
from zoneinfo import ZoneInfo  # ログ時刻をJSTに固定する
try:
    import msvcrt as _MSVCRT  # Windows用ロック
except Exception:
    _MSVCRT = None
try:
    import fcntl as _FCNTL  # Unix用ロック
except Exception:
    _FCNTL = None

REPO_ROOT = Path(__file__).resolve().parents[2]

# 起動直後ガード：想定外の python.exe（system Python）から呼ばれたら即終了し、
# ついでに子プロセス用に PYTHONEXECUTABLE を .venv に固定する。
if sys.platform.startswith("win"):
    _EARLY_EXPECTED_VENV = (REPO_ROOT / ".venv" / "Scripts" / "python.exe").resolve()
    _EARLY_CURRENT = Path(sys.executable).resolve()
    if _EARLY_EXPECTED_VENV.exists():
        # 子プロセス（multiprocessingなど）も .venv の python を使わせる
        try:
            import multiprocessing as _MP
            _MP.set_executable(str(_EARLY_EXPECTED_VENV))
        except Exception:
            pass
        # venv でない（prefix==base_prefix）か、想定と違う exe なら即終了
        if (sys.prefix == sys.base_prefix) or (_EARLY_CURRENT != _EARLY_EXPECTED_VENV):
            print(
                f"[BF-MMBOT guard] unexpected python executable: {_EARLY_CURRENT} "
                f"(expected .venv python at {_EARLY_EXPECTED_VENV}). exit this extra process.",
                file=sys.stderr,
            )
            os._exit(0)
        os.environ.setdefault("PYTHONEXECUTABLE", str(_EARLY_EXPECTED_VENV))

# 再帰起動ガード：この環境変数が付いた子プロセス（multiprocessingや誤起動）が来たら即終了。
if os.environ.get("BFMMBOT_TRADE_MAIN"):
    print("[BF-MMBOT guard] detected child/secondary src.cli.trade process → exit", file=sys.stderr)
    os._exit(0)
os.environ["BFMMBOT_TRADE_MAIN"] = "1"

# この関数は「どの python.exe から起動されているか」を確認して、
# 想定外（.venv ではなく system Python）からの二重起動であれば、
# すぐに静かに終了するためのガードです。
def _ensure_venv_python_or_exit() -> None:
    """プロジェクト直下の .venv の python 以外からの起動なら、すぐに終了する。"""
    # src/cli/trade.py から2つ上がプロジェクトルート（E:\BF-MMBOT を想定）
    project_root = Path(__file__).resolve().parents[2]

    # Windows の .venv/Scripts/python.exe を想定したパス
    venv_python = (project_root / ".venv" / "Scripts" / "python.exe").resolve()

    # 今このプロセスで動いている python.exe のフルパス
    current_python = Path(sys.executable).resolve()

    # venv判定（sys.prefix と sys.base_prefix が同じなら system Python）
    in_venv = sys.prefix != sys.base_prefix

    # Windows かつ .venv の python.exe が存在するときだけチェックする
    if sys.platform.startswith("win") and venv_python.exists():
        # .venv の python ではなく、別の python.exe から起動されているなら
        # あるいは venv で起動されていないなら
        if (not in_venv) or (current_python != venv_python):
            # ここが「system Python から -m src.cli.trade が呼ばれた」ケースに当たる
            # エラーレベルは 0 にしておく（あくまで余計な子プロセスを静かに終わらせるだけ）
            print(
                f"[BF-MMBOT guard] unexpected python executable: {current_python} "
                f"(expected .venv python at {venv_python}). exit this extra process.",
                file=sys.stderr,
            )
            sys.exit(0)

def _debug_log_process_boot():
    """プロセス起動時に、一度だけPIDなどを表示して多重起動や子プロセスかどうかを確認するための関数。"""
    try:
        import multiprocessing  # この関数内だけで使用（起動元が子プロセスかを見るため）
        proc_name = multiprocessing.current_process().name
    except Exception:
        proc_name = "unknown"
    base_exe = getattr(sys, "_base_executable", None)
    print(
        "[BF-MMBOT boot] "
        f"pid={os.getpid()} "
        f"ppid={os.getppid()} "
        f"exe={sys.executable} "
        f"base_exe={base_exe} "
        f"proc_name={proc_name} "
        f"argv={sys.argv}",
        flush=True,
    )
    try:
        debug_path = REPO_ROOT / "logs/runtime/boot_debug.log"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open("a", encoding="utf-8") as fh:
            fh.write(
                f"{datetime.now().isoformat()} "
                f"pid={os.getpid()} ppid={os.getppid()} exe={sys.executable} base_exe={base_exe} "
                f"proc_name={proc_name} argv={' '.join(sys.argv)} "
                f"BFMMBOT_TRADE_MAIN={os.environ.get('BFMMBOT_TRADE_MAIN')}\n"
            )
    except Exception:
        pass

def _mutex_name(env_name: str | None, config_path: str) -> str:
    """何をするか：Windows名前付きMutex用の一意な名前を作る"""
    base = f"{env_name or 'unknown'}|{Path(config_path).resolve()}"
    h = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"Global\\BFMMBOT_{h}"


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
    # 何をするか：全シンクのフォーマットにPIDを含め、同時起動時に発生源を判別しやすくする
    fmt_with_pid = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | pid={process.id} | {name}:{function}:{line} - {message}"
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
        sink_ids.append(logger.add(path, level=level, rotation=rotation, enqueue=True, format=fmt_with_pid))
        seen_paths.add(key)

    resolved_run = _format_log_template(run_template, strategy)
    if not resolved_run:
        resolved_run = "logs/runtime/run.log"
    _add_sink(resolved_run, base_level)

    resolved_strategy = _format_log_template(strategy_template, strategy)
    if resolved_strategy:
        _add_sink(resolved_strategy, strategy_level)

    return sink_ids

def _force_jst_logger() -> None:
    """何をするか：loguruのタイムスタンプをJSTで出力するようにパッチする"""
    jst = ZoneInfo("Asia/Tokyo")
    def _patch(record):
        try:
            record["time"] = record["time"].astimezone(jst)
        except Exception:
            pass
    logger.configure(patcher=_patch)

def _lock_path(config_path: str, env_name: str | None) -> Path:
    """何をするか：env＋config名でロックファイルパスを決める"""
    safe_env = (env_name or "unknown").replace("/", "_").replace("\\", "_")
    cfg_stem = Path(config_path).name.replace(".", "_").replace(" ", "_")
    return REPO_ROOT / "logs/runtime" / f"trade_{safe_env}_{cfg_stem}.lock"

def _acquire_file_lock(lock_path: Path):
    """何をするか：ファイルロックを非ブロッキング取得する。取れなければ None を返す"""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_path.open("a+")
    try:
        fh.seek(0)
        fh.write("0")  # locking の長さ確保
        fh.flush()
        fh.seek(0)
        if _MSVCRT is not None:
            try:
                _MSVCRT.locking(fh.fileno(), _MSVCRT.LK_NBLCK, 1)
            except OSError:
                fh.close()
                return None
        elif _FCNTL is not None:
            try:
                _FCNTL.flock(fh, _FCNTL.LOCK_EX | _FCNTL.LOCK_NB)
            except OSError:
                fh.close()
                return None
        else:
            # どちらも無い環境ではロックできないので諦めて None
            fh.close()
            return None
        # ここまで来ればロック確保済み。情報を書き出して保持する
        fh.seek(0)
        fh.truncate()
        fh.write(
            f"pid={os.getpid()} started={datetime.now().isoformat()} cmd={' '.join(sys.argv)}\n"
        )
        fh.flush()
        return fh
    except Exception:
        try:
            fh.close()
        except Exception:
            pass
        raise

def _release_file_lock(fh) -> None:
    """何をするか：ファイルロックを解除してファイルも掃除する"""
    if fh is None:
        return
    try:
        if _MSVCRT is not None:
            try:
                _MSVCRT.locking(fh.fileno(), _MSVCRT.LK_UNLCK, 1)
            except Exception:
                pass
        elif _FCNTL is not None:
            try:
                _FCNTL.flock(fh, _FCNTL.LOCK_UN)
            except Exception:
                pass
        path = None
        try:
            path = Path(fh.name)
        except Exception:
            path = None
        try:
            fh.close()
        finally:
            if path:
                try:
                    path.unlink()
                except Exception:
                    pass
    except Exception:
        pass

def _acquire_windows_mutex(env_name: str | None, config_path: str):
    """何をするか：Windows名前付きMutexを非ブロッキング取得する。取れなければ None"""
    if os.name != "nt":
        return None
    try:
        import ctypes  # 遅延import（環境によっては無い場合がある）
        from ctypes import wintypes
    except Exception:
        return None
    name = _mutex_name(env_name, config_path)
    try:
        kernel = ctypes.windll.kernel32
        kernel.SetLastError(0)
        handle = kernel.CreateMutexW(ctypes.c_void_p(None), True, wintypes.LPCWSTR(name))
        if not handle:
            return None
        ERROR_ALREADY_EXISTS = 183
        last_err = kernel.GetLastError()
        if last_err == ERROR_ALREADY_EXISTS:
            # 既に他プロセスが保持
            kernel.ReleaseMutex(handle)
            kernel.CloseHandle(handle)
            return None
        return handle
    except Exception:
        return None

def _release_windows_mutex(handle) -> None:
    """何をするか：Windows Mutexの解放"""
    if os.name != "nt" or handle is None:
        return
    try:
        import ctypes
    except Exception:
        return
    try:
        kernel = ctypes.windll.kernel32
        kernel.ReleaseMutex(handle)
        kernel.CloseHandle(handle)
    except Exception:
        pass

def _acquire_singleton_lock(lock_path: Path, env_name: str | None, config_path: str):
    """何をするか：多重起動を避けるロックを取得（WindowsはMutex優先）"""
    mutex_handle = _acquire_windows_mutex(env_name, config_path)
    if os.name == "nt" and mutex_handle is None:
        return None
    fh = _acquire_file_lock(lock_path)
    if fh is None:
        _release_windows_mutex(mutex_handle)
        return None
    return (fh, mutex_handle)

def _release_singleton_lock(lock_obj) -> None:
    """何をするか：取得したロック（Mutex＋ファイル）を解除する"""
    if not lock_obj:
        return
    fh, mutex_handle = lock_obj
    _release_file_lock(fh)
    _release_windows_mutex(mutex_handle)

def _ensure_running_from_project_venv() -> None:
    """何をするか：プロジェクト直下の .venv の python 以外から呼ばれたら即終了する"""
    exe_path = Path(sys.executable).resolve()
    project_root = Path(__file__).resolve().parents[2]  # .../src/cli/trade.py から2階層上がプロジェクトルート想定

    windows_venv_python = project_root / ".venv" / "Scripts" / "python.exe"
    posix_venv_python = project_root / ".venv" / "bin" / "python"

    expected_python = None
    if windows_venv_python.exists():
        expected_python = windows_venv_python.resolve()
    elif posix_venv_python.exists():
        expected_python = posix_venv_python.resolve()
    else:
        # .venv 自体が無い環境ではチェックしない（従来どおり継続）
        return

    if exe_path != expected_python:
        print(
            "[BF-MMBOT] この trade CLI はプロジェクト直下の .venv の python からのみ起動してください。\n"
            f"  期待している python: {expected_python}\n"
            f"  現在の python: {exe_path}"
        )
        sys.exit(1)

from src.core.utils import load_config  # 【関数】設定ローダー（base＋上書き）:contentReference[oaicite:12]{index=12}
from src.runtime.engine import PaperEngine  # 【関数】paperエンジン（本ステップ）
try:
    from src.runtime.engine import run_paper  # 何をするか：標準のペーパー入口（無い環境もあるのでtryで受ける）
except Exception:
    run_paper = None  # 何をするか：無いときは後段で動的に探すためのダミーにする


def _parse_args() -> argparse.Namespace:
    """【関数】引数を読む：--config（必須）/ --strategy（#1 既定 か #2）"""
    p = argparse.ArgumentParser(description="Run paper trading (real-time)")
    p.add_argument("--config", required=True, help="configs/paper.yml など")
    p.add_argument(
        "--env",
        choices=["paper", "live", "backtest"],
        help=" CLIからenvを直接切り替える。未指定なら設定ファイルのenvを使用。",
    )
    p.add_argument(
        "--strategy",
        nargs="*",
        default=None,
        choices=["stall_then_strike", "cancel_add_gate", "age_microprice", "zero_reopen_pop"],
        help="どの戦略で動かすか（省略時は config[strategies] を使用する）",
    )
    p.add_argument(
        "--duration-min",
        type=float,
        default=None,
        help="paper実行の最大稼働時間（分）。指定すると経過後に自動停止する",
    )
    p.add_argument("--dry-run", action="store_true", help="何をするか：liveでも実発注せず疎通確認だけ行う（安全テスト）")
    p.add_argument("--paper", action="store_true", help="何をするか：取引所へ発注せず、板に当たれば fills をシミュレートする")

    return p.parse_args()

def main() -> None:
    """【関数】エントリ：設定を読み、paperエンジンを走らせる"""
    _debug_log_process_boot()  # 起動ごとにPID/PPID/argvを出力し、多重起動や親子関係を調べる
    _ensure_running_from_project_venv()  # 何をするか：.venv 以外の python から起動されていないかを先に確認
    load_dotenv(find_dotenv())  # 何をするか：プロジェクト直下の .env を読み込んでから run_live を呼ぶ
    _force_jst_logger()  # 何をするか：loguru出力をJST表記に統一する
    args = _parse_args()
    cfg = load_config(args.config, env=args.env)
    lock_handle = None
    lock = _lock_path(args.config, getattr(cfg, "env", None))
    lock_handle = _acquire_singleton_lock(lock, getattr(cfg, "env", None), args.config)
    if lock_handle is None:
        existing = ""
        try:
            existing = lock.read_text(errors="ignore").strip()
        except Exception:
            existing = ""
        logger.error(f"別プロセスが実行中のため開始しません lock={lock} info='{existing}'")
        raise SystemExit(1)
    strategy_cfg = _cfg_get(cfg, "strategy_cfg", None)

    raw_cli = getattr(args, "strategy", None)
    if raw_cli:
        strategy_names: List[str] = []  # 何をするか：CLI指定があればカンマ区切りにも対応して正規化する
        for entry in raw_cli:
            for part in str(entry).split(","):
                name = part.strip()
                if name:
                    strategy_names.append(name)
    else:
        cfg_strategies = getattr(cfg, "strategies", None)
        if cfg_strategies is None:
            strategy_names = []
        elif isinstance(cfg_strategies, Sequence) and not isinstance(cfg_strategies, (str, bytes)):
            strategy_names = [str(s) for s in cfg_strategies if s]
        else:
            strategy_names = [str(cfg_strategies)]

    if not strategy_names:
        logger.error("strategy_not_specified CLIまたはconfigのstrategiesが空です")
        raise SystemExit(1)

    try:
        cfg["strategies"] = strategy_names  # 何をするか：後段が参照する戦略名リストをcfgへ確定させる（Mapping互換時）
    except Exception:
        setattr(cfg, "strategies", strategy_names)  # 何をするか：BaseModel等でも同じリストを保持する

    # NOTE: マルチ戦略対応を見据えてリストは保持しつつ、現状の呼び出しは先頭のみ使用する
    selected_strategy = strategy_names[0]
    normalized_strategies = tuple(strategy_names)  # 何をするか：後段に渡す戦略リストを確定（タプルで不変化）

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
                paper_kwargs = {
                    "strategies": normalized_strategies,
                    "strategy_cfg": strategy_cfg,
                }
                try:
                    rp(cfg, selected_strategy, **paper_kwargs)  # 何をするか：見つけた入口でペーパー運転を開始
                except TypeError:
                    try:
                        rp(cfg, selected_strategy, strategy_cfg=strategy_cfg)  # 互換：旧シグネチャ（strategy_cfg未対応）の場合は従来呼び出し
                    except TypeError:
                        rp(cfg, selected_strategy)  # 互換：さらに古いシグネチャ（strategyのみ）の場合
            else:
                # 遅延インポート（live環境のみ読み込む）
                from src.runtime.live import run_live
                run_live(
                    cfg,
                    selected_strategy,
                    dry_run=args.dry_run,
                    strategies=normalized_strategies,
                    strategy_cfg=strategy_cfg,
                )  # 何をするか：従来どおりlive/dry-run
            return  # 何をするか：live 分岐ではここで終了（paper へは進まない）

        if cfg.env != "paper":
            logger.warning(f"env is '{cfg.env}' (expected 'paper') - 続行はします")
        engine = PaperEngine(
            cfg,
            selected_strategy,
            strategies=normalized_strategies,
            strategy_cfg=strategy_cfg,
        )
        duration_min = getattr(args, "duration_min", None)
        if duration_min is not None and duration_min > 0:
            # 何をするか：指定分数が経過したら run_paper をキャンセルして自動停止する
            async def _run_with_timeout() -> None:
                task = asyncio.create_task(engine.run_paper())
                try:
                    await asyncio.wait_for(task, timeout=duration_min * 60.0)
                except asyncio.TimeoutError:
                    logger.info(f"paper timeout reached ({duration_min:.2f} min) - cancelling")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            try:
                asyncio.run(_run_with_timeout())
            except KeyboardInterrupt:
                pass
        else:
            try:
                asyncio.run(engine.run_paper())
            except KeyboardInterrupt:
                pass
    finally:
        _release_singleton_lock(lock_handle)
        for sink_id in sink_ids:
            logger.remove(sink_id)

if __name__ == "__main__":
    # これは「.venv の python 以外から src.cli.trade が起動されたらすぐ終了する」ガードです。
    _ensure_venv_python_or_exit()

    # ここから先は、.venv の python.exe から起動されたプロセスだけが実行されます。
    main()
