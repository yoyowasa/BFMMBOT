#!/usr/bin/env python3
# 何をするスクリプトか：改修の“現状”を一括点検し、OK/MISSINGを行で表示する（表は使わない）

from pathlib import Path  # 何をするか：ファイル/パス操作に使う

def _repo_root() -> Path:
    """何をする関数か：リポジトリのルート候補を推定して返す（tools/ 配下に置く想定）"""
    here = Path(__file__).resolve()
    return here.parents[1] if here.parent.name == "tools" else Path.cwd()

def _read_text(p: Path) -> str:
    """何をする関数か：UTF-8でテキスト読み込み（失敗時は空文字）"""
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return ""

def check_live_py(repo: Path) -> None:
    """何をする関数か：live.py に入れるべき主な改修点の有無をOK/MISSINGで出す"""
    path = repo / "src/runtime/live.py"
    s = _read_text(path)
    print("## src/runtime/live.py")
    def chk(name: str, *need: str) -> None:
        ok = all(x in s for x in need)
        print(f"- {name}: {'OK' if ok else 'MISSING'}")
    chk("WS再接続ラッパ", "def _stream_with_reconnect(", "for ev in _stream_with_reconnect(")
    chk("鮮度ガード(max_stale_ms)", "stale_ms = ", "last_ev_at = ", "reason=\"stale_data\"")
    chk("スプレッド広すぎガード", "max_spread_bp", "reason=\"wide_spread\"")
    chk("異常板ガード(bad_book)", "reason=\"bad_book\"")
    chk("アクティブ上限ガード", "max_active_orders", "reason=\"active_guard\"")
    chk("start/stop heartbeat", "event=\"start\"", "def _mk_atexit(", "event=\"stop\"")
    chk("Authチェック", "AuthError", "reason=\"auth\"")
    chk("未捕捉例外→kill", "def _mk_excepthook(", "sys.excepthook = _mk_excepthook(", "reason=\"exception\"")
    chk("戦略評価の3引数", "actions = strat.evaluate(ob, now, cfg)")
    chk("decision_log 最小シグネチャ", "decision_log.add(ts=now.isoformat(), strategy=strategy_name, decision=", "eta_ms=int(")
    chk("Killはdry-runで無効", "(not dry_run) and _check_kill(")
    chk("TTL取消: deadline None非対象", "meta.get(\"deadline\") is not None")
    chk("Fillsを毎周回で反映", "fills = _pull_fill_deltas(ex, live_orders)", "for side, px, sz, tag, done in fills")
    chk("発注前 正規化", "def _normalize_px_sz(", "px, sz = _normalize_px_sz(cfg, px, sz)")
    chk("メンテ/Funding pause", "reason=\"maintenance\"", "reason=\"funding\"")
    chk("メンテ/Funding CSV", "def _csv_event_write(", "maintenance.csv", "funding.csv")
    chk("dry-run時間制限", "dry_run_max_sec", "reason=\"dryrun_done\"")
    chk("建玉シード", "def _seed_inventory_and_avg_px", "pnl_state = (lambda a,n:")
    chk("アクティブ注文シード", "def _seed_live_orders_from_active(", "_seed_live_orders_from_active(ex, live_orders)")

def check_realtime_py(repo: Path) -> None:
    """何をする関数か：realtime.py のWS実装（async本体と同期ブリッジ）の有無を点検する"""
    path = repo / "src/core/realtime.py"
    s = _read_text(path)
    print("## src/core/realtime.py")
    print(f"- event_stream(async): {'OK' if 'async def event_stream(' in s else 'MISSING'}")
    need_bridge = ("def stream_events(" in s) and ("Queue" in s) and ("threading" in s)
    print(f"- stream_events(同期ブリッジ): {'OK' if need_bridge else 'MISSING'}")

def check_cli(repo: Path) -> None:
    """何をする関数か：CLIのdotenv導線が入っているかを点検する"""
    h = _read_text(repo / "src/cli/health.py")
    t = _read_text(repo / "src/cli/trade.py")
    print("## src/cli/health.py")
    print(f"- dotenv import: {'OK' if 'from dotenv import load_dotenv' in h else 'MISSING'}")
    print(f"- load_dotenv 呼び出し: {'OK' if 'load_dotenv(find_dotenv())' in h else 'MISSING'}")
    print("## src/cli/trade.py")
    print(f"- dotenv import: {'OK' if 'from dotenv import load_dotenv' in t else 'MISSING'}")
    print(f"- load_dotenv 呼び出し: {'OK' if 'load_dotenv(find_dotenv())' in t else 'MISSING'}")

def check_config(repo: Path) -> None:
    """何をする関数か：configs/live.yml の主要キー（本件で使う分）が入っているかを点検する"""
    s = _read_text(repo / "configs/live.yml")
    print("## configs/live.yml")
    def show(key: str) -> None:
        print(f"- {key}: {'OK' if key in s else 'MISSING'}")
    for key in [
        "product_code:", "guard:", "max_stale_ms:", "max_spread_bp:",
        "orders:", "max_inflight", "gate:", "max_inflight_per_key:",
        "risk:", "max_active_orders:", "max_inventory:", "limit_qty:", "kill:",
        "mode_switch:", "maintenance:", "funding_calc_jst:", "funding_transfer_lag_hours:",
        "logging:", "rotate_mb:", "size:", "step:", "min:", "default:", "dry_run_max_sec:",
    ]:
        show(key)

def main() -> None:
    """何をする関数か：各チェック関数を順に呼び出し、OK/MISSING を表示する"""
    repo = _repo_root()
    check_live_py(repo)
    check_realtime_py(repo)
    check_cli(repo)
    check_config(repo)

if __name__ == "__main__":
    main()
