#!/usr/bin/env python3
# 何をするスクリプトか：観察モード(dry-run)のログを要約し、稼働時間・pause理由の件数・skip件数・戦略エラー件数をテキストで表示する

from pathlib import Path  # 何をするか：ファイル/パス操作に使う
import json               # 何をするか：heartbeat.ndjsonの各行(JSON)を読む
from collections import Counter  # 何をするか：件数集計に使う
import re                 # 何をするか：run.logの行パターン抽出に使う
from datetime import datetime  # 何をするか：ISO時刻の差分(稼働秒)を計算

def _repo_root() -> Path:
    """何をする関数か：リポジトリのルートを推定して返す（tools/ 配下に置く想定）"""
    here = Path(__file__).resolve()
    return here.parents[1] if here.parent.name == "tools" else Path.cwd()

def _read_lines(path: Path):
    """何をする関数か：テキストファイルを安全に1行ずつ読み出す（無ければ空イテレータ）"""
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []

def _parse_iso(ts: str):
    """何をする関数か：ISO8601文字列をdatetimeへ（失敗時はNone）"""
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None

def load_heartbeat(repo: Path):
    """何をする関数か：heartbeat.ndjson を行ごとにJSONとして読み込む（壊れた行は無視）"""
    hb_path = repo / "logs" / "runtime" / "heartbeat.ndjson"
    items = []
    for line in _read_lines(hb_path):
        line = line.strip()
        if not line:
            continue
        try:
            items.append(json.loads(line))
        except Exception:
            # 何をするか：壊れた行は捨てる（観察の妨げにしない）
            pass
    return items

def summarize_heartbeat(hb_items):
    """何をする関数か：heartbeatのstart/stop/killから稼働時間とpause理由の件数を集計する"""
    starts = [_parse_iso(x.get("ts", "")) for x in hb_items if x.get("event") == "start"]
    stops = [_parse_iso(x.get("ts", "")) for x in hb_items if x.get("event") in ("stop", "kill")]
    all_ts = [_parse_iso(x.get("ts", "")) for x in hb_items if x.get("ts")]
    ts_start = min([t for t in starts if t], default=None)
    ts_stop = max([t for t in stops if t], default=(max([t for t in all_ts if t], default=None)))
    runtime_sec = int((ts_stop - ts_start).total_seconds()) if (ts_start and ts_stop) else None

    pauses = Counter()
    for x in hb_items:
        if x.get("event") == "pause":
            reason = x.get("reason", "unknown")
            pauses[reason] += 1

    return {"start": ts_start, "stop": ts_stop, "runtime_sec": runtime_sec, "pauses": pauses}

def analyze_runlog(repo: Path):
    """何をする関数か：run.logからdry-runのskip件数や戦略エラー件数を集計する"""
    log_path = repo / "logs" / "runtime" / "run.log"
    skip_pat = re.compile(r"live\[dry_run\]: skip place")
    strat_err_pat = re.compile(r"strategy error:")
    eval_fail_pat = re.compile(r"strategy evaluate failed:")

    skip = strat_err = eval_fail = 0
    for ln in _read_lines(log_path):
        if skip_pat.search(ln):
            skip += 1
        if strat_err_pat.search(ln):
            strat_err += 1
        if eval_fail_pat.search(ln):
            eval_fail += 1
    return {"skip_place": skip, "strategy_error": strat_err, "evaluate_failed": eval_fail}

def main():
    """何をする関数か：heartbeatとrun.logを読み、要点をテキストで標準出力へ出す"""
    repo = _repo_root()

    hb_items = load_heartbeat(repo)
    hb_sum = summarize_heartbeat(hb_items)
    rl_sum = analyze_runlog(repo)

    print("# observation summary")  # 何をするか：章タイトル（表ではなくテキスト）
    if hb_sum["start"] and hb_sum["stop"]:
        print(f"- heartbeat: start={hb_sum['start'].isoformat()} stop={hb_sum['stop'].isoformat()} runtime_sec={hb_sum['runtime_sec']}")
    else:
        print("- heartbeat: start/stop 未検出（まだ起動中か、ログが不足しています）")

    # 何をするか：pause理由の件数を多い順に上位だけ自然文で出す（表は使わない）
    if hb_sum["pauses"]:
        print("- pause reasons (desc):")
        for reason, cnt in hb_sum["pauses"].most_common(10):
            print(f"  * {reason}: {cnt}")
    else:
        print("- pause reasons: なし")

    # 何をするか：dry-runのskip件数や戦略エラー件数
    print(f"- dry-run skip place: {rl_sum['skip_place']}")
    if rl_sum["strategy_error"] or rl_sum["evaluate_failed"]:
        print(f"- strategy errors: error={rl_sum['strategy_error']} evaluate_failed={rl_sum['evaluate_failed']}")
    else:
        print("- strategy errors: 0")

if __name__ == "__main__":
    main()
