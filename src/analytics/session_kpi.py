from __future__ import annotations

import argparse
import json
import math
from bisect import bisect_right  # 直前の意思決定(ts)を2秒以内で探すのに使う
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
import yaml  # 設定YAMLを読むために使う（analyticsの窓/件数を外部から受け取る）

# ---------- ユーティリティ ----------

def parse_ts(v):
    """[関数] 文字列/数値の時刻をUTCのdatetimeへそろえる（壊れ値はNone）。
    何をする？ → ログの時刻を比較・集計できる形に整えます。"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if v > 1e12:  # ms判定
            v = v / 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
        except Exception:
            return None
    return None


def load_ndjson(path: Path):
    """[関数] NDJSONを安全に読み込む（壊れ行はスキップして落ちないように）。
    何をする？ → 1行=1イベントのJSONをPythonの辞書にして並べます。"""
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                # 壊れ行は読み飛ばし（集計は続行）
                continue
    return rows


def ts_range(rows, key="ts"):
    """[関数] tsの最小・最大・件数を返す。
    何をする？ → ログが「いつからいつまで」あるかを確認します。"""
    ts_list = [parse_ts(r.get(key)) for r in rows if r.get(key) is not None]
    ts_list = [t for t in ts_list if t is not None]
    if not ts_list:
        return None, None, 0
    return min(ts_list), max(ts_list), len(ts_list)


def load_yaml(path: Path) -> dict:
    """[関数] 設定YAMLを安全に読み込む。
    何をする？ → analytics.decision_to_trade_window_ms / analytics.min_bucket_n を外部から受け取れるようにします。"""
    if not path:
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


# ---------- 品質診断ユーティリティ（意思決定→約定の因果を“短距離”で見る） ----------

def _safe_float(x):
    """[関数] 数値に安全変換（None/空はNoneのまま）。
    何をする？ → decision_logの特徴量を欠損ごと受け入れます。"""
    try:
        return float(x)
    except Exception:
        return None


def associate_trades_with_decisions(trades: list[dict], decisions: list[dict], window_ms: int = 1500):
    """[関数] 各約定に“直前の意思決定(同一strategy)”を1件だけ結びつける。
    何をする？ → trade.ts の ≤1.5秒前にあった decision.ts を見つけ、特徴量を約定へ付与します。"""
    # strategyごとに decision を時刻ソートして索引を作る
    per = {}  # strategy -> [(ts, feats_dict), ...]（時刻昇順）
    per_ts = {}  # strategy -> [ts, ts, ...] だけを抜き出した配列（bisect用）

    for d in decisions:
        s = d.get("strategy")
        ts = parse_ts(d.get("ts"))
        if not s or ts is None:
            continue
        feats = {
            "ca_ratio": _safe_float(d.get("ca_ratio")),
            "best_age_ms": _safe_float(d.get("best_age_ms")),
            "eta_ms": _safe_float(d.get("eta_ms")),
            "expected_edge_bp": _safe_float(d.get("expected_edge_bp")),
        }
        per.setdefault(s, []).append((ts, feats))

    for s in list(per.keys()):
        per[s].sort(key=lambda x: x[0])
        per_ts[s] = [t for t, _ in per[s]]
        if not per_ts[s]:
            del per[s], per_ts[s]

    pairs = []  # {strategy, pnl, win, feats{...}} の列
    for r in trades:
        s = r.get("strategy") or r.get("tag") or "unknown"
        if s not in per:
            continue
        ts = parse_ts(r.get("ts"))
        if ts is None:
            continue
        pnl = float(r.get("pnl", 0.0))
        idx = bisect_right(per_ts[s], ts) - 1
        if idx < 0:
            continue
        dt_ms = (ts - per_ts[s][idx]).total_seconds() * 1000.0
        if dt_ms <= window_ms:
            pairs.append({
                "strategy": s,
                "pnl": pnl,
                "win": pnl > 0,
                "feats": per[s][idx][1],
            })
    return pairs


def _quartile_edges(vals: list[float]):
    """[関数] 四分位境界（Q1, Q2, Q3）を返す（単純な順位法）。
    何をする？ → “低←→高”を4つの箱に分ける境目を作ります。"""
    v = sorted([x for x in vals if x is not None])
    n = len(v)
    if n == 0:
        return None
    if n == 1:
        return (v[0], v[0], v[0])
    # 0..n-1 の位置に対して 25/50/75% の地点を丸めて取得
    q1 = v[int((n - 1) * 0.25)]
    q2 = v[int((n - 1) * 0.50)]
    q3 = v[int((n - 1) * 0.75)]
    return (q1, q2, q3)


def _bucket_name(x, q1, q2, q3, unit=""):
    """[関数] 値xがどの四分位レンジかを人に伝わる短い名前に変換。
    何をする？ → “低側/中低/中高/高側”の4レンジ名を返します。"""
    if x is None:
        return "欠損"
    if x <= q1:
        return f"低側(≤{q1:.3f}{unit})"
    if x <= q2:
        return f"中低({q1:.3f}–{q2:.3f}{unit})"
    if x <= q3:
        return f"中高({q2:.3f}–{q3:.3f}{unit})"
    return f"高側(>{q3:.3f}{unit})"


def diagnose_quality(pairs: list[dict], min_bucket_n: int = 20):
    """[関数] “特徴量の四分位レンジ”ごとの勝率・PnLの差分を戦略別に要約。
    何をする？ → best_age_ms / ca_ratio / eta_ms / expected_edge_bp を対象に、
    四分位ごとの勝率が“ベースライン”より良い/悪いを短くレポート化します。"""
    if not pairs:
        return ["診断対象がありません（decision↔tradeの対応ゼロ）"]

    feats_list = [
        ("best_age_ms", "ms"),
        ("ca_ratio", ""),
        ("eta_ms", "ms"),
        ("expected_edge_bp", "bp"),
    ]
    lines = []
    by_strat = {}
    for row in pairs:
        by_strat.setdefault(row["strategy"], []).append(row)

    for s, rows in by_strat.items():
        base_wr = (sum(1 for r in rows if r["win"]) / len(rows)) * 100.0 if rows else 0.0
        lines.append(f"- {s}：ベースライン勝率 {base_wr:.1f}%（対応 {len(rows)} 件）")
        for key, unit in feats_list:
            vals = [r["feats"].get(key) for r in rows if r["feats"].get(key) is not None]
            if len(vals) < max(8, min_bucket_n):
                continue
            q = _quartile_edges(vals)
            if q is None:
                continue
            q1, q2, q3 = q
            buckets = {}
            for r in rows:
                x = r["feats"].get(key)
                if x is None:
                    continue
                name = _bucket_name(x, q1, q2, q3, unit)
                b = buckets.setdefault(name, {"n": 0, "win": 0, "pnl": 0.0})
                b["n"] += 1
                b["win"] += 1 if r["win"] else 0
                b["pnl"] += r["pnl"]
            scored = []
            for name, stat in buckets.items():
                n = stat["n"]
                if n < min_bucket_n:
                    continue
                wr = (stat["win"] / n) * 100.0
                delta = wr - base_wr
                scored.append((delta, wr, n, name))
            if not scored:
                continue
            scored.sort(key=lambda x: x[0], reverse=True)
            best = scored[0]
            worst = scored[-1]
            if best[0] > 0:
                lines.append(f"  ・{key}: {best[3]} が“効き所” → 勝率 +{best[0]:.1f}pt（{best[2]}件）")
            if worst[0] < 0:
                lines.append(f"  ・{key}: {worst[3]} は“要注意” → 勝率 {worst[0]:.1f}pt（{worst[2]}件）")
    return lines


# ---------- KPI計算 ----------

def kpi_trades(trades: list[dict]):
    """[関数] トレードKPIを計算（勝率・PnL・ドローダウン・擬似Sharpeなど）。
    何をする？ → trade_logの各行を1取引として集計します。"""
    pnl_list, fee_list, equity = [], [], []
    win = loss = 0
    notional = 0.0
    by_strategy = Counter()
    by_strategy_win = Counter()
    cum = 0.0

    for r in trades:
        pnl = float(r.get("pnl", 0.0))
        fee = float(r.get("fee", 0.0))
        pnl_list.append(pnl)
        fee_list.append(fee)
        sname = r.get("strategy") or r.get("tag") or "unknown"
        by_strategy[sname] += 1
        if pnl > 0:
            win += 1
            by_strategy_win[sname] += 1
        else:
            loss += 1
        px = float(r.get("px", 0.0))
        sz = float(r.get("sz", 0.0))
        notional += abs(px * sz)
        cum += pnl
        equity.append(cum)

    n = len(pnl_list)
    total_pnl = sum(pnl_list)
    total_fee = sum(fee_list)
    net_pnl = total_pnl - total_fee
    avg_win = mean([x for x in pnl_list if x > 0]) if win else 0.0
    avg_loss = mean([x for x in pnl_list if x <= 0]) if loss else 0.0
    win_rate = (win / n * 100.0) if n else 0.0
    exp_per_trade = (net_pnl / n) if n else 0.0

    peak = float("-inf")
    mdd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        mdd = max(mdd, peak - v)

    if n > 1:
        mu = mean(pnl_list)
        var = mean([(x - mu) ** 2 for x in pnl_list])
        stdev = math.sqrt(var)
        pseudo_sharpe = (mu / stdev) if stdev > 0 else 0.0
    else:
        pseudo_sharpe = 0.0

    strat_lines = []
    for sname, cnt in by_strategy.most_common():
        wr = (by_strategy_win[sname] / cnt * 100.0) if cnt else 0.0
        strat_lines.append((sname, cnt, wr))

    return {
        "n": n,
        "win": win,
        "loss": loss,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "total_fee": total_fee,
        "net_pnl": net_pnl,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "exp_per_trade": exp_per_trade,
        "mdd": mdd,
        "pseudo_sharpe": pseudo_sharpe,
        "notional": notional,
        "strats": strat_lines,
    }


def kpi_orders(orders: list[dict]):
    """[関数] オーダーKPIを計算（place/fill/cancel/partial、充足率など）。
    何をする？ → order_logを数えて“どれだけ約定したか”を見ます。"""
    place = cancel = fill = partial = 0
    tifs = Counter()
    reasons = Counter()
    for r in orders:
        act = r.get("action")
        if act == "place":
            place += 1
        elif act == "cancel":
            cancel += 1
        elif act == "fill":
            fill += 1
        elif act == "partial":
            partial += 1
        if r.get("tif"):
            tifs[r["tif"]] += 1
        if r.get("reason"):
            reasons[r["reason"]] += 1

    fill_ratio = (fill / place * 100.0) if place else 0.0
    cancel_ratio = (cancel / place * 100.0) if place else 0.0
    return {
        "place": place,
        "fill": fill,
        "cancel": cancel,
        "partial": partial,
        "fill_ratio": fill_ratio,
        "cancel_ratio": cancel_ratio,
        "tifs": dict(tifs),
        "reasons_top": reasons.most_common(5),
    }


def kpi_decisions(decisions: list[dict]):
    """[関数] 意思決定ログの分布（decision種別・期待エッジ・ETAなど）。
    何をする？ → ゲート（CA比・Age・ETA等）が効いていたかの“手がかり”を出します。"""
    decisions_ct = Counter([r.get("decision", "") for r in decisions if r.get("decision")])
    strategies_ct = Counter([r.get("strategy", "") for r in decisions if r.get("strategy")])

    def take_numbers(key):
        vals = [float(r.get(key)) for r in decisions if r.get(key) is not None]
        if not vals:
            return None, None
        return median(vals), mean(vals)

    med_edge, avg_edge = take_numbers("expected_edge_bp")
    med_eta, avg_eta = take_numbers("eta_ms")
    med_age, avg_age = take_numbers("best_age_ms")
    med_car, avg_car = take_numbers("ca_ratio")

    return {
        "decisions_top": decisions_ct.most_common(5),
        "strategies_top": strategies_ct.most_common(5),
        "edge": (med_edge, avg_edge),
        "eta": (med_eta, avg_eta),
        "age": (med_age, avg_age),
        "car": (med_car, avg_car),
    }


# ---------- メイン（箇条書きで出力） ----------

def main():
    """[関数] セッションKPIを計算して箇条書きで表示する入口。
    何をする？ → 引数で指定したフォルダからNDJSONを読み、表なしで要点だけ出します。"""
    p = argparse.ArgumentParser()
    p.add_argument("--dir", type=Path, required=True, help="NDJSONがあるフォルダ（/mnt/data など）")
    p.add_argument("--config", type=Path, required=False, help="設定YAML（analytics.* を参照）")
    args = p.parse_args()

    cfg = load_yaml(args.config) if args.config else {}
    analytics_cfg = (cfg.get("analytics") or {}) if isinstance(cfg, dict) else {}
    window_ms = int(analytics_cfg.get("decision_to_trade_window_ms", 1500))
    min_bucket_n = int(analytics_cfg.get("min_bucket_n", 20))

    base = args.dir
    f_heartbeat = base / "1022heartbeat.ndjson"
    f_paper_start = base / "1022paper_start.ndjson"
    f_order_log = base / "1022order_log.ndjson"
    f_trade_log = base / "1022trade_log.ndjson"
    f_decision = base / "1022decision_log.ndjson"

    hb = load_ndjson(f_heartbeat)
    ps = load_ndjson(f_paper_start)
    ol = load_ndjson(f_order_log)
    tl = load_ndjson(f_trade_log)
    dl = load_ndjson(f_decision)

    # 期間
    hb_s, hb_e, hb_n = ts_range(hb)
    ol_s, ol_e, ol_n = ts_range(ol)
    tl_s, tl_e, tl_n = ts_range(tl)
    dl_s, dl_e, dl_n = ts_range(dl)
    ps_s, _, _ = ts_range(ps)

    def fmt(x):
        return x.isoformat().replace("+00:00", "Z") if x else "-"

    print("【期間（UTC）】")
    print(f"- paper_start: {fmt(ps_s)}")
    print(f"- heartbeat : {fmt(hb_s)} → {fmt(hb_e)}（{hb_n}件）")
    print(f"- order_log : {fmt(ol_s)} → {fmt(ol_e)}（{ol_n}行）")
    print(f"- trade_log : {fmt(tl_s)} → {fmt(tl_e)}（{tl_n}約定）")
    print(f"- decision  : {fmt(dl_s)} → {fmt(dl_e)}（{dl_n}行）")

    # KPI
    tk = kpi_trades(tl)
    ok = kpi_orders(ol)
    dk = kpi_decisions(dl)

    print("\n【トレードKPI】")
    print(f"- 取引回数: {tk['n']}")
    print(f"- 勝率: {tk['win_rate']:.1f}%（勝ち {tk['win']} / 負け {tk['loss']}）")
    print(
        f"- 総PnL: {tk['total_pnl']:.6f} / 総手数料: {tk['total_fee']:.6f} / 正味PnL: {tk['net_pnl']:.6f}"
    )
    print(f"- 1取引あたり期待値: {tk['exp_per_trade']:.6f}")
    print(f"- 平均勝ち: {tk['avg_win']:.6f} / 平均負け: {tk['avg_loss']:.6f}")
    print(f"- 最大ドローダウン(擬似): {tk['mdd']:.6f} / 擬似Sharpe: {tk['pseudo_sharpe']:.3f}")
    print(f"- 名目合計(参考): {tk['notional']:.6f}")

    print("\n【戦略別 内訳】")
    for sname, cnt, wr in tk["strats"]:
        print(f"- {sname}: 件数 {cnt}, 勝率 {wr:.1f}%")

    print("\n【オーダーKPI】")
    print(
        f"- place: {ok['place']}, fill: {ok['fill']}, cancel: {ok['cancel']}, partial: {ok['partial']}"
    )
    print(f"- 充足率（fill/place）: {ok['fill_ratio']:.1f}%")
    print(f"- キャンセル比（cancel/place）: {ok['cancel_ratio']:.1f}%")
    if ok["tifs"]:
        tif_text = ", ".join([f"{k}={v}" for k, v in ok["tifs"].items()])
        print(f"- TIF内訳: {tif_text}")
    if ok["reasons_top"]:
        reason_text = ", ".join([f"{k}×{v}" for k, v in ok["reasons_top"]])
        print(f"- reason上位: {reason_text}")

    print("\n【意思決定ログ 概観】")
    if dk["decisions_top"]:
        dec_text = ", ".join([f"{k}×{v}" for k, v in dk["decisions_top"]])
        print(f"- decision内訳: {dec_text}")
    if dk["strategies_top"]:
        st_text = ", ".join([f"{k}×{v}" for k, v in dk["strategies_top"]])
        print(f"- strategy内訳: {st_text}")
    if dk["edge"][0] is not None:
        print(f"- expected_edge_bp: 中央値 {dk['edge'][0]:.3f}, 平均 {dk['edge'][1]:.3f}")
    if dk["eta"][0] is not None:
        print(f"- eta_ms: 中央値 {dk['eta'][0]:.1f} ms, 平均 {dk['eta'][1]:.1f} ms")
    if dk["age"][0] is not None:
        print(f"- best_age_ms: 中央値 {dk['age'][0]:.1f} ms, 平均 {dk['age'][1]:.1f} ms")
    if dk["car"][0] is not None:
        print(f"- ca_ratio: 中央値 {dk['car'][0]:.3f}, 平均 {dk['car'][1]:.3f}")

    print("\n【設定（診断パラメタ）】")
    print(f"- window_ms: {window_ms} / min_bucket_n: {min_bucket_n}")

    # 品質診断（tradeに直前decisionを結び付け、四分位で“効き所/要注意”を見る）
    pairs = associate_trades_with_decisions(tl, dl, window_ms=window_ms)
    qlines = diagnose_quality(pairs, min_bucket_n=min_bucket_n)

    print("\n【品質診断（戦略の“効き所／要注意”）】")
    for ln in qlines:
        print(ln)

    print(
        "\n【メモ】この集計は“在庫はFillで数える／HTTPは答え合わせ”の原則と、ログ仕様・BT要件に準拠しています。"
    )
    print("      参考：ログ列の定義・KPI/BT要件・戦略ON/OFF方針。")


if __name__ == "__main__":
    main()

