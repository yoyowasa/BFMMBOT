from __future__ import annotations

import argparse
import json
import math
from bisect import bisect_right  # 直前の意思決定(ts)を2秒以内で探すのに使う
from math import isfinite  # 実数判定（NaN/infを除外するために使う）
from collections import Counter
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


def load_ndjson(path: Path | None):
    """[関数] NDJSONを安全に読み込む（壊れ行はスキップして落ちないように）。
    何をする？ → 1行=1イベントのJSONをPythonの辞書にして並べます。"""
    rows = []
    if not path or not path.exists():
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


def _find_latest(base: Path, pattern: str):
    """[関数] ディレクトリ内でパターンにマッチするNDJSONの“更新日時が最新”を返す。
    何をする？ → 1022など日付接頭辞に依らず、自動で最新ファイルを選びます。"""
    cands = list(base.glob(pattern))
    if not cands:
        return None
    try:
        cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    except Exception:
        cands.sort()
    return cands[0]


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


# ---------- オーダー品質（place→partial→fill/cancel を束ねて評価） ----------

def _parse_corr_tag_reason(reason: str | None):
    """[関数] reason文字列から corr / tag / head(先頭トークン) を抽出する。
    何をする？ → ライフ連結の鍵（corr）と集計軸（tag）、取消理由の大枠(head)を得ます。"""
    corr = None
    tag = None
    head = None
    if not reason:
        return corr, tag, head
    txt = str(reason)
    parts = [x.strip() for x in txt.split(";") if x.strip()]
    head = parts[0] if parts else None
    for p in parts:
        if p.startswith("corr="):
            corr = p.split("=", 1)[1].strip() or None
        if p.startswith("tag="):
            tag = p.split("=", 1)[1].strip() or None
    # 一部のplaceでは先頭に素の tag が置かれるケースを補完
    if tag is None and parts:
        if "=" not in parts[0]:
            tag = parts[0]
    return corr, tag, head


def build_order_lifecycle(orders: list[dict]):
    """[関数] 同一corrの注文イベントを束ね、place→(partial/fill)*→(fill or cancel)の一連を作る。
    何をする？ → 1件の注文の滞在時間/成否/TTL超過/総約定サイズなどを求めます。"""
    by_corr: dict[str, dict] = {}
    for r in orders:
        act = r.get("action")
        ts = parse_ts(r.get("ts"))
        tif = r.get("tif")
        ttl_ms = r.get("ttl_ms")
        sz = r.get("sz")
        corr, tag, head = _parse_corr_tag_reason(r.get("reason"))
        if corr is None:
            # corrが無いものは束ねられないのでスキップ（集計に混ぜない）
            continue
        node = by_corr.setdefault(
            corr,
            {
                "corr": corr,
                "tag": tag or "-",
                "place_ts": None,
                "tif": None,
                "ttl_ms": None,
                "place_sz": None,
                "fills": [],  # (ts, sz)
                "cancel_ts": None,
                "cancel_head": None,  # ttl/kill/window/strategy など先頭トークン
            },
        )
        if act == "place":
            node["place_ts"] = ts
            node["tif"] = tif or node.get("tif")
            node["ttl_ms"] = int(ttl_ms) if ttl_ms is not None else node.get("ttl_ms")
            node["place_sz"] = float(sz) if sz is not None else node.get("place_sz")
            if tag and not node.get("tag"):
                node["tag"] = tag
        elif act in ("fill", "partial"):
            if ts is not None and sz is not None:
                try:
                    node["fills"].append((ts, float(sz)))
                except Exception:
                    pass
        elif act == "cancel":
            node["cancel_ts"] = ts
            node["cancel_head"] = head or node.get("cancel_head")
    # 事後計算
    lifecycles = []
    for corr, node in by_corr.items():
        place_ts = node.get("place_ts")
        end_ts = None
        end_kind = None  # "fill" or "cancel" or None
        # 最終イベントの時刻
        last_fill_ts = max((t for (t, _) in node.get("fills") or []), default=None)
        if node.get("cancel_ts") and (last_fill_ts is None or node["cancel_ts"] > last_fill_ts):
            end_ts = node["cancel_ts"]
            end_kind = "cancel"
        elif last_fill_ts is not None:
            end_ts = last_fill_ts
            end_kind = "fill"
        # 充足（1回でもfill/partialがあればTrue）
        any_fill = bool(node.get("fills"))
        # 総約定サイズ
        total_filled = sum(sz for (_, sz) in node.get("fills") or [])
        place_sz = node.get("place_sz") or 0.0
        # FOK成功=総約定サイズが発注サイズ以上、IOC成功=総約定サイズ>0、GTC成功=any_fill
        tif = (node.get("tif") or "").upper()
        fok_ok = total_filled >= place_sz - 1e-12 if place_sz else any_fill
        ioc_ok = total_filled > 0.0
        gtc_ok = any_fill
        success = (gtc_ok if tif == "GTC" else (ioc_ok if tif == "IOC" else (fok_ok if tif == "FOK" else any_fill)))
        # TTL超過（cancelで、place→end が ttl_ms を越えた）
        ttl_ms = node.get("ttl_ms")
        dwell_ms = None
        ttl_over = False
        if place_ts and end_ts:
            dwell_ms = int((end_ts - place_ts).total_seconds() * 1000.0)
            if end_kind == "cancel" and ttl_ms is not None and dwell_ms > int(ttl_ms):
                ttl_over = True
        lifecycles.append(
            {
                "corr": corr,
                "tag": node.get("tag") or "-",
                "tif": tif or "-",
                "place_ts": place_ts,
                "end_ts": end_ts,
                "end_kind": end_kind,
                "dwell_ms": dwell_ms,
                "ttl_ms": ttl_ms,
                "ttl_over": ttl_over,
                "any_fill": any_fill,
                "total_filled": total_filled,
                "place_sz": place_sz,
                "success": success,
                "cancel_head": node.get("cancel_head"),
            }
        )
    return lifecycles


def summarize_order_quality(lifecycles: list[dict]):
    """[関数] 注文ライフ(=by corr)を戦略/タグごとに要約する。
    何をする？ → fill率、TTL超過率、滞在時間(P50/P95)、TIF別成功率、取消理由top3 を短文で出します。"""
    if not lifecycles:
        return ["注文イベントに corr が無く、品質集計を実施できませんでした"]

    by_tag: dict[str, list[dict]] = {}
    for row in lifecycles:
        by_tag.setdefault(row.get("tag") or "-", []).append(row)

    lines = []
    for tag, rows in sorted(by_tag.items(), key=lambda kv: len(kv[1]), reverse=True):
        n = len(rows)
        if n == 0:
            continue
        # fill率 = any_fill / place
        any_fill_ct = sum(1 for r in rows if r.get("any_fill"))
        fill_rate = any_fill_ct / n * 100.0
        # TTL超過
        ttl_over_ct = sum(1 for r in rows if r.get("ttl_over"))
        ttl_over_rate = ttl_over_ct / n * 100.0
        # 滞在時間（ms → s）
        dwell_vals = [r.get("dwell_ms") for r in rows if r.get("dwell_ms") is not None]
        dwell_vals.sort()
        p50 = (dwell_vals[int((len(dwell_vals) - 1) * 0.50)] / 1000.0) if dwell_vals else None
        p95 = (dwell_vals[int((len(dwell_vals) - 1) * 0.95)] / 1000.0) if dwell_vals else None
        # TIF別成功
        by_tif: dict[str, list[bool]] = {}
        for r in rows:
            by_tif.setdefault(r.get("tif") or "-", []).append(bool(r.get("success")))
        tif_bits = []
        for tif, arr in by_tif.items():
            if not arr:
                continue
            tif_bits.append(f"{tif}成功 {sum(1 for x in arr if x)/len(arr):.0%}（n={len(arr)}）")
        # 取消理由top3
        cancels = [r.get("cancel_head") for r in rows if r.get("end_kind") == "cancel" and r.get("cancel_head")]
        top3 = []
        if cancels:
            c_ct = Counter(cancels)
            top3 = [f"{k}({c_ct[k]/len(cancels):.0%})" for k, _ in c_ct.most_common(3)]
        # 1行
        parts = [
            f"tag={tag}: fill率 {fill_rate:.0f}%, TTL超過 {ttl_over_rate:.0f}%",
            f"滞在P50 {p50:.2f}s" if p50 is not None else "滞在P50 -",
            f"滞在P95 {p95:.2f}s" if p95 is not None else "滞在P95 -",
        ]
        if tif_bits:
            parts.append(", ".join(tif_bits))
        if top3:
            parts.append("取消理由 top=" + " / ".join(top3))
        lines.append("- " + ", ".join(parts))
    return lines


def diagnose_order_gate_correlation(lifecycles: list[dict], decisions: list[dict], window_ms: int = 1500, min_bucket_n: int = 30):
    """[関数] 注文(place)の直前decisionを結び、ca_ratio/eta_msの分位で失敗率の傾向を見る。
    何をする？ → “CA比が悪いほど失敗多い？”など簡易に傾向を1〜2行で返します。"""
    # strategy(tag)ごとに decision を時刻ソート
    per = {}
    per_ts = {}
    for d in decisions:
        s = d.get("strategy")
        ts = parse_ts(d.get("ts"))
        if not s or ts is None:
            continue
        feats = {
            "ca_ratio": _safe_float(d.get("ca_ratio")),
            "eta_ms": _safe_float(d.get("eta_ms")),
        }
        per.setdefault(s, []).append((ts, feats))
    for s in list(per.keys()):
        per[s].sort(key=lambda x: x[0])
        per_ts[s] = [t for t, _ in per[s]]
        if not per_ts[s]:
            del per[s], per_ts[s]

    # placeに最も近い直前decisionを拾う
    samples = []  # {tag, fail(bool), ca_ratio, eta_ms}
    for o in lifecycles:
        tag = o.get("tag")
        if tag not in per:
            continue
        ts = o.get("place_ts")
        if ts is None:
            continue
        idx = bisect_right(per_ts[tag], ts) - 1
        if idx < 0:
            continue
        dt_ms = (ts - per_ts[tag][idx]).total_seconds() * 1000.0
        if dt_ms > window_ms:
            continue
        feats = per[tag][idx][1]
        samples.append({
            "tag": tag,
            "fail": not bool(o.get("success")),
            "ca_ratio": feats.get("ca_ratio"),
            "eta_ms": feats.get("eta_ms"),
        })

    if not samples:
        return ["ゲート照合: 対応ゼロ（order.place↔decisionが見つからず）"]

    def _report(key: str, unit: str = ""):
        vals = [s.get(key) for s in samples if s.get(key) is not None]
        if len(vals) < min_bucket_n * 2:
            return None
        edges = _quartile_edges(vals)
        if not edges:
            return None
        q1, q2, q3 = edges
        buckets = {}
        for s in samples:
            x = s.get(key)
            if x is None:
                continue
            name = _bucket_name(x, q1, q2, q3, unit)
            st = buckets.setdefault(name, {"n": 0, "fail": 0})
            st["n"] += 1
            st["fail"] += 1 if s.get("fail") else 0
        base_fail = sum(s.get("fail") for s in samples) / len(samples) * 100.0
        scored = []
        for name, st in buckets.items():
            if st["n"] < min_bucket_n:
                continue
            fr = st["fail"] / st["n"] * 100.0
            scored.append((fr - base_fail, fr, st["n"], name))
        if not scored:
            return None
        scored.sort()
        worst = scored[-1]  # 失敗率が高い
        best = scored[0]    # 失敗率が低い
        return f"  ・{key}: {worst[3]}で失敗↑(+{worst[0]:.1f}pt), {best[3]}で失敗↓({best[0]:+.1f}pt)"

    lines = ["- ゲート照合（decision直前→order.place）"]
    r1 = _report("ca_ratio")
    if r1:
        lines.append(r1)
    r2 = _report("eta_ms", "ms")
    if r2:
        lines.append(r2)
    if len(lines) == 1:
        lines.append("  ・十分なサンプルがなく判定不可")
    return lines

# ---------- セッション検出と絞り込み ----------

def detect_session_window(in_dir: Path):
    """[関数] セッション開始・終了の自動検出。
    何をする？ → in_dir内の *paper_start*.ndjson / *heartbeat*.ndjson の ts を読み、
    start/end を推定して (start_ts, end_ts) を返します。無いときは None にして後段でフォールバック。"""
    def _safe_first_ts(path: Path):
        rows = load_ndjson(path)
        if not rows:
            return None
        return parse_ts(rows[0].get("ts")) or parse_ts(rows[0].get("timestamp"))

    def _safe_last_ts(path: Path):
        rows = load_ndjson(path)
        if not rows:
            return None
        ts_list = []
        for r in rows:
            t = parse_ts(r.get("ts")) or parse_ts(r.get("timestamp"))
            if t is not None:
                ts_list.append(t)
        return max(ts_list) if ts_list else None

    start_ts = None
    end_ts = None
    # paper_start: 最初の1件の ts を開始候補に
    for p in sorted(in_dir.glob("*paper_start*.ndjson")):
        t = _safe_first_ts(p)
        if t is not None:
            start_ts = t if start_ts is None else min(start_ts, t)
    # heartbeat: 最後の ts を終了候補に
    for p in sorted(in_dir.glob("*heartbeat*.ndjson")):
        t = _safe_last_ts(p)
        if t is not None:
            end_ts = t if end_ts is None else max(end_ts, t)
    return start_ts, end_ts


def filter_by_window(rows: list[dict], start_ts, end_ts):
    """[関数] ts が start_ts〜end_ts に入る行だけ残す。
    何をする？ → セッション外のデータを落として、集計の“混入”を防ぎます。"""
    if not rows:
        return rows
    out = []
    for r in rows:
        t = parse_ts(r.get("ts")) or parse_ts(r.get("timestamp"))
        if t is None:
            continue
        if (start_ts is not None and t < start_ts):
            continue
        if (end_ts is not None and t > end_ts):
            continue
        out.append(r)
    return out


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
            px = _safe_float(r.get("px")) or _safe_float(r.get("price"))  # 実現bp算出の分母（名目）
            sz = _safe_float(r.get("sz")) or _safe_float(r.get("size"))   # 実現bp算出の分母（名目）
            pairs.append({
                "strategy": s,
                "pnl": pnl,
                "px": px,   # 実現bp用
                "sz": sz,   # 実現bp用
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


# ---------- 期待エッジの校正（expected vs realized） ----------

def _realized_edge_bp(pnl, px, sz):
    """[関数] 実現エッジ[bp]を計算する。
    何をする？ → pnl を名目(|px|×|sz|)で割って bp換算し、データ不足は None を返す。"""
    try:
        if pnl is None or px is None or sz is None:
            return None
        denom = abs(float(px)) * abs(float(sz))
        if denom <= 0:
            return None
        return float(pnl) / denom * 10_000.0
    except Exception:
        return None


def _quantile_edges(vals: list[float], k: int):
    """[関数] k分位の境界値（k-1個）を返す。
    何をする？ → デシル(k=10)や四分位(k=4)の境目を作る。"""
    v = sorted([x for x in vals if x is not None and isfinite(x)])
    n = len(v)
    if n == 0:
        return None
    edges = []
    for j in range(1, k):
        edges.append(v[int((n - 1) * (j / k))])
    return edges


def calibrate_expected_edge(pairs: list[dict], min_per_bin: int = 15):
    """[関数] expected_edge_bp の分位ビンごとに、実現bpの平均とバイアス(実現−期待)を要約する。
    何をする？ → 期待が“盛りすぎ/控えめ”かを短文で返す。データが多ければデシル、少なければ四分位で実施。"""
    rows = []
    for r in pairs:
        ee = r["feats"].get("expected_edge_bp") if r.get("feats") else None
        rbp = _realized_edge_bp(r.get("pnl"), r.get("px"), r.get("sz"))
        if ee is None or rbp is None:
            continue
        rows.append((float(ee), float(rbp), r.get("strategy", "unknown")))
    if len(rows) < max(8, min_per_bin * 4):
        return [
            "校正対象が不足（decision↔trade対応のうち expected_edge_bp/実現bp が揃った件が少ない）"
        ]

    K = 10 if len(rows) >= 100 else 4
    edges = _quantile_edges([ee for ee, _, _ in rows], K)
    if not edges:
        return ["分位境界を作れませんでした"]

    def _bin_name(x, edges):
        idx = 0
        while idx < len(edges) and x > edges[idx]:
            idx += 1
        return ("D" if K == 10 else "Q") + str(idx + 1)

    bucket = {}
    by_strat = {}
    for ee, rbp, strat in rows:
        name = _bin_name(ee, edges)
        st = bucket.setdefault(name, {"n": 0, "ee": 0.0, "rbp": 0.0})
        st["n"] += 1
        st["ee"] += ee
        st["rbp"] += rbp
        by_strat.setdefault(strat, 0)
        by_strat[strat] += 1

    names_sorted = sorted(bucket.keys(), key=lambda s: int(s[1:]))
    lines = []
    lines.append(f"- 分位: {('デシル(10分割)' if K==10 else '四分位(4分割)')}・最小{min_per_bin}件/ビンで集計")
    for name in names_sorted:
        st = bucket[name]
        if st["n"] < min_per_bin:
            continue
        ee_avg = st["ee"] / st["n"]
        rbp_avg = st["rbp"] / st["n"]
        bias = rbp_avg - ee_avg
        verdict = "≒一致" if abs(bias) < 0.5 else ("期待>実現（楽観）" if bias < 0 else "期待<実現（控えめ）")
        lines.append(
            f"  ・{name}: 期待{ee_avg:.2f}bp → 実現{rbp_avg:.2f}bp（差 {bias:+.2f}bp）＝{verdict} [n={st['n']}]"
        )
    if len(lines) == 1:
        return ["有効なビンがありません（閾値 min_per_bin を下げると出ます）"]
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
    p.add_argument("--save", action="store_true", help="logs/analytics/SESSION_ID/ にテキストを保存する")
    p.add_argument("--session-auto", action="store_true", default=True, help="paper_start/heartbeat でセッション範囲を自動検出する")
    args = p.parse_args()

    cfg = load_yaml(args.config) if args.config else {}
    analytics_cfg = (cfg.get("analytics") or {}) if isinstance(cfg, dict) else {}
    window_ms = int(analytics_cfg.get("decision_to_trade_window_ms", 1500))
    min_bucket_n = int(analytics_cfg.get("min_bucket_n", 20))

    base = args.dir
    # 日付接頭辞に依存せず、最新のファイルを自動検出
    f_heartbeat = _find_latest(base, "*heartbeat*.ndjson")
    f_paper_start = _find_latest(base, "*paper_start*.ndjson")
    f_order_log = _find_latest(base, "*order_log*.ndjson")
    f_trade_log = _find_latest(base, "*trade_log*.ndjson")
    f_decision = _find_latest(base, "*decision_log*.ndjson")

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

    # セッション範囲（start/end）を自動検出して、trade/decision をその範囲に絞る
    s_ts = e_ts = None
    if args.session_auto:
        s_ts, e_ts = detect_session_window(args.dir)
        if s_ts is None or e_ts is None:
            # フォールバック：trade_log の最小・最大 ts を使う（片方だけ欠けても可）
            t_times = [parse_ts(r.get("ts")) for r in tl if parse_ts(r.get("ts")) is not None]
            if s_ts is None and t_times:
                s_ts = min(t_times)
            if e_ts is None and t_times:
                e_ts = max(t_times)
        tl = filter_by_window(tl, s_ts, e_ts)
        dl = filter_by_window(dl, s_ts, e_ts)
        ol = filter_by_window(ol, s_ts, e_ts)  # order_log も同じ範囲で絞る（集計の混入防止）

        print("\n【セッション範囲（自動検出）】")
        print(f"- start: {fmt(s_ts) if s_ts else '-'}")
        print(f"- end  : {fmt(e_ts) if e_ts else '-'}")
        print(f"- orders: {len(ol)} / trades: {len(tl)} / decisions: {len(dl)} （絞り込み後の件数）")

    # KPI（trade/decision はセッション範囲で集計）
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

    # 期待エッジの校正（expected vs realized）：データが多ければデシル、少なければ四分位
    cal_lines = calibrate_expected_edge(pairs, min_per_bin=max(15, min_bucket_n))  # 運用に合わせて最小件数をYAML値で連動
    print("\n【エッジ校正（expected_edge_bp ↔ 実現bp）】")
    for ln in cal_lines:
        print(ln)

    # オーダー品質（corr単位でライフを連結し、戦略/タグごとに要約）
    lifecycles = build_order_lifecycle(ol)
    oq_lines = summarize_order_quality(lifecycles)
    gate_lines = diagnose_order_gate_correlation(lifecycles, dl, window_ms=window_ms, min_bucket_n=max(30, min_bucket_n))

    print("\n【オーダー品質（place→fill/cancel）】")
    for ln in oq_lines:
        print(ln)
    for ln in gate_lines:
        print(ln)

    # 保存（任意）
    if args.save:
        def _sid(a, b):
            def _safe_str(x):
                return x.strftime("%Y%m%d_%H%M%S") if x else "unknown"
            return f"{_safe_str(a)}-{_safe_str(b)}"

        sess_id = _sid(s_ts, e_ts)
        out_dir = Path("logs") / "analytics" / sess_id
        out_dir.mkdir(parents=True, exist_ok=True)
        # 1) session_summary
        summary = []
        summary.append("【トレードKPI】")
        summary.append(f"- 件数 {tk['n']} / 勝率 {tk['win_rate']:.1f}% / Sharpe擬似 {tk['pseudo_sharpe']:.3f}")
        summary.append(f"- 正味PnL {tk['net_pnl']:.6f} / DD {tk['mdd']:.6f}")
        # 在庫サマリ（可能なら）
        inv_vals = [float(r.get("inventory_after")) for r in tl if r.get("inventory_after") is not None]
        if inv_vals:
            summary.append(f"- 在庫: min {min(inv_vals):.6f} / max {max(inv_vals):.6f} / end {inv_vals[-1]:.6f}")
        # エッジ校正の要点（先頭2〜3行）
        summary.append("【エッジ校正 要点】")
        for ln in cal_lines[:3]:
            summary.append(ln)
        (out_dir / "session_summary.txt").write_text("\n".join(summary), encoding="utf-8")

        # 2) order_quality
        oq = ["【オーダー品質】"] + oq_lines + gate_lines
        (out_dir / "order_quality.txt").write_text("\n".join(oq), encoding="utf-8")

        # 3) edge_calibration
        ec = ["【エッジ校正（expected vs realized）】"] + cal_lines
        (out_dir / "edge_calibration.txt").write_text("\n".join(ec), encoding="utf-8")
        print(f"\n【保存】logs/analytics/{sess_id}/ にサマリを書き出しました")

    print(
        "\n【メモ】この集計は“在庫はFillで数える／HTTPは答え合わせ”の原則と、ログ仕様・BT要件に準拠しています。"
    )
    print("      参考：ログ列の定義・KPI/BT要件・戦略ON/OFF方針。")


if __name__ == "__main__":
    main()
