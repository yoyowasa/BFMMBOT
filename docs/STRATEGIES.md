# STRATEGIES（戦略一覧と設定キー）

## 共通（全戦略）
- 戦略は `StrategyBase.evaluate(ob, now, cfg)` が actions を返し、engine/live が `place`/`cancel_tag` を実行します。
- 設定は `features.*`（共通しきい値）と `strategy_cfg.<strategy>.*`（戦略ごとの上書き）で調整します。
- 戦略の識別は主に **order tag**（ordersログの reason/tag）で行います。

## stall_then_strike（#1：静止→一撃）
- 目的：Best が静止している局面で、短TTLの指値を置いて刺さりを狙い、条件外なら即撤退。
- 主なタグ：
  - `stall` / `stall|stall_then_strike`
- 主要キー（例）：
  - `features.stall_T_ms`（静止トリガ）
  - `features.stall_ttl_ms`（短TTL）
  - `features.stall_min_mp_edge_bp` / `features.stall_min_spread_tick`
  - `strategy_cfg.stall_then_strike.take_profit_ticks` / `stop_ticks`
  - `strategy_cfg.stall_then_strike.ttl_bands`（帯域TTL）
- 特徴量（decision_log）：
  - `queue_eta_ms` を本命として保持し、互換の `eta_ms` にも同値を入れます。

## cancel_add_gate（#2：キャンセル比ゲート）
- 目的：C/A比（Cancel/Add）が落ち着いている時だけ出し、悪化したら即キャンセルして撤退。
- 主なタグ：
  - `ca_gate`
- 主要キー（例）：
  - `features.ca_ratio_win_ms`（観測窓）
  - `features.ca_threshold`（静か判定）
  - `features.ca_ratio_cap`（異常値クリップ）
  - `strategy_cfg.cancel_add_gate.taker_zscore_threshold`（毒性退避の閾値）

## age_microprice（#3：age×microprice）
- 目的：Best の age と microprice の偏りで片面を試す実験枠（A/B用）。
- 主なタグ：
  - `age_mp`
- 主要キー（例）：
  - `features.age_ms`
  - `features.mp_offset_tick`

## zero_reopen_pop（スプレッド0→再拡大の1拍）
- 目的：スプレッドが一瞬0になった直後の再拡大だけを狙うイベント型の戦略。
- 主なタグ：
  - `zero_reopen` / `zero_reopen_take` / `zero_reopen_flat`
- 詳細仕様：`docs/CODEX_ZERO_REOPEN_POP.md`
