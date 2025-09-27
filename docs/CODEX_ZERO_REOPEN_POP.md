# CODEX: Zero→Reopen Pop（スプレッド一瞬0→再拡大の1拍だけ取る）

## 1. 要約（小学生にもわかる説明）
- **ドア（スプレッド）が一瞬ピタッと閉じる＝0**になったあと、**すぐ“少しだけ開いた”瞬間**だけ1回だけ手を出して、**当たったら1段（+1tick）で逃げる**作戦です。
- ふだんは**何もしない**で待ち、**合図が出た“数百msだけ”**片面に**最小ロット**を置いて**秒速で利確IOC**します（常時両面を出し続ける“普通のMM”ではありません）。

---

## 2. 背景と前提
- **データ源**：bitFlyer Lightning のリアルタイム **board / executions**（WS）。  
  チャネル：`lightning_board_*` / `lightning_executions_*`（FX_BTC_JPY を想定）。  
  指値は GTC / IOC / FOK 利用可（TTLはローカル管理で実装）。  
- **ねらい**：**「スプレッド=0 → 直後に≥1tickへ再拡大」**という**離散イベント**だけを合図に、**毒性の低い瞬間**を薄く拾う。  
- **運用**：標準のガードレール（在庫・PnL・DD・mid急変・メンテ/ファンディング窓）に**完全準拠**。  

---

## 3. ロジック仕様（詳細）

### 3.1 シグナル（合図）
1) **ゼロ検知**：`spread_tick == 0` を検出した**時刻 t0 を記録**する。  
2) **再拡大確認**：t0 から **1秒以内**に `spread_tick >= 1` へ戻ったら **候補オン**。  
3) **安全ゲート**（いずれも満たすときだけ発注）  
   - **health_ok()**：既存の鮮度・在庫・スプレッド上限・Kill等のヘルスチェックがOK。  
   - **cooloff_ms 経過**：直近アクションからの冷却を満たす（連打・毒性回避）。  
   - **TTL運用**：置いた指値は短寿命（700–900ms）で自然に剥がす前提。

### 3.2 エントリー（どちら側に置く？）
- 直感：**再拡大の“外側”で、逆向きに1tick待つ。**  
- 実装簡便案：  
  `place_buy = (best_ask - mid) >= (mid - best_bid)`  
  - `place_buy == True` → **BUY** を **mid−1tick** に最小ロットでGTC+TTL。  
  - `False` → **SELL** を **mid+1tick** に最小ロットでGTC+TTL。  
- **タグ**：`tag="zero_reopen"`（後続のログ/AB判定で識別可能に）。

### 3.3 利確（秒速で逃げる）
- **約定 fill を受けたら即時**、反対側 **±1tick** に **IOC** を1発（`tag="zero_reopen_take"`）。  
- IOCが不成立でも **TTLとガード**で自然に剥がす／撤退条件で即取消。

### 3.4 撤退（やめる合図）
- **再クロス（再び spread=0）**／**Best更新**／**TTL超過**／**health_ok() 失効** → **即撤退**。  
- 在庫上限やKillスイッチなど**上位リスク制御**が優先。

---

## 4. パラメータ（初期値の目安）
- `min_spread_tick = 1`（再拡大の下限）  
- `ttl_ms = 700–900`（素早い自然キャンセル）  
- `size_min = 取引所最小ロット`（まずは極小）  
- `cooloff_ms = 250`（連打禁止・毒性回避）  
（YAMLの `features.zero_reopen_pop.*` へ**外出し推奨**）

---

## 5. ワークフロー（イベント→意思決定→発注→利確→撤退）

1) **Board受信**：差分をローカル板へ適用。`spread_tick`/`best_bid/ask`/`mid` を更新。  
2) **ゼロ検知**：`spread_tick==0` で `last_spread_zero_ms ← now`。  
3) **再拡大チェック**：`now - last_spread_zero_ms ≤ 1000ms` かつ `spread_tick ≥ 1` で候補オン。  
4) **安全ゲート**：`health_ok()==True` かつ `now - last_action_ms ≥ cooloff_ms` を確認。  
5) **サイド決定**：上記の簡便判定で **BUY@mid−1tick** または **SELL@mid+1tick** を選ぶ。  
6) **発注**：**最小ロット**・**GTC+TTL**・`tag="zero_reopen"` で片面1枚だけ置く。  
7) **ログ**：意思決定時に `spread_state / ttl_ms / cooloff_ms / side / px / expected_edge` を記録。  
8) **利確**：**fill** を受けたら**即IOC**で ±1tick に出して**秒速撤退**。  
9) **撤退**：Best更新／再クロス／TTL超過／ガード失効 → **取消**。  
10) **メトリクス**：`win_rate(+5〜10s後)/adverse_move_P95/fill_ratio` を日次で集計。  

---

## 6. 関数と責務（1行で“何をするか”を固定）

- **【関数】spread_state.update**  
  役割：`spread_tick==0` を検知した時刻を記録し、**“ゼロ直後”**かどうかを判定できる状態を保つ。

- **【関数】signal.zero_reopen_ready**  
  役割：**ゼロ直後 & 再拡大≥1tick & safety全OK & クールオフ済み**を満たすかを真偽で返す。

- **【関数】decision.pick_side_and_price**  
  役割：再拡大の“外側”で**逆向き1tick**の指値価格とサイド（BUY/SELL）を決める。

- **【関数】orders.place_zero_reopen**  
  役割：**最小ロット**で **GTC+TTL** の片面指値を一発だけ出す（`tag="zero_reopen"`）。

- **【関数】fills.on_zero_reopen_fill**  
  役割：fill受信で**即IOC**の±1tick利確を返す（`tag="zero_reopen_take"`）。

- **【関数】guards.health_ok**  
  役割：鮮度/在庫/スプ上限/モード（Healthy/Caution/Halted）等の**標準ガード**を一括判定。

- **【関数】logging.decision_log**  
  役割：合図・価格・TTL・cooloff・side・px・想定エッジ等を1行で**意思決定ログ**へ書く。

---

## 7. ログと運用（Codexに載せる約束ごと）

- **注文ログ**：`ts, action(place/cancel/fill), tif, ttl_ms, px, sz, reason`（タグで戦略識別）。  
- **トレードログ**：`ts, side, px, sz, fee, pnl, strategy, tag, inventory_after`。  
- **意思決定ログ**：`ts, strategy, features_json(spread_state等), decision, expected_edge_bp`。  
- **運用ガード**：mid30s変化率、在庫上限、日次DD、メンテ/ファンディング窓で**自動停止/縮小**。  
- **AB導入**：この戦略は“合図が少ない”ため**A/B比較が容易**。ヒット率・逆行P95で効果検証する。

---

## 8. 失敗パターン（やってはいけない振る舞い）
- TTLを長くして**置きっぱなし**にする（→普通のMM化）。  
- 合図が無いのに**常時両面**を置く。  
- ガード/モード（Caution/Halted）を**無視**して出し続ける。  
→ いずれも**毒性上昇**で期待値が崩れます。“**合図が重なる瞬間だけ**薄く出す”が正解。

---

## 9. バックテスト（録画リプレイの最短レシピ）
1) WSテープ（NDJSON）を再生し、**ゼロ→再拡大**のヒット時刻を列挙。  
2) 同じ箇所で TTL・ガード・利確IOC を適用し、**勝率(+5〜10s)**・**逆行P95**・**手数料後PnL**を集計。  
3) **A/B**：ガード閾値（cooloff/ttl/min_spread_tick）を“数字だけ”調整し、毒性を最小化する。

---

## 10. 起動・停止の型（運用者ポケットガイド）
- **起動**：復元→初回整合→WS購読→**ウォームアップ（連続OKでHealthy）**の順。  
- **停止**：新規停止（Halted）→キュー吐き切り→最終整合→スナップ保存→WSクローズ。  
- **異常時**：**Halted（決済のみ許可）→整合チェック→HTTPリプレイ→復帰**を最優先に。

参考・根拠（設計と運用の出典）

戦略リスト/Zero→Reopenの骨子・WSチャンネル/TIF可用性（bitFlyer MMボット戦略の設計メモ）。

CFD向けフル構成/ログ仕様/CLI・設定・ロジック仕様（#7 Zero→Reopenを含む）/運用ワークフロー。

在庫“自炊”ワークフロー：WS→キュー→適用→整合→永続化→アラートの基本線（引継ぎ用の手順集）。

ロールアウト/多段導入・障害対応・安全装置（Caution/Halted）の運用規約。

バックテスト/リプレイ検証・設定外出し・EOD・SLO/SLAの物差し。
