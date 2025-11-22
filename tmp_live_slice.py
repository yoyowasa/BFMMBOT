def run_live(
    cfg: Any,
    strategy_name: str,
    dry_run: bool = True,
    *,
    strategies: Sequence[str] | str | None = None,
    strategy_cfg=None,
) -> None:
    """
    live（本番）を起動する関数（最小版・導線）。
    - 何をするか：API鍵の取得→ exchange adapter で疎通確認（未発注）
    - ねらい：鍵/署名/権限/ネットワークの不備を先に見つける（小ロット本番の前段）
    - 次ステップ：dry_run=False とイベントループ/TTL取消/戦略呼び出しをこのファイルに追記
    """
    # 何をするか：.env から API キーを読む（.env 運用はワークフロー文書に準拠）
    api_key = os.getenv("BF_API_KEY")
    api_secret = os.getenv("BF_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("BF_API_KEY / BF_API_SECRET が .env から読めません（.env を確認してください）")

    # 何をするか：CFD前提の既定銘柄。cfg に product_code があればそれを使う
    product_code = getattr(cfg, "product_code", "FX_BTC_JPY")
    tick = float(getattr(cfg, "tick_size", 1))  # 何をするか：価格をこの最小刻みに丸める（例：JPYなら1）


    # 何をするか：exchange adapter で「未約定一覧」を1件だけ取得し、疎通を確かめる
    strategy_list = _normalize_strategy_names(strategy_name, strategies)
    if len(strategy_list) > 1:
        primary_strategy = strategy_list[0]
        summary_name = MultiStrategy._compose_name(strategy_list)
    else:
        primary_strategy = strategy_list[0]
        summary_name = primary_strategy

    with BitflyerExchange(api_key, api_secret, product_code=product_code) as ex:
        # 先に最低限の参照を初期化しておく（ネスト関数内の未定義警告を避ける）
        hb_path: Path = Path("logs/runtime/heartbeat.ndjson")
        hb_path.parent.mkdir(parents=True, exist_ok=True)
        orders_dir = Path("logs/orders"); orders_dir.mkdir(parents=True, exist_ok=True)
        trades_dir = Path("logs/trades"); trades_dir.mkdir(parents=True, exist_ok=True)
        analytics_dir = Path("logs/analytics"); analytics_dir.mkdir(parents=True, exist_ok=True)
        order_log = OrderLog(orders_dir / "orders.parquet", mirror_ndjson=orders_dir / "order_log_0001.ndjson")
        trade_log = TradeLog(trades_dir / "trades.parquet", mirror_ndjson=trades_dir / "trade_log_0001.ndjson")
        decision_log = DecisionLog(analytics_dir / "decision.parquet", mirror_ndjson=analytics_dir / "decision_log_0001.ndjson")
        live_orders: dict[str, dict] = {}
        pnl_state: dict[str, float] = {"pos": 0.0, "avg_px": 0.0}
        # 直近N件だけ保持するリングバッファ（errors.recent_buffer_len で上書き可）
        try:
            err_buf_len = 200
            err_cfg = getattr(cfg, "errors", None)
            if err_cfg is None and isinstance(cfg, Mapping):
                err_cfg = cfg.get("errors")
            if err_cfg is not None:
                err_buf_len = int(getattr(err_cfg, "recent_buffer_len", None) or (err_cfg.get("recent_buffer_len") if isinstance(err_cfg, Mapping) else 200) or 200)
        except Exception:
            err_buf_len = 200
        recent_errors = deque(maxlen=err_buf_len)

        def _record_recent_error(*, kind: str, code: int | None, message: str) -> None:
            """REST送信エラーを直近N件だけリングバッファに積む（在庫は動かさない）。"""
            ts = _now_utc().isoformat()
            try:
                item = {"ts": ts, "kind": kind, "code": code, "message": message}
            except Exception:
                item = {"ts": ts, "kind": str(kind), "code": code, "message": str(message)}
            try:
                recent_errors.append(item)
            except Exception:
                pass
            if code == -205:
                logger.warning("recent_errors[-205]: {item}", item=item)
            else:
                logger.warning("recent_errors: {item}", item=item)

        # 起動直後のオートフラット（在庫が非ゼロなら reduce 方向に MARKET+IOC を1回送る）
        def _auto_flatten_on_start() -> None:
            try:
                # 設定: デフォルト有効、cfg.risk.auto_flatten_on_start.enabled で上書き
                risk_node = getattr(cfg, "risk", None)
                af = getattr(risk_node, "auto_flatten_on_start", None)
                if af is None and isinstance(risk_node, Mapping):
                    af = risk_node.get("auto_flatten_on_start")
                enabled = True if af is None else bool(getattr(af, "enabled", True) if not isinstance(af, Mapping) else af.get("enabled", True))
                if not enabled:
                    return

                q = float(pnl_state.get("pos", 0.0))
                if q == 0.0:
                    return

                side = "SELL" if q > 0 else "BUY"
                qty = abs(q)
                if qty <= 0.0:
                    return

                if dry_run:
                    logger.info("auto_flatten_on_start[dry_run]: skip place IOC (side={side}, qty={qty})", side=side, qty=qty)
                    return

                logger.info("auto_flatten_on_start: try place IOC (side={side}, qty={qty})", side=side, qty=qty)
                try:
                    acc = ex.place_ioc_reduce_only(side=side, size=qty, tag="auto_flatten_on_start")
                except Exception as e:
                    msg = str(e)
                    is_205 = ("-205" in msg) or ("Margin amount is insufficient" in msg)
                    _record_recent_error(kind="sendchildorder", code=(-205 if is_205 else None), message=msg)
                    if not is_205:
                        raise
                    return
                if not acc:
                    return

                # live_orders 登録とロギング
                now0 = _now_utc()
                o = SimpleNamespace(side=side, price=None, size=qty, tag="auto_flatten_on_start", tif="IOC", ttl_ms=None, reduce_only=True)
                live_orders[str(acc)] = {"deadline": None, "order": o, "executed": 0.0, "avg_price": 0.0}
                order_log.add(ts=now0.isoformat(), action="place", tif="IOC", ttl_ms=None, px=None, sz=qty, reason="auto_flatten_on_start")
                _hb_write(hb_path, event="place", ts=now0.isoformat(), acc=str(acc), reason="auto_flatten_on_start", tif="IOC", ttl_ms=None, px=None, sz=qty)
            except Exception:
                # 起動時の安全装置なので失敗しても運転継続
                return

        # 停止直前のオートフラット（在庫が0になるまで MARKT+IOC を短い間隔で繰り返し送る）
        def _auto_flatten_on_kill(max_attempts: int = 8, min_step_qty: float = 0.001, sleep_ms_between: int = 300) -> None:
            try:
                if dry_run:
                    logger.info("auto_flatten_on_kill[dry_run]: skip flatten")
                    return
                # 小さな尻尾で打ち切るしきい値
                dust_eps = float(min_step_qty) * 0.5
                for attempt in range(1, int(max_attempts) + 1):
                    try:
                        q = float(_net_inventory_btc(ex))
                    except Exception:
                        q = 0.0
                    if abs(q) <= dust_eps:
                        logger.info("auto_flatten_on_kill: done (tiny tail |Q|={q})", q=q)
                        break
                    side = "SELL" if q > 0.0 else "BUY"
                    qty = abs(q)
                    logger.info(
                        "auto_flatten_on_kill: attempt {attempt}/{max_attempts} place IOC (side={side}, qty={qty})",
                        attempt=attempt, max_attempts=max_attempts, side=side, qty=qty,
                    )
                    try:
                        acc = ex.place_ioc_reduce_only(side=side, size=qty, tag="auto_flatten_on_kill")
                    except Exception as e:
                        msg = str(e)
                        is_205 = ("-205" in msg) or ("Margin amount is insufficient" in msg)
                        _record_recent_error(kind="sendchildorder", code=(-205 if is_205 else None), message=msg)
                        if not is_205:
                            raise
                        time.sleep(float(sleep_ms_between) / 1000.0)
                        continue
                    if not acc:
                        time.sleep(float(sleep_ms_between) / 1000.0)
                        continue

                    now1 = _now_utc()
                    o = SimpleNamespace(side=side, price=None, size=qty, tag="auto_flatten_on_kill", tif="IOC", ttl_ms=None, reduce_only=True)
                    live_orders[str(acc)] = {"deadline": None, "order": o, "executed": 0.0, "avg_price": 0.0}
                    order_log.add(ts=now1.isoformat(), action="place", tif="IOC", ttl_ms=None, px=None, sz=qty, reason="auto_flatten_on_kill")
                    _hb_write(hb_path, event="place", ts=now1.isoformat(), acc=str(acc), reason="auto_flatten_on_kill", tif="IOC", ttl_ms=None, px=None, sz=qty)
                    time.sleep(float(sleep_ms_between) / 1000.0)
            except Exception:
                return
        # 自動縮小（auto-reduce）: 在庫が0でないときに減らす方向だけMARKET+IOCを投げる補助関数
        throttle_until = None
        _last_tx_at = None
        def _maybe_auto_reduce(now: datetime) -> None:
            """
            在庫が非ゼロのときだけ reduce 方向へ MARKET×IOC を1回送る（tag=auto_reduce）。
            - profit_only/force_on_risk/min_step_qty は cfg.risk.auto_reduce に従う（未設定は安全側既定）
            - -205 は recent_errors に記録のみ。他は従来どおり上へ送出。
            """
            nonlocal throttle_until, _last_tx_at
            try:
                risk_node = getattr(cfg, "risk", None)
                ar = getattr(risk_node, "auto_reduce", None)
                if ar is None and isinstance(risk_node, Mapping):
                    ar = risk_node.get("auto_reduce")
                enabled = bool(getattr(ar, "enabled", False) if ar is not None else False)
                profit_only = bool(getattr(ar, "profit_only", True) if ar is not None else True)
                force_on_risk = bool(getattr(ar, "force_on_risk", True) if ar is not None else True)
                min_step_qty = float(getattr(ar, "min_step_qty", 0.001) if ar is not None else 0.001)
                if not enabled:
                    return

                q = float(pnl_state.get("pos", 0.0))
                a = float(pnl_state.get("avg_px", 0.0) or 0.0)
                if q == 0.0:
                    return

                try:
                    bid_px = _best_px(getattr(ob, "best_bid", None))
                    ask_px = _best_px(getattr(ob, "best_ask", None))
                    mark = None if (bid_px is None or ask_px is None or ask_px <= 0 or bid_px <= 0) else (bid_px + ask_px) / 2.0
                except Exception:
                    mark = None

                side = "SELL" if q > 0 else "BUY"
                if profit_only and (a and mark is not None):
                    exp_per_unit = (mark - a) if q > 0 else (a - mark)
                    if exp_per_unit < 0 and not force_on_risk:
                        return

                qty = min(abs(q), float(min_step_qty))
                if qty <= 0.0:
                    return

                if dry_run:
                    logger.info("auto_reduce[dry_run]: skip place IOC (side={side}, qty={qty})", side=side, qty=qty)
                    return

                logger.info("auto_reduce: try place IOC (side={side}, qty={qty})", side=side, qty=qty)
                try:
                    acc = ex.place_ioc_reduce_only(side=side, size=qty, tag="auto_reduce")
                except Exception as e:
                    msg = str(e)
                    is_205 = ("-205" in msg) or ("Margin amount is insufficient" in msg)
                    _record_recent_error(kind="sendchildorder", code=(-205 if is_205 else None), message=msg)
                    if not is_205:
                        raise
                    return

                if not acc:
                    return

                o = SimpleNamespace(side=side, price=None, size=qty, tag="auto_reduce", tif="IOC", ttl_ms=None, reduce_only=True)
                live_orders[str(acc)] = {"deadline": None, "order": o, "executed": 0.0, "avg_price": 0.0}

                order_log.add(ts=now.isoformat(), action="place", tif="IOC", ttl_ms=None, px=None, sz=qty, reason="auto_reduce")
                _hb_write(hb_path, event="place", ts=now.isoformat(), acc=str(acc), reason="auto_reduce", tif="IOC", ttl_ms=None, px=None, sz=qty)
                _last_tx_at = now

            except RateLimitError as e:
                logger.error(f"live: exchange RateLimit on auto_reduce → cooldown: {e}")
                throttle_until = _now_utc() + timedelta(seconds=10)
            except Exception:
                return
            stop_event = Event()  # 何をするか：停止フラグ（signal 受信で立てる）

            def _on_signal(signum, frame) -> None:
                logger.warning(f"signal received: {signum} → cancel all & halt")  # 何をするか：受信をログ
                stop_event.set()  # 何をするか：イベントループに停止を伝える

            signal.signal(signal.SIGINT, _on_signal)   # 何をするか：Ctrl+C（SIGINT）で停止
            signal.signal(signal.SIGTERM, _on_signal)  # 何をするか：SIGTERM（停止要求）で停止
            # Windows の Ctrl+Break（SIGBREAK）にも反応させ、安全停止へ誘導
            if hasattr(signal, "SIGBREAK"):
                try:
                    signal.signal(signal.SIGBREAK, _on_signal)
                except Exception:
                    pass

            ob = OrderBook()  # 何をするか：ローカル板（戦略の入力）を用意
            cfg_payload = _safe_config_dict(cfg)
            if not cfg_payload and isinstance(cfg, Mapping):
                cfg_payload = dict(cfg)
            if strategy_list:
                cfg_payload["strategies"] = list(strategy_list)
