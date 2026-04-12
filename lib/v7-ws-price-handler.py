"""
V7 WS Price Handler — React to Polymarket WS price updates for instant entries/exits.

Replaces poll-based enforce-price, TP, SL, and hedge checks with WS-driven evaluation.
When a price update arrives for a subscribed token, the handler:
  - Maps token_id -> conditionId
  - Evaluates: enforce-price entry (no position) or TP/SL/hedge (has position)
  - Debounces per-cid to avoid double evaluation
  - Uses asyncio.Lock per cid for concurrency safety
"""

import asyncio
import importlib.util
import logging
from pathlib import Path

logger = logging.getLogger("scalp_v7")

_dir = Path(__file__).resolve().parent


def _import_kebab(name: str, filepath: str):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_v7_actions = _import_kebab("v7_act", str(_dir / "v7-seed-and-cut-strategy-actions.py"))
_v7_entry = _import_kebab("v7_entry", str(_dir / "v7-entry-side-selection-logic.py"))
# NOTE: pace module loaded from bot._v7_pace (shared instance) — not imported here
# to avoid separate _price_history dicts between WS handler and REST path


class WsPriceHandler:
    """Reacts to WS price updates — enforce-price entry, TP, SL, hedge."""

    def __init__(self, bot, cfg):
        self._bot = bot
        self._cfg = cfg
        # token_id -> conditionId
        self._token_to_cid: dict[str, str] = {}
        # conditionId -> asyncio.Lock (created on demand)
        self._locks: dict[str, asyncio.Lock] = {}
        # conditionId -> True if currently being evaluated (debounce)
        self._processing: dict[str, bool] = {}
        # conditionIds that received updates while evaluation was in-flight
        self._pending_reevaluate: set[str] = set()
        # Last logged mid price per cid (deduplicate WS log spam)
        self._last_logged_mid: dict[str, float] = {}
        # Last logged skip reason per cid (deduplicate entry-skip logs)
        self._last_skip_reason: dict[str, str] = {}
        # Prediction confirmation: track consecutive qualifying ticks per cid
        # Stores (side, count) — reset when side changes or gate fails
        # pred confirm now on bot._pred_confirm (shared, advanced by HTTP polls)
        # Last logged regime chop count per symbol (deduplicate regime logs)
        self._last_regime_chop: dict[str, int] = {}
        # Monotonic timestamp of last WS price callback (for idle detection)
        self._last_callback_mono: float = 0.0

    def _log_skip(self, cid: str, slug: str, yes_mid: float, no_mid: float,
                   reason: str):
        """Log entry skip, deduplicated — only when reason changes per cid."""
        prev = self._last_skip_reason.get(cid)
        if prev == reason:
            return
        self._last_skip_reason[cid] = reason
        # Strip leading "skip: " to avoid double prefix
        _r = reason[6:] if reason.startswith("skip: ") else reason
        # Append weighted_signal + momentum from cached prediction
        _sym = slug.split("-")[0].upper() if slug else "BTC"
        _cached = self._bot._last_prediction.get(_sym)
        _ws_tag = ""
        if _cached and _cached[0]:
            _ws = _cached[7] if len(_cached) > 7 else 0.0
            _mom = _cached[5] if len(_cached) > 5 else 0.0
            _qs = _cached[3] if len(_cached) > 3 else 0.0
            _ws_tag = f" qs={_qs:+.3f} ws={_ws:+.3f} mom={_mom:+.3f}"
        logger.info(f"[WS-SCAN] {slug} YES=${yes_mid:.3f} NO=${no_mid:.3f}"
                    f" — skip: {_r}{_ws_tag}")

    def _update_regime(self, slug: str, yes_mid: float,
                       yes_bid: float, yes_ask: float):
        """Run regime detection on WS tick — record price/spread, classify, log."""
        bot = self._bot
        cfg = self._cfg
        market_symbol = slug.split("-")[0].upper() if slug else "BTC"

        # Record spread tick for regime spread-expansion signal
        spread_pct = ((yes_ask - yes_bid) / yes_mid
                      if yes_mid > 0 and yes_ask > yes_bid else 0.0)
        bot._record_price_tick(market_symbol, yes_mid, spread_pct)

        # Read prediction for regime signals (momentum + noise)
        _pred, _conf, _, _, _, _mom, _noise, *_ = bot.read_prediction(market_symbol)
        regime, chop_cnt, details = bot._detect_regime(
            market_symbol, _conf, _mom, _noise)

        # Debounce regime: require N consecutive same readings before switching
        prev = bot._last_regime.get(market_symbol)
        debounce_n = cfg.REGIME_DEBOUNCE_COUNT
        if regime != prev:
            # Different from current regime — track consecutive count
            pending = bot._regime_pending.get(market_symbol)
            if regime == pending:
                bot._regime_pending_count[market_symbol] = bot._regime_pending_count.get(market_symbol, 0) + 1
            else:
                bot._regime_pending[market_symbol] = regime
                bot._regime_pending_count[market_symbol] = 1
            if bot._regime_pending_count.get(market_symbol, 0) >= debounce_n:
                # Confirmed switch
                bot._last_regime[market_symbol] = regime
                bot._regime_pending.pop(market_symbol, None)
                bot._regime_pending_count.pop(market_symbol, None)
            else:
                return  # Not enough consecutive readings yet — skip
        else:
            # Same as current regime — reset pending
            bot._regime_pending.pop(market_symbol, None)
            bot._regime_pending_count.pop(market_symbol, None)

        # Log regime: only on transitions (reduce noise)
        if regime != prev and prev:
            logger.info(f"[REGIME] {market_symbol} {prev} -> {regime}")

    def rebuild_token_map(self):
        """Rebuild token_id -> conditionId mapping after market discovery."""
        self._token_to_cid.clear()
        self._last_skip_reason.clear()
        for cid, info in self._bot._known_markets.items():
            self._token_to_cid[info["yes_token_id"]] = cid
            self._token_to_cid[info["no_token_id"]] = cid

    def _trigger_evaluate(self, cid: str):
        """Schedule async evaluation for a cid (main or dual), with debounce."""
        if self._processing.get(cid):
            self._pending_reevaluate.add(cid)
            return
        self._processing[cid] = True
        asyncio.ensure_future(self._evaluate_safe(cid))

    def on_price_update(self, token_id: str, bid: float, ask: float, mid: float):
        """Sync callback from WS feed — schedule async evaluation.

        Runs on the same asyncio event loop (websockets is async, not threaded).
        Uses ensure_future directly instead of call_soon_threadsafe.
        """
        import time as _time
        self._last_callback_mono = _time.monotonic()
        # Record pace history for ALL incoming ticks — including pre-subscribed
        # next-window tokens that aren't in _token_to_cid yet. This lets pace
        # accumulate ~5s of history before the new window officially starts.
        if mid > 0:
            self._bot._v7_pace.record_price(token_id, mid)
        cid = self._token_to_cid.get(token_id)
        if not cid:
            return
        # Log WS price tick only for position's held side, deduplicated
        pos = self._bot.state.positions.get(cid)
        if pos and pos.phase not in ("TP_CLOSED",):
            info = self._bot._known_markets.get(cid, {})
            is_yes = token_id == info.get("yes_token_id")
            pos_side_yes = pos.yes_tokens > 0
            if is_yes == pos_side_yes:
                # Only log when mid price actually changes
                if self._last_logged_mid.get(cid) != mid:
                    self._last_logged_mid[cid] = mid
                    s = "Y" if is_yes else "N"
                    entry = pos.yes_entry_price if is_yes else pos.no_entry_price
                    _fee = getattr(self._cfg, 'TAKER_FEE_PCT', 0)
                    pnl = ((mid * (1 - _fee) - entry) / entry * 100) if entry > 0 else 0
                    pnl_str = f"\033[32m{pnl:+.1f}%\033[0m" if pnl > 0 else (
                        f"\033[31m{pnl:+.1f}%\033[0m" if pnl < 0 else f"{pnl:+.1f}%")
                    logger.info(f"[PM-WS] {s} bid={bid:.3f} ask={ask:.3f} mid={mid:.3f} pnl={pnl_str}")
        self._trigger_evaluate(cid)
        # Also trigger dual position evaluation (separate async task)
        dual_cid = cid + "_dual"
        if dual_cid in self._bot.state.positions:
            self._trigger_evaluate(dual_cid)

    async def _evaluate_safe(self, cid: str):
        """Wrapper with error handling and debounce cleanup.

        After evaluation, checks if new updates arrived during processing.
        If so, runs one more evaluation to avoid missing TP/SL threshold crossings.
        """
        try:
            await self._evaluate(cid)
        except Exception as e:
            logger.error(f"[WS-HANDLER] Evaluate error for {cid[:12]}: {e}")
        finally:
            # Check if updates arrived while we were evaluating
            if cid in self._pending_reevaluate:
                self._pending_reevaluate.discard(cid)
                try:
                    await self._evaluate(cid)
                except Exception as e:
                    logger.error(f"[WS-HANDLER] Re-evaluate error for {cid[:12]}: {e}")
            self._processing[cid] = False

    async def _evaluate(self, cid: str):
        """Per-cid locked evaluation: enforce-price entry or TP/SL/hedge."""
        if cid not in self._locks:
            self._locks[cid] = asyncio.Lock()

        async with self._locks[cid]:
            # Resolve base cid for market info (strip _dual suffix)
            base_cid = cid[:-5] if cid.endswith("_dual") else cid
            info = self._bot._known_markets.get(base_cid)
            if not info:
                return

            yes_id = info["yes_token_id"]
            no_id = info["no_token_id"]
            slug = info.get("slug", "")

            # Get WS prices
            pm_feed = self._bot._pm_feed
            if not pm_feed:
                return
            yes_bid, yes_ask, yes_mid = pm_feed.get_prices(yes_id)
            no_bid, no_ask, no_mid = pm_feed.get_prices(no_id)

            if yes_mid <= 0 and no_mid <= 0:
                return  # no usable price data yet


            # --- Has position (main or dual): TP / SL / hedge ---
            if cid in self._bot.state.positions:
                await self._evaluate_position(cid, yes_mid, no_mid, yes_ask, no_ask)
                return

            # --- No position: check pace detection entry (WS-driven) ---
            if (self._cfg.PACE_DETECT
                    and base_cid in self._bot._known_markets
                    and base_cid not in self._bot._seeded_this_window):
                await self._evaluate_pace_entry(
                    base_cid, info, yes_id, no_id, slug,
                    yes_mid, no_mid, yes_ask, no_ask)
            return

    async def _evaluate_position(self, cid: str,
                                 yes_mid: float, no_mid: float,
                                 yes_ask: float, no_ask: float):
        """TP / SL / hedge checks for existing position."""
        pos = self._bot.state.positions.get(cid)
        if not pos or pos.phase == "TP_CLOSED":
            return

        # Update current prices from WS (skip resolved prices)
        if 0 < yes_mid < 0.999:
            pos.yes_price = yes_mid
        if 0 < no_mid < 0.999:
            pos.no_price = no_mid

        cfg = self._cfg
        market_symbol = pos.market_slug.split("-")[0].upper() if pos.market_slug else "BTC"

        # --- EXIT_PRICE: sell entry side at specific price (not hedge/dual) ---
        if cfg.EXIT_PRICE > 0 and not pos.is_dual and pos.phase in ("ENTERED", "HEDGED"):
            entry_side = pos.entry_side
            entry_mid = yes_mid if entry_side == "YES" else no_mid
            if entry_mid >= cfg.EXIT_PRICE:
                logger.info(
                    f"[EXIT_PRICE] {pos.market_slug}: {entry_side} mid=${entry_mid:.3f} "
                    f">= target=${cfg.EXIT_PRICE:.3f} — selling entry side")
                sold = await _v7_actions.sell_all_positions(
                    self._bot, pos, cfg.MAX_RETRIES, reason="EXIT_PRICE")
                if sold:
                    # Cancel unfilled hedge limit orders + sell filled hedge tokens
                    await _v7_actions.cleanup_hedge_after_exit(
                        self._bot, pos, cfg.MAX_RETRIES)
                    self._bot._save_state()
                    return

        # --- Centralized TP/SL ---
        regime = "TREND"
        sold = await _v7_actions.evaluate_tp_tsl(
            self._bot, pos, regime, cfg, cfg.MAX_RETRIES, cfg.CLOB_HOST)
        if sold:
            self._bot._save_state()
            return

        # --- Hedge check (only in "hedge" volatility mode) ---
        if pos.phase == "ENTERED" and cfg.VOLATILITY_MODE == "hedge":
            hedge_price = self._bot._get_effective_hedge_price(market_symbol)
            if hedge_price > 0:
                await _v7_actions.check_hedge_opportunity(
                    self._bot, pos, cfg.CLOB_HOST, hedge_price,
                    cfg.HEDGE_ENTRY_PRICE_USD, cfg.MAX_RETRIES)
                if pos.phase == "HEDGED":
                    self._bot._save_state()

    async def _evaluate_pace_entry(self, cid: str, info: dict,
                                   yes_id: str, no_id: str, slug: str,
                                   yes_mid: float, no_mid: float,
                                   yes_ask: float, no_ask: float):
        """WS pace detection entry — fires when PM price surges on one side."""
        import time as _time
        cfg = self._cfg

        if cfg.MANAGE_POSITIONS_ONLY:
            return

        wts = self._bot._get_window_ts()
        elapsed_sec = _time.time() - wts

        pace_side, fak_price, pace_pct = self._bot._v7_pace.evaluate_pace(
            cfg, yes_id, no_id, yes_mid, no_mid, yes_ask, no_ask,
            elapsed_sec, slug, self._bot._log)
        if not pace_side:
            return

        # Gate: require prediction agreement with >= 73% confidence
        _sym = slug.split("-")[0].upper() if slug else "BTC"
        # Force fresh poll (cached may be stale after restart)
        _pred = self._bot.read_prediction(_sym)
        _pred_dir = _pred[0] if _pred else ""  # "UP" or "DOWN"
        _pred_side = "YES" if _pred_dir == "UP" else ("NO" if _pred_dir == "DOWN" else "")
        _pred_conf = _pred[1] if _pred else 0
        if not _pred_side:
            self._bot._log(
                f"[PACE] {slug} — skip: no prediction available", "INFO")
            return
        if _pred_conf < 0.73:
            self._bot._log(
                f"[PACE] {slug} — skip: prediction={_pred_dir} conf={_pred_conf:.0%} "
                f"< 73% (too low)", "INFO")
            return
        if _pred_side != pace_side:
            self._bot._log(
                f"[PACE] {slug} — skip: pace={pace_side} but prediction={_pred_dir} "
                f"conf={_pred_conf:.0%} (disagree)", "INFO")
            return

        # Build market dict directly from known info (cid + slug are already correct)
        market = {"conditionId": cid, "slug": slug,
                  "clobTokenIds": [yes_id, no_id]}
        _entry_meta = {
            "entry_type": "PACE",
            "pace_pct": round(pace_pct, 3),
            "pace_window_sec": cfg.PACE_DETECT_WINDOW_SEC,
            "elapsed_sec": round(elapsed_sec, 1),
            "yes_mid": round(yes_mid, 4), "no_mid": round(no_mid, 4),
            "prediction": _pred[0], "confidence": round(_pred[1], 4),
            "quality_score": round(_pred[3], 4) if _pred[3] else 0,
            "cross_pair_agreement": _pred[4],
            "momentum": round(_pred[5], 4) if _pred[5] else 0,
            "weighted_signal": round(_pred[7], 4) if len(_pred) > 7 and _pred[7] else 0,
        }

        pos = await _v7_actions.execute_entry(
            self._bot, market, yes_id, no_id, pace_side, fak_price,
            cfg.POSITION_SIZE_USD, cfg.MAX_RETRIES,
            max_fill_price=cfg.PACE_DETECT_PRICE_CAP,
            entry_meta=_entry_meta)
        if pos:
            pos.entry_type = "PACE"
            self._bot._seeded_this_window.add(cid)
            self._bot._save_state()

    async def _evaluate_entry(self, cid: str, info: dict,
                              yes_id: str, no_id: str, slug: str,
                              yes_mid: float, no_mid: float,
                              yes_ask: float, no_ask: float):
        """WS entry: pre-flight gates + shared side-selection + execute."""
        import time as _time
        cfg = self._cfg

        # Manage-only mode — no new entries
        if cfg.MANAGE_POSITIONS_ONLY:
            return

        wts = self._bot._get_window_ts()
        elapsed_sec = _time.time() - wts

        # --- Pace detection: fires early before timing gate (PM price surge) ---
        if (cfg.PACE_DETECT
                and cid in self._bot._known_markets
                and cid not in self._bot._seeded_this_window
                and cid not in self._bot.state.positions):
            pace_side, fak_price, _ = self._bot._v7_pace.evaluate_pace(
                cfg, yes_id, no_id, yes_mid, no_mid, yes_ask, no_ask,
                elapsed_sec, slug, self._bot._log)
            if pace_side:
                market = {"conditionId": cid, "slug": slug,
                          "clobTokenIds": [yes_id, no_id]}
                pos = await _v7_actions.execute_entry(
                    self._bot, market, yes_id, no_id, pace_side, fak_price,
                    cfg.POSITION_SIZE_USD, cfg.MAX_RETRIES,
                    max_fill_price=cfg.PACE_DETECT_PRICE_CAP)
                if pos:
                    pos.entry_type = "PACE"
                    self._bot._seeded_this_window.add(cid)
                    self._bot._save_state()
                return

        # Entry timing gate — cross-market uses 5-10min window, normal uses ENTRY_MINUTE
        elapsed_min = elapsed_sec / 60
        if cfg.CROSS_MARKET_TRADE:
            if elapsed_min < cfg.CROSS_MARKET_ENTRY_MIN:
                self._log_skip(cid, slug, yes_mid, no_mid,
                               f"cross: waiting min {elapsed_min:.1f}/{cfg.CROSS_MARKET_ENTRY_MIN}")
                return
            if elapsed_min > cfg.CROSS_MARKET_ENTRY_MAX:
                self._log_skip(cid, slug, yes_mid, no_mid,
                               f"cross: past entry window (min {elapsed_min:.1f} > {cfg.CROSS_MARKET_ENTRY_MAX})")
                return
            # Log cross-market entry evaluation details
            _leading = "YES" if yes_mid > no_mid else "NO"
            _trailing = "NO" if _leading == "YES" else "YES"
            _lead_price = yes_mid if _leading == "YES" else no_mid
            _trail_price = no_mid if _leading == "YES" else yes_mid
            self._bot._log(
                f"[CROSS] {slug} Y=${yes_mid:.3f}/N=${no_mid:.3f} — evaluating: "
                f"{cfg.WINDOW_MINUTES}m leading={_leading}(${_lead_price:.3f}) "
                f"trailing={_trailing}(${_trail_price:.3f}) "
                f"min {elapsed_min:.1f} in [{cfg.CROSS_MARKET_ENTRY_MIN}-{cfg.CROSS_MARKET_ENTRY_MAX}]",
                "INFO")
        elif elapsed_min < cfg.ENTRY_MINUTE:
            self._log_skip(cid, slug, yes_mid, no_mid,
                           f"waiting: min {elapsed_min:.1f}/{cfg.ENTRY_MINUTE}")
            return
        if cfg.ENTRY_MAX_MINUTE > 0 and elapsed_min > cfg.ENTRY_MAX_MINUTE:
            self._log_skip(cid, slug, yes_mid, no_mid,
                           f"skip: too late (min {elapsed_min:.1f} > max {cfg.ENTRY_MAX_MINUTE})")
            return

        # Double-entry prevention
        if cid in self._bot._seeded_this_window:
            return
        if cid in self._bot.state.positions:
            return

        # Trading hours gate
        if cfg.TRADING_WINDOW_START >= 0 and cfg.TRADING_WINDOW_END >= 0:
            import datetime as _dt
            _now = _dt.datetime.now(_dt.timezone.utc)
            _now_mins = _now.hour * 60 + _now.minute
            if cfg.TRADING_WINDOW_START <= cfg.TRADING_WINDOW_END:
                outside = (_now_mins < cfg.TRADING_WINDOW_START
                           or _now_mins >= cfg.TRADING_WINDOW_END)
            else:
                outside = (_now_mins >= cfg.TRADING_WINDOW_END
                           and _now_mins < cfg.TRADING_WINDOW_START)
            if outside:
                self._log_skip(cid, slug, yes_mid, no_mid, "outside trading hours")
                return

        # Capital gate
        if not self._bot._capital_ok:
            self._log_skip(cid, slug, yes_mid, no_mid, "insufficient capital")
            return

        # Choppy market gate
        if self._bot._is_choppy_paused():
            self._log_skip(cid, slug, yes_mid, no_mid, "choppy market (watchdog)")
            return

        # Volatility gate
        market_symbol = slug.split("-")[0].upper() if slug else "BTC"
        if not self._bot._check_volatility_entry_gate(market_symbol):
            self._log_skip(cid, slug, yes_mid, no_mid, "volatility gate")
            return

        # --- Shared entry-side decision (uses cached prediction, no HTTP poll) ---
        _lock_had_key = market_symbol in self._bot._locked_prediction
        side, ask, prediction, confidence, quality_score, cross_pair_agreement, skip_reason = \
            _v7_entry.decide_entry_side(
                cfg, yes_mid, no_mid, yes_ask, no_ask,
                self._bot.read_prediction_cached, self._bot._locked_prediction,
                market_symbol, elapsed_min,
                read_hyper_fn=getattr(self._bot, "read_hyper_prediction", None))
        # Log when decide_entry_side freshly wrote the prediction lock
        if not _lock_had_key and market_symbol in self._bot._locked_prediction:
            self._bot._log(
                f"[PRED-LOCK] {slug} locked prediction: {prediction} "
                f"({confidence:.0%}) at min {elapsed_min:.1f}", "INFO")

        if not side:
            _skip_prefix = "cross: " if cfg.CROSS_MARKET_TRADE else ""
            self._log_skip(cid, slug, yes_mid, no_mid,
                           f"{_skip_prefix}{skip_reason or 'no side selected'}")
            return

        # CROSS mode: only buy the cheap/trailing side (reversion strategy)
        if cfg.CROSS_MARKET_TRADE and side:
            _leading = "YES" if yes_mid > no_mid else "NO"
            if side == _leading:
                self._log_skip(cid, slug, yes_mid, no_mid,
                               f"cross: {side} is leading (${yes_mid if side == 'YES' else no_mid:.3f}), need trailing for reversion")
                return

        # --- Prediction confirmation gate (shared counter, advanced by HTTP polls in main loop) ---
        _confirm_n = cfg.PRED_CONFIRM_TICKS
        if _confirm_n > 0:
            _prev_side, _prev_count = self._bot._pred_confirm.get(market_symbol, (None, 0))
            if side != _prev_side or _prev_count < _confirm_n:
                self._log_skip(cid, slug, yes_mid, no_mid,
                               f"pred confirm {_prev_count}/{_confirm_n} ({side})")
                return

        mid_price = yes_mid if side == "YES" else no_mid

        # Build market dict directly from known info (cid + slug are already correct)
        market = {"conditionId": cid, "slug": slug,
                  "clobTokenIds": [yes_id, no_id]}

        # Build prediction tag for entry log
        _pred_tag = ""
        if cfg.PREDICTION_SOURCE != "none":
            _q_tag = f" q={quality_score:.2f}" if quality_score > 0 else ""
            _pred_tag = f" pred={prediction or 'none'}({confidence:.0%}){_q_tag}"

        # Hybrid maker/taker (mirrors REST path)
        pm_feed = self._bot._pm_feed
        yes_bid = pm_feed.get_best_bid(yes_id) if pm_feed else 0
        no_bid = pm_feed.get_best_bid(no_id) if pm_feed else 0
        _entry_conf = confidence if cfg.PREDICTION_SOURCE != "none" else 0
        _entry_bid = yes_bid if side == "YES" else no_bid
        _entry_spread = ask - _entry_bid if _entry_bid > 0 else 0
        _use_maker = (cfg.MAKER_MODE
                      and _entry_conf >= cfg.MIN_CONFIDENCE
                      and _entry_conf < cfg.MAKER_CONFIDENCE
                      and _entry_spread >= cfg.MAKER_MIN_SPREAD
                      and _entry_bid > 0)

        if _use_maker:
            self._bot._log(
                f"WS-MAKER ENTRY {slug}: {side} mid=${mid_price:.3f} "
                f"bid=${_entry_bid:.3f} spread=${_entry_spread:.3f} "
                f"conf={_entry_conf:.0%}{_pred_tag} — maker ${cfg.POSITION_SIZE_USD}",
                "ENTRY")
            pos = await _v7_actions.execute_maker_entry(
                self._bot, market, yes_id, no_id, side, _entry_bid,
                cfg.POSITION_SIZE_USD, cfg.MAX_RETRIES,
                max_fill_price=cfg.BUY_BAND_HIGH)
        else:
            # Cap FAK price at mid + buffer to avoid overpaying spread
            _fak_price = ask
            if cfg.BUY_MAX_ABOVE_MID > 0:
                _fak_price = min(ask, mid_price + cfg.BUY_MAX_ABOVE_MID)
            self._bot._log(
                f"WS-ENTRY {slug}: {side} mid=${mid_price:.3f}"
                f"{_pred_tag} — buying ${cfg.POSITION_SIZE_USD}", "ENTRY")
            pos = await _v7_actions.execute_entry(
                self._bot, market, yes_id, no_id, side, _fak_price,
                cfg.POSITION_SIZE_USD, cfg.MAX_RETRIES,
                max_fill_price=cfg.BUY_BAND_HIGH)

        # Hedge limit order after entry
        if pos and cfg.HEDGE_PRICE > 0 and cfg.HEDGE_ENTRY_PRICE_USD > 0:
            await _v7_actions.place_entry_hedge_limit(
                self._bot, pos, cfg.HEDGE_PRICE,
                cfg.HEDGE_ENTRY_PRICE_USD, cfg.MAX_RETRIES)
        # Dual mode: buy opposite side with independent TP/SL
        if pos and cfg.DUAL_MODE_ENABLED:
            opp_side = "NO" if side == "YES" else "YES"
            opp_ask = no_ask if opp_side == "NO" else yes_ask
            if opp_ask > 0:
                await _v7_actions.execute_dual_entry(
                    self._bot, market, yes_id, no_id, opp_side, opp_ask,
                    cfg.DUAL_POSITION_SIZE_USD, cfg.DUAL_TP_RATIO,
                    cfg.DUAL_SL_RATIO, cfg.MAX_RETRIES)
        self._bot._save_state()
