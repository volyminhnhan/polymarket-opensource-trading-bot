"""
V7 Strategy Actions — Prediction-based entry + optional hedge (5m markets).

Strategy:
  1. ENTRY — After ENTRY_MINUTE: enter predicted winning side (USD-based).
  2. HEDGE — Volatility-based hedge via OKX WS pricing.
  3. HOLD — Wait for market resolution. No sells.

Called by ScalpingBotv7 with bot instance reference for state access.
"""

import importlib.util
import logging
import asyncio
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("scalp_v7")

_dir = Path(__file__).resolve().parent


def _import_kebab(name: str, filepath: str):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_v7_engine = _import_kebab("v7_engine", str(_dir / "v7-prediction-hedge-strategy-engine.py"))
_core_exec = _import_kebab("v5_exec", str(_dir / "konis-core-order-execution-with-retry.py"))
_v7_pos = _import_kebab("v7_pos", str(_dir / "v7-hedge-position.py"))
_cfg = _import_kebab("v7_cfg", str(_dir / "v7-bot-config-and-logging.py"))
HedgePosition = _v7_pos.HedgePosition


async def _fetch_entry_side_vwap(pos, clob_host: str):
    """Fetch bid VWAP and best bid for the entry side of a position.
    Returns (vwap, best_bid) or (None, None) if unavailable."""
    if not clob_host:
        return None, None
    side = pos.entry_side
    tokens = pos.yes_tokens if side == "YES" else pos.no_tokens
    if tokens <= 0:
        return None, None
    token_id = pos.yes_token_id if side == "YES" else pos.no_token_id
    try:
        book = await _v7_engine.get_orderbook(clob_host, token_id)
        vwap, avail = _calc_bid_vwap(book, tokens)
        # Extract best bid price
        bids = book.get("bids") or []
        best_bid = None
        for b in bids:
            px = float(b.get("price", 0))
            if 0.01 < px < 0.99:
                best_bid = px
                break
        if vwap and vwap > 0:
            return vwap, best_bid
        return None, best_bid
    except Exception as e:
        logger.debug(f"[VWAP] Orderbook fetch error: {e}")
    return None, None


def _calc_bid_vwap(book: dict, token_count: float):
    """Walk bid levels to calculate VWAP for selling token_count tokens.
    Returns (vwap, total_available) or (None, 0)."""
    bids = book.get("bids") or []
    valid = []
    for b in bids:
        try:
            px = float(b.get("price", 0))
            sz = float(b.get("size", 0))
            if 0.01 < px < 0.99 and sz > 0:
                valid.append((px, sz))
        except (ValueError, TypeError):
            continue
    if not valid:
        return None, 0.0
    valid.sort(key=lambda x: x[0], reverse=True)
    fill = 0.0
    cost = 0.0
    for px, sz in valid:
        take = min(sz, token_count - fill)
        fill += take
        cost += take * px
        if fill >= token_count:
            break
    return (cost / fill if fill > 0 else None), fill


async def execute_entry(bot, market: dict, yes_id: str, no_id: str,
                        side: str, ask_price: float,
                        amount_usd: float, max_retries: int,
                        max_fill_price: float = 0,
                        entry_meta: dict = None) -> Optional[HedgePosition]:
    """Enter one side of the market (prediction-based or enforce-price triggered).
    max_fill_price: reject if ask already above this (0=disabled).
    entry_meta: optional dict of entry state fields to include in mongo log."""
    cid = market.get("conditionId", "")
    slug = market.get("slug", "")

    if cid in bot.state.positions:
        bot._log(f"[ENTRY SKIP] {slug} {side}: already has position", "WARN")
        return None
    if cid in bot._seeded_this_window:
        bot._log(f"[ENTRY SKIP] {slug} {side}: already seeded this window", "WARN")
        return None

    # Max slippage gate — reject if ask already above max fill price
    if max_fill_price > 0 and ask_price > max_fill_price:
        bot._log(f"[ENTRY SKIP] {slug} {side}: ask ${ask_price:.3f} > max ${max_fill_price:.3f}", "WARN")
        return None
    # Apply slippage tolerance to max_fill_price for the actual buy order
    # Entry decision uses strict BUY_BAND_HIGH, but buy order allows small slippage
    if max_fill_price > 0:
        max_fill_price = max_fill_price * (1 + _cfg.BUY_MAX_FILL_SLIPPAGE)

    if not bot.dry_run and not bot._refresh_balance():
        bot._log(f"[ENTRY SKIP] {slug} {side}: cannot verify balance", "ERROR")
        return None

    if amount_usd > bot.current_balance:
        bot._log(f"[ENTRY SKIP] {slug} {side}: balance ${bot.current_balance:.2f} < ${amount_usd:.2f}", "WARN")
        bot._seeded_this_window.add(cid)
        return None

    bot._seeded_this_window.add(cid)
    token_id = yes_id if side == "YES" else no_id
    bot._log(f"ENTRY {slug}: {side} @ ${ask_price:.3f} (${amount_usd:.2f})", "ENTRY")

    filled_tokens = amount_usd / ask_price if ask_price > 0 else 0
    fill_price = ask_price

    if bot.dry_run:
        bot.current_balance -= amount_usd
    else:
        addr = bot.trader.trading_address if bot.trader else ""
        result = await _v7_engine.execute_buy(
            bot.trader, token_id, amount_usd, False, max_retries,
            ask_price=ask_price, trading_address=addr,
            max_fill_price=max_fill_price)
        if not result:
            bot._log(f"[ENTRY FAIL] {side} buy failed for {slug}", "ERROR")
            return None
        filled_tokens = result.get("filled_tokens", filled_tokens)
        fill_price = result.get("avg_price", ask_price)
        amount_usd = result.get("usd_spent", amount_usd)
        bot._refresh_balance()

    yes_tokens = filled_tokens if side == "YES" else 0.0
    no_tokens = filled_tokens if side == "NO" else 0.0
    yes_entry = fill_price if side == "YES" else 0.0
    no_entry = fill_price if side == "NO" else 0.0

    # Per-pair SL/TP override from config
    _pair_sym = slug.split("-")[0].upper() if slug else ""
    _pair_sl, _pair_tp = _cfg.PAIR_SL_TP.get(_pair_sym, (0.0, 0.0))

    pos = HedgePosition(
        market_slug=slug, condition_id=cid,
        yes_token_id=yes_id, no_token_id=no_id,
        yes_tokens=yes_tokens, yes_entry_price=yes_entry,
        no_tokens=no_tokens, no_entry_price=no_entry,
        entry_side=side, entry_cost=amount_usd,
        phase="ENTERED", entry_time=time.time(),
        yes_price=fill_price if side == "YES" else 0.0,
        no_price=fill_price if side == "NO" else 0.0,
        sl_ratio=_pair_sl, tp_ratio=_pair_tp,
    )
    bot.state.positions[cid] = pos

    bot._log(
        f"  FILLED {filled_tokens:.0f} {side} @ ${fill_price:.3f} "
        f"= ${amount_usd:.2f}", "ENTRY")

    if bot.mongo:
        _trade_doc = {
            "type": "V7_ENTRY", "market_slug": slug,
            "side": side, "tokens": filled_tokens,
            "price": fill_price, "cost": amount_usd,
            "timestamp": time.time(),
        }
        if entry_meta:
            _trade_doc.update(entry_meta)
        bot.mongo.log_trade(_trade_doc)
    return pos


async def execute_maker_entry(bot, market: dict, yes_id: str, no_id: str,
                              side: str, bid_price: float,
                              amount_usd: float, max_retries: int,
                              max_fill_price: float = 0) -> Optional[HedgePosition]:
    """Maker entry: place GTC limit at bid+1tick instead of taking from ask.

    Returns HedgePosition if any tokens filled immediately, None otherwise.
    Unfilled portion rests in orderbook as maker (auto-cancelled on resolution).
    """
    cid = market.get("conditionId", "")
    slug = market.get("slug", "")

    if cid in bot.state.positions:
        bot._log(f"[MAKER SKIP] {slug} {side}: already has position", "WARN")
        return None
    if cid in bot._seeded_this_window:
        bot._log(f"[MAKER SKIP] {slug} {side}: already seeded this window", "WARN")
        return None

    # Maker price = best bid + offset (configurable, default 1 tick = 0.01)
    maker_price = round(bid_price + _cfg.MAKER_OFFSET, 2)
    if max_fill_price > 0 and maker_price > max_fill_price:
        bot._log(f"[MAKER SKIP] {slug} {side}: maker ${maker_price:.3f} > max ${max_fill_price:.3f}", "WARN")
        return None

    if not bot.dry_run and not bot._refresh_balance():
        bot._log(f"[MAKER SKIP] {slug} {side}: cannot verify balance", "ERROR")
        return None
    if amount_usd > bot.current_balance:
        bot._log(f"[MAKER SKIP] {slug} {side}: balance ${bot.current_balance:.2f} < ${amount_usd:.2f}", "WARN")
        bot._seeded_this_window.add(cid)
        return None

    bot._seeded_this_window.add(cid)
    token_id = yes_id if side == "YES" else no_id
    bot._log(f"MAKER ENTRY {slug}: {side} limit=${maker_price:.3f} (bid=${bid_price:.3f}+1tick) ${amount_usd:.2f}", "ENTRY")

    filled_tokens = amount_usd / maker_price if maker_price > 0 else 0
    fill_price = maker_price

    if bot.dry_run:
        bot.current_balance -= amount_usd
    else:
        addr = bot.trader.trading_address if bot.trader else ""
        result = await _v7_engine.execute_limit_buy(
            bot.trader, token_id, maker_price, amount_usd, False, max_retries)
        if not result:
            bot._log(f"[MAKER FAIL] {side} limit buy failed for {slug}", "ERROR")
            return None
        # GTC immediate fill info — may be 0 if order fully resting
        taking = float(result.get("takingAmount", 0) or 0)
        making = float(result.get("makingAmount", 0) or 0)
        if taking > 0 and making > 0:
            filled_tokens = taking
            fill_price = making / taking
            amount_usd = making
        elif result.get("status") == "simulated":
            pass  # dry run handled above
        else:
            # Order resting — no immediate fill; position created with limit estimate
            bot._log(f"  MAKER RESTING {slug}: GTC order at ${maker_price:.3f} — waiting for fill", "ENTRY")
            filled_tokens = amount_usd / maker_price
            fill_price = maker_price
        bot._refresh_balance()

    yes_tokens = filled_tokens if side == "YES" else 0.0
    no_tokens = filled_tokens if side == "NO" else 0.0
    yes_entry = fill_price if side == "YES" else 0.0
    no_entry = fill_price if side == "NO" else 0.0

    # Per-pair SL/TP override from config (maker path)
    _pair_sym = slug.split("-")[0].upper() if slug else ""
    _pair_sl, _pair_tp = _cfg.PAIR_SL_TP.get(_pair_sym, (0.0, 0.0))

    pos = HedgePosition(
        market_slug=slug, condition_id=cid,
        yes_token_id=yes_id, no_token_id=no_id,
        yes_tokens=yes_tokens, yes_entry_price=yes_entry,
        no_tokens=no_tokens, no_entry_price=no_entry,
        entry_side=side, entry_cost=amount_usd,
        phase="ENTERED", entry_time=time.time(),
        yes_price=fill_price if side == "YES" else 0.0,
        no_price=fill_price if side == "NO" else 0.0,
        sl_ratio=_pair_sl, tp_ratio=_pair_tp,
    )
    bot.state.positions[cid] = pos

    bot._log(
        f"  MAKER FILLED {filled_tokens:.0f} {side} @ ${fill_price:.3f} "
        f"= ${amount_usd:.2f}", "ENTRY")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_MAKER_ENTRY", "market_slug": slug,
            "side": side, "tokens": filled_tokens,
            "price": fill_price, "cost": amount_usd,
            "maker_price": maker_price, "bid_price": bid_price,
            "timestamp": time.time(),
        })
    return pos


async def execute_dual_entry(bot, market: dict, yes_id: str, no_id: str,
                             side: str, ask_price: float,
                             amount_usd: float, tp_ratio: float,
                             sl_ratio: float, max_retries: int) -> Optional[HedgePosition]:
    """Dual mode: buy opposite side after enforce-price triggers, with per-position TP/SL."""
    cid = market.get("conditionId", "")
    slug = market.get("slug", "")
    dual_cid = cid + "_dual"

    if dual_cid in bot.state.positions:
        bot._log(f"[DUAL SKIP] {slug} {side}: already has dual position", "WARN")
        return None
    if dual_cid in bot._seeded_this_window:
        bot._log(f"[DUAL SKIP] {slug} {side}: dual already seeded this window", "WARN")
        return None

    if ask_price <= 0:
        bot._log(f"[DUAL SKIP] {slug}: {side} ask=${ask_price:.3f} invalid", "WARN")
        return None

    if not bot.dry_run and not bot._refresh_balance():
        bot._log(f"[DUAL SKIP] Cannot verify balance for {slug}", "ERROR")
        return None

    if amount_usd > bot.current_balance:
        bot._log(f"[DUAL SKIP] Balance ${bot.current_balance:.2f} < ${amount_usd:.2f}", "WARN")
        bot._seeded_this_window.add(dual_cid)
        return None

    bot._seeded_this_window.add(dual_cid)
    token_id = yes_id if side == "YES" else no_id
    bot._log(
        f"DUAL {slug}: {side} @ ${ask_price:.3f} (${amount_usd:.2f}) "
        f"TP={tp_ratio:.0%} SL={sl_ratio:.0%}", "ENTRY")

    filled_tokens = amount_usd / ask_price if ask_price > 0 else 0
    fill_price = ask_price

    if bot.dry_run:
        bot.current_balance -= amount_usd
    else:
        addr = bot.trader.trading_address if bot.trader else ""
        result = await _v7_engine.execute_buy(
            bot.trader, token_id, amount_usd, False, max_retries,
            ask_price=ask_price, trading_address=addr)
        if not result:
            bot._log(f"[DUAL FAIL] {side} buy failed for {slug}", "ERROR")
            return None
        filled_tokens = result.get("filled_tokens", filled_tokens)
        fill_price = result.get("avg_price", ask_price)
        amount_usd = result.get("usd_spent", amount_usd)
        bot._refresh_balance()

    pos = HedgePosition(
        market_slug=slug, condition_id=cid,
        yes_token_id=yes_id, no_token_id=no_id,
        yes_tokens=filled_tokens if side == "YES" else 0.0,
        yes_entry_price=fill_price if side == "YES" else 0.0,
        no_tokens=filled_tokens if side == "NO" else 0.0,
        no_entry_price=fill_price if side == "NO" else 0.0,
        entry_side=side, entry_cost=amount_usd,
        phase="ENTERED", entry_time=time.time(),
        yes_price=fill_price if side == "YES" else 0.0,
        no_price=fill_price if side == "NO" else 0.0,
        tp_ratio=tp_ratio, sl_ratio=sl_ratio, is_dual=True,
    )
    bot.state.positions[dual_cid] = pos

    bot._log(
        f"  DUAL FILLED {filled_tokens:.0f} {side} @ ${fill_price:.3f} "
        f"= ${amount_usd:.2f}", "ENTRY")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_DUAL_ENTRY", "market_slug": slug,
            "side": side, "tokens": filled_tokens,
            "price": fill_price, "cost": amount_usd,
            "tp_ratio": tp_ratio, "sl_ratio": sl_ratio,
            "timestamp": time.time(),
        })
    return pos


async def execute_yolo_entry(bot, market: dict, yes_id: str, no_id: str,
                             side: str, ask_price: float,
                             amount_usd: float, max_retries: int,
                             volatility_hedge_price: float = 0) -> Optional[HedgePosition]:
    """Yolo mode: split buy — 50% market at volatility_hedge_price, 50% at best ask.

    ask_price: current lowest ask in orderbook (best ask).
    volatility_hedge_price: configured hedge price threshold (V7_VOLATILITY_HEDGE_PRICE).
    When volatility_hedge_price > 0: splits amount 50/50 into two market buys.
    When volatility_hedge_price <= 0: single market buy at ask_price (legacy behavior).
    """
    cid = market.get("conditionId", "")
    slug = market.get("slug", "")

    if cid in bot.state.positions:
        bot._log(f"[YOLO SKIP] {slug} {side}: already has position", "WARN")
        return None
    if cid in bot._seeded_this_window:
        bot._log(f"[YOLO SKIP] {slug} {side}: already seeded this window", "WARN")
        return None

    if not bot.dry_run and not bot._refresh_balance():
        bot._log(f"[YOLO SKIP] {slug} {side}: cannot verify balance", "ERROR")
        return None

    if amount_usd > bot.current_balance:
        bot._log(f"[YOLO SKIP] Balance ${bot.current_balance:.2f} < ${amount_usd:.2f}", "WARN")
        bot._seeded_this_window.add(cid)
        return None

    bot._seeded_this_window.add(cid)
    token_id = yes_id if side == "YES" else no_id

    # Split buy: 50% at volatility_hedge_price, 50% at best ask
    do_split = volatility_hedge_price > 0 and ask_price < volatility_hedge_price
    if do_split:
        half_a = amount_usd / 2  # for volatility_hedge_price
        half_b = amount_usd - half_a  # for best ask (no rounding loss)
        bot._log(
            f"YOLO {slug}: {side} split — "
            f"${half_a:.2f}@${volatility_hedge_price:.3f} + "
            f"${half_b:.2f}@${ask_price:.3f} (best ask)", "ENTRY")
    else:
        half_a = amount_usd
        half_b = 0
        bot._log(
            f"YOLO {slug}: {side} @ ${ask_price:.3f} (${amount_usd:.2f})", "ENTRY")

    total_tokens = 0.0
    total_spent = 0.0
    weighted_price_sum = 0.0  # for calculating avg entry price

    # --- Buy A: at volatility_hedge_price (or full amount if no split) ---
    price_a = volatility_hedge_price if do_split else ask_price
    if bot.dry_run:
        tokens_a = half_a / price_a if price_a > 0 else 0
        total_tokens += tokens_a
        total_spent += half_a
        weighted_price_sum += tokens_a * price_a
        bot.current_balance -= half_a
    else:
        addr = bot.trader.trading_address if bot.trader else ""
        result_a = await _v7_engine.execute_buy(
            bot.trader, token_id, half_a, False, max_retries,
            ask_price=price_a, trading_address=addr)
        if not result_a:
            bot._log(f"[YOLO FAIL] {side} buy-A failed for {slug}", "ERROR")
            return None
        tokens_a = result_a.get("filled_tokens", half_a / price_a)
        price_a_fill = result_a.get("avg_price", price_a)
        spent_a = result_a.get("usd_spent", half_a)
        total_tokens += tokens_a
        total_spent += spent_a
        weighted_price_sum += tokens_a * price_a_fill
        bot._refresh_balance()

    if do_split:
        tag_a = f"@${volatility_hedge_price:.3f}"
    else:
        tag_a = f"@${ask_price:.3f}"
    bot._log(
        f"  YOLO BUY-A {tokens_a:.0f} {side} {tag_a} "
        f"= ${half_a:.2f}", "ENTRY")

    # --- Buy B: at best ask (only when splitting) ---
    # Cooldown between orders to avoid rate limits / let book settle
    if do_split and half_b > 0:
        tokens_b = 0
        await asyncio.sleep(2)
        if bot.dry_run:
            tokens_b = half_b / ask_price if ask_price > 0 else 0
            total_tokens += tokens_b
            total_spent += half_b
            weighted_price_sum += tokens_b * ask_price
            bot.current_balance -= half_b
        else:
            if not bot._refresh_balance():
                bot._log(f"[YOLO] Buy-B skip: balance check failed", "WARN")
            elif half_b > bot.current_balance:
                bot._log(
                    f"[YOLO] Buy-B skip: balance ${bot.current_balance:.2f} "
                    f"< ${half_b:.2f}", "WARN")
            else:
                result_b = await _v7_engine.execute_buy(
                    bot.trader, token_id, half_b, False, max_retries,
                    ask_price=ask_price, trading_address=addr)
                if result_b:
                    tokens_b = result_b.get("filled_tokens", half_b / ask_price)
                    price_b_fill = result_b.get("avg_price", ask_price)
                    spent_b = result_b.get("usd_spent", half_b)
                    total_tokens += tokens_b
                    total_spent += spent_b
                    weighted_price_sum += tokens_b * price_b_fill
                    bot._refresh_balance()
                else:
                    bot._log(
                        f"[YOLO] Buy-B failed for {slug} — "
                        f"continuing with Buy-A only", "WARN")
                    tokens_b = 0

        actual_b_spent = half_b if tokens_b > 0 else 0.0
        bot._log(
            f"  YOLO BUY-B {tokens_b:.0f} {side} @${ask_price:.3f} "
            f"= ${actual_b_spent:.2f} (best ask)", "ENTRY")

    # Calculate weighted average entry price
    avg_price = (weighted_price_sum / total_tokens) if total_tokens > 0 else ask_price

    yes_tokens = total_tokens if side == "YES" else 0.0
    no_tokens = total_tokens if side == "NO" else 0.0
    yes_entry = avg_price if side == "YES" else 0.0
    no_entry = avg_price if side == "NO" else 0.0

    # Per-pair SL/TP override from config (yolo path)
    _pair_sym = slug.split("-")[0].upper() if slug else ""
    _pair_sl, _pair_tp = _cfg.PAIR_SL_TP.get(_pair_sym, (0.0, 0.0))

    pos = HedgePosition(
        market_slug=slug, condition_id=cid,
        yes_token_id=yes_id, no_token_id=no_id,
        yes_tokens=yes_tokens, yes_entry_price=yes_entry,
        no_tokens=no_tokens, no_entry_price=no_entry,
        entry_side=side, entry_cost=total_spent,
        phase="ENTERED", entry_time=time.time(),
        yes_price=avg_price if side == "YES" else 0.0,
        no_price=avg_price if side == "NO" else 0.0,
        sl_ratio=_pair_sl, tp_ratio=_pair_tp,
    )
    bot.state.positions[cid] = pos

    bot._log(
        f"  YOLO TOTAL {total_tokens:.0f} {side} avg@${avg_price:.3f} "
        f"= ${total_spent:.2f}", "ENTRY")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_YOLO", "market_slug": slug,
            "side": side, "tokens": total_tokens,
            "price": avg_price, "cost": total_spent,
            "split": do_split,
            "timestamp": time.time(),
        })
    return pos


async def place_entry_hedge_limit(bot, pos: HedgePosition,
                                  hedge_price: float, amount_usd: float,
                                  max_retries: int):
    """Place GTC limit buy on opposite side immediately after entry.

    Fire-and-forget: order sits in the book until filled or market resolves.
    Sets phase=HEDGED to prevent tick-by-tick check_hedge_opportunity from
    trying to double-hedge via market buy.
    """
    if hedge_price <= 0 or amount_usd <= 0:
        return

    hedge_side = "NO" if pos.entry_side == "YES" else "YES"
    hedge_token_id = pos.no_token_id if pos.entry_side == "YES" else pos.yes_token_id
    tokens = amount_usd / hedge_price if hedge_price > 0 else 0

    bot._log(
        f"HEDGE LIMIT {pos.market_slug}: {hedge_side} ~{tokens:.0f} tokens "
        f"@ ${hedge_price:.3f} (${amount_usd:.2f})", "ENTRY")

    result = await _v7_engine.execute_limit_buy(
        bot.trader, hedge_token_id, hedge_price, amount_usd,
        bot.dry_run, max_retries)

    if result:
        if pos.entry_side == "YES":
            pos.no_entry_price = hedge_price
        else:
            pos.yes_entry_price = hedge_price
        pos.hedge_cost = amount_usd
        pos.phase = "HEDGED"
        if not bot.dry_run:
            bot._refresh_balance()
        else:
            bot.current_balance -= amount_usd
        bot._log(
            f"  HEDGE LIMIT PLACED {hedge_side} @ ${hedge_price:.3f} "
            f"(GTC, left for resolution)", "ENTRY")

        if bot.mongo:
            bot.mongo.log_trade({
                "type": "V7_HEDGE_LIMIT", "market_slug": pos.market_slug,
                "side": hedge_side, "price": hedge_price, "cost": amount_usd,
                "timestamp": time.time(),
            })
    else:
        bot._log(
            f"[HEDGE LIMIT FAIL] {hedge_side} @ ${hedge_price:.3f} "
            f"for {pos.market_slug}", "ERROR")


async def check_prediction_flip(bot, pos: HedgePosition,
                                max_retries: int,
                                market: dict = None,
                                yes_ask: float = 0, no_ask: float = 0,
                                amount_usd: float = 0,
                                flip_min_confidence: float = 0.90,
                                flip_min_minute: float = 1,
                                flip_max_attempts: int = 1,
                                flip_max_minute: float = 4,
                                window_start: float = 0,
                                buy_band_low: float = 0.20,
                                buy_band_high: float = 0.65) -> bool:
    """If prediction flipped against unhedged position, sell and buy opposite.

    Returns True if flip-sell was executed.
    """
    if pos.phase != "ENTERED":
        return False
    hedge_tokens = pos.no_tokens if pos.entry_side == "YES" else pos.yes_tokens
    if hedge_tokens > 0:
        return False

    elapsed_min = (time.time() - pos.entry_time) / 60
    if elapsed_min < flip_min_minute:
        return False

    if pos.flip_count >= flip_max_attempts:
        return False

    if window_start > 0:
        window_elapsed_min = (time.time() - window_start) / 60
        if window_elapsed_min >= flip_max_minute:
            return False

    market_symbol = pos.market_slug.split("-")[0].upper() if pos.market_slug else "BTC"
    prediction, confidence, _, _q = bot.read_prediction(market_symbol)
    if not prediction or confidence < flip_min_confidence:
        return False

    entry_pred = "UP" if pos.entry_side == "YES" else "DOWN"
    if prediction == entry_pred:
        return False

    bot._log(
        f"FLIP {pos.market_slug}: prediction={prediction} vs entry={pos.entry_side} "
        f"(conf={confidence:.0%}, {elapsed_min:.1f}min) — sell + buy opposite", "WARN")

    if pos.entry_side == "YES":
        tokens, token_id = pos.yes_tokens, pos.yes_token_id
    else:
        tokens, token_id = pos.no_tokens, pos.no_token_id

    if tokens < 1:
        return False

    addr = bot.trader.trading_address if bot.trader else ""
    result = await _core_exec.execute_sell(
        bot.trader, token_id, tokens, bot.dry_run, max_retries,
        retry_delay=0.2, trading_address=addr)

    if not result:
        bot._log(f"[FLIP FAIL] Sell failed for {pos.market_slug}", "ERROR")
        return False

    if result.get("status") == "already_sold":
        bot._log(f"[FLIP] {pos.entry_side} already sold on-chain — zeroing tokens", "WARN")
        if pos.entry_side == "YES":
            pos.yes_tokens = 0
        else:
            pos.no_tokens = 0
        usd = 0
        sold = tokens
    else:
        sold = result.get("filled_tokens", 0)
        usd = result.get("usd_received", 0)
        sell_price = result.get("avg_price", 0)
        entry = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price

        if pos.entry_side == "YES":
            pos.yes_tokens = max(0, pos.yes_tokens - sold)
        else:
            pos.no_tokens = max(0, pos.no_tokens - sold)

    entry = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price
    realized_loss = usd - (sold * entry)
    bot.state.total_pnl += realized_loss
    pos.phase = "CUT"
    pos._cut_ts = time.time()

    if bot.dry_run:
        bot.current_balance += usd
    bot._refresh_balance()

    bot._log(
        f"  FLIP SOLD {sold:.0f} {pos.entry_side} @ ${sell_price:.3f} "
        f"= ${usd:.2f} (pnl ${realized_loss:.2f})", "WARN")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_FLIP_SELL", "market_slug": pos.market_slug,
            "side": pos.entry_side, "tokens": sold,
            "price": sell_price, "usd": usd,
            "loss": realized_loss, "prediction": prediction,
            "confidence": confidence,
            "timestamp": time.time(),
        })

    # Add flip-sell trade to dashboard so it shows in trade history
    if hasattr(bot, 'dashboard'):
        _pnl_pct = (realized_loss / pos.entry_cost * 100) if pos.entry_cost > 0 else 0
        bot.dashboard.add_trade({
            "side": pos.entry_side, "entry_time": pos.entry_time,
            "exit_time": time.time(), "entry_price": entry,
            "exit_price": round(sell_price, 4),
            "pnl_percent": round(_pnl_pct, 1),
            "pnl_cash": round(realized_loss, 2),
            "exit_reason": "FLIP_SELL",
            "market_slug": pos.market_slug,
            "window_ts": getattr(bot, '_last_window_ts', 0),
        })

    # --- Buy opposite side ---
    if not market or amount_usd <= 0:
        return True

    flip_side = "NO" if pos.entry_side == "YES" else "YES"
    flip_ask = no_ask if flip_side == "NO" else yes_ask
    flip_token_id = pos.no_token_id if flip_side == "NO" else pos.yes_token_id

    if flip_ask <= 0:
        bot._log(f"[FLIP BUY SKIP] {flip_side} ask=${flip_ask:.3f} invalid", "WARN")
        return True

    if flip_ask < buy_band_low or flip_ask > buy_band_high:
        bot._log(
            f"[FLIP BUY SKIP] {flip_side} ask=${flip_ask:.3f} outside band "
            f"[${buy_band_low}-${buy_band_high}]", "WARN")
        return True

    if not bot.dry_run and not bot._refresh_balance():
        return True
    if amount_usd > bot.current_balance:
        bot._log(
            f"[FLIP BUY SKIP] Balance ${bot.current_balance:.2f} < "
            f"${amount_usd:.2f}", "WARN")
        return True

    bot._log(
        f"FLIP BUY {pos.market_slug}: {flip_side} @ ${flip_ask:.3f} "
        f"(${amount_usd:.2f})", "ENTRY")

    if bot.dry_run:
        filled_tokens = amount_usd / flip_ask
        fill_price = flip_ask
        bot.current_balance -= amount_usd
    else:
        addr = bot.trader.trading_address if bot.trader else ""
        buy_result = await _v7_engine.execute_buy(
            bot.trader, flip_token_id, amount_usd, False, max_retries,
            ask_price=flip_ask, trading_address=addr)
        if not buy_result:
            bot._log(
                f"[FLIP BUY FAIL] {flip_side} for {pos.market_slug}", "ERROR")
            return True
        filled_tokens = buy_result.get("filled_tokens", amount_usd / flip_ask)
        fill_price = buy_result.get("avg_price", flip_ask)
        amount_usd = buy_result.get("usd_spent", amount_usd)
        bot._refresh_balance()

    cid = pos.condition_id
    # Per-pair SL/TP override from config (flip path)
    _pair_sym = pos.market_slug.split("-")[0].upper() if pos.market_slug else ""
    _pair_sl, _pair_tp = _cfg.PAIR_SL_TP.get(_pair_sym, (0.0, 0.0))

    new_pos = HedgePosition(
        market_slug=pos.market_slug, condition_id=cid,
        yes_token_id=pos.yes_token_id, no_token_id=pos.no_token_id,
        yes_tokens=filled_tokens if flip_side == "YES" else 0.0,
        yes_entry_price=fill_price if flip_side == "YES" else 0.0,
        no_tokens=filled_tokens if flip_side == "NO" else 0.0,
        no_entry_price=fill_price if flip_side == "NO" else 0.0,
        entry_side=flip_side, entry_cost=amount_usd,
        phase="ENTERED", entry_time=time.time(),
        flip_count=pos.flip_count + 1,
        yes_price=fill_price if flip_side == "YES" else pos.yes_price,
        no_price=fill_price if flip_side == "NO" else pos.no_price,
        sl_ratio=_pair_sl, tp_ratio=_pair_tp,
    )
    bot.state.positions[cid] = new_pos

    bot._log(
        f"  FLIP BOUGHT {filled_tokens:.0f} {flip_side} @ ${fill_price:.3f} "
        f"= ${amount_usd:.2f}", "ENTRY")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_FLIP_BUY", "market_slug": pos.market_slug,
            "side": flip_side, "tokens": filled_tokens,
            "price": fill_price, "cost": amount_usd,
            "prediction": prediction, "confidence": confidence,
            "timestamp": time.time(),
        })
    return True


async def check_hedge_opportunity(bot, pos: HedgePosition,
                                  clob_host: str, hedge_price: float,
                                  amount_usd: float, max_retries: int):
    """Check if opposite side is cheap enough for hedge buy."""
    if not pos.entry_side:
        return

    if pos.entry_side == "YES":
        hedge_side = "NO"
        hedge_token_id = pos.no_token_id
    else:
        hedge_side = "YES"
        hedge_token_id = pos.yes_token_id

    book = await _v7_engine.get_orderbook(clob_host, hedge_token_id)
    _, ask, _ = _v7_engine.get_best_prices(book)

    if ask > hedge_price or ask <= 0:
        return

    bot._log(
        f"HEDGE {pos.market_slug}: {hedge_side} ask=${ask:.3f} <= "
        f"${hedge_price} -> buying ${amount_usd:.2f}", "ENTRY")

    if bot.dry_run:
        hedge_tokens = amount_usd / ask
        if pos.entry_side == "YES":
            pos.no_tokens = hedge_tokens
            pos.no_entry_price = ask
        else:
            pos.yes_tokens = hedge_tokens
            pos.yes_entry_price = ask
        pos.hedge_cost = amount_usd
        pos.phase = "HEDGED"
        bot.current_balance -= amount_usd
        return

    addr = bot.trader.trading_address if bot.trader else ""
    result = await _v7_engine.execute_buy(
        bot.trader, hedge_token_id, amount_usd, False, max_retries,
        ask_price=ask, trading_address=addr)
    if not result:
        bot._log(f"[HEDGE FAIL] {hedge_side} buy failed for {pos.market_slug}", "ERROR")
        return

    hedge_tokens = result.get("filled_tokens", 0)
    fill_price = result.get("avg_price", ask)
    cost = result.get("usd_spent", amount_usd)

    if pos.entry_side == "YES":
        pos.no_tokens = hedge_tokens
        pos.no_entry_price = fill_price
    else:
        pos.yes_tokens = hedge_tokens
        pos.yes_entry_price = fill_price

    pos.hedge_cost = cost
    pos.phase = "HEDGED"
    bot._refresh_balance()

    bot._log(
        f"  HEDGE OK {hedge_tokens:.0f} {hedge_side} @ ${fill_price:.3f} "
        f"= ${cost:.2f}", "ENTRY")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_HEDGE", "market_slug": pos.market_slug,
            "side": hedge_side, "tokens": hedge_tokens,
            "price": fill_price, "cost": cost,
            "timestamp": time.time(),
        })


async def check_cut_loss(bot, pos: HedgePosition, cut_loss_pct: float,
                         max_retries: int) -> bool:
    """Check if main position loss exceeds threshold. If so, sell main (keep hedge)."""
    try:
        return await _check_cut_loss_inner(bot, pos, cut_loss_pct, max_retries)
    except Exception as e:
        logger.error(f"[CUT ERROR] {pos.market_slug}: {e}")
        return False


async def _check_cut_loss_inner(bot, pos: HedgePosition, cut_loss_pct: float,
                                max_retries: int) -> bool:
    if pos.entry_side == "YES":
        tokens, entry, price = pos.yes_tokens, pos.yes_entry_price, pos.yes_price
        token_id = pos.yes_token_id
    else:
        tokens, entry, price = pos.no_tokens, pos.no_entry_price, pos.no_price
        token_id = pos.no_token_id

    if tokens < 1 or entry <= 0:
        return False

    loss_pct = (entry - price) / entry
    if loss_pct < cut_loss_pct:
        return False

    bot._log(
        f"CUT LOSS {pos.market_slug}: {pos.entry_side} "
        f"loss={loss_pct:.1%} > {cut_loss_pct:.0%} | "
        f"{tokens:.0f} tokens @ ${price:.3f} (entry ${entry:.3f})", "WARN")

    addr = bot.trader.trading_address if bot.trader else ""
    result = await _core_exec.execute_sell(
        bot.trader, token_id, tokens, bot.dry_run, max_retries,
        retry_delay=0.2, trading_address=addr)

    if not result:
        bot._log(f"[CUT FAIL] Sell failed for {pos.market_slug} — will retry next tick", "ERROR")
        pos.phase = "CUT"
        return False

    if result.get("status") == "already_sold":
        bot._log(f"[CUT] {pos.entry_side} already sold on-chain — zeroing tokens", "WARN")
        sold = tokens
        usd = 0
        if pos.entry_side == "YES":
            pos.yes_tokens = 0
        else:
            pos.no_tokens = 0
    else:
        sold = result.get("filled_tokens", 0)
        usd = result.get("usd_received", 0)
        sell_price = result.get("avg_price", 0)
        if pos.entry_side == "YES":
            pos.yes_tokens = max(0, pos.yes_tokens - sold)
        else:
            pos.no_tokens = max(0, pos.no_tokens - sold)

    realized_loss = usd - (sold * entry)
    bot.state.total_pnl += realized_loss
    pos.phase = "CUT"
    pos._cut_ts = time.time()
    pos._cut_usd_received = usd

    if bot.dry_run:
        bot.current_balance += usd

    bot._refresh_balance()

    remaining = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
    if remaining >= 1:
        bot._log(
            f"  CUT PARTIAL sold {sold:.0f}/{tokens:.0f} {pos.entry_side} @ "
            f"${sell_price:.3f} = ${usd:.2f} (loss ${realized_loss:.2f}) | "
            f"{remaining:.0f} remaining — will retry next tick", "WARN")
    else:
        bot._log(
            f"  CUT OK sold {sold:.0f} {pos.entry_side} @ ${sell_price:.3f} "
            f"= ${usd:.2f} (loss ${realized_loss:.2f})", "WARN")

    bot._seeded_this_window.add(pos.condition_id)

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_CUT_LOSS", "market_slug": pos.market_slug,
            "side": pos.entry_side, "tokens": sold,
            "price": sell_price, "usd": usd,
            "loss": realized_loss, "loss_pct": loss_pct,
            "timestamp": time.time(),
        })

    # Add losing trade to dashboard so it shows in trade history
    if remaining < 1 and hasattr(bot, 'dashboard'):
        _exit_p = sell_price if sell_price > 0 else price
        _pnl_pct = (realized_loss / pos.entry_cost * 100) if pos.entry_cost > 0 else 0
        bot.dashboard.add_trade({
            "side": pos.entry_side, "entry_time": pos.entry_time,
            "exit_time": time.time(), "entry_price": entry,
            "exit_price": round(_exit_p, 4),
            "pnl_percent": round(_pnl_pct, 1),
            "pnl_cash": round(realized_loss, 2),
            "exit_reason": "CUT_LOSS",
            "market_slug": pos.market_slug,
            "window_ts": getattr(bot, '_last_window_ts', 0),
        })

    return True


async def check_flip_after_cut(bot, pos: HedgePosition, max_retries: int,
                               clob_host: str = "",
                               profit_pct: float = 0.14,
                               buy_band_low: float = 0.20,
                               buy_band_high: float = 0.65) -> bool:
    """After CUT: buy opposite tokens for resolution recovery."""
    if pos.phase != "CUT":
        return False
    main_tokens = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
    if main_tokens >= 1:
        return False
    if getattr(pos, '_flip_after_cut_done', False):
        return False

    opp_side = "NO" if pos.entry_side == "YES" else "YES"
    existing_opp = pos.no_tokens if pos.entry_side == "YES" else pos.yes_tokens

    total_invested = pos.entry_cost + pos.hedge_cost
    usd_back = getattr(pos, '_cut_usd_received', 0)
    target = total_invested * (1 + profit_pct)
    current_return = usd_back + existing_opp

    if current_return >= target:
        bot._log(
            f"[RECOVERY OK] {pos.market_slug}: {opp_side}={existing_opp:.0f} "
            f"already covers target=${target:.2f} (return=${current_return:.2f})", "INFO")
        pos._flip_after_cut_done = True
        return False

    opp_token_id = pos.no_token_id if pos.entry_side == "YES" else pos.yes_token_id
    book = await _v7_engine.get_orderbook(clob_host, opp_token_id)
    _, ask, _ = _v7_engine.get_best_prices(book)

    if ask <= 0 or ask < buy_band_low or ask > buy_band_high:
        bot._log(
            f"[RECOVERY SKIP] {pos.market_slug}: {opp_side} ask=${ask:.3f} "
            f"outside band [${buy_band_low}-${buy_band_high}]", "WARN")
        pos._flip_after_cut_done = True
        return False

    denom = 1.0 - (1 + profit_pct) * ask
    if denom <= 0:
        bot._log(
            f"[RECOVERY SKIP] {pos.market_slug}: {opp_side} ask=${ask:.3f} "
            f"too high for {profit_pct:.0%} ROI", "WARN")
        pos._flip_after_cut_done = True
        return False

    tokens_needed = (target - usd_back - existing_opp) / denom
    if tokens_needed < 1:
        pos._flip_after_cut_done = True
        return False

    import math
    tokens_needed = math.ceil(tokens_needed)
    buy_cost = tokens_needed * ask

    if buy_cost < 1.0:
        tokens_needed = math.ceil(1.0 / ask) if ask > 0 else 1
        buy_cost = tokens_needed * ask

    if not bot.dry_run and not bot._refresh_balance():
        return False
    if buy_cost > bot.current_balance:
        bot._log(
            f"[RECOVERY SKIP] {pos.market_slug}: need ${buy_cost:.2f} "
            f"but balance=${bot.current_balance:.2f}", "WARN")
        pos._flip_after_cut_done = True
        return False

    bot._log(
        f"RECOVERY BUY {pos.market_slug}: {tokens_needed:.0f} {opp_side} "
        f"@ ${ask:.3f} (${buy_cost:.2f}) | existing={existing_opp:.0f} "
        f"target=${target:.2f}", "ENTRY")

    if bot.dry_run:
        filled = tokens_needed
        fill_price = ask
        bot.current_balance -= buy_cost
    else:
        addr = bot.trader.trading_address if bot.trader else ""
        result = await _v7_engine.execute_buy(
            bot.trader, opp_token_id, buy_cost, False, max_retries,
            ask_price=ask, trading_address=addr)
        if not result:
            bot._log(f"[RECOVERY FAIL] Buy failed for {pos.market_slug}", "ERROR")
            pos._flip_after_cut_done = True
            return False
        filled = result.get("filled_tokens", tokens_needed)
        fill_price = result.get("avg_price", ask)
        buy_cost = result.get("usd_spent", buy_cost)
        bot._refresh_balance()

    if pos.entry_side == "YES":
        old_opp = pos.no_tokens
        pos.no_tokens += filled
        pos.no_entry_price = ((pos.no_entry_price * old_opp + fill_price * filled)
                              / (old_opp + filled)) if (old_opp + filled) > 0 else fill_price
    else:
        old_opp = pos.yes_tokens
        pos.yes_tokens += filled
        pos.yes_entry_price = ((pos.yes_entry_price * old_opp + fill_price * filled)
                               / (old_opp + filled)) if (old_opp + filled) > 0 else fill_price
    pos.hedge_cost += buy_cost
    pos._flip_after_cut_done = True

    new_total = existing_opp + filled
    new_invested = pos.entry_cost + pos.hedge_cost
    projected_return = usd_back + new_total
    bot._log(
        f"  RECOVERY OK {filled:.0f} {opp_side} @ ${fill_price:.3f} "
        f"= ${buy_cost:.2f} | total {opp_side}={new_total:.0f} "
        f"projected=${projected_return:.2f} vs invested=${new_invested:.2f}", "ENTRY")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_RECOVERY_BUY", "market_slug": pos.market_slug,
            "side": opp_side, "tokens": filled,
            "price": fill_price, "cost": buy_cost,
            "existing_opp": existing_opp,
            "target": target, "projected_return": projected_return,
            "timestamp": time.time(),
        })
    return True


async def evaluate_tp_tsl(bot, pos, regime: str, cfg, max_retries: int,
                          clob_host: str = "") -> bool:
    """Centralized TP/SL evaluation — used by both REST scan and WS handler.

    Returns True if position was closed/sold.
    """
    if pos.phase not in ("ENTERED", "HEDGED") or not pos.entry_side:
        return False
    # Prevent concurrent TP/SL evaluation (WS + REST race)
    if pos.is_selling:
        return False

    entry_side = pos.entry_side
    main_p = pos.yes_price if entry_side == "YES" else pos.no_price
    entry_p = pos.yes_entry_price if entry_side == "YES" else pos.no_entry_price
    tokens = pos.yes_tokens if entry_side == "YES" else pos.no_tokens

    if entry_p <= 0 or main_p <= 0 or tokens <= 0:
        return False

    # --- Price ceiling: sell immediately when price hits 0.99 (near-resolved) ---
    if main_p >= 0.99:
        bot._log(
            f"[CEIL] {pos.market_slug} {entry_side} price=${main_p:.3f} >= $0.99"
            f" — selling (near-resolved)", "WARN")
        sold = await sell_all_positions(bot, pos, max_retries, reason="CEIL_099")
        if sold:
            return True

    # Use bid VWAP for realistic PnL (mid overstates on thin books), minus taker fee
    vwap_p, best_bid = await _fetch_entry_side_vwap(pos, clob_host)
    effective_p = vwap_p if vwap_p else main_p
    pnl_ratio = (effective_p * (1 - cfg.TAKER_FEE_PCT) - entry_p) / entry_p
    # Mid-based PnL for TP_IGNORE_VWAP mode
    mid_pnl_ratio = (main_p * (1 - cfg.TAKER_FEE_PCT) - entry_p) / entry_p
    if vwap_p and abs(main_p - vwap_p) > 0.01:
        _bid_tag = f" bid=${best_bid:.3f}" if best_bid else ""
        bot._log(
            f"[VWAP] {pos.market_slug} {entry_side}: mid=${main_p:.3f}{_bid_tag} VWAP=${vwap_p:.3f} "
            f"pnl={pnl_ratio:+.1%}", "INFO")

    # --- TP check ---
    tp_ratio = pos.tp_ratio if pos.tp_ratio > 0 else cfg.TP_RATIO
    _tp_pnl = mid_pnl_ratio if cfg.TP_IGNORE_VWAP else pnl_ratio
    if tp_ratio > 0 and _tp_pnl >= tp_ratio:
        if cfg.TP_IGNORE_VWAP:
            # Sell at best bid (skip VWAP validation)
            bot._log(
                f"[TP-MID] {pos.market_slug} {entry_side}: mid_pnl={mid_pnl_ratio:+.1%} >= "
                f"tp={tp_ratio:.0%} — selling (VWAP ignored)", "TP")
            sold = await sell_all_positions(bot, pos, max_retries, reason="TP_MID")
            if sold:
                return True
        else:
            sold = await check_combined_tp(bot, pos, tp_ratio, max_retries, clob_host)
            if sold:
                return True

    # --- TSL ---
    if cfg.TSL_ENABLED and entry_p > 0:
        half = cfg.TSL_STEP / 2
        tsl_changed = False
        if pos.tsl_level == 0 and pnl_ratio >= cfg.TSL_STEP:
            pos.tsl_level = 1
            pos.tsl_floor = half
            tsl_changed = True
        while pos.tsl_level > 0:
            next_trigger = cfg.TSL_STEP + pos.tsl_level * half
            if pnl_ratio >= next_trigger:
                pos.tsl_level += 1
                pos.tsl_floor = pos.tsl_level * half
                tsl_changed = True
            else:
                break
        if tsl_changed:
            bot._log(
                f"[TSL] {pos.market_slug} L{pos.tsl_level}: "
                f"floor -> {pos.tsl_floor:+.0%} (pnl={pnl_ratio:+.1%})", "INFO")
        if pos.tsl_floor > 0 and pnl_ratio >= 0 and pnl_ratio <= pos.tsl_floor:
            bot._log(
                f"[TSL] {pos.market_slug} FLOOR HIT: "
                f"{pnl_ratio:+.1%} <= {pos.tsl_floor:+.0%} (L{pos.tsl_level})", "WARN")
            sold = await sell_all_positions(bot, pos, max_retries, reason="TSL")
            if sold:
                return True

    # --- Time stop: exit at breakeven+ if held too long without TP ---
    if cfg.TIME_STOP_SEC > 0 and pos.entry_time > 0:
        hold_sec = time.time() - pos.entry_time
        if hold_sec >= cfg.TIME_STOP_SEC and pnl_ratio >= cfg.TIME_STOP_MIN_PNL:
            bot._log(
                f"[TIME_STOP] {pos.market_slug} {entry_side}: "
                f"held {hold_sec:.0f}s >= {cfg.TIME_STOP_SEC}s, "
                f"pnl={pnl_ratio:+.1%} >= floor {cfg.TIME_STOP_MIN_PNL:+.1%} -> EXIT",
                "WARN")
            sold = await sell_all_positions(bot, pos, max_retries, reason="TIME_STOP")
            if sold:
                return True

    # --- SL (VWAP-aware) ---
    sl_ratio = pos.sl_ratio if pos.sl_ratio > 0 else cfg.SL_RATIO
    if sl_ratio > 0:
        sold = await check_sl_vwap(bot, pos, sl_ratio, max_retries, clob_host)
        if sold:
            return True

    return False


async def check_combined_tp(bot, pos: HedgePosition, tp_pct: float,
                            max_retries: int,
                            clob_host: str = "",
                            max_slippage: float = 0.05) -> Optional[bool]:
    """If combined value vs cost exceeds tp_pct, sell both sides."""
    # Already-closed guard — see sell_all_positions for rationale
    _entry_tokens = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
    if pos.phase in ("TP_CLOSED", "RESOLVED", "CUT") or _entry_tokens < 1:
        logger.debug(
            f"[TP] {pos.market_slug} already closed "
            f"(phase={pos.phase} entry_tokens={_entry_tokens:.0f}) — skipping")
        return None
    # Concurrency guard: prevent WS handler + periodic scan from selling simultaneously
    if pos.is_selling:
        logger.warning(f"[TP] {pos.market_slug} already selling — skipping duplicate exit")
        return None
    pos.is_selling = True
    try:
        return await _check_combined_tp_inner(
            bot, pos, tp_pct, max_retries, clob_host, max_slippage)
    except Exception as e:
        logger.error(f"[TP ERROR] {pos.market_slug}: {e}")
        return None
    finally:
        pos.is_selling = False


async def _check_combined_tp_inner(bot, pos: HedgePosition, tp_pct: float,
                                   max_retries: int,
                                   clob_host: str = "",
                                   max_slippage: float = 0.05) -> Optional[bool]:
    # Grace period: skip TP sell after entry (PACE=3s, normal=config TSL_GRACE_SECONDS)
    _tp_grace = 3 if getattr(pos, 'entry_type', '') in ("FME", "PACE") else _cfg.TSL_GRACE_SECONDS
    if pos.entry_time > 0 and (time.time() - pos.entry_time) < _tp_grace:
        bot._log(f"[TP GRACE] {pos.market_slug}: entry <{_tp_grace}s ago, skipping", "INFO")
        return None

    cost = pos.total_cost()
    if cost <= 0:
        return None

    # Use VWAP for realistic profit calculation (mid overstates on thin books), minus taker fee
    _fee_mult = 1 - _cfg.TAKER_FEE_PCT
    mid_value = pos.current_value() * _fee_mult
    mid_profit = (mid_value - cost) / cost
    if mid_profit < tp_pct:
        return None

    # Fetch VWAP for entry side to get realistic fill price
    side = pos.entry_side
    tokens = pos.yes_tokens if side == "YES" else pos.no_tokens
    mid = pos.yes_price if side == "YES" else pos.no_price
    vwap_price, _ = await _fetch_entry_side_vwap(pos, clob_host)

    # Calculate VWAP-based value: entry side at VWAP, hedge side at mid (minus fee)
    if vwap_price and tokens > 0:
        hedge_tokens = pos.no_tokens if side == "YES" else pos.yes_tokens
        hedge_mid = pos.no_price if side == "YES" else pos.yes_price
        vwap_value = (tokens * vwap_price + hedge_tokens * hedge_mid) * _fee_mult
        profit_pct = (vwap_value - cost) / cost
        # Block if VWAP profit is below TP threshold
        if profit_pct < tp_pct:
            bot._log(
                f"COMBINED TP \033[33mVWAP BLOCK\033[0m {pos.market_slug}: "
                f"mid={mid_profit:+.1%} but VWAP={profit_pct:+.1%} < {tp_pct:.0%} "
                f"(mid=${mid:.3f} VWAP=${vwap_price:.3f})", "INFO")
            return None
    else:
        profit_pct = mid_profit

    # Ignore TP when OKX price is moving strongly (let position ride momentum)
    # Track peak profit; exit if profit pulls back from peak by IGNORE_TP_PULLBACK
    ignore_thresh = _cfg.IGNORE_TP_PERC.get(
        pos.market_slug.split("-")[0].upper(),
        _cfg.IGNORE_TP_PERC_DEFAULT)
    if ignore_thresh > 0 and hasattr(bot, "_okx_feed") and bot._okx_feed:
        sym = pos.market_slug.split("-")[0].upper()
        move = bot._okx_feed.get_move_pct(sym)
        if move >= ignore_thresh:
            # Update peak profit tracker
            pos.ignore_tp_peak = max(pos.ignore_tp_peak, profit_pct)
            pullback = _cfg.IGNORE_TP_PULLBACK
            exit_level = pos.ignore_tp_peak - pullback
            # Pullback exit: profit fell from peak by pullback threshold
            if exit_level > tp_pct and profit_pct < exit_level:
                bot._log(
                    f"\033[33mPULLBACK EXIT\033[0m {pos.market_slug}: "
                    f"profit={profit_pct:.1%} < peak({pos.ignore_tp_peak:.1%}) - {pullback:.0%} "
                    f"= {exit_level:.1%} | {sym} move={move:.3f}%", "ENTRY")
                # Fall through to sell below
            else:
                bot._log(
                    f"IGNORE TP {pos.market_slug}: {sym} move={move:.3f}% >= {ignore_thresh}% "
                    f"(profit={profit_pct:.1%}, peak={pos.ignore_tp_peak:.1%}) "
                    f"\033[32m{sym} moves > threshold, hold for redemption\033[0m", "INFO")
                return None

    # VWAP-confirmed TP — sell with FAK
    vwap_str = f" VWAP=${vwap_price:.3f}" if vwap_price else ""
    profit_usd = profit_pct * cost
    bot._log(
        f"COMBINED TP {pos.market_slug}: profit={profit_pct:.1%} > {tp_pct:.0%} "
        f"(${profit_usd:.2f}) | mid=${mid:.3f}{vwap_str} cost=${cost:.2f}", "ENTRY")

    addr = bot.trader.trading_address if bot.trader else ""
    total_received = 0.0

    # Only sell entry side — hedge side is left for resolution
    sell_sides = []
    side = pos.entry_side  # "YES" or "NO"
    tokens = pos.yes_tokens if side == "YES" else pos.no_tokens
    mid = pos.yes_price if side == "YES" else pos.no_price
    if tokens > 0:
        if mid <= 0.01:
            bot._log(f"  TP SKIP {side}: ${mid:.3f} <= $0.01, let resolve", "INFO")
        elif mid >= 0.99:
            bot._log(f"  TP SKIP {side}: ${mid:.3f} >= $0.99, let resolve to $1", "INFO")
        else:
            sell_sides.append((side, tokens, tokens * mid))

    # Sell all tokens at once on TP
    for side, tokens, _ in sell_sides:
        sell_qty = int(tokens)
        token_id = pos.yes_token_id if side == "YES" else pos.no_token_id
        bot._log(f"  TP SELL {side}: {sell_qty:.0f}/{tokens:.0f} tokens", "INFO")
        mid = pos.yes_price if side == "YES" else pos.no_price
        # Abort TP sell retry if WS price no longer satisfies TP
        _tp = tp_pct
        _entry = pos.yes_entry_price if side == "YES" else pos.no_entry_price
        def _tp_price_ok():
            live = pos.yes_price if side == "YES" else pos.no_price
            if live <= 0 or _entry <= 0:
                return True  # can't check, proceed
            pnl = (live - _entry) / _entry
            return pnl >= _tp
        result = await _core_exec.execute_sell(
            bot.trader, token_id, sell_qty, bot.dry_run, max_retries,
            retry_delay=0.2, trading_address=addr, mid_price=mid,
            price_check_fn=_tp_price_ok)
        if result:
            if result.get("status") == "already_sold":
                bot._log(f"  TP {side} already sold on-chain — zeroing tokens", "WARN")
                if side == "YES":
                    pos.yes_tokens = 0
                else:
                    pos.no_tokens = 0
            else:
                sold = result.get("filled_tokens", 0)
                usd = result.get("usd_received", 0)
                price = result.get("avg_price", 0)
                total_received += usd
                bot._log(
                    f"  TP SELL {side} {sold:.0f} @ ${price:.3f} = ${usd:.2f}", "ENTRY")
                if side == "YES":
                    pos.yes_tokens = max(0, pos.yes_tokens - sold)
                else:
                    pos.no_tokens = max(0, pos.no_tokens - sold)
        else:
            bot._log(f"  [TP SELL FAIL] {side} for {pos.market_slug}", "ERROR")

    # Check entry side sold (hedge side left for resolution)
    entry_tokens = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
    entry_sold = entry_tokens < 1

    if entry_sold:
        # PnL: only count entry side cost (hedge resolves separately)
        realized = total_received - pos.entry_cost
        bot.state.total_pnl += realized
        pos.phase = "TP_CLOSED"
        pos._closed_ts = time.time()
        # Cancel unfilled hedge limit orders (filled hedge tokens stay for resolution)
        if not bot.dry_run and bot.trader:
            hedge_tid = pos.no_token_id if pos.entry_side == "YES" else pos.yes_token_id
            try:
                bot.trader.clob.cancel_market_orders(asset_id=hedge_tid)
                bot._log(f"  [TP] Cancelled hedge orders for {pos.market_slug}", "INFO")
            except Exception as e:
                bot._log(f"  [TP] Hedge cancel failed for {pos.market_slug}: {e}", "WARN")
    else:
        remaining_cost = (pos.yes_tokens * pos.yes_entry_price +
                          pos.no_tokens * pos.no_entry_price)
        sold_cost = pos.entry_cost - remaining_cost
        realized = total_received - sold_cost if sold_cost > 0 else 0
        bot.state.total_pnl += realized
        bot._log(
            f"  [TP PARTIAL] {pos.market_slug}: "
            f"Y={pos.yes_tokens:.0f} N={pos.no_tokens:.0f} remaining, "
            f"will retry next tick", "WARN")

    if bot.dry_run:
        bot.current_balance += total_received

    bot._refresh_balance()
    bot._log(
        f"  TP {'DONE' if entry_sold else 'PARTIAL'} {pos.market_slug}: "
        f"received=${total_received:.2f} realized=${realized:.2f}", "ENTRY")

    if bot.mongo:
        bot.mongo.log_trade({
            "type": "V7_COMBINED_TP", "market_slug": pos.market_slug,
            "profit_pct": profit_pct, "profit_usd": realized,
            "cost": pos.entry_cost, "received": total_received,
            "fully_closed": entry_sold,
            "timestamp": time.time(),
        })

    # Add trade to dashboard for UI display
    if entry_sold and hasattr(bot, 'dashboard'):
        _entry_p = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price
        exit_price = total_received / (pos.entry_cost / _entry_p) if pos.entry_cost > 0 and _entry_p > 0 else 0
        pnl_pct = (realized / pos.entry_cost * 100) if pos.entry_cost > 0 else 0
        bot.dashboard.add_trade({
            "side": pos.entry_side,
            "entry_time": pos.entry_time,
            "exit_time": time.time(),
            "entry_price": _entry_p,
            "exit_price": round(exit_price, 4),
            "pnl_percent": round(pnl_pct, 1),
            "pnl_cash": round(realized, 2),
            "exit_reason": "COMBINED_TP",
            "market_slug": pos.market_slug,
            "window_ts": getattr(bot, '_last_window_ts', 0),
        })

    return entry_sold


async def sell_all_positions(bot, pos: HedgePosition,
                             max_retries: int, reason: str = "TP") -> bool:
    """Sell entry side of a position (hedge side left for resolution)."""
    # Already-closed guard: a prior exit path already processed this position.
    # Without this, a concurrent second exit (e.g. SL + TP_MID racing) would
    # enter `_sell_all_inner` with tokens=0, skip the sell block, but still book
    # `realized = $0 - entry_cost = -$entry_cost` as a phantom -100% loss.
    _entry_tokens = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
    if pos.phase in ("TP_CLOSED", "RESOLVED", "CUT") or _entry_tokens < 1:
        logger.debug(
            f"[{reason}] {pos.market_slug} already closed "
            f"(phase={pos.phase} entry_tokens={_entry_tokens:.0f}) — skipping")
        return False
    # Concurrency guard: prevent WS handler + periodic scan from selling simultaneously
    if pos.is_selling:
        logger.warning(f"[{reason}] {pos.market_slug} already selling — skipping duplicate exit")
        return False
    pos.is_selling = True
    try:
        return await _sell_all_inner(bot, pos, max_retries, reason)
    except Exception as e:
        logger.error(f"[{reason} ERROR] {pos.market_slug}: {e}")
        return False
    finally:
        pos.is_selling = False


async def _sell_all_inner(bot, pos: HedgePosition,
                          max_retries: int, reason: str) -> bool:
    # Grace period: skip sell for N seconds after entry (on-chain balance not yet synced)
    # Manual exit bypasses grace period — user wants out NOW
    _grace = _cfg.TSL_GRACE_SECONDS
    if reason != "MANUAL_EXIT" and pos.entry_time > 0 and (time.time() - pos.entry_time) < _grace:
        bot._log(f"[{reason} GRACE] {pos.market_slug}: entry <{_grace}s ago, skipping", "INFO")
        return False

    # Only sell entry side — hedge side is left for resolution
    addr = bot.trader.trading_address if bot.trader else ""
    total_received = 0.0
    # Snapshot entry_cost before async sell — prevents race with API sync overwriting it
    frozen_entry_cost = pos.entry_cost

    side = pos.entry_side  # "YES" or "NO"
    tokens = pos.yes_tokens if side == "YES" else pos.no_tokens
    mid = pos.yes_price if side == "YES" else pos.no_price
    if tokens > 0:
        if mid <= 0.01:
            # Near $0 — let resolve, tokens worthless
            resolve_pnl = -pos.entry_cost
            bot._log(f"  {reason} SKIP {side}: ${mid:.3f} <= $0.01, let resolve to $0", "INFO")
            pos.phase = "TP_CLOSED"
            pos._closed_ts = time.time()
            bot.state.total_pnl += resolve_pnl
            if hasattr(bot, 'dashboard'):
                bot.dashboard.add_trade({
                    "side": side, "entry_time": pos.entry_time, "exit_time": time.time(),
                    "entry_price": pos.yes_entry_price if side == "YES" else pos.no_entry_price,
                    "exit_price": 0.0,
                    "pnl_percent": -100.0, "pnl_cash": round(resolve_pnl, 2),
                    "exit_reason": f"{reason}_RESOLVE", "market_slug": pos.market_slug,
                    "window_ts": getattr(bot, '_last_window_ts', 0),
                })
            return True  # Handled — stop rechecking
        elif mid >= 0.99:
            # Near $1 — let resolve, tokens worth ~$1 each (minus taker fee)
            resolve_pnl = tokens * 1.0 * (1 - _cfg.TAKER_FEE_PCT) - pos.entry_cost
            bot._log(f"  {reason} SKIP {side}: ${mid:.3f} >= $0.99, let resolve to $1", "INFO")
            pos.phase = "TP_CLOSED"
            pos._closed_ts = time.time()
            bot.state.total_pnl += resolve_pnl
            if bot.dry_run:
                bot.current_balance += pos.entry_cost + resolve_pnl
            if hasattr(bot, 'dashboard'):
                bot.dashboard.add_trade({
                    "side": side, "entry_time": pos.entry_time, "exit_time": time.time(),
                    "entry_price": pos.yes_entry_price if side == "YES" else pos.no_entry_price,
                    "exit_price": 1.0,
                    "pnl_percent": round(resolve_pnl / pos.entry_cost * 100, 1) if pos.entry_cost > 0 else 0,
                    "pnl_cash": round(resolve_pnl, 2),
                    "exit_reason": f"{reason}_RESOLVE", "market_slug": pos.market_slug,
                    "window_ts": getattr(bot, '_last_window_ts', 0),
                })
            return True  # Handled — stop rechecking
        else:
            # SL recheck: if price recovered since SL was triggered, abort sell
            if reason == "SL":
                _fresh_mid = pos.yes_price if side == "YES" else pos.no_price
                _entry_p = pos.yes_entry_price if side == "YES" else pos.no_entry_price
                if _entry_p > 0 and _fresh_mid > 0:
                    _fresh_loss = (_entry_p - _fresh_mid) / _entry_p
                    _sl = pos.sl_ratio if pos.sl_ratio > 0 else _cfg.SL_RATIO
                    if _fresh_loss < _sl * 0.7:
                        bot._log(
                            f"  [SL CANCEL] {pos.market_slug}: price recovered "
                            f"mid=${_fresh_mid:.3f} loss={_fresh_loss:.1%} < {_sl*0.7:.1%}", "INFO")
                        return False
            # TSL recheck: if price dropped below entry since TSL was triggered, abort sell
            # Reset TSL state so it can re-trigger later; let SL handle if loss deepens
            if reason == "TSL":
                _fresh_mid = pos.yes_price if side == "YES" else pos.no_price
                _entry_p = pos.yes_entry_price if side == "YES" else pos.no_entry_price
                if _entry_p > 0 and _fresh_mid > 0:
                    _fresh_pnl = (_fresh_mid - _entry_p) / _entry_p
                    if _fresh_pnl < 0:
                        bot._log(
                            f"  [TSL ABORT] {pos.market_slug}: price below entry "
                            f"mid=${_fresh_mid:.3f} pnl={_fresh_pnl:+.1%} — resetting TSL, defer to SL",
                            "WARN")
                        pos.tsl_level = 0
                        pos.tsl_floor = 0
                        return False
            # VWAP slippage guard: block sell if fill price much worse than mid
            # Skip guard for TP_MID (user chose to ignore VWAP for TP)
            token_id = pos.yes_token_id if side == "YES" else pos.no_token_id
            vwap, _ = await _fetch_entry_side_vwap(pos, _cfg.CLOB_HOST)
            if reason != "TP_MID":
                if vwap and mid > 0.05:
                    slippage = (mid - vwap) / mid
                    if slippage > 0.20:
                        bot._log(
                            f"  [{reason} VWAP BLOCK] {side}: mid=${mid:.3f} VWAP=${vwap:.3f} "
                            f"slippage={slippage:.0%} > 20% — skipping sell", "WARN")
                        return False
                    elif slippage > 0.05:
                        bot._log(
                            f"  [{reason} VWAP WARN] {side}: mid=${mid:.3f} VWAP=${vwap:.3f} "
                            f"slippage={slippage:.0%}", "WARN")
            result = await _core_exec.execute_sell(
                bot.trader, token_id, tokens, bot.dry_run, max_retries,
                retry_delay=0.2, trading_address=addr, mid_price=vwap or mid)
            if result:
                if result.get("status") == "already_sold":
                    bot._log(f"  {reason} {side} already sold on-chain — zeroing tokens", "WARN")
                    if side == "YES":
                        pos.yes_tokens = 0
                    else:
                        pos.no_tokens = 0
                else:
                    sold = result.get("filled_tokens", 0)
                    usd = result.get("usd_received", 0)
                    price = result.get("avg_price", 0)
                    total_received += usd
                    bot._log(f"  {reason} SELL {side} {sold:.0f} @ ${price:.3f} = ${usd:.2f}", "ENTRY")
                    if side == "YES":
                        pos.yes_tokens = max(0, pos.yes_tokens - sold)
                    else:
                        pos.no_tokens = max(0, pos.no_tokens - sold)
            else:
                bot._log(f"  [{reason} SELL FAIL] {side} for {pos.market_slug}", "ERROR")

    # Apply taker fee to sell proceeds
    if _cfg.TAKER_FEE_PCT > 0 and total_received > 0:
        total_received *= (1 - _cfg.TAKER_FEE_PCT)

    # Check entry side sold (hedge side left for resolution)
    entry_tokens = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
    entry_sold = entry_tokens < 1
    # Use frozen cost to avoid race with API sync that may recalculate entry_cost
    realized = total_received - frozen_entry_cost if frozen_entry_cost > 0 else 0

    if entry_sold:
        bot.state.total_pnl += realized
        pos.phase = "TP_CLOSED"
        pos._closed_ts = time.time()

    if bot.dry_run:
        bot.current_balance += total_received

    bot._refresh_balance()
    bot._log(
        f"  {reason} {'DONE' if entry_sold else 'PARTIAL'} {pos.market_slug}: "
        f"received=${total_received:.2f} realized=${realized:.2f}", "ENTRY")

    if bot.mongo:
        _pnl_pct = (realized / frozen_entry_cost * 100) if frozen_entry_cost > 0 else 0
        _entry_p = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price
        bot.mongo.log_trade({
            "type": f"V7_{reason}", "market_slug": pos.market_slug,
            "side": pos.entry_side, "entry_type": getattr(pos, 'entry_type', ''),
            "entry_price": _entry_p,
            "profit_usd": realized, "profit_pct": round(_pnl_pct, 2),
            "cost": frozen_entry_cost, "received": total_received,
            "fully_closed": entry_sold, "timestamp": time.time(),
        })

    # Add trade to dashboard for UI display
    if entry_sold and hasattr(bot, 'dashboard'):
        pnl_pct = (realized / frozen_entry_cost * 100) if frozen_entry_cost > 0 else 0
        _entry_p = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price
        exit_price = total_received / (frozen_entry_cost / _entry_p) if frozen_entry_cost > 0 and _entry_p > 0 else 0
        bot.dashboard.add_trade({
            "side": pos.entry_side,
            "entry_time": pos.entry_time,
            "exit_time": time.time(),
            "entry_price": _entry_p,
            "exit_price": round(exit_price, 4),
            "pnl_percent": round(pnl_pct, 1),
            "pnl_cash": round(realized, 2),
            "exit_reason": reason,
            "market_slug": pos.market_slug,
            "window_ts": getattr(bot, '_last_window_ts', 0),
        })

    return entry_sold


async def cleanup_hedge_after_exit(bot, pos: HedgePosition,
                                    max_retries: int) -> None:
    """Cancel unfilled hedge limit orders and sell any filled hedge tokens."""
    if bot.dry_run or not bot.trader:
        return
    hedge_side = "NO" if pos.entry_side == "YES" else "YES"
    hedge_tid = pos.no_token_id if pos.entry_side == "YES" else pos.yes_token_id
    # Cancel unfilled hedge limit orders
    try:
        bot.trader.clob.cancel_market_orders(asset_id=hedge_tid)
        logger.info(f"[EXIT_PRICE] Cancelled hedge limit orders for {pos.market_slug}")
    except Exception:
        pass  # non-fatal: order may already be filled/expired
    # Sell any filled hedge tokens
    hedge_tokens = pos.no_tokens if pos.entry_side == "YES" else pos.yes_tokens
    hedge_mid = pos.no_price if pos.entry_side == "YES" else pos.yes_price
    if hedge_tokens > 0 and 0.01 < hedge_mid < 0.99:
        addr = bot.trader.trading_address if bot.trader else ""
        result = await _core_exec.execute_sell(
            bot.trader, hedge_tid, hedge_tokens, bot.dry_run, max_retries,
            retry_delay=0.2, trading_address=addr, mid_price=hedge_mid)
        if result and result.get("status") != "already_sold":
            sold = result.get("filled_tokens", 0)
            usd = result.get("usd_received", 0)
            price = result.get("avg_price", 0)
            logger.info(f"[EXIT_PRICE] Sold hedge {hedge_side} {sold:.0f} @ ${price:.3f} = ${usd:.2f}")
            if hedge_side == "YES":
                pos.yes_tokens = max(0, pos.yes_tokens - sold)
            else:
                pos.no_tokens = max(0, pos.no_tokens - sold)


async def check_sl_vwap(bot, pos: HedgePosition, sl_ratio: float,
                         max_retries: int, clob_host: str = "") -> Optional[bool]:
    """VWAP-aware SL: fetch orderbook bid VWAP for realistic exit price.

    Since VWAP <= mid (selling eats down bid side), this triggers SL earlier
    on thin books where actual fill price is much worse than mid.

    Returns True if sold, None if not triggered.
    """
    side = pos.entry_side
    tokens = pos.yes_tokens if side == "YES" else pos.no_tokens
    entry = pos.yes_entry_price if side == "YES" else pos.no_entry_price
    mid = pos.yes_price if side == "YES" else pos.no_price

    if tokens <= 0 or entry <= 0 or mid <= 0:
        return None

    # Quick mid check — skip if not approaching SL (saves REST call)
    mid_loss = (entry - mid) / entry
    if mid_loss < sl_ratio * 0.5:
        return None

    # Fetch orderbook for VWAP (realistic fill price)
    vwap_price = mid  # fallback if no orderbook
    if clob_host:
        token_id = pos.yes_token_id if side == "YES" else pos.no_token_id
        try:
            book = await _v7_engine.get_orderbook(clob_host, token_id)
            vwap, avail = _calc_bid_vwap(book, tokens)
            if vwap and vwap > 0:
                vwap_price = vwap
        except Exception as e:
            logger.debug(f"[SL VWAP] Orderbook fetch error: {e}")

    vwap_loss = (entry - vwap_price * (1 - _cfg.TAKER_FEE_PCT)) / entry
    if vwap_loss >= sl_ratio:
        bot._log(
            f"SL {pos.market_slug}: {side} VWAP loss={vwap_loss:+.1%} >= {sl_ratio:.0%} "
            f"(mid=${mid:.3f} VWAP=${vwap_price:.3f})", "WARN")
        return await sell_all_positions(bot, pos, max_retries, reason="SL")
    elif mid_loss >= sl_ratio * 0.7:
        # Approaching SL — log VWAP for visibility
        bot._log(
            f"[SL WATCH] {pos.market_slug}: {side} mid_loss={mid_loss:+.1%} "
            f"VWAP_loss={vwap_loss:+.1%} (VWAP=${vwap_price:.3f})", "INFO")

    return None
