"""
V5 Order Execution — Buy/sell wrappers matching V4's proven pattern.

Retry logic: own loop with asyncio.sleep, refresh creds per attempt,
single internal attempt per call, budget tracking for buys.
Token calc: takingAmount/makingAmount from CLOB response (V4 pattern).
"""
import asyncio
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

logger = logging.getLogger("scalp_v5")

# Shared thread pool for blocking sell/buy I/O — lets asyncio.gather truly parallelize
_sell_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sell-io")


async def execute_buy(trader, token_id: str, amount_usdc: float,
                      dry_run: bool, max_retries: int = 5,
                      retry_delay: float = 1.0,
                      target_tokens: int = 0,
                      ask_price: float = 0,
                      slippage_buffer: float = 0.95,
                      trading_address: str = "",
                      max_fill_price: float = 0) -> Optional[dict]:
    """Buy tokens. If target_tokens > 0, retry until token target filled.
    Otherwise retry until USD budget spent. Returns {filled_tokens, avg_price, usd_spent}.
    ask_price: current orderbook ask, used as initial price estimate.
    slippage_buffer: fraction of remaining to request per attempt (0.95 = 5% buffer)
      to prevent FOK overfill. Remainder caught by retry loop.
    trading_address: if provided, checks USDC balance via RPC before buying.
    max_fill_price: reject if best ask exceeds this price (0=disabled)."""
    if dry_run:
        price = ask_price if ask_price > 0 else 0.50
        tokens = target_tokens if target_tokens > 0 else amount_usdc / price
        return {"status": "simulated", "filled_tokens": tokens,
                "avg_price": price, "usd_spent": tokens * price}
    if not trader:
        return None

    try:
        from py_clob_client.clob_types import OrderType
    except ImportError:
        OrderType = None

    # Pre-check: verify USDC balance via RPC before entering retry loop
    if trading_address:
        try:
            from utils.subgraph_positions import fetch_usdc_balance
            usdc_bal = fetch_usdc_balance(trading_address)
            if usdc_bal is not None:
                if usdc_bal < 1.0:
                    logger.warning(f"[BUY] USDC balance ${usdc_bal:.2f} — insufficient, aborting")
                    return None
                if usdc_bal < amount_usdc:
                    logger.warning(f"[BUY] USDC balance ${usdc_bal:.2f} < ${amount_usdc:.2f} — reducing order")
                    amount_usdc = usdc_bal * 0.95  # 5% buffer for fees
                    if amount_usdc < 1.0:
                        logger.warning(f"[BUY] Adjusted amount ${amount_usdc:.2f} too small, aborting")
                        return None
        except Exception as e:
            logger.warning(f"[BUY] USDC balance pre-check error: {e} — proceeding")

    total_usd = 0.0
    total_tokens = 0.0
    last_price = ask_price if ask_price > 0 else 0.50

    for attempt in range(1, max_retries + 1):
        # Calculate remaining USD to spend
        if target_tokens > 0:
            remaining_tokens = target_tokens - total_tokens
            if remaining_tokens < 5:  # Polymarket min order = 5 tokens
                break
            remaining = remaining_tokens * last_price
            remaining = max(remaining, 0.50)
        else:
            remaining = amount_usdc - total_usd
            if remaining < 0.50:
                break
            # Polymarket min order = 5 tokens; skip retry if remaining
            # can't cover 5 tokens — otherwise buy_by_amount_usdc inflates
            # to 5 tokens and overspends the budget
            if remaining < 5 * last_price:
                break
        try:
            trader.refresh_api_creds()
            # Apply slippage buffer on large orders to prevent overfill
            order_amount = remaining * slippage_buffer if remaining > 5.0 else remaining
            # Pass exact token count for token-target mode
            exact_tokens = int(target_tokens - total_tokens) if target_tokens > 0 else 0
            result = trader.buy_by_amount_usdc(
                token_id=token_id,
                amount_usdc=order_amount,
                order_type=OrderType.FAK if OrderType else None,
                neg_risk=False,
                tick_size=0.01,
                max_retries=max_retries,
                retry_delay=retry_delay,
                target_tokens=exact_tokens,
                max_fill_price=max_fill_price,
            )

            # Abort immediately if best ask exceeded max_fill_price
            if result.get("error") == "best_ask_exceeds_max_fill_price":
                logger.warning(f"[BUY] Aborting — best ask exceeds max fill price")
                return None

            taking = float(result.get("takingAmount", 0) or 0)
            making = float(result.get("makingAmount", 0) or 0)

            if taking > 0 and making > 0:
                actual_price = making / taking
                actual_tokens = taking
                if actual_price > 0.99:
                    actual_tokens = making / last_price if last_price > 0 else taking
                    actual_price = last_price
                total_usd += making
                total_tokens += actual_tokens
                last_price = actual_price
                target_info = f"{total_tokens:.0f}/{target_tokens}" if target_tokens > 0 else f"${total_usd:.2f}/${amount_usdc:.2f}"
                logger.info(f"[BUY] Attempt {attempt}: filled "
                            f"{actual_tokens:.0f} @ ${actual_price:.3f} "
                            f"(${making:.2f}), total {target_info}")
                # Check if done — strict budget: stop once we've spent the target amount
                if target_tokens > 0:
                    if total_tokens >= target_tokens - 1:
                        break
                elif total_usd >= amount_usdc:
                    break
                await asyncio.sleep(retry_delay)
                continue

            if result.get("success"):
                total_usd += remaining
                total_tokens += remaining / last_price if last_price > 0 else 0
                break

            logger.warning(f"[BUY FAIL] Attempt {attempt}: "
                           f"{result.get('errorMsg', result.get('error', '?'))}")
        except Exception as e:
            logger.warning(f"[BUY ERROR] Attempt {attempt}: {e}")
            # Ghost fill detection: timeout may mean order filled on-chain
            if trading_address:
                try:
                    from utils.subgraph_positions import verify_position_balance
                    on_chain = verify_position_balance(trading_address, token_id)
                    if on_chain and on_chain > total_tokens + 1:
                        ghost = on_chain - total_tokens
                        logger.warning(f"[BUY] GHOST FILL detected: {ghost:.0f} tokens on-chain after timeout")
                        total_tokens = on_chain
                        total_usd = on_chain * last_price
                        # Wait for last attempt to settle, then re-check
                        await asyncio.sleep(2)
                        on_chain2 = verify_position_balance(trading_address, token_id)
                        if on_chain2 and on_chain2 > total_tokens + 1:
                            extra = on_chain2 - total_tokens
                            logger.warning(f"[BUY] Additional ghost fill: {extra:.0f} more tokens settled")
                            total_tokens = on_chain2
                            total_usd = on_chain2 * last_price
                        break
                except Exception as rpc_err:
                    logger.warning(f"[BUY] Ghost fill check failed: {rpc_err}")

        if attempt < max_retries:
            await asyncio.sleep(retry_delay)

    if total_usd <= 0:
        logger.warning(f"[BUY] All {max_retries} attempts failed — $0 filled")
        return None
    return {"status": "filled", "filled_tokens": total_tokens,
            "avg_price": last_price, "usd_spent": total_usd}


async def execute_sell(trader, token_id: str, tokens: float,
                       dry_run: bool, max_retries: int = 5,
                       retry_delay: float = 1.0,
                       trading_address: str = "",
                       mid_price: float = 0.50,
                       price_check_fn=None) -> Optional[dict]:
    """Sell tokens with retry for remaining. Returns {filled_tokens, avg_price, usd_received}.
    filled_tokens reflects ACTUAL sold amount, not requested.
    trading_address: if provided, checks on-chain balance via RPC between retries.
    mid_price: current mid price for value-based stop ($1 threshold).
    price_check_fn: callable() -> bool, returns False to abort retries.
        Used by TP sells to abort when price drops below entry price."""
    if dry_run:
        sim_price = mid_price if mid_price > 0 else 0.50
        return {"status": "simulated", "filled_tokens": tokens,
                "avg_price": sim_price, "usd_received": tokens * sim_price}
    if not trader:
        return None

    try:
        from py_clob_client.clob_types import OrderType
    except ImportError:
        OrderType = None

    # Import RPC balance check (V3-proven pattern)
    _verify_balance = None
    if trading_address:
        try:
            from utils.subgraph_positions import verify_position_balance
            _verify_balance = verify_position_balance
        except ImportError:
            logger.warning("[SELL] verify_position_balance not available")

    # Pre-check: verify on-chain balance before entering retry loop
    # Avoids wasting all retries on "not enough balance/allowance" when
    # in-memory token count diverges from actual on-chain state
    if _verify_balance and trading_address:
        try:
            onchain = _verify_balance(trading_address, token_id)
            if onchain is not None:
                onchain_value = onchain * mid_price
                if onchain_value < 1.0:
                    logger.info(f"[SELL] On-chain value ${onchain_value:.2f} < $1 — tokens already sold")
                    return {"status": "already_sold", "filled_tokens": tokens,
                            "avg_price": 0, "usd_received": 0}
                if abs(onchain - tokens) > 0.5:
                    logger.info(f"[SELL] Adjusted quantity: {tokens:.4f} -> {onchain:.4f} (on-chain balance)")
                    tokens = onchain
            else:
                logger.warning("[SELL] On-chain balance check failed — proceeding with in-memory count")
        except Exception as _e:
            logger.warning(f"[SELL] On-chain pre-check error: {_e} — proceeding with in-memory count")

    total_sold = 0.0
    total_usd = 0.0
    remaining = tokens

    for attempt in range(1, max_retries + 1):
        if remaining < 1:
            break
        # Abort retry if price check fails (e.g. TP sell but price dropped below entry)
        if price_check_fn and attempt > 1:
            if not price_check_fn():
                logger.warning("[SELL] Abort: price_check_fn returned False — price no longer favorable")
                break
        try:
            trader.refresh_api_creds()
            # Run blocking sell in thread pool so other markets can process in parallel
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                _sell_executor,
                functools.partial(
                    trader.sell_position,
                    token_id=token_id,
                    size=remaining,
                    order_type=OrderType.FAK if OrderType else None,
                    neg_risk=False,
                    tick_size=0.01,
                    max_retries=2,  # Low: let execute_sell handle retries with RPC balance check
                )
            )

            taking = float(result.get("takingAmount", 0) or 0)
            making = float(result.get("makingAmount", 0) or 0)

            if taking > 0:
                # Determine actual tokens sold from makingAmount if available
                if making > 0 and making != taking:
                    # makingAmount = tokens sold, takingAmount = USDC received
                    sold_this = making
                    price_this = taking / making
                else:
                    # Fallback: both equal (USDC) — estimate tokens from price
                    sold_this = remaining  # Assume full fill
                    price_this = taking / remaining if remaining > 0 else 0.50

                # Sanity check: if price is < 1 cent, sell essentially failed
                if price_this < 0.01:
                    logger.warning(f"[SELL] Attempt {attempt}: price ${price_this:.4f} "
                                   f"too low (${taking:.2f} for {sold_this:.0f} tokens), skipping")
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                    continue

                total_sold += sold_this
                total_usd += taking
                remaining = tokens - total_sold
                logger.info(f"[SELL] Attempt {attempt}: sold {sold_this:.0f} @ "
                            f"${price_this:.3f} (${taking:.2f}), "
                            f"total {total_sold:.0f}/{tokens:.0f}")
                if remaining < 1:
                    break
                await asyncio.sleep(retry_delay)
                continue

            if result.get("success"):
                total_sold = tokens
                total_usd += remaining * 0.50
                break

            logger.warning(f"[SELL FAIL] Attempt {attempt}/{max_retries}: "
                           f"{result.get('errorMsg', result.get('error', '?'))}")
        except Exception as e:
            logger.warning(f"[SELL ERROR] Attempt {attempt}/{max_retries}: {e}")

        # Check on-chain balance via RPC before retrying (also in thread pool)
        if _verify_balance and attempt < max_retries:
            loop = asyncio.get_running_loop()
            rpc_balance = await loop.run_in_executor(
                _sell_executor,
                functools.partial(_verify_balance, trading_address, token_id))
            if rpc_balance is not None:
                rpc_value = rpc_balance * mid_price
                if rpc_value < 1.0:
                    logger.info(f"[SELL] RPC value ${rpc_value:.2f} < $1 — treating as sold")
                    break
                if abs(rpc_balance - remaining) > 0.5:
                    logger.info(f"[SELL] RPC balance: {rpc_balance:.1f} "
                                f"(local: {remaining:.1f})")
                    remaining = rpc_balance

        if attempt < max_retries:
            await asyncio.sleep(retry_delay)

    # Post-sell: check on-chain balance and retry until value < $1
    # Handles ghost fills (API says fail but on-chain sold) and partial fills
    # Key insight: Polygon settlement takes 5-10s — first check must wait long enough
    if _verify_balance and trading_address:
        # Save original sell result before any phantom adjustment
        _orig_sold = total_sold
        _orig_usd = total_usd
        for _post in range(3):  # up to 3 post-sell verification rounds
            # Wait longer on first check (settlement lag) vs follow-up checks
            _wait = 4.0 if _post == 0 else 2.0
            try:
                await asyncio.sleep(_wait)
                loop = asyncio.get_running_loop()
                actual_balance = await loop.run_in_executor(
                    _sell_executor,
                    functools.partial(_verify_balance, trading_address, token_id))
                if actual_balance is None:
                    break  # RPC failed, trust what we have
                actual_value = actual_balance * mid_price
                if actual_value < 1.0:
                    # All tokens gone — sell settled successfully
                    if total_sold <= 0 and _orig_sold > 0:
                        # Phantom was detected but sell actually settled — restore original
                        logger.info(f"[SELL] Settlement confirmed: on-chain value=${actual_value:.2f}"
                                    f" — restoring original result ({_orig_sold:.0f} @ ${_orig_usd:.2f})")
                        total_sold = _orig_sold
                        total_usd = _orig_usd
                    elif total_sold <= 0:
                        logger.info(f"[SELL] Ghost fill: API reported fail but "
                                    f"on-chain value=${actual_value:.2f} < $1 — tokens sold")
                        return {"status": "already_sold", "filled_tokens": tokens,
                                "avg_price": 0, "usd_received": 0}
                    break  # Tokens gone — done
                # Tokens still on-chain (value >= $1) — adjust counts and retry sell
                actual_sold = tokens - actual_balance
                # Always trust on-chain balance over API claims (phantom fill protection)
                if total_sold > 0 and abs(actual_sold - total_sold) > 0.5:
                    logger.warning(
                        f"[SELL] PHANTOM ADJUST: claimed {total_sold:.0f} sold but "
                        f"on-chain has {actual_balance:.0f} remaining "
                        f"(actual sold: {actual_sold:.0f})")
                    if actual_sold > 0:
                        avg_price_adj = total_usd / total_sold if total_sold > 0 else 0.50
                        total_usd = actual_sold * avg_price_adj
                        total_sold = actual_sold
                    else:
                        # Nothing actually sold — reset counts, will retry or wait for settlement
                        total_usd = 0.0
                        total_sold = 0.0
                # Cancel pending open orders before retry
                remaining = actual_balance
                logger.info(f"[SELL] Post-verify retry: {remaining:.1f} tokens still on-chain")
                try:
                    trader.clob.cancel_market_orders(asset_id=token_id)
                    logger.info(f"[SELL] Cancelled pending orders for token before retry")
                except Exception as _ce:
                    logger.warning(f"[SELL] Cancel pending orders failed (non-fatal): {_ce}")
                await asyncio.sleep(1.0)  # Pause after cancel for settlement
                try:
                    trader.refresh_api_creds()
                    result = await loop.run_in_executor(
                        _sell_executor,
                        functools.partial(
                            trader.sell_position,
                            token_id=token_id, size=remaining,
                            order_type=OrderType.FAK if OrderType else None,
                            neg_risk=False, tick_size=0.01, max_retries=2))
                    taking = float(result.get("takingAmount", 0) or 0)
                    making = float(result.get("makingAmount", 0) or 0)
                    if taking > 0 and making > 0:
                        total_sold += making
                        total_usd += taking
                        logger.info(f"[SELL] Post-verify sold {making:.0f} @ "
                                    f"${taking/making:.3f} (${taking:.2f})")
                        break  # trust API result
                except Exception as _retry_err:
                    # If retry fails with balance error, first sell may be settling
                    _err_msg = str(_retry_err)
                    if "balance" in _err_msg and "matched orders" in _err_msg and _orig_sold > 0:
                        logger.info(f"[SELL] Retry blocked by matched orders — "
                                    f"first sell likely settling, waiting...")
                        # Don't break — next loop iteration will re-check on-chain
                    else:
                        logger.warning(f"[SELL] Post-verify retry error: {_retry_err}")
            except Exception as e:
                logger.warning(f"[SELL] Post-sell balance check failed: {e}")
                break

    if total_sold <= 0:
        logger.warning(f"[SELL] All attempts failed — 0 tokens sold")
        return None

    avg_price = total_usd / total_sold if total_sold > 0 else 0.50
    return {"status": "filled", "filled_tokens": total_sold,
            "avg_price": avg_price, "usd_received": total_usd}


async def execute_limit_buy(trader, token_id: str, price: float,
                            amount_usdc: float, dry_run: bool,
                            max_retries: int = 3) -> Optional[dict]:
    """Place a GTC limit buy order (fire-and-forget hedge).

    Returns result dict on success, None on failure.
    The order sits in the book until filled or market resolves.
    """
    tokens = amount_usdc / price if price > 0 else 0
    if tokens < 5:
        logger.warning(f"[LIMIT BUY] Skip: {tokens:.0f} tokens < 5 minimum")
        return None

    if dry_run:
        return {"status": "simulated", "order_id": "dry_run",
                "tokens": tokens, "price": price, "cost": amount_usdc}
    if not trader:
        return None

    try:
        trader.refresh_api_creds()
        result = trader.buy_limit_order(
            token_id=token_id, price=price, size=tokens,
            neg_risk=False, tick_size=0.01, max_retries=max_retries)
        if result and result.get("success"):
            logger.info(f"[LIMIT BUY] GTC order placed: {tokens:.0f} tokens @ ${price:.3f}")
            return result
        logger.warning(f"[LIMIT BUY] Failed: {result}")
        return None
    except Exception as e:
        logger.error(f"[LIMIT BUY ERROR] {e}")
        return None
