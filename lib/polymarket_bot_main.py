#!/usr/bin/env python3
"""
Polymarket BTC 15m trading helper (latest py-clob-client, Jan 2026+)

Features:
- Fetch market by URL or slug (Gamma API)
- Get best price / midpoint (CLOB)
- Execute trade as MARKET ORDER by USDC amount (supports $1 test)
- Check positions + PnL (Data API /positions)

Install:
  pip install -U py-clob-client python-dotenv httpx eth-account

Env (.env):
  PRIVATE_KEY=0x...
  FUNDER_ADDRESS=0x...        # REQUIRED for proxy wallets (Magic/email, Safe, etc.)
  SIGNATURE_TYPE=0            # 0=EOA, 1=Magic/email, 2=Proxy/Safe (see py-clob-client docs)
  CLOB_HOST=https://clob.polymarket.com
  GAMMA_HOST=https://gamma-api.polymarket.com
  DATA_HOST=https://data-api.polymarket.com
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

import httpx
from dotenv import load_dotenv
from eth_account import Account

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    BalanceAllowanceParams,
    AssetType,
    MarketOrderArgs,
    OrderArgs,
    OrderType,
)
from py_clob_client.order_builder.constants import BUY


# -------------------------
# Models
# -------------------------

@dataclass(frozen=True)
class MarketRef:
    slug: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    min_tick_size: Optional[float] = None
    neg_risk: Optional[bool] = None
    title: Optional[str] = None
    end_date: Optional[str] = None


# -------------------------
# Helpers
# -------------------------

def die(msg: str, code: int = 1) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise SystemExit(code)


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return float(v)


def extract_slug_from_url(url_or_slug: str) -> str:
    # Accept:
    # - full URL: https://polymarket.com/event/btc-updown-15m-1768638600
    # - slug: btc-updown-15m-1768638600
    s = url_or_slug.strip()
    if s.startswith("http://") or s.startswith("https://"):
        m = re.search(r"/event/([^/?#]+)", s)
        if not m:
            die(f"Could not extract slug from URL: {s}")
        return m.group(1)
    return s


def normalize_sig_type(sig_type: int) -> str:
    return {0: "EOA", 1: "Magic/email", 2: "Proxy/Safe"}.get(sig_type, f"Unknown({sig_type})")


# -------------------------
# Polymarket client wrapper
# -------------------------

class PolymarketTrader:
    def __init__(
        self,
        private_key: str,
        signature_type: int,
        funder_address: Optional[str],
        clob_host: str,
        gamma_host: str,
        data_host: str,
        chain_id: int = 137,
        bot_trader_address: Optional[str] = None,
    ) -> None:
        self.private_key = private_key
        self.signature_type = signature_type
        self.clob_host = clob_host
        self.gamma_host = gamma_host
        self.data_host = data_host
        self.chain_id = chain_id

        self.signing_address = Account.from_key(private_key).address

        # CRITICAL: For EOA wallets (sig_type=0), funder MUST be the signing address
        # For proxy/safe wallets (sig_type=2), funder should be the proxy address
        if funder_address:
            self.funder_address = funder_address
        elif signature_type == 0:
            # EOA wallet - funder must equal signer
            self.funder_address = self.signing_address
            logger.debug(f"EOA wallet: Setting funder to signing address: {self.funder_address}")
        else:
            self.funder_address = funder_address  # Keep None for other types (will likely fail)

        # Priority: bot_trader_address > funder_address > signing_address
        self.trading_address = bot_trader_address or self.funder_address or self.signing_address

        # L1 client (private key auth), we derive/attach L2 api creds immediately.
        # py-clob-client quickstart recommends create_or_derive_api_creds() then set_api_creds()
        logger.debug(f"Initializing ClobClient: signing={self.signing_address} funder={self.funder_address} sig_type={signature_type}")

        self.clob = ClobClient(
            clob_host,
            key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=self.funder_address,
        )
        self._api_nonce = 0  # Track current API key nonce
        self._consecutive_auth_fails = 0  # Track consecutive auth failures
        self._auth_lock = __import__("threading").Lock()
        self.clob.set_api_creds(self.clob.create_or_derive_api_creds())

    def refresh_api_creds(self, force: bool = False) -> None:
        """
        Refresh API credentials by re-deriving them.
        force=True: increment nonce to create a brand new API key
        (useful when existing key is consumed/expired server-side).
        """
        if force:
            self._api_nonce = min(self._api_nonce + 1, 10)  # Cap at 10
            logger.warning(f"[AUTH] Force-refresh: creating new API key with nonce={self._api_nonce}")
        nonce = self._api_nonce if self._api_nonce > 0 else None
        try:
            new_creds = self.clob.create_or_derive_api_creds(nonce)
            self.clob.set_api_creds(new_creds)
            if force:
                self._consecutive_auth_fails = 0  # Only reset after force-refresh
                logger.info(f"[AUTH] New API key active (nonce={self._api_nonce})")
            else:
                logger.debug("API credentials refreshed successfully")
        except Exception as e:
            logger.warning(f"Failed to refresh API creds (nonce={self._api_nonce}): {e}")

    def _handle_auth_failure(self, context: str) -> None:
        """Track consecutive auth failures; escalate to force-refresh after 2."""
        with self._auth_lock:
            self._consecutive_auth_fails += 1
            if self._consecutive_auth_fails >= 2:
                logger.warning(f"[{context}] {self._consecutive_auth_fails} consecutive auth failures — force-refreshing with new nonce")
                self.refresh_api_creds(force=True)
            else:
                self.refresh_api_creds()

    def _on_trade_success(self) -> None:
        """Reset auth failure counter after a confirmed successful trade."""
        self._consecutive_auth_fails = 0

    # -------- Market discovery (Gamma) --------

    def get_market_by_slug(self, slug: str) -> MarketRef:
        """
        Gamma Markets API is the canonical way to map slug -> conditionId + tokenIds.
        We'll try a couple of common Gamma shapes.
        """
        url_candidates = [
            f"{self.gamma_host}/markets?slug={slug}",
            f"{self.gamma_host}/markets/slug/{slug}",
        ]

        last_err: Optional[str] = None
        with httpx.Client(timeout=20) as client:
            for url in url_candidates:
                try:
                    r = client.get(url)
                    r.raise_for_status()
                    data = r.json()
                    market_obj = self._pick_gamma_market_object(data)
                    if not market_obj:
                        last_err = f"Gamma response had no market for slug={slug} (url={url})"
                        continue
                    return self._parse_gamma_market(market_obj, slug)
                except Exception as e:
                    last_err = f"{type(e).__name__}: {e}"

        die(last_err or f"Failed to fetch market for slug={slug}")

    def _pick_gamma_market_object(self, data: Any) -> Optional[Dict[str, Any]]:
        # Common shapes:
        # 1) list[market]
        # 2) {"markets":[...]}
        # 3) {"data":[...]}
        # 4) single market object
        if isinstance(data, list):
            return data[0] if data else None
        if isinstance(data, dict):
            for k in ("markets", "data", "results"):
                v = data.get(k)
                if isinstance(v, list) and v:
                    return v[0]
            # maybe already a market object
            if "conditionId" in data or "condition_id" in data:
                return data
        return None

    def _parse_gamma_market(self, m: Dict[str, Any], slug: str) -> MarketRef:
        condition_id = (
            m.get("conditionId")
            or m.get("condition_id")
            or m.get("conditionID")
        )
        if not condition_id:
            die("Gamma market missing conditionId/condition_id")

        # Token IDs appear in a few common fields:
        # - clobTokenIds: ["<yes>", "<no>"]
        # - tokens: [{... clobTokenId ...}, ...]
        yes_token, no_token = self._extract_token_ids(m)

        # Use None-safe lookups (don't use 'or' as False/0 are valid values)
        min_tick = None
        for k in ("minimum_tick_size", "minTickSize", "min_tick_size"):
            if k in m and m[k] is not None:
                min_tick = m[k]
                break

        neg_risk = None
        for k in ("negRisk", "negativeRisk", "negative_risk"):
            if k in m and m[k] is not None:
                neg_risk = m[k]
                break

        title = m.get("title") or m.get("question") or m.get("name")
        end_date = m.get("endDate") or m.get("end_date")

        # BTC 15m markets are typically neg_risk=True with tick_size=0.01
        # Default to these if not found (safer than None)
        final_neg_risk = bool(neg_risk) if neg_risk is not None else True
        final_tick_size = float(min_tick) if min_tick is not None else 0.01

        return MarketRef(
            slug=slug,
            condition_id=str(condition_id),
            yes_token_id=str(yes_token),
            no_token_id=str(no_token),
            min_tick_size=final_tick_size,
            neg_risk=final_neg_risk,
            title=title,
            end_date=end_date,
        )

    def _extract_token_ids(self, m: Dict[str, Any]) -> Tuple[str, str]:
        import json

        # Preferred: clobTokenIds (may be JSON string or list)
        for k in ("clobTokenIds", "clob_token_ids", "clobTokenIDs"):
            v = m.get(k)
            # Handle JSON string format: "[\"token1\", \"token2\"]"
            if isinstance(v, str):
                try:
                    v = json.loads(v)
                except json.JSONDecodeError:
                    continue
            if isinstance(v, list) and len(v) >= 2:
                return str(v[0]), str(v[1])

        # Alternative: tokens array with outcome mapping
        tokens = m.get("tokens") or m.get("outcomes") or m.get("outcomeTokens")
        if isinstance(tokens, list) and len(tokens) >= 2:
            # Try to map Yes/No by label if available
            yes = None
            no = None
            for t in tokens:
                if not isinstance(t, dict):
                    continue
                name = (t.get("outcome") or t.get("name") or t.get("label") or "").strip().lower()
                tid = t.get("clobTokenId") or t.get("clob_token_id") or t.get("tokenId") or t.get("token_id")
                if not tid:
                    continue
                if name in ("yes", "up"):
                    yes = str(tid)
                elif name in ("no", "down"):
                    no = str(tid)
            if yes and no:
                return yes, no

            # Fallback: take first two
            t0 = tokens[0]
            t1 = tokens[1]
            tid0 = t0.get("clobTokenId") or t0.get("tokenId") or t0.get("token_id")
            tid1 = t1.get("clobTokenId") or t1.get("tokenId") or t1.get("token_id")
            if tid0 and tid1:
                return str(tid0), str(tid1)

        die("Could not find YES/NO token ids in Gamma market payload")

    # -------- Prices / orderbook (CLOB) --------

    def get_midpoint(self, token_id: str) -> float:
        result = self.clob.get_midpoint(token_id)
        # SDK returns dict like {"mid": "0.5"} or just the value
        if isinstance(result, dict):
            return float(result.get("mid") or result.get("midpoint") or 0)
        return float(result)

    def get_best_buy_price(self, token_id: str) -> float:
        # "side='BUY'" returns best buy price (bid)
        result = self.clob.get_price(token_id, side="BUY")
        if isinstance(result, dict):
            return float(result.get("price") or 0)
        return float(result)

    def get_best_sell_price(self, token_id: str) -> float:
        # best sell price (ask)
        result = self.clob.get_price(token_id, side="SELL")
        if isinstance(result, dict):
            return float(result.get("price") or 0)
        return float(result)

    # -------- Balances (CLOB) --------

    def get_usdc_balance_and_allowance(self) -> Dict[str, Any]:
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=self.signature_type)
        return self.clob.get_balance_allowance(params)

    # -------- Positions + PnL (Data API) --------

    def get_positions_rpc(self, market: "MarketRef") -> List[Dict[str, Any]]:
        """
        Fetch positions for a specific market using Chainstack RPC directly.
        NO Data API needed - uses on-chain balance queries.

        Args:
            market: MarketRef with yes_token_id and no_token_id

        Returns:
            List of positions with balance > 0
        """
        try:
            from subgraph_positions import fetch_market_positions_rpc
            positions = fetch_market_positions_rpc(
                address=self.trading_address,
                yes_token_id=market.yes_token_id,
                no_token_id=market.no_token_id,
                condition_id=market.condition_id,
                title=market.title or market.slug  # MarketRef uses 'title', not 'question'
            )
            # Get current prices from CLOB to enrich position data
            if positions:
                for pos in positions:
                    try:
                        token_id = pos.get("asset")
                        orderbook = self.clob.get_order_book(token_id)
                        if orderbook and orderbook.bids:
                            best_bid = float(orderbook.bids[0].price)
                            pos["curPrice"] = best_bid
                            pos["currentValue"] = pos["size"] * best_bid
                    except Exception:
                        pass  # Keep default price
            return positions
        except ImportError:
            logger.warning("subgraph_positions not available")
            return []
        except Exception as e:
            logger.warning(f"RPC position fetch failed: {e}")
            return []

    def get_positions(self, condition_id: Optional[str] = None, limit: int = 20) -> Any:
        """
        Fetch positions with fallback: Subgraph first (reliable), then Data API.

        The Data API often returns 504 timeouts. Subgraph queries on-chain data directly.
        """
        # Try subgraph first (more reliable, no 504 timeouts)
        try:
            from subgraph_positions import fetch_positions_with_fallback
            positions = fetch_positions_with_fallback(self.trading_address, self.data_host, limit)
            if positions:
                # Filter by condition_id if specified
                if condition_id:
                    positions = [p for p in positions if p.get("conditionId") == condition_id]
                return positions
        except ImportError:
            logger.debug("subgraph_positions not available, using Data API")
        except Exception as e:
            logger.warning(f"Subgraph fetch failed: {e}, falling back to Data API")

        # Fallback to Data API
        params: Dict[str, Any] = {"user": self.trading_address, "limit": limit}
        if condition_id:
            params["market"] = condition_id

        max_retries = 3
        for attempt in range(max_retries):
            try:
                with httpx.Client(timeout=30) as client:
                    r = client.get(f"{self.data_host}/positions", params=params)
                    r.raise_for_status()
                    return r.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (502, 503, 504) and attempt < max_retries - 1:
                    import time
                    wait = (attempt + 1) * 5
                    logger.warning(f"Data API {e.response.status_code}, retry {attempt + 1}/{max_retries} in {wait}s...")
                    time.sleep(wait)
                    continue
                raise
            except httpx.TimeoutException:
                if attempt < max_retries - 1:
                    import time
                    wait = (attempt + 1) * 5
                    logger.warning(f"Data API timeout, retry {attempt + 1}/{max_retries} in {wait}s...")
                    time.sleep(wait)
                    continue
                raise
        return []

    # -------- Trading (CLOB) --------

    def buy_by_amount_usdc(
        self,
        token_id: str,
        amount_usdc: float,
        order_type: OrderType = OrderType.FOK,
        neg_risk: bool = True,
        tick_size: float = 0.01,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        target_tokens: int = 0,
        max_fill_price: float = 0,
    ) -> Dict[str, Any]:
        """
        Buy tokens by USDC amount using limit order API (create_order).

        Uses OrderArgs with size (tokens) to avoid py-clob-client decimal precision bug
        in create_market_order which rounds taker_amount to 4 decimals instead of 2.

        Retries on failures by refreshing orderbook.
        """
        import time
        from py_clob_client.clob_types import PartialCreateOrderOptions

        if amount_usdc <= 0:
            die("amount_usdc must be > 0")

        logger.info(f"[BUY] Starting buy_by_amount_usdc: ${amount_usdc:.2f}, order_type={order_type}, neg_risk={neg_risk}")

        last_error = None
        remaining = amount_usdc
        total_tokens_filled = 0

        for attempt in range(max_retries):
            try:
                # Refresh orderbook each attempt
                orderbook = self.clob.get_order_book(token_id)
                if not orderbook or not orderbook.asks:
                    logger.debug(f"[RETRY {attempt+1}/{max_retries}] No asks available, waiting...")
                    time.sleep(retry_delay)
                    continue

                # Filter out asks with invalid prices (empty strings, None, etc.)
                valid_asks = [a for a in orderbook.asks if a.price and str(a.price).strip()]
                if not valid_asks:
                    logger.debug(f"[RETRY {attempt+1}/{max_retries}] No valid asks, waiting...")
                    time.sleep(retry_delay)
                    continue

                # Sort asks by price (lowest first) and find fillable liquidity
                asks = sorted(valid_asks, key=lambda x: float(x.price))

                # Find best price with enough size
                best_ask = None
                for ask in asks:
                    try:
                        ask_size_usdc = float(ask.size) * float(ask.price)
                        if ask_size_usdc >= remaining * 0.5:  # At least 50% fillable
                            best_ask = ask
                            break
                    except (ValueError, TypeError):
                        continue  # Skip malformed ask

                if not best_ask:
                    # Use highest ask if no single order can fill
                    best_ask = asks[-1] if asks else None

                if not best_ask:
                    logger.debug(f"[RETRY {attempt+1}/{max_retries}] No suitable ask found")
                    time.sleep(retry_delay)
                    continue

                best_price = float(best_ask.price)
                available_size = float(best_ask.size)

                # Reject if best ask exceeds max fill price (slippage protection)
                if max_fill_price > 0 and best_price > max_fill_price:
                    logger.warning(f"[BUY] Best ask ${best_price:.3f} > max_fill ${max_fill_price:.3f} — aborting")
                    return {"error": "best_ask_exceeds_max_fill_price"}

                logger.info(f"[BUY] Best ask: {available_size:.2f} @ ${best_price} (attempt {attempt+1}/{max_retries})")

                # Adjust order size: cap at available liquidity
                order_amount = min(remaining, available_size * best_price)

                # V2.9 FIX: Polymarket API decimal precision requirements:
                # - maker_amount (USDC spent): max 2 decimals
                # - taker_amount (tokens received): max 4 decimals
                #
                # Strategy: Start with clean 2-decimal USDC, derive tokens from it.
                # This ensures maker_amount is always clean 2 decimals.

                # Step 1: Calculate target USDC and floor to 2 decimals
                target_usdc = math.floor(order_amount * 100) / 100

                # Step 2: Calculate tokens
                if target_tokens > 0:
                    # Exact token count from caller — skip floor/min enforcement
                    tokens = target_tokens
                else:
                    # USD-based: derive tokens, floor to avoid overspend
                    raw_tokens = target_usdc / best_price
                    # Stop if remaining can't cover min 5 tokens (avoid inflating order)
                    if raw_tokens < 5 and total_tokens_filled > 0:
                        logger.info(f"[BUY] Remaining ${target_usdc:.2f} < 5 tokens, stopping")
                        break
                    tokens = max(5, math.floor(raw_tokens))

                # Golden rule: round up tokens if order < $1 minimum
                if tokens * best_price < 1.0:
                    tokens = math.ceil(1.0 / best_price)

                final_usdc = tokens * best_price
                final_usdc_rounded = round(final_usdc, 2)

                # Use OrderArgs (limit order) with size in tokens
                order_args = OrderArgs(
                    token_id=token_id,
                    price=best_price,
                    size=float(tokens),  # Tokens with max 4 decimals
                    side=BUY,
                )

                # Detailed logging for debugging
                usdc_estimate = tokens * best_price
                logger.info(f"[BUY] Creating order: {tokens:.4f} tokens @ ${best_price} = ${usdc_estimate:.2f}")
                logger.debug(f"[BUY] token_id={token_id[:20]}... order_type={order_type} neg_risk={neg_risk}")

                options = PartialCreateOrderOptions(neg_risk=neg_risk, tick_size=str(tick_size))
                signed = self.clob.create_order(order_args, options)

                # Post order
                logger.debug("[BUY] Signed order created, posting to API...")
                result = self.clob.post_order(signed, order_type)
                logger.debug(f"[BUY] API Response: {result}")

                if result.get("success"):
                    making_val = result.get("makingAmount", 0)
                    taking_val = result.get("takingAmount", 0)
                    filled = float(making_val) if making_val not in (None, "", "0") else 0.0
                    tokens = float(taking_val) if taking_val not in (None, "", "0") else 0.0
                    remaining -= filled
                    total_tokens_filled += tokens
                    logger.info(f"[BUY] FILLED ${filled:.4f} ({tokens:.2f} tokens) | Remaining: ${remaining:.4f}")

                    if remaining <= 0.01:  # Consider fully filled
                        total_filled = amount_usdc - remaining
                        logger.info(f"[BUY] Fully filled: ${total_filled:.2f} ({total_tokens_filled:.2f} tokens)")
                        self._on_trade_success()
                        return {"success": True, "makingAmount": total_filled, "takingAmount": total_tokens_filled}

                    # Partial fill - continue to fill remaining
                    logger.debug(f"[BUY] Partial fill, continuing for ${remaining:.2f}")
                    time.sleep(retry_delay)
                    continue
                else:
                    last_error = result
                    logger.warning(f"[BUY] FAILED: {result.get('errorMsg', result)}")

            except Exception as e:
                last_error = str(e)
                error_msg = str(e).lower()
                logger.error(f"[BUY] Exception: {e}")

                # Check for balance/allowance issues — refresh CLOB allowance once
                if "balance" in error_msg or "allowance" in error_msg:
                    if attempt == 0:
                        try:
                            params = BalanceAllowanceParams(
                                asset_type=AssetType.COLLATERAL,
                                signature_type=self.signature_type)
                            self.clob.update_balance_allowance(params)
                            logger.info("[BUY] Refreshed CLOB allowance")
                        except Exception as ae:
                            logger.warning(f"[BUY] Allowance refresh failed: {ae}")
                    logger.warning(f"[BUY] Balance/allowance issue, retrying... ({attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue

                # FOK/FAK failure - retry with fresh orderbook
                if "fully filled" in error_msg or "fok" in error_msg or "fak" in error_msg or "no match" in error_msg:
                    logger.warning(f"[BUY] Order not filled at ${best_price}, refreshing orderbook... ({attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue

                # Invalid signature
                if "invalid signature" in error_msg:
                    logger.warning(f"[BUY] Invalid signature ({attempt+1}/{max_retries})")
                    self._handle_auth_failure("BUY")
                    time.sleep(retry_delay)
                    continue

                # Other errors
                logger.error(f"[BUY] Unknown error: {e}")
                raise

        # All retries exhausted - but check if we got partial fills
        total_filled = amount_usdc - remaining
        if total_filled > 0.50:  # Got at least $0.50 worth of fills
            logger.info(f"[BUY] Partial success: filled ${total_filled:.2f} ({total_tokens_filled:.2f} tokens) of ${amount_usdc:.2f}")
            self._on_trade_success()
            return {"success": True, "makingAmount": total_filled, "takingAmount": total_tokens_filled or total_filled, "partial": True}
        return {"success": False, "error": f"Failed after {max_retries} attempts: {last_error}"}

    def buy_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
        neg_risk: bool = True,
        tick_size: float = 0.01,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Place a limit BUY order at a SPECIFIC price.

        Unlike buy_by_amount_usdc which uses best_ask from orderbook,
        this places a resting order at the exact price specified.

        Args:
            token_id: The token to buy
            price: The limit price (will NOT fill above this price)
            size: Number of tokens to buy
            neg_risk: Market neg_risk setting
            tick_size: Market tick size
            max_retries: Number of retry attempts

        Returns:
            Result dict with success status and order details
        """
        import time
        from py_clob_client.clob_types import PartialCreateOrderOptions

        logger.info(f"[BUY LIMIT] Placing limit order: {size:.4f} tokens @ ${price:.4f}")

        # Round price to tick_size
        price = round(price / tick_size) * tick_size

        # V2.9 FIX: Polymarket API decimal precision requirements:
        # - maker_amount (USDC): max 2 decimals
        # - taker_amount (tokens): max 4 decimals
        # Strategy: Calculate USDC first (2 decimals), then derive tokens (4 decimals)
        raw_usdc = size * price
        target_usdc = math.floor(raw_usdc * 100) / 100  # Floor USDC to 2 decimals

        # Enforce $1 minimum order value
        if target_usdc < 1.0:
            target_usdc = 1.0

        # Derive tokens from clean USDC (4 decimal precision)
        tokens = math.floor(target_usdc / price * 10000) / 10000

        if tokens < 0.0001:
            return {"success": False, "error": "size_too_small"}

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                order_args = OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=float(tokens),
                    side=BUY,
                )

                options = PartialCreateOrderOptions(neg_risk=neg_risk, tick_size=str(tick_size))
                signed = self.clob.create_order(order_args, options)

                # GTC = Good Till Cancelled (resting limit order)
                result = self.clob.post_order(signed, OrderType.GTC)
                logger.info(f"[BUY LIMIT] Result: {result}")

                if result.get("success"):
                    return result
                else:
                    last_error = result.get("errorMsg", str(result))
                    logger.warning(f"[BUY LIMIT] Attempt {attempt} failed: {last_error}")

            except Exception as e:
                last_error = str(e)
                logger.error(f"[BUY LIMIT] Attempt {attempt} error: {e}")

                if "invalid signature" in str(e).lower():
                    self._handle_auth_failure("BUY LIMIT")
                    continue

            time.sleep(0.5)

        return {"success": False, "error": f"Failed after {max_retries} attempts: {last_error}"}

    def sell_position(
        self,
        token_id: str,
        size: float,
        order_type: OrderType = OrderType.FAK,  # V3.5: FAK allows partial fills (FOK fails on low liquidity)
        neg_risk: bool = False,  # Match BUY default - BTC up/down markets use neg_risk=False
        tick_size: float = 0.01,
        max_retries: int = 10,  # Increased from 3 - TP/SL exits should keep trying
        min_price: float = 0,  # V3.73: Price floor — reject sell if live best_bid < min_price (prevents selling at loss after market flip)
    ) -> Dict[str, Any]:
        """
        Sell/close position by selling tokens at best bid price.
        Includes retry logic with credential refresh for 'invalid signature' errors.

        V3.5: Changed default from FOK to FAK:
        - FOK (Fill-Or-Kill): Must fill 100% or cancel entirely - fails on low liquidity
        - FAK (Fill-And-Kill): Fill as much as possible, cancel rest - handles partial fills

        Returns raw CLOB response. For SELL: makingAmount = tokens sold, takingAmount = USDC received.
        """
        import time
        from py_clob_client.clob_types import PartialCreateOrderOptions
        from py_clob_client.order_builder.constants import SELL

        if size <= 0:
            die("size must be > 0")

        # CRITICAL: Refresh API credentials BEFORE selling
        # After a BUY, the nonce changes and old creds become invalid
        logger.debug("[SELL] Refreshing API credentials before sell...")
        self.refresh_api_creds()

        last_error = None

        for attempt in range(max_retries):
            # Initialize for error handling
            sell_size = None
            best_price = None
            available_size = None

            try:
                # Get orderbook to find best bid price
                orderbook = self.clob.get_order_book(token_id)
                if not orderbook or not orderbook.bids:
                    # No liquidity - market likely resolved, position should be redeemed instead
                    logger.warning(f"[SELL] No bids - market resolved, use redeem")
                    return {"success": False, "error": "no_bids", "message": "Market resolved - no liquidity, use redeem instead"}

                # Filter out bids with invalid prices (empty strings, None, etc.)
                valid_bids = [b for b in orderbook.bids if b.price and str(b.price).strip()]
                if not valid_bids:
                    logger.warning(f"[SELL] No valid bids for token")
                    return {"success": False, "error": "no_valid_bids", "message": "No valid bid prices"}

                # Find best (highest) bid price
                best_bid = max(valid_bids, key=lambda x: float(x.price))
                best_price = float(best_bid.price)
                available_size = float(best_bid.size)

                # V3.73: Price floor — abort if live price dropped below min_price
                # Prevents selling at a loss when market flips between snapshot and execution
                if min_price > 0 and best_price < min_price:
                    logger.warning(f"[SELL] Live bid ${best_price:.4f} < min_price ${min_price:.4f} — market moved, aborting sell")
                    return {"success": False, "error": "price_below_floor", "message": f"Live bid ${best_price:.4f} below floor ${min_price:.4f}"}

                # Sell in chunks if needed (like reference bot)
                sell_size = min(size, available_size)

                # V2.12 FIX: Polymarket API requirements:
                # 1. Minimum $1 USD per order
                # 2. Minimum 5 tokens per order
                # 3. taker_amount (USDC) = tokens * price → max 2 decimals

                # First check if we have enough tokens for minimum requirements
                min_tokens_for_1_usd = math.ceil(1.0 / best_price)
                min_tokens = max(5, min_tokens_for_1_usd)

                if size < min_tokens:
                    # Not enough tokens to meet minimums - sell all we have
                    # API may reject if below minimum, but we try anyway
                    logger.warning(f"[SELL] Size {size:.2f} below min {min_tokens} tokens, attempting anyway")
                    sell_size = size
                else:
                    # Sell in chunks if needed (like reference bot)
                    sell_size = min(size, available_size)

                # Calculate USDC and apply 2 decimal precision
                raw_usdc = sell_size * best_price
                target_usdc = math.floor(raw_usdc * 100) / 100  # Floor USDC to 2 decimals

                # Derive tokens from clean USDC (4 decimal precision)
                sell_size = math.floor(target_usdc / best_price * 10000) / 10000

                if sell_size < 0.0001:
                    logger.warning(f"[SELL] Size {sell_size:.6f} too small after rounding")
                    return {"success": False, "error": "amount_too_small"}

                # Use OrderArgs (limit order) with size in tokens
                order_args = OrderArgs(
                    token_id=token_id,
                    price=best_price,
                    size=float(sell_size),  # Tokens with max 4 decimals
                    side=SELL,
                )

                # Detailed logging
                usdc_estimate = sell_size * best_price
                logger.info(f"[SELL] Creating order: {sell_size:.4f} tokens @ ${best_price} = ${usdc_estimate:.2f} (attempt {attempt+1}/{max_retries})")
                logger.debug(f"[SELL] token_id={token_id[:20]}... order_type={order_type} neg_risk={neg_risk}")

                options = PartialCreateOrderOptions(neg_risk=neg_risk, tick_size=str(tick_size))
                signed = self.clob.create_order(order_args, options)

                # Post order
                logger.debug("[SELL] Signed order, posting to API...")
                result = self.clob.post_order(signed, order_type)

                if result.get("success"):
                    logger.info(f"[SELL] SUCCESS! {sell_size:.4f} tokens @ ${best_price}")
                    self._on_trade_success()
                    return result

                # Check for signature error in response
                error_msg = str(result.get("errorMsg", result.get("error", "")))
                logger.warning(f"[SELL] Error: {error_msg}")

                if "invalid signature" in error_msg.lower():
                    logger.warning(f"[SELL] Invalid signature ({attempt+1}/{max_retries})")
                    self._handle_auth_failure("SELL")
                    time.sleep(1)
                    continue

                last_error = result
                return result  # Non-signature error, return immediately

            except Exception as e:
                error_str = str(e).lower()
                last_error = str(e)
                logger.error(f"[SELL ORDER] Exception: {e}")

                # Detailed error diagnosis
                logger.debug(f"[DIAGNOSE] Error analysis:")
                logger.debug(f"  - Position size to sell: {size}")
                if sell_size is not None:
                    logger.debug(f"  - Attempted sell size: {sell_size}")
                    logger.debug(f"  - Best bid price: ${best_price}")
                    logger.debug(f"  - Available liquidity: {available_size} tokens")
                    logger.debug(f"  - Order value: ~${sell_size * best_price:.2f}")
                else:
                    logger.debug(f"  - Error occurred before order creation (getting orderbook)")

                # Check for "no orderbook" error - market resolved, leave for redemption
                if "no orderbook" in error_str or "orderbook exists" in error_str:
                    logger.warning(f"[DIAGNOSE] Reason: MARKET_RESOLVED - no orderbook exists")
                    logger.warning(f"[SELL ORDER] Market resolved - leave for redemption")
                    return {"success": False, "error": "no_bids", "message": "Market resolved - no orderbook, use redeem"}

                # Check for balance/allowance issues — refresh CLOB allowance once
                if "balance" in error_str or "allowance" in error_str:
                    logger.warning(f"[DIAGNOSE] Reason: BALANCE_OR_ALLOWANCE_ISSUE")
                    if attempt == 0:
                        try:
                            params = BalanceAllowanceParams(
                                asset_type=AssetType.COLLATERAL,
                                signature_type=self.signature_type)
                            self.clob.update_balance_allowance(params)
                            logger.info("[SELL] Refreshed CLOB allowance")
                        except Exception as ae:
                            logger.warning(f"[SELL] Allowance refresh failed: {ae}")
                    logger.info(f"[RETRY {attempt+1}/{max_retries}] Will retry with fresh orderbook...")
                    self.refresh_api_creds()
                    time.sleep(1)
                    continue

                # Check for invalid signature in exception
                if "invalid signature" in error_str:
                    logger.warning(f"[SELL] Invalid signature in exception ({attempt+1}/{max_retries})")
                    self._handle_auth_failure("SELL")
                    time.sleep(1)
                    continue

                # FOK/FAK failure - retry with fresh orderbook
                if "fully filled" in error_str or "fok" in error_str or "fak" in error_str or "no match" in error_str:
                    logger.warning(f"[DIAGNOSE] Reason: ORDER_NOT_FILLED - insufficient liquidity")
                    if sell_size is not None:
                        logger.debug(f"  - Wanted: {sell_size} tokens at ${best_price}")
                        logger.debug(f"  - Available at best bid: {available_size} tokens")
                    logger.info(f"[RETRY {attempt+1}/{max_retries}] Will retry with fresh orderbook...")
                    time.sleep(0.5)
                    continue

                # Other errors - don't retry
                logger.error(f"[DIAGNOSE] Reason: UNKNOWN_ERROR")
                logger.error(f"[ERROR] Sell failed: {e}")
                return {"success": False, "error": str(e)}

        # All retries exhausted
        return {"success": False, "error": f"Failed after {max_retries} attempts: {last_error}"}

    def close_position_by_condition(
        self,
        condition_id: str,
        neg_risk: bool = False,  # Match BUY default - BTC up/down markets use neg_risk=False
        tick_size: float = 0.01
    ) -> Dict[str, Any]:
        """
        Close all positions for a given market (condition_id).
        Returns summary of sell results with accurate success status.
        """
        positions = self.get_positions(condition_id=condition_id)
        if not positions:
            return {"success": False, "error": "No positions found"}

        results = []
        all_success = True
        any_no_bids = False
        total_sold = 0.0

        for pos in positions:
            size = float(pos.get("size", 0))
            if size <= 0:
                continue

            token_id = pos.get("asset")
            outcome = pos.get("outcome", "Unknown")

            logger.info(f"[CLOSE] Selling {size} {outcome} tokens...")

            try:
                result = self.sell_position(
                    token_id=token_id,
                    size=size,
                    neg_risk=neg_risk,
                    tick_size=tick_size
                )

                # Track actual success/failure
                if result.get("error") == "no_bids":
                    any_no_bids = True
                    all_success = False
                    logger.warning(f"[CLOSE] No liquidity for {outcome} - market resolved, leaving for redemption")
                elif not result.get("success"):
                    all_success = False
                    logger.error(f"[CLOSE] Sell failed for {outcome}: {result.get('error', 'unknown')}")
                else:
                    total_sold += size
                    logger.info(f"[CLOSE] Successfully sold {size} {outcome} tokens")

                results.append({"outcome": outcome, "size": size, "result": result})
            except Exception as e:
                all_success = False
                results.append({"outcome": outcome, "size": size, "error": str(e)})
                logger.error(f"[CLOSE] Exception selling {outcome}: {e}")

        return {
            "success": all_success,
            "no_bids": any_no_bids,
            "total_sold": total_sold,
            "closed": results
        }

    # -------- Convenience --------

    def token_for_side(self, market: MarketRef, side: str) -> str:
        s = side.strip().upper()
        if s in ("YES", "UP"):
            return market.yes_token_id
        if s in ("NO", "DOWN"):
            return market.no_token_id
        die("side must be one of: YES/NO (or UP/DOWN)")


# -------------------------
# CLI
# -------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="polymarket_bot_main.py",
        description="Trade / positions / pnl for a Polymarket market (latest py-clob-client).",
    )
    p.add_argument("--market", required=True, help="Market URL or slug (e.g. https://polymarket.com/event/... or btc-updown-15m-...)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("market", help="Print market details + token IDs")

    px = sub.add_parser("price", help="Print midpoint + best bid/ask for YES and NO")
    px.add_argument("--which", default="YES", choices=["YES", "NO"], help="Which token to print first")

    pos = sub.add_parser("positions", help="List open positions for this user (optionally filtered to this market)")
    pos.add_argument("--all", action="store_true", help="Show all positions (not only this market)")

    pnl = sub.add_parser("pnl", help="Print PnL summary (this market by default)")
    pnl.add_argument("--all", action="store_true", help="Summarize all positions")

    tr = sub.add_parser("trade", help="Execute a market buy by USDC amount (fixes shares-vs-dollars issues)")
    tr.add_argument("--side", required=True, choices=["YES", "NO"], help="Buy YES or NO")
    tr.add_argument("--amount", type=float, default=None, help="USDC amount to spend (e.g. 2.0)")
    tr.add_argument("--test-1usd", action="store_true", help="Force a $1.00 test transaction")
    tr.add_argument("--order-type", choices=["FOK", "FAK"], default="FOK", help="FOK=fill-or-kill, FAK=fill-and-kill")
    tr.add_argument("--yes", action="store_true", help="Skip interactive confirmation")

    cl = sub.add_parser("close", help="Close/sell position in this market")
    cl.add_argument("--yes", action="store_true", help="Skip interactive confirmation")

    mon = sub.add_parser("monitor", help="Monitor position and auto-exit based on pct_profit/pct_loss")
    mon.add_argument("--interval", type=int, default=5, help="Check interval in seconds (default: 5)")
    mon.add_argument("--take-profit", type=float, default=None, help="Override pct_profit from .env (e.g. 0.10 for 10%%)")
    mon.add_argument("--stop-loss", type=float, default=None, help="Override pct_loss from .env (e.g. -0.05 for -5%%)")

    return p


def main() -> None:
    load_dotenv()

    private_key = os.getenv("PRIVATE_KEY") or os.getenv("PK") or os.getenv("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        die("Missing PRIVATE_KEY (or PK / POLYMARKET_PRIVATE_KEY) in env")

    signature_type = int(os.getenv("SIGNATURE_TYPE") or os.getenv("POLYMARKET_SIGNATURE_TYPE") or "0")
    funder = os.getenv("FUNDER_ADDRESS") or os.getenv("POLYMARKET_FUNDER") or os.getenv("YOUR_PROXY_WALLET") or None

    # If signature_type implies proxy, strongly recommend funder
    if signature_type in (1, 2) and not funder:
        die(f"SIGNATURE_TYPE={signature_type} ({normalize_sig_type(signature_type)}) requires FUNDER_ADDRESS / POLYMARKET_FUNDER")

    clob_host = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
    gamma_host = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
    data_host = os.getenv("DATA_HOST", "https://data-api.polymarket.com")

    args = build_arg_parser().parse_args()
    slug = extract_slug_from_url(args.market)

    trader = PolymarketTrader(
        private_key=private_key,
        signature_type=signature_type,
        funder_address=funder,
        clob_host=clob_host,
        gamma_host=gamma_host,
        data_host=data_host,
    )

    market = trader.get_market_by_slug(slug)

    if args.cmd == "market":
        print("== MARKET ==")
        print(f"slug:         {market.slug}")
        print(f"title:        {market.title}")
        print(f"condition_id: {market.condition_id}")
        print(f"YES token:    {market.yes_token_id}")
        print(f"NO token:     {market.no_token_id}")
        print(f"min_tick:     {market.min_tick_size}")
        print(f"neg_risk:     {market.neg_risk}")
        print(f"end_date:     {market.end_date}")
        print()
        print("== WALLET ==")
        print(f"signature_type: {signature_type} ({normalize_sig_type(signature_type)})")
        print(f"signing_address: {trader.signing_address}")
        print(f"trading_address: {trader.trading_address}")
        return

    if args.cmd == "price":
        for name, token in [("YES", market.yes_token_id), ("NO", market.no_token_id)]:
            mid = trader.get_midpoint(token)
            bid = trader.get_best_buy_price(token)
            ask = trader.get_best_sell_price(token)
            print(f"{name}: mid={mid:.4f} bid={bid:.4f} ask={ask:.4f}")
        return

    if args.cmd in ("positions", "pnl"):
        if getattr(args, "all", False):
            positions = trader.get_positions(condition_id=None)
        else:
            positions = trader.get_positions(condition_id=market.condition_id)

        if not positions:
            print("No positions found.")
            return

        if args.cmd == "positions":
            print(f"Found {len(positions)} position(s):")
            for p in positions:
                print("-" * 60)
                print(f"title:        {p.get('title')}")
                print(f"slug:         {p.get('slug')}")
                print(f"conditionId:  {p.get('conditionId')}")
                print(f"outcome:      {p.get('outcome')}")
                print(f"size:         {p.get('size')}")
                print(f"avgPrice:     {p.get('avgPrice')}")
                print(f"curPrice:     {p.get('curPrice')}")
                print(f"cashPnl:      {p.get('cashPnl')}")
                print(f"percentPnl:   {p.get('percentPnl')}")
            return

        # pnl summary
        total_cur = 0.0
        total_init = 0.0
        total_cash = 0.0
        for p in positions:
            total_cur += float(p.get("currentValue") or 0.0)
            total_init += float(p.get("initialValue") or 0.0)
            total_cash += float(p.get("cashPnl") or 0.0)

        pct = (total_cash / total_init * 100.0) if total_init > 0 else 0.0
        scope = "ALL POSITIONS" if getattr(args, "all", False) else f"MARKET {market.slug}"
        print(f"== PNL SUMMARY ({scope}) ==")
        print(f"initialValue: ${total_init:.4f}")
        print(f"currentValue: ${total_cur:.4f}")
        print(f"cashPnl:      ${total_cash:.4f}")
        print(f"percentPnl*:  {pct:+.2f}%   (*computed from totals)")
        return

    if args.cmd == "trade":
        amount = 1.0 if args.test_1usd else args.amount
        if amount is None:
            die("Provide --amount (e.g. 2.0) or use --test-1usd")

        token_id = trader.token_for_side(market, args.side)

        order_type = OrderType.FOK if args.order_type == "FOK" else OrderType.FAK

        # basic preflight
        bal = trader.get_usdc_balance_and_allowance()
        bal_amt = float(bal.get("balance") or 0.0)
        allow_amt = float(bal.get("allowance") or 0.0)

        print("== TRADE PREFLIGHT ==")
        print(f"market: {market.slug}")
        print(f"side:   BUY {args.side}")
        print(f"amount: ${amount:.2f} USDC")
        print(f"token:  {token_id}")
        print(f"USDC balance (CLOB):    {bal_amt:.6f}")
        print(f"USDC allowance (CLOB):  {allow_amt:.6f}")
        print()

        # interactive confirmation
        if not args.yes:
            resp = input("Type 'CONFIRM' to place a REAL order: ").strip().upper()
            if resp != "CONFIRM":
                print("Cancelled.")
                return

        resp = trader.buy_by_amount_usdc(token_id=token_id, amount_usdc=amount, order_type=order_type)
        print("== ORDER RESPONSE ==")
        print(resp)
        return

    if args.cmd == "close":
        positions = trader.get_positions(condition_id=market.condition_id)
        if not positions:
            print("No positions to close.")
            return

        print("== POSITIONS TO CLOSE ==")
        for p in positions:
            print(f"  {p.get('outcome')}: {p.get('size')} tokens @ avg {p.get('avgPrice')} | PnL: {p.get('percentPnl', 0):+.2f}%")

        if not args.yes:
            resp = input("\nType 'CONFIRM' to close ALL positions: ").strip().upper()
            if resp != "CONFIRM":
                print("Cancelled.")
                return

        result = trader.close_position_by_condition(
            condition_id=market.condition_id,
            neg_risk=market.neg_risk,
            tick_size=market.min_tick_size
        )
        print("== CLOSE RESULT ==")
        print(result)
        return

    if args.cmd == "monitor":
        import time

        # PnL thresholds from args or env
        pct_profit = args.take_profit if args.take_profit is not None else env_float("pct_profit", 0.10)
        pct_loss = args.stop_loss if args.stop_loss is not None else env_float("pct_loss", -0.20)
        interval = args.interval

        print("== POSITION MONITOR ==")
        print(f"Market:       {market.slug}")
        print(f"Take profit:  {pct_profit:+.1%}")
        print(f"Stop loss:    {pct_loss:+.1%}")
        print(f"Interval:     {interval}s")
        print("-" * 50)
        print("Press Ctrl+C to stop\n")

        try:
            while True:
                positions = trader.get_positions(condition_id=market.condition_id)

                if not positions:
                    logger.info(f"[{time.strftime('%H:%M:%S')}] No position found. Waiting...")
                    time.sleep(interval)
                    continue

                for pos in positions:
                    size = float(pos.get("size", 0))
                    if size <= 0:
                        continue

                    outcome = pos.get("outcome", "?")
                    avg_price = float(pos.get("avgPrice", 0))
                    cur_price = float(pos.get("curPrice", 0))
                    cash_pnl = float(pos.get("cashPnl", 0))
                    pct_pnl = float(pos.get("percentPnl", 0)) / 100  # API returns percentage, convert to decimal
                    current_value = float(pos.get("currentValue", 0))

                    logger.info(
                        f"[{time.strftime('%H:%M:%S')}] {outcome}: "
                        f"size={size:.4f} | "
                        f"avg={avg_price:.4f} | "
                        f"cur={cur_price:.4f} | "
                        f"PnL: {pct_pnl:+.1%} (${cash_pnl:+.2f}) | "
                        f"value=${current_value:.2f}"
                    )

                    # Check exit conditions
                    if pct_pnl >= pct_profit:
                        logger.info(f"[EXIT] TAKE PROFIT triggered at {pct_pnl:+.1%} >= {pct_profit:+.1%}")
                        result = trader.close_position_by_condition(
                            condition_id=market.condition_id,
                            neg_risk=market.neg_risk,
                            tick_size=market.min_tick_size
                        )
                        logger.info(f"[EXIT] Result: {result}")
                        return

                    elif pct_pnl <= pct_loss:
                        logger.warning(f"[EXIT] STOP LOSS triggered at {pct_pnl:+.1%} <= {pct_loss:+.1%}")
                        result = trader.close_position_by_condition(
                            condition_id=market.condition_id,
                            neg_risk=market.neg_risk,
                            tick_size=market.min_tick_size
                        )
                        logger.info(f"[EXIT] Result: {result}")
                        return

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("[MONITOR] Stopped by user.")
        return


if __name__ == "__main__":
    main()
