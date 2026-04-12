"""
Polymarket CLOB WebSocket Orderbook Feed — Real-time best bid/ask for fast enforce-price entry.

Connects to Polymarket CLOB WebSocket, subscribes to market channels by token ID,
and tracks best bid/ask prices with low latency (vs 2s REST polling).

Usage:
    feed = PolymarketOrderbookFeed()
    asyncio.create_task(feed.run())
    await feed.subscribe(["token_id_1", "token_id_2"])
    best_ask = feed.get_best_ask("token_id_1")  # e.g., 0.73
    mid = feed.get_mid("token_id_1")            # e.g., 0.725
"""

import asyncio
import json
import logging
import time

logger = logging.getLogger("scalp_v5")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
MAX_BACKOFF = 5  # seconds — fast reconnect for real-time SL/TP


class PolymarketOrderbookFeed:
    """Async Polymarket CLOB WebSocket feed — streams best bid/ask per token."""

    def __init__(self, ws_url: str = WS_URL, on_price_update=None):
        self.ws_url = ws_url
        # token_id -> {"best_bid": float, "best_ask": float, "mid": float}
        self._prices: dict[str, dict] = {}
        self._subscribed_tokens: set[str] = set()
        self._pending_tokens: set[str] = set()  # tokens to subscribe on next connect
        self._ws = None
        self._running = False
        self._on_price_update = on_price_update

    def get_best_ask(self, token_id: str) -> float:
        """Get latest best ask for token. Returns 0 if no data."""
        data = self._prices.get(token_id)
        return data.get("best_ask", 0.0) if data else 0.0

    def get_best_bid(self, token_id: str) -> float:
        """Get latest best bid for token. Returns 0 if no data."""
        data = self._prices.get(token_id)
        return data.get("best_bid", 0.0) if data else 0.0

    def get_mid(self, token_id: str) -> float:
        """Get latest mid price for token. Returns 0 if no data."""
        data = self._prices.get(token_id)
        return data.get("mid", 0.0) if data else 0.0

    def get_prices(self, token_id: str) -> tuple[float, float, float]:
        """Returns (best_bid, best_ask, mid). All 0 if no data."""
        data = self._prices.get(token_id)
        if not data:
            return 0.0, 0.0, 0.0
        return data.get("best_bid", 0.0), data.get("best_ask", 0.0), data.get("mid", 0.0)

    def get_last_update_ts(self, token_id: str) -> float:
        """Returns monotonic timestamp of last price update for token. 0 if never updated."""
        data = self._prices.get(token_id)
        return data.get("_ts", 0.0) if data else 0.0

    def clear_subscriptions(self):
        """Remove all tracked subscriptions (call between windows to prevent stale tokens)."""
        self._subscribed_tokens.clear()
        self._pending_tokens.clear()
        self._prices.clear()

    async def subscribe(self, token_ids: list[str]):
        """Subscribe to additional token IDs. Sends subscribe message if connected."""
        new_tokens = [t for t in token_ids if t not in self._subscribed_tokens]
        if not new_tokens:
            return
        self._pending_tokens.update(new_tokens)
        self._subscribed_tokens.update(new_tokens)
        # If already connected, subscribe immediately
        if self._ws is not None:
            try:
                msg = json.dumps({"assets_ids": new_tokens, "type": "market"})
                await self._ws.send(msg)
                logger.info(f"[PM-WS] Subscribed {len(new_tokens)} token(s) (live)")
            except Exception as e:
                logger.warning(f"[PM-WS] Subscribe send failed: {e} — will retry on reconnect")

    async def run(self):
        """Main loop — connect to Polymarket WS, auto-reconnect on failure."""
        self._running = True
        backoff = 1
        while self._running:
            try:
                await self._connect()
                backoff = 1  # reset on clean exit
            except asyncio.CancelledError:
                self._running = False
                return
            except Exception as e:
                logger.warning(f"[PM-WS] Connection error: {e} — reconnecting in {backoff}s")
                self._prices.clear()  # mark data stale on disconnect
                self._ws = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

    async def _connect(self):
        """Single WebSocket connection session."""
        try:
            import websockets
        except ImportError:
            logger.error("[PM-WS] websockets package not installed. pip install websockets")
            self._running = False
            return

        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
            self._ws = ws
            logger.info("[PM-WS] Connected to Polymarket CLOB WebSocket")

            # Subscribe all tracked tokens on (re)connect
            all_tokens = list(self._subscribed_tokens)
            if all_tokens:
                msg = json.dumps({"assets_ids": all_tokens, "type": "market"})
                await ws.send(msg)
                logger.info(f"[PM-WS] Subscribed {len(all_tokens)} token(s)")

            while self._running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    self._handle_message(raw)
                except asyncio.TimeoutError:
                    # Send ping to keep alive
                    await ws.ping()
                except asyncio.CancelledError:
                    self._running = False
                    return

        self._ws = None

    def _handle_message(self, raw: str):
        """Parse Polymarket book update and extract best bid/ask."""
        try:
            # Messages can be a list of events or a single event
            data = json.loads(raw)
            events = data if isinstance(data, list) else [data]
            for event in events:
                event_type = event.get("event_type") or event.get("type", "")
                if event_type not in ("book", "price_change"):
                    continue
                asset_id = event.get("asset_id", "")
                if not asset_id:
                    continue
                self._update_prices(asset_id, event)
        except Exception as e:
            logger.debug(f"[PM-WS] Message parse error: {e}")

    def _update_prices(self, token_id: str, event: dict):
        """Extract best bid/ask from book or price_change event."""
        bids = event.get("bids", [])
        asks = event.get("asks", [])

        # book events have lists of {price, size} dicts; find best
        best_bid = 0.0
        best_ask = 0.0

        if bids:
            try:
                best_bid = max(float(b["price"]) for b in bids if float(b.get("size", 0)) > 0)
            except (ValueError, KeyError):
                pass

        if asks:
            try:
                best_ask = min(float(a["price"]) for a in asks if float(a.get("size", 0)) > 0)
            except (ValueError, KeyError):
                pass

        # price_change events may use top-level price field (tick update)
        if not best_bid and not best_ask:
            price = float(event.get("price", 0) or 0)
            side = event.get("side", "").upper()
            if price > 0:
                if side == "BUY":
                    best_bid = price
                elif side == "SELL":
                    best_ask = price

        if best_bid <= 0 and best_ask <= 0:
            return  # no usable price data

        prev = self._prices.get(token_id, {})
        # Preserve existing values if event only has one side
        new_bid = best_bid if best_bid > 0 else prev.get("best_bid", 0.0)
        new_ask = best_ask if best_ask > 0 else prev.get("best_ask", 0.0)
        mid = (new_bid + new_ask) / 2 if new_bid > 0 and new_ask > 0 else (new_bid or new_ask)

        self._prices[token_id] = {"best_bid": new_bid, "best_ask": new_ask, "mid": mid, "_ts": time.monotonic()}

        if self._on_price_update:
            try:
                self._on_price_update(token_id, new_bid, new_ask, mid)
            except Exception as e:
                logger.debug(f"[PM-WS] on_price_update callback error: {e}")

    def set_on_price_update(self, fn):
        """Set or replace the price-update callback (for post-init registration)."""
        self._on_price_update = fn

    @property
    def is_connected(self) -> bool:
        """True if WS connection is active."""
        return self._ws is not None and self._running

    async def force_reconnect(self):
        """Close WS to trigger auto-reconnect with fresh book snapshot."""
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
            self._prices.clear()

    def stop(self):
        """Stop the feed."""
        self._running = False
