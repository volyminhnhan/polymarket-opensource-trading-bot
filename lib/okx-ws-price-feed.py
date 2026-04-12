"""
OKX WebSocket Price Feed — Live BTC/ETH pricing for volatility-based hedge decisions.

Connects to OKX public WebSocket, subscribes to ticker channels,
tracks 15-min window open prices, and calculates running candle move %.

Usage:
    feed = OkxPriceFeed(symbols=["BTC", "ETH"])
    asyncio.create_task(feed.run())
    move_pct = feed.get_move_pct("BTC")  # e.g., 0.23 means 0.23%
"""

import asyncio
import json
import logging
import time

logger = logging.getLogger("scalp_v5")

# OKX instId mapping: symbol -> OKX instrument
SYMBOL_TO_INST = {
    "BTC": "BTC-USDT",
    "ETH": "ETH-USDT",
    "SOL": "SOL-USDT",
    "XRP": "XRP-USDT",
}

DEFAULT_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


class OkxPriceFeed:
    """Async OKX WebSocket price feed with 15-min volatility tracking."""

    def __init__(self, symbols: list[str] = None, ws_url: str = DEFAULT_WS_URL):
        self.symbols = [s.upper() for s in (symbols or ["BTC", "ETH"])]
        self.ws_url = ws_url
        self._prices: dict[str, dict] = {}  # {SYMBOL: {current, window_open, window_ts, move_pct}}
        self._running = False

    def get_move_pct(self, symbol: str) -> float:
        """Get current 15-min candle move % for a symbol. Returns 0.0 if no data."""
        data = self._prices.get(symbol.upper())
        if not data or not data.get("window_open"):
            return 0.0
        return data.get("move_pct", 0.0)

    def get_price(self, symbol: str) -> float:
        """Get current price for a symbol. Returns 0.0 if no data."""
        data = self._prices.get(symbol.upper())
        return data.get("current", 0.0) if data else 0.0

    def get_window_open(self, symbol: str) -> float:
        """Get window open price for a symbol. Returns 0.0 if no data."""
        data = self._prices.get(symbol.upper())
        return data.get("window_open", 0.0) if data else 0.0

    def get_status(self) -> dict:
        """Get status of all tracked symbols."""
        return {s: {
            "current": d.get("current", 0),
            "window_open": d.get("window_open", 0),
            "move_pct": d.get("move_pct", 0),
        } for s, d in self._prices.items()}

    def _get_window_ts(self) -> int:
        """Get current 15-min window start timestamp."""
        return (int(time.time()) // 900) * 900

    def _update_price(self, symbol: str, price: float):
        """Update price and recalculate move %."""
        now_window = self._get_window_ts()
        data = self._prices.get(symbol)

        if not data or data.get("window_ts") != now_window:
            # New window — set open price
            self._prices[symbol] = {
                "current": price,
                "window_open": price,
                "window_ts": now_window,
                "move_pct": 0.0,
            }
            return

        data["current"] = price
        if data["window_open"] > 0:
            data["move_pct"] = abs(price - data["window_open"]) / data["window_open"] * 100
        else:
            data["move_pct"] = 0.0

    async def run(self):
        """Main loop — connect to OKX WS, auto-reconnect on failure."""
        self._running = True
        while self._running:
            try:
                await self._connect()
            except asyncio.CancelledError:
                self._running = False
                return
            except Exception as e:
                logger.warning(f"[OKX-WS] Connection error: {e} — reconnecting in 5s")
                # Clear prices on disconnect so callers know data is stale
                self._prices.clear()
                await asyncio.sleep(5)

    async def _connect(self):
        """Single WebSocket connection session."""
        try:
            import websockets
        except ImportError:
            logger.error("[OKX-WS] websockets package not installed. pip install websockets")
            self._running = False
            return

        inst_ids = [SYMBOL_TO_INST[s] for s in self.symbols if s in SYMBOL_TO_INST]
        if not inst_ids:
            logger.error(f"[OKX-WS] No valid symbols: {self.symbols}")
            self._running = False
            return

        subscribe_msg = json.dumps({
            "op": "subscribe",
            "args": [{"channel": "tickers", "instId": iid} for iid in inst_ids],
        })

        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
            await ws.send(subscribe_msg)
            logger.info(f"[OKX-WS] Connected, subscribed: {inst_ids}")

            while self._running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    msg = json.loads(raw)
                    self._handle_message(msg)
                except asyncio.TimeoutError:
                    # Send ping to keep alive
                    await ws.send("ping")
                except asyncio.CancelledError:
                    self._running = False
                    return

    def _handle_message(self, msg: dict):
        """Parse OKX ticker message and update prices."""
        if "data" not in msg or "arg" not in msg:
            return
        inst_id = msg["arg"].get("instId", "")
        # Reverse lookup: instId -> symbol
        symbol = None
        for s, iid in SYMBOL_TO_INST.items():
            if iid == inst_id:
                symbol = s
                break
        if not symbol:
            return

        for tick in msg["data"]:
            last_price = float(tick.get("last", 0))
            if last_price > 0:
                self._update_price(symbol, last_price)

    def stop(self):
        """Stop the feed."""
        self._running = False
