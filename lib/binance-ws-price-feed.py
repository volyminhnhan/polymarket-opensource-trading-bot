"""
Binance WebSocket Price Feed — Live BTC/ETH/SOL/XRP pricing for V7 bot.

Drop-in replacement for okx-ws-price-feed.py. Same interface:
  get_price(), get_move_pct(), get_window_open(), get_status(), stop()

Connects to Binance public WebSocket (no auth), subscribes to mini-ticker streams.
Tracks 15-min window open prices and calculates running candle move %.

Usage:
    feed = BinancePriceFeed(symbols=["BTC", "ETH"])
    asyncio.create_task(feed.run())
    move_pct = feed.get_move_pct("BTC")  # e.g., 0.23 means 0.23%
"""

import asyncio
import json
import logging
import time

logger = logging.getLogger("scalp_v7")

# Binance stream names: symbol -> stream suffix (lowercase)
SYMBOL_TO_STREAM = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}

# Reverse lookup: stream prefix -> symbol
STREAM_TO_SYMBOL = {v: k for k, v in SYMBOL_TO_STREAM.items()}

DEFAULT_WS_URL = "wss://stream.binance.com:9443/ws"


class BinancePriceFeed:
    """Async Binance WebSocket price feed with 15-min volatility tracking.

    Same public API as OkxPriceFeed — drop-in replacement.
    """

    def __init__(self, symbols: list[str] = None, ws_url: str = DEFAULT_WS_URL):
        self.symbols = [s.upper() for s in (symbols or ["BTC", "ETH"])]
        self.ws_url = ws_url
        self._prices: dict[str, dict] = {}
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
        """Main loop — connect to Binance WS, auto-reconnect on failure."""
        self._running = True
        while self._running:
            try:
                await self._connect()
            except asyncio.CancelledError:
                self._running = False
                return
            except Exception as e:
                logger.warning(f"[BINANCE-WS] Connection error: {e} — reconnecting in 5s")
                self._prices.clear()
                await asyncio.sleep(5)

    async def _connect(self):
        """Single WebSocket connection session using combined stream."""
        try:
            import websockets
        except ImportError:
            logger.error("[BINANCE-WS] websockets package not installed. pip install websockets")
            self._running = False
            return

        streams = [SYMBOL_TO_STREAM[s] for s in self.symbols if s in SYMBOL_TO_STREAM]
        if not streams:
            logger.error(f"[BINANCE-WS] No valid symbols: {self.symbols}")
            self._running = False
            return

        # Binance combined stream URL: /stream?streams=btcusdt@miniTicker/ethusdt@miniTicker
        stream_names = "/".join(f"{s}@miniTicker" for s in streams)
        url = f"{self.ws_url}/{stream_names}"

        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            logger.info(f"[BINANCE-WS] Connected, streams: {streams}")

            while self._running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    msg = json.loads(raw)
                    self._handle_message(msg)
                except asyncio.TimeoutError:
                    # Binance sends pings automatically, just continue
                    pass
                except asyncio.CancelledError:
                    self._running = False
                    return

    def _handle_message(self, msg: dict):
        """Parse Binance miniTicker message and update prices.

        Binance miniTicker format:
          {"e": "24hrMiniTicker", "s": "BTCUSDT", "c": "67890.50", ...}
        Combined stream wraps it:
          {"stream": "btcusdt@miniTicker", "data": {"e": "...", "s": "BTCUSDT", "c": "..."}}
        """
        # Handle combined stream wrapper
        data = msg.get("data", msg)

        ticker_symbol = data.get("s", "")  # e.g. "BTCUSDT"
        last_price_str = data.get("c", "0")  # "c" = close/last price

        # Map BTCUSDT -> BTC
        prefix = ticker_symbol.replace("USDT", "").upper()
        symbol = prefix if prefix in SYMBOL_TO_STREAM else None

        if not symbol:
            return

        try:
            last_price = float(last_price_str)
        except (ValueError, TypeError):
            return

        if last_price > 0:
            self._update_price(symbol, last_price)

    def stop(self):
        """Stop the feed."""
        self._running = False
