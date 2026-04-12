"""
WebSocket prediction client — connects to whale_dashboard WS endpoint
for zero-latency prediction updates (replaces HTTP polling).

Usage:
    client = WsPredictionClient(ws_url, username, password)
    asyncio.create_task(client.run())       # background task
    pred = client.get_prediction("BTC")     # instant read from cache
"""
import asyncio
import base64
import json
import logging
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Return type matches read_prediction_signal_http:
# (prediction, confidence, elapsed, quality, cross_pair_agreement, momentum, noise, weighted_signal)
_EMPTY = ("", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0)


class WsPredictionClient:
    """Maintains a persistent WS connection to the prediction service.

    Caches latest prediction per symbol in memory.
    Bot reads from cache (0ms) instead of HTTP (1-5s).
    Auto-reconnects on disconnect.
    """

    def __init__(self, ws_url: str, username: str, password: str,
                 max_stale_sec: float = 30.0):
        self.ws_url = ws_url
        self.username = username
        self.password = password
        self.max_stale_sec = max_stale_sec
        # Cache: {symbol: (parsed_prediction_dict, receive_timestamp)}
        self._cache: dict[str, Tuple[dict, float]] = {}
        self._connected = False
        self._task: Optional[asyncio.Task] = None

    @property
    def connected(self) -> bool:
        return self._connected

    def get_prediction(self, symbol: str = "BTC") -> Tuple[str, float, float, float, int, float, float, float]:
        """Read cached prediction for symbol. Returns same tuple as HTTP version.

        Returns empty if no data or data older than max_stale_sec.
        """
        entry = self._cache.get(symbol.upper())
        if not entry:
            return _EMPTY
        data, recv_ts = entry
        # Check staleness based on when we received the WS update
        if time.time() - recv_ts > self.max_stale_sec:
            return _EMPTY
        return self._extract_fields(data)

    def get_price_change_pct(self, symbol: str = "BTC") -> float:
        """Read live CEX price change % from cached prediction (multi-exchange combined)."""
        entry = self._cache.get(symbol.upper())
        if not entry:
            return 0.0
        data, recv_ts = entry
        if time.time() - recv_ts > self.max_stale_sec:
            return 0.0
        # price_change_pct is at top level of prediction tick
        return float(data.get("price_change_pct", 0))

    def _extract_fields(self, data: dict) -> Tuple[str, float, float, float, int, float, float, float]:
        """Extract prediction fields from parsed WS message (same format as HTTP response)."""
        if data.get("status") in ("stale", "no_data"):
            return _EMPTY
        prediction = (data.get("prediction") or "").upper()
        if prediction not in ("UP", "DOWN"):
            return _EMPTY
        confidence = float(data.get("confidence", 0))
        elapsed = float(data.get("elapsed_minutes", 0))
        raw = data.get("raw", {})
        cw = raw.get("current_window", {})
        quality = float(cw.get("quality_score", 0))
        cross_pair = int(cw.get("cross_pair_agreement", 0))
        momentum = float(cw.get("momentum_accumulated", 0))
        noise = float(cw.get("small_candle_ratio", 0))
        weighted_signal = float(cw.get("weighted_signal", 0))
        return prediction, confidence, elapsed, quality, cross_pair, momentum, noise, weighted_signal

    async def run(self):
        """Connect to WS and receive prediction updates forever (auto-reconnect)."""
        while True:
            try:
                import websockets
                # Build auth token as base64(user:pass) in query param
                token = base64.b64encode(
                    f"{self.username}:{self.password}".encode()
                ).decode()
                url = f"{self.ws_url}?token={token}"
                logger.info(f"[WS-PRED] Connecting to {self.ws_url}")
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    self._connected = True
                    logger.info("[WS-PRED] Connected — receiving prediction stream")
                    async for raw_msg in ws:
                        try:
                            msg = json.loads(raw_msg)
                            if msg.get("type") == "prediction":
                                symbol = msg.get("symbol", "BTC").upper()
                                self._cache[symbol] = (msg, time.time())
                                logger.debug(
                                    f"[WS-PRED] {symbol}: {msg.get('prediction')} "
                                    f"({msg.get('confidence', 0):.0%})")
                        except json.JSONDecodeError:
                            if raw_msg == "pong":
                                continue
                            logger.debug(f"[WS-PRED] Non-JSON message: {raw_msg[:50]}")
            except ImportError:
                logger.error("[WS-PRED] websockets package not installed. pip install websockets")
                return
            except Exception as e:
                logger.warning(f"[WS-PRED] Disconnected: {e} — reconnecting in 3s")
            finally:
                self._connected = False
            await asyncio.sleep(3)

    def start(self) -> asyncio.Task:
        """Start the WS client as a background task. Returns the task."""
        self._task = asyncio.create_task(self.run())
        return self._task
