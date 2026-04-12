"""
V7 Prediction Hedge Strategy Engine — Core trading logic (5m markets).
Handles: orderbook, market discovery, prediction signal.
Buy/sell execution delegated to konis-core-order-execution-with-retry.py (shared).
"""
import importlib.util
import json
import logging
from pathlib import Path
from typing import Optional, Tuple
import httpx

def _import_kebab(name: str, filepath: str):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_dir = Path(__file__).resolve().parent
_core_orders = _import_kebab("v5_orders", str(_dir / "konis-core-order-execution-with-retry.py"))

logger = logging.getLogger("scalp_v7")

# Shared HTTP client — connection pooling prevents TCP exhaustion
_http_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=10, limits=httpx.Limits(max_connections=10, max_keepalive_connections=5))
    return _http_client


async def get_midpoint(clob_host: str, token_id: str) -> float:
    """Fetch midpoint price for a token — lightweight alternative to full orderbook."""
    try:
        client = _get_client()
        resp = await client.get(f"{clob_host}/midpoint",
                                params={"token_id": token_id}, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return float(data.get("mid", 0) or 0)
    except Exception as e:
        logger.debug(f"Midpoint fetch failed for {token_id[:12]}: {e}")
    return 0.0


async def get_orderbook(clob_host: str, token_id: str) -> dict:
    try:
        client = _get_client()
        resp = await client.get(f"{clob_host}/book", params={"token_id": token_id})
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"Orderbook fetch HTTP {resp.status_code} for {token_id[:12]}")
    except Exception as e:
        logger.warning(f"Failed to fetch orderbook: {type(e).__name__}: {e}")
    return {"bids": [], "asks": []}


def get_book_depth(book: dict) -> float:
    """Sum total size (USD) across all bids and asks in an order book."""
    total = 0.0
    for side_key in ("bids", "asks"):
        for entry in (book.get(side_key) or []):
            try:
                total += float(entry.get("size", 0))
            except (ValueError, TypeError):
                pass
    return total


def get_best_prices(book: dict) -> Tuple[float, float, float]:
    """Extract best bid, ask, mid from orderbook."""
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bid_prices = [float(b["price"]) for b in bids if b.get("price")]
    ask_prices = [float(a["price"]) for a in asks if a.get("price")]
    best_bid = max(bid_prices) if bid_prices else 0.0
    best_ask = min(ask_prices) if ask_prices else 1.0
    mid = (best_bid + best_ask) / 2 if best_bid > 0 else best_ask
    return best_bid, best_ask, mid


async def find_market(gamma_host: str, slug_pattern: str, window_ts: int) -> Optional[dict]:
    """Find a Polymarket 5m market by slug pattern + window timestamp."""
    slug = f"{slug_pattern}-{window_ts}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{gamma_host}/markets", params={"slug": slug})
            data = resp.json()
            if isinstance(data, list) and data:
                return data[0]
    except Exception as e:
        logger.warning(f"Failed to find market {slug}: {e}")
    return None


def parse_market_tokens(market: dict) -> Tuple[str, str]:
    """Extract YES and NO token IDs from market data."""
    clob_tokens = market.get("clobTokenIds", [])
    if isinstance(clob_tokens, str):
        clob_tokens = json.loads(clob_tokens)
    if len(clob_tokens) < 2:
        return "", ""

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = []

    yes_token_id, no_token_id = clob_tokens[0], clob_tokens[1]
    if len(outcomes) >= 2:
        for i, outcome in enumerate(outcomes):
            lower = outcome.lower() if outcome else ""
            if lower in ("yes", "up"):
                yes_token_id = clob_tokens[i]
            elif lower in ("no", "down"):
                no_token_id = clob_tokens[i]

    return yes_token_id, no_token_id


def read_prediction_signal(redis_client,
                           redis_key: str = "polymarket_latest") -> Tuple[str, float, float, float, int, float, float, float]:
    """Read prediction from Redis key.

    Returns:
        (prediction, confidence, elapsed_minutes, quality_score, cross_pair_agreement,
         momentum_accumulated, small_candle_ratio)
        prediction: "UP" or "DOWN" or "" if unavailable
    """
    if not redis_client:
        return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
    try:
        raw = redis_client.get(redis_key)
        if not raw:
            return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
        data = json.loads(raw)
        # Staleness check: Redis key has 60s TTL, but verify timestamp
        import time
        ts_str = data.get("timestamp", "")
        if ts_str:
            from datetime import datetime, timezone
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                age = time.time() - ts.timestamp()
                if age > 120:  # Stale if > 2 minutes old
                    logger.debug(f"[REDIS] Stale prediction data: {age:.0f}s old")
                    return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
            except (ValueError, TypeError):
                pass
        window = data.get("current_window", {})
        prediction = (window.get("prediction", "") or "").upper()
        if prediction not in ("UP", "DOWN"):
            return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
        confidence = float(window.get("confidence", 0))
        elapsed = float(window.get("elapsed_minutes", 0))
        quality = float(window.get("quality_score", 0))
        cross_pair_agreement = int(window.get("cross_pair_agreement", 0))
        momentum = float(window.get("momentum_accumulated", 0))
        small_candle_ratio = float(window.get("small_candle_ratio", 0))
        weighted_signal = float(window.get("weighted_signal", 0))
        return prediction, confidence, elapsed, quality, cross_pair_agreement, momentum, small_candle_ratio, weighted_signal
    except Exception as e:
        logger.warning(f"[REDIS] Failed to read prediction: {e}")
        return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0


def read_prediction_signal_http(url: str, username: str,
                                 password: str,
                                 symbol: str = "BTC",
                                 expected_window_ts: int = 0) -> Tuple[str, float, float, float, int, float, float, float]:
    """Read prediction from HTTP API.

    Constructs per-symbol URL (e.g. /prediction/SOL).
    Falls back to picking symbol from multi-symbol JSON response.
    Returns: (prediction, confidence, elapsed, quality_score, cross_pair_agreement,
              momentum_accumulated, small_candle_ratio).
    """
    if not url:
        return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
    try:
        import time
        sym = symbol.upper()
        # Per-symbol URL: append /{symbol} to base URL
        symbol_url = f"{url}/{sym}"
        t0 = time.monotonic()
        resp = httpx.get(symbol_url, auth=(username, password), timeout=5.0)
        latency_ms = (time.monotonic() - t0) * 1000
        resp.raise_for_status()
        data = resp.json()
        logger.debug(
            f"[HTTP] prediction {sym}: {resp.status_code} in {latency_ms:.0f}ms "
            f"(server={resp.headers.get('server', '?')}, "
            f"content-length={resp.headers.get('content-length', '?')})")

        # Pick per-symbol prediction from multi-symbol response (if API returns all)
        if sym in data:
            data = data[sym]
        elif isinstance(data, dict) and any(k.isupper() and len(k) <= 5 for k in data):
            # Multi-symbol response but requested symbol missing — skip
            logger.warning(f"[HTTP] No prediction for {sym}, skipping (available: {list(data.keys())})")
            return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0

        if data.get("status") == "stale":
            logger.debug(f"[HTTP] Stale prediction: age={data.get('age_seconds', '?')}s")
            return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
        if data.get("status") == "no_data":
            return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0

        prediction = (data.get("prediction") or "").upper()
        if prediction not in ("UP", "DOWN"):
            return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
        confidence = float(data.get("confidence", 0))
        elapsed = float(data.get("elapsed_minutes", 0))
        # quality_score, cross_pair_agreement, momentum, noise from raw.current_window
        raw = data.get("raw", {})
        cw = raw.get("current_window", {})
        # Reject stale predictions from previous window (race condition at boundary)
        if expected_window_ts > 0:
            api_window_ts = int(cw.get("window_start_ts", 0))
            if api_window_ts > 0 and api_window_ts != expected_window_ts:
                # For larger windows (e.g. 15m), the API returns 5m-aligned
                # timestamps. Accept if API window falls within bot's window.
                # expected_window_ts is floor-aligned to WINDOW_SECONDS.
                diff = api_window_ts - expected_window_ts
                if 0 < diff < 900:  # within 15m max window
                    pass  # API window is within bot's larger window — OK
                else:
                    logger.warning(
                        f"[HTTP] stale prediction: API window={api_window_ts} != "
                        f"bot window={expected_window_ts}, rejecting")
                    return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
        quality = float(cw.get("quality_score", 0))
        cross_pair_agreement = int(cw.get("cross_pair_agreement", 0))
        momentum = float(cw.get("momentum_accumulated", 0))
        small_candle_ratio = float(cw.get("small_candle_ratio", 0))
        weighted_signal = float(cw.get("weighted_signal", 0))
        return prediction, confidence, elapsed, quality, cross_pair_agreement, momentum, small_candle_ratio, weighted_signal
    except httpx.TimeoutException as e:
        import time as _t
        err_type = type(e).__name__  # ConnectTimeout vs ReadTimeout vs WriteTimeout vs PoolTimeout
        logger.warning(
            f"[HTTP] prediction timeout ({err_type}): {e} "
            f"(url={url}/{symbol.upper()}, timeout=5.0s)")
        return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
    except httpx.ConnectError as e:
        logger.warning(
            f"[HTTP] prediction connect error: {type(e).__name__}: {e} "
            f"(url={url}/{symbol.upper()})")
        return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0
    except Exception as e:
        logger.warning(
            f"[HTTP] prediction failed ({type(e).__name__}): {e} "
            f"(url={url}/{symbol.upper()})")
        return "", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0


# Re-export order execution from shared v5 module
execute_buy = _core_orders.execute_buy
execute_sell = _core_orders.execute_sell
execute_limit_buy = _core_orders.execute_limit_buy
