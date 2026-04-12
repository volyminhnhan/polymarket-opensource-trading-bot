"""
V7 Oracle Gate — CEX-implied probability as early-entry accelerator.

Computes oracle YES/NO probability from Binance price vs window open using
sigmoid transform + 30s momentum prediction. Fed by V7's existing BinancePriceFeed.

No WebSocket of its own — pure computation module.

Usage:
    gate = OracleGate(k_base=200, min_diff=0.02, window_minutes=5)
    gate.update("BTC", current_price=67500, window_open=67400)
    signal = gate.get_signal("BTC", poly_yes_mid=0.52)
    if signal and signal.divergence >= 0.02:
        print(f"Oracle: {signal.direction}, div={signal.divergence:.3f}")
"""

import math
import time
from collections import deque
from typing import Dict, List, NamedTuple, Optional, Tuple


class OracleSignal(NamedTuple):
    """Oracle gate output: probability, divergence from Polymarket, direction."""
    oracle_yes: float    # CEX-implied YES probability [0, 1]
    divergence: float    # abs(oracle_yes - poly_yes_mid)
    direction: str       # "UP" | "DOWN" | "NEUTRAL"


# --- Pure functions (verbatim from v4-oracle-imbalance-engine.py) ---

def compute_oracle_yes(cex_now: float, cex_start: float, k: float) -> float:
    """Sigmoid probability: how likely price will be above open at window end."""
    if cex_start <= 0 or cex_now <= 0:
        return 0.5
    pct_change = (cex_now - cex_start) / cex_start
    exponent = -k * pct_change
    exponent = max(-500, min(500, exponent))
    return 1.0 / (1.0 + math.exp(exponent))


def predict_price(price_ticks: List[Tuple[float, float]], predict_secs: float) -> float:
    """Linear regression on recent (timestamp, price) ticks -> extrapolate forward.
    Clamped to +/-1% of last price to avoid wild extrapolation.
    """
    if len(price_ticks) < 3:
        return price_ticks[-1][1] if price_ticks else 0.0

    t0 = price_ticks[0][0]
    n = len(price_ticks)
    sum_x = sum_y = sum_xy = sum_x2 = 0.0
    for ts, price in price_ticks:
        x = ts - t0
        sum_x += x
        sum_y += price
        sum_xy += x * price
        sum_x2 += x * x

    denom = n * sum_x2 - sum_x * sum_x
    if abs(denom) < 1e-12:
        return price_ticks[-1][1]

    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n

    last_x = price_ticks[-1][0] - t0
    predicted = intercept + slope * (last_x + predict_secs)

    last_price = price_ticks[-1][1]
    max_delta = last_price * 0.01
    return max(last_price - max_delta, min(last_price + max_delta, predicted))


def time_weighted_k(k_base: float, window_ts: int, window_minutes: int) -> float:
    """Scale k by time elapsed in window. More time -> steeper sigmoid."""
    now = time.time()
    elapsed = now - window_ts
    total = window_minutes * 60
    ratio = min(max(elapsed / total, 0.0), 1.0)
    return k_base * (1.0 + 2.0 * ratio * ratio)


def compute_window_ts(window_minutes: int) -> int:
    """Current window start timestamp, aligned to window_minutes."""
    now = int(time.time())
    return now - (now % (window_minutes * 60))


# --- OracleGate class ---

class OracleGate:
    """Stateful oracle gate: accumulates ticks from BinancePriceFeed, computes signals."""

    def __init__(self, k_base: float = 200, min_diff: float = 0.02,
                 window_minutes: int = 5, momentum_window: int = 30,
                 predict_seconds: int = 30):
        self.k_base = k_base
        self.min_diff = min_diff
        self.window_minutes = window_minutes
        self.predict_seconds = predict_seconds
        self._ticks: Dict[str, deque] = {}
        self._window_open: Dict[str, float] = {}
        self._current_window_ts: int = 0
        self._momentum_window = momentum_window

    def update(self, symbol: str, price: float, window_open: float):
        """Feed latest price from BinancePriceFeed. Called each scan cycle."""
        if price <= 0:
            return
        # Window change detection — clear ticks on new window
        wts = compute_window_ts(self.window_minutes)
        if wts != self._current_window_ts:
            self._current_window_ts = wts
            self._ticks.clear()
            self._window_open.clear()

        if symbol not in self._ticks:
            self._ticks[symbol] = deque(maxlen=self._momentum_window)
        self._ticks[symbol].append((time.time(), price))
        self._window_open[symbol] = window_open if window_open > 0 else price

    def get_signal(self, symbol: str, poly_yes_mid: float) -> Optional[OracleSignal]:
        """Compute oracle signal for a symbol. Returns None if insufficient data."""
        ticks = self._ticks.get(symbol)
        if not ticks or len(ticks) < 3:
            return None
        w_open = self._window_open.get(symbol, 0)
        if w_open <= 0:
            return None

        # Momentum-predicted price (30s lookahead)
        predicted = predict_price(list(ticks), self.predict_seconds)
        # Time-weighted sigmoid steepness
        k_eff = time_weighted_k(self.k_base, self._current_window_ts, self.window_minutes)
        # Oracle probability
        oracle_yes = compute_oracle_yes(predicted, w_open, k_eff)

        # Divergence from Polymarket
        divergence = abs(oracle_yes - poly_yes_mid) if poly_yes_mid > 0 else 0.0

        # Direction classification
        if oracle_yes > 0.51:
            direction = "UP"
        elif oracle_yes < 0.49:
            direction = "DOWN"
        else:
            direction = "NEUTRAL"

        return OracleSignal(
            oracle_yes=round(oracle_yes, 4),
            divergence=round(divergence, 4),
            direction=direction,
        )
