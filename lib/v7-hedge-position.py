"""
V7 Hedge Position — Data classes for prediction entry + hold strategy (5m markets).

HedgePosition tracks a single-side entry (prediction-based) with optional
cheap hedge on the opposite side. Held until market resolution.

Phases: ENTERED -> HEDGED (optional) -> archived at window reset.
"""

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class HedgePosition:
    """Per-market per-window position for V7 prediction entry strategy."""
    market_slug: str
    condition_id: str
    yes_token_id: str
    no_token_id: str

    # Token balances (synced from API each tick)
    yes_tokens: float = 0.0
    yes_entry_price: float = 0.0
    no_tokens: float = 0.0
    no_entry_price: float = 0.0

    # Entry tracking
    entry_side: str = ""       # "YES" or "NO" — prediction-based entry
    entry_cost: float = 0.0    # USD spent on main entry
    hedge_cost: float = 0.0    # USD spent on hedge (opposite side)

    # State: ENTERED or HEDGED
    phase: str = "ENTERED"
    entry_time: float = 0.0
    flip_count: int = 0

    # Per-position TP/SL overrides (0 = use global config)
    tp_ratio: float = 0.0
    sl_ratio: float = 0.0
    is_dual: bool = False  # True for dual-mode opposite-side positions

    # Current prices (updated each tick from orderbook)
    yes_price: float = 0.0
    no_price: float = 0.0

    # Entry type: "" = normal prediction, "PACE" = Pace Detection, "FME" = legacy
    entry_type: str = ""

    # Peak profit % tracked when IGNORE_TP holds position (trailing pullback exit)
    ignore_tp_peak: float = 0.0

    # Stepped Trailing Stop Loss state (ratchet up only — floor never decreases)
    tsl_level: int = 0          # Current step level (0=initial, 1=first step, etc.)
    tsl_floor: float = -1.0     # Current SL floor (-1 = not active yet)

    # Concurrency guard: prevents WS handler + periodic scan from selling simultaneously
    is_selling: bool = False

    def current_value(self) -> float:
        """Mark-to-market value of all held tokens."""
        return self.yes_tokens * self.yes_price + self.no_tokens * self.no_price

    def total_cost(self) -> float:
        """Total USD spent (entry + hedge)."""
        return self.entry_cost + self.hedge_cost

    def current_pnl(self) -> float:
        return self.current_value() - self.total_cost()


@dataclass
class BotState:
    """Global bot state across all markets."""
    positions: Dict[str, HedgePosition] = field(default_factory=dict)
    total_pnl: float = 0.0
    settled_count: int = 0
    win_count: int = 0
