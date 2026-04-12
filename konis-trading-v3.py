#!/usr/bin/env python3
"""
Konis Polymarket Scalping Bot V3 (Dual-Side Momentum Strategy)

Strategy: Buy cheap side, maintain both sides, let positions resolve naturally.
Based on copy-trading analysis: no stop-loss, ride price movements.

Key features:
- Dual-side maintenance: Hold both YES and NO when prices allow
- Wider entry band: 0.25-0.65 (vs v2's 0.10-0.30)
- No single-side max loss (DISABLED) - let positions end naturally
- In fast moving markets, the cheap side flips → profit
- Balance profit/loss by buying the loss side at cheap prices
- Redis integration for CEX price momentum signals
"""

import argparse
import asyncio
import csv
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import httpx
import redis
from dotenv import load_dotenv


def _parse_early_args():
    """Parse CLI args before loading .env (needed for --env flag)."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env", "-e", type=str, default=None,
                        help="Path to .env config file")
    parser.add_argument("--markets", "-m", type=str, default=None,
                        help="Path to markets JSON config file")
    parser.add_argument("--log-suffix", type=str, default=None,
                        help="Suffix for log/dryrun files (e.g., 'wider-tf')")
    parser.add_argument("--begin-balance", type=float, default=None,
                        help="Override begin session balance for first window (e.g., --begin-balance=416)")
    args, _ = parser.parse_known_args()
    return args


def _get_log_suffix() -> str:
    """Get log suffix from CLI args. Auto-detect 'wider-tf' if custom markets file used."""
    if _early_args.log_suffix:
        return f"-{_early_args.log_suffix}"
    # Auto-add suffix if using custom markets file (not default scalping_markets.json)
    if _early_args.markets:
        markets_name = Path(_early_args.markets).stem.lower()
        if "long-term" in markets_name or "wider" in markets_name or "1h" in markets_name:
            return "-wider-tf"
    return ""


# Parse early to get --env before loading config
_early_args = _parse_early_args()

# Load .env BEFORE importing terminal_ui
# Use CLI --env if provided, else default to polymarket_konis root
if _early_args.env:
    _env_path = Path(_early_args.env)
else:
    _env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(_env_path)

# Ensure project root and lib/ are in sys.path
_project_root = str(Path(__file__).resolve().parent)
_lib_dir = str(Path(__file__).resolve().parent / "lib")
for _p in [_project_root, _lib_dir]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from terminal_ui import ScalpingDashboard  # type: ignore
from market_sell_processor import MarketSellProcessor  # V3.10: parallel sells

# V3: Import PM WS feed + WS prediction client (standalone v7 modules, reused directly)
_script_dir = Path(__file__).resolve().parent / "lib"
try:
    import importlib.util as _ilu
    _pm_ws_spec = _ilu.spec_from_file_location("pm_ws", str(_script_dir / "polymarket-ws-orderbook-feed.py"))
    _pm_ws_mod = _ilu.module_from_spec(_pm_ws_spec)
    _pm_ws_spec.loader.exec_module(_pm_ws_mod)
    PolymarketOrderbookFeed = _pm_ws_mod.PolymarketOrderbookFeed
except Exception:
    PolymarketOrderbookFeed = None  # type: ignore

try:
    _ws_pred_spec = _ilu.spec_from_file_location("ws_pred", str(_script_dir / "v7-ws-prediction-client.py"))
    _ws_pred_mod = _ilu.module_from_spec(_ws_pred_spec)
    _ws_pred_spec.loader.exec_module(_ws_pred_mod)
    WsPredictionClient = _ws_pred_mod.WsPredictionClient
except Exception:
    WsPredictionClient = None  # type: ignore

try:
    from subgraph_positions import verify_position_balance  # type: ignore
except Exception:  # pragma: no cover
    def verify_position_balance(*args, **kwargs):
        return None

# Reuse position fetching from list_positions (DRY)
try:
    from list_positions import fetch_positions
except Exception:  # pragma: no cover
    def fetch_positions(*args, **kwargs):
        return []

try:
    from mongo_persistence import PolymarketMongoPersistence  # type: ignore
except Exception:  # pragma: no cover
    PolymarketMongoPersistence = None  # type: ignore


# ============ Configuration ============
DRY_RUN = os.getenv("SCALPING_DRY_RUN", "false").lower() in ("true", "1", "yes")
DRYRUNS_DIR = Path(__file__).resolve().parent / "dryruns"
_default_state_dir = Path(__file__).resolve().parent / "logs" / "session_state"
SESSION_STATE_DIR = Path(os.getenv("SESSION_STATE_DIR", str(_default_state_dir)))
SESSION_STATE_DIR.mkdir(parents=True, exist_ok=True)
PERSISTENT_BALANCE_PATH = SESSION_STATE_DIR / "persistent_balance.json"
LAST_CONFIGS_PATH = SESSION_STATE_DIR / "last_configs.env"

# Polymarket API minimums (enforced globally)
POLYMARKET_MIN_TOKENS = 5
POLYMARKET_MIN_USD = 1.0

CHECK_INTERVAL = int(os.getenv("SCALPING_CHECK_INTERVAL", "2"))
SIMULATED_BALANCE = float(os.getenv("SCALPING_SIMULATED_BALANCE", "1000.0"))

# DCA tokens per buy
DCA_CHUNK_TOKENS = float(os.getenv("SCALPING_DCA_CHUNK_TOKENS", "7.0"))
MAX_POSITION_TOKENS = float(os.getenv("SCALPING_MAX_POSITION_TOKENS", "100.0"))

# V3: Max total cost per position per market (all buys: DCA, hedge, rebalance, boost)
MAX_POSITION_COST_USD = float(os.getenv("V3_MAX_POSITION_COST_USD", "100"))

# Market window duration (5 or 15 minutes)
V3_WINDOW_MINUTES = int(os.getenv("V3_WINDOW_MINUTES", "15"))
V3_WINDOW_SECONDS = V3_WINDOW_MINUTES * 60

# Enabled markets (comma-separated short names: eth,btc,sol,xrp)
# Empty = all markets in JSON are enabled
V3_ENABLED_MARKETS = [m.strip().lower() for m in
                      os.getenv("V3_ENABLED_MARKETS", "").split(",") if m.strip()]

# Legacy USD-based settings (kept for class constructor compatibility)
TRADE_CHUNK_USD = 10.0  # Not used in V3 token-based strategy
MAX_POSITION_USD = 100.0  # Not used in V3 token-based strategy

# Pending order timeout - auto-clear stale pending orders
PENDING_ORDER_TIMEOUT = float(os.getenv("SCALPING_PENDING_ORDER_TIMEOUT", "10.0"))

# V3-LEGACY: removed in DCA-to-winner refactor
# DCA_INTO_LOSERS = os.getenv("SCALPING_DCA_INTO_LOSERS", "true").lower() in ("true", "1", "yes")
# DCA_BUY_DROP_PCT = float(os.getenv("SCALPING_DCA_BUY_DROP_PCT", "0.05"))

# V3-LEGACY: removed in DCA-to-winner refactor (always dual-side now)
# ENTRY_MODE = os.getenv("SCALPING_ENTRY_MODE", "cheap").lower()
ENTRY_MODE = "dual"  # DCA-to-winner: kept for backward compat, check_entry ignores this

# V3-LEGACY: replaced by V3_BUY_BAND_HIGH
# BUY_BAND_HIGH = float(os.getenv("SCALPING_BUY_BAND_HIGH", "0.70"))

# ============ V3: DCA-to-Winner Strategy ============
# Buy both sides equally at entry, then DCA into winner + sell loser
V3_ENTRY_AMOUNT_USD = float(os.getenv("V3_ENTRY_AMOUNT_USD", "10"))       # $ per side at entry (base amount)
V3_ENTRY_LEADER_MULT = float(os.getenv("V3_ENTRY_LEADER_MULT", "1.0"))   # multiplier for leader (higher-price) side
V3_ENTRY_TRAILER_MULT = float(os.getenv("V3_ENTRY_TRAILER_MULT", "1.0")) # multiplier for trailer (lower-price) side
V3_DCA_TRIGGER_PCT = float(os.getenv("V3_DCA_TRIGGER_PCT", "0.10"))       # 10% VWAP gain triggers DCA+sell
V3_DCA_AMOUNT_USD = float(os.getenv("V3_DCA_AMOUNT_USD", "10"))           # $ per DCA buy into winner
V3_DCA_COOLDOWN_SEC = float(os.getenv("V3_DCA_COOLDOWN_SEC", "120"))      # Cooldown between DCA-WIN buys (default 2min)
V3_DCA_CUTOFF_MINUTES = float(os.getenv("V3_DCA_CUTOFF_MINUTES", "2"))    # Stop DCA in last N minutes of window (avoid late reversals)
V3_ENTRY_CUTOFF_MINUTES = float(os.getenv("V3_ENTRY_CUTOFF_MINUTES", "2"))  # V3.99: Stop new entries in last N minutes (avoid window-end wipeouts)
V3_ENTRY_MIN_CONFIDENCE = float(os.getenv("V3_ENTRY_MIN_CONFIDENCE", "0"))    # V3.99: Min prediction confidence to enter (0=disabled)
V3_ENTRY_MIN_QUALITY = float(os.getenv("V3_ENTRY_MIN_QUALITY", "0"))          # V3.99: Min quality_score to enter (0=disabled)
V3_ENTRY_MIN_MOMENTUM = float(os.getenv("V3_ENTRY_MIN_MOMENTUM", "0"))       # V3.100: Min abs(momentum) aligned with prediction direction (0=disabled)
V3_ENTRY_PREDICTION_SOURCE = os.getenv("V3_ENTRY_PREDICTION_SOURCE", "api").lower().strip()  # "api" = godeye prediction, "ml" = XGBoost model
V3_FLIP_FOLLOW_ENABLED = os.getenv("V3_FLIP_FOLLOW_ENABLED", "false").lower() in ("true", "1", "yes")
V3_FLIP_FOLLOW_MIN_PRICE = float(os.getenv("V3_FLIP_FOLLOW_MIN_PRICE", "0.40"))  # loser price range for flip mode
V3_FLIP_FOLLOW_MAX_PRICE = float(os.getenv("V3_FLIP_FOLLOW_MAX_PRICE", "0.47"))  # loser price range for flip mode
V3_DCA_DELAY_MINUTES = float(os.getenv("V3_DCA_DELAY_MINUTES", "0"))     # V3.57: Delay DCA until N minutes into window (avoid early noise/reversals)
V3_DCA_WINNER_MIN_PRICE = float(os.getenv("V3_DCA_WINNER_MIN_PRICE", "0.60"))  # Min price to declare a winner side
V3_DCA_MIN_QUALITY_SCORE = float(os.getenv("V3_DCA_MIN_QUALITY_SCORE", "0.40"))  # Min quality_score to allow DCA (0=disabled)
V3_SELL_LOSER_MIN_PRICE = float(os.getenv("V3_SELL_LOSER_MIN_PRICE", "0.25"))  # Don't sell loser below this
V3_SELL_LOSER_PCT = float(os.getenv("V3_SELL_LOSER_PCT", "0.30"))             # Sell this % of loser tokens per DCA cycle
V3_FJ_TRIGGER_PRICE = float(os.getenv("V3_FJ_TRIGGER_PRICE", "0.15"))        # Final Justification triggers when loser <= this
V3_FJ_MAX_LOSS_PCT = float(os.getenv("V3_FJ_MAX_LOSS_PCT", "0.25"))         # V3.94: relaxed from 10% to 25% — tight target caused FJ deadlock
V3_REBALANCE_MAX_LOSS_PCT = float(os.getenv("V3_REBALANCE_MAX_LOSS_PCT", "0.25"))  # V3.85: Rebalance winner floor — max loss% if winner resolves at $1 (0=disabled)
V3_REBALANCE_HEDGE_PCT = float(os.getenv("V3_REBALANCE_HEDGE_PCT", "0.0"))       # V3.94: default 0% — hedge buys bleed money, let rebalance DCA into winner instead
# V3.88: Cheap loser DCA — buy cheap loser tokens in CHOP as flip insurance
V3_CHEAP_LOSER_DCA_DROP_PCT = float(os.getenv("V3_CHEAP_LOSER_DCA_DROP_PCT", "0.37"))  # Loser must drop this % below VWAP to trigger
V3_CHEAP_LOSER_DCA_MAX = int(os.getenv("V3_CHEAP_LOSER_DCA_MAX", "3"))                 # Max cheap loser buys per session
V3_CHEAP_LOSER_DCA_AMOUNT_PCT = float(os.getenv("V3_CHEAP_LOSER_DCA_AMOUNT_PCT", "0.50"))  # % of entry amount per buy
V3_FJ_MIN_WIN_PCT = float(os.getenv("V3_FJ_MIN_WIN_PCT", "0.02"))          # V3.94: relaxed from 5% to 2% — tight target caused FJ deadlock at high winner prices
V3_DCA_LOSER_MAX_PRICE = float(os.getenv("V3_DCA_LOSER_MAX_PRICE", "0.35"))  # V3.7: Only DCA into loser when price <= this
V3_DCA_MODE = os.getenv("V3_DCA_MODE", "loser").strip().lower()              # V3.7: "winner" = current DCA-to-winner, "loser" = counter-trend DCA-to-loser
V3_FJ_MAX_TOKENS_PER_SIDE = int(os.getenv("V3_FJ_MAX_TOKENS_PER_SIDE", "200"))  # V3.49: Hard cap per side to prevent insane FJ token counts
V3_FJ_LAST_MIN = float(os.getenv("V3_FJ_LAST_MIN", os.getenv("V3_FJ_SIMULTANEOUS_LAST_MIN", "2.0")))  # V3.51: FJ only fires early if winner-wins >= TP after insurance; otherwise waits until last N minutes
V3_FJ_ENFORCE_PRICE = float(os.getenv("V3_FJ_ENFORCE_PRICE", "0"))  # V3.12: Force FJ when winner >= this price (0=disabled), bypasses time gate
V3_LOTTERY_TICKET_USD = float(os.getenv("V3_LOTTERY_TICKET_USD", "5"))         # $ reserved for lottery ticket buy of ultra-cheap tokens
V3_LOTTERY_TICKET_PRICE = float(os.getenv("V3_LOTTERY_TICKET_PRICE", "0.10"))  # Buy lottery when loser drops <= this price
V3_DCA_LOSS_CUT_PCT = float(os.getenv("V3_DCA_LOSS_CUT_PCT", "0.30"))        # Cut loss when side drops 30% from VWAP
V3_DCA_LOSS_CUT_SELL_PCT = float(os.getenv("V3_DCA_LOSS_CUT_SELL_PCT", "0.50"))  # Sell 50% of tokens on loss cut (keep rest for recovery)
V3_PROGRESSIVE_RECOUP_ENABLED = os.getenv("V3_PROGRESSIVE_RECOUP_ENABLED", "true").lower() == "true"  # V3.92: Sell cheap loser + buy winner near resolution
# V3_WINNER_TRAILING_STOP removed — caused aggressive sells on minor pullbacks
V3_MAX_REBALANCE_CYCLES = int(os.getenv("V3_WINNER_PARTIAL_TP_MAX", "3"))  # V3.74: Max rebalance sell+rebuy cycles per session (0=unlimited)
V3_RESOLVE_SKIP_SELL_PRICE = float(os.getenv("V3_RESOLVE_SKIP_SELL_PRICE", "0.93"))  # Skip TP sell if winner > this — let it resolve
V3_RESOLVE_TARGET_PRICE = float(os.getenv("V3_RESOLVE_TARGET_PRICE", "0.97"))  # Expected resolution value per winning token (lower = exit earlier)
V3_TAKER_FEE_PCT = float(os.getenv("V3_TAKER_FEE_PCT", "0.02"))  # 2% Polymarket taker fee for PnL estimation

# V3.8: Hybrid rebalance mode (V3_DCA_MODE="rebalance")
# TREND regime → DCA to winner, CHOP regime → rebalance sell+rebuy
V3_REBALANCE_GAIN_PCT = float(os.getenv("V3_REBALANCE_GAIN_PCT", "0.05"))   # Min VWAP gain (after fee) to trigger rebalance sell
V3_REBALANCE_SELL_PCT = float(os.getenv("V3_REBALANCE_SELL_PCT", "0.30"))   # % of tokens to sell per rebalance cycle
V3_REGIME_TREND_CONFIDENCE = float(os.getenv("V3_REGIME_TREND_CONFIDENCE", "0.92"))  # Min confidence for TREND regime
V3_REGIME_TREND_MOMENTUM = float(os.getenv("V3_REGIME_TREND_MOMENTUM", "0.18"))     # Min abs(momentum) for TREND regime
V3_PRED_CONFIRM_TICKS = int(os.getenv("V3_PRED_CONFIRM_TICKS", "3"))               # 0=OFF; require N consecutive same-direction prediction ticks before TREND DCA fires
# V3.81: Reset peak when winner drops below this price (prevents stuck peak blocking DCA)
V3_REBALANCE_PEAK_RESET_PRICE = float(os.getenv("V3_REBALANCE_PEAK_RESET_PRICE", "0.70"))

# ML model gate for DCA — XGBoost classifier filters DCA buys (late mode: 150→300s model)
V3_ML_DCA_ENABLED = os.getenv("V3_ML_DCA_ENABLED", "false").lower() in ("true", "1", "yes")
V3_ML_MIN_PROBA = float(os.getenv("V3_ML_MIN_PROBA", "0.50"))  # min ML probability to allow DCA (0.0-1.0)

# ML model gate for ENTRY — XGBoost early mode filters + biases entry sizing
V3_ML_ENTRY_ENABLED = os.getenv("V3_ML_ENTRY_ENABLED", "false").lower() in ("true", "1", "yes")
V3_ML_ENTRY_MODE = os.getenv("V3_ML_ENTRY_MODE", "tick_cross").lower().strip()  # "early"|"late"|"tick"|"tick_cross"
V3_ML_ENTRY_MIN_PROBA = float(os.getenv("V3_ML_ENTRY_MIN_PROBA", "0.50"))
V3_ML_ENTRY_WINNER_MULT = float(os.getenv("V3_ML_ENTRY_WINNER_MULT", "2.0"))  # winner side multiplier when ML agrees
V3_ML_ENTRY_LOSER_MULT = float(os.getenv("V3_ML_ENTRY_LOSER_MULT", "1.0"))    # loser side multiplier when ML agrees

# V3: Buy band — only enter when ask price is within this range
BUY_BAND_LOW = float(os.getenv("V3_BUY_BAND_LOW", "0.30"))
BUY_BAND_HIGH = float(os.getenv("V3_BUY_BAND_HIGH", "0.70"))
TRAILER_BAND_MULT = float(os.getenv("V3_TRAILER_BAND_MULT", "1.3"))  # V3.99: Wider ceiling for trailer side in dual entry
TRAILER_BAND_HIGH = min(BUY_BAND_HIGH * TRAILER_BAND_MULT, 0.95)     # Cap at 0.95 to avoid near-settled markets
FAVORED_MIN_PRICE = BUY_BAND_LOW
FAVORED_MAX_PRICE = BUY_BAND_HIGH

# V3-LEGACY: removed in DCA-to-winner refactor
# ENTRY_DELAY_MIN = float(os.getenv("V3_ENTRY_DELAY_MIN", "2"))
ENTRY_DELAY_MIN = 0  # DCA-to-winner: no delay

# Cooldown between market actions
MARKET_ACTION_COOLDOWN_SEC = float(os.getenv("SCALPING_MARKET_ACTION_COOLDOWN_SEC", "8.0"))

# V3-LEGACY: removed in DCA-to-winner refactor (always dual-side from start)
# DUAL_SIDE_ENABLED = os.getenv("SCALPING_DUAL_SIDE_ENABLED", "true").lower() in ("true", "1", "yes")
# DUAL_SIDE_MIN_MAIN_TOKENS = float(os.getenv("SCALPING_DUAL_SIDE_MIN_MAIN_TOKENS", "20"))
DUAL_SIDE_ENABLED = True  # DCA-to-winner: always dual-side

# V3-LEGACY: removed in DCA-to-winner refactor (replaced by equal dual-side entry + insurance)
# HEDGE_ENABLED = os.getenv("SCALPING_HEDGE_ENABLED", "true").lower() in ("true", "1", "yes")
# HEDGE_RATIO = float(os.getenv("SCALPING_HEDGE_RATIO", "1.0"))
# HEDGE_MIN_TOKENS = float(os.getenv("SCALPING_HEDGE_MIN_TOKENS", "5"))
# HEDGE_COOLDOWN_SEC = float(os.getenv("SCALPING_HEDGE_COOLDOWN_SEC", "60.0"))
# HEDGE_MAX_COST_PCT = float(os.getenv("SCALPING_HEDGE_MAX_COST_PCT", "1.0"))
HEDGE_ENABLED = False  # DCA-to-winner: no legacy hedging
HEDGE_RATIO = 1.0      # kept: used by execute_rebalance_buy
HEDGE_MIN_TOKENS = 5   # kept: used by execute_rebalance_buy
HEDGE_COOLDOWN_SEC = 60.0  # kept: used by dead-code methods (still compiled)
HEDGE_MAX_COST_PCT = 1.0   # kept: used by get_hedge_budget (called from execute_rebalance_buy)

# ============ Combined TP ============
# Close ALL positions when combined profit reaches target
COMBINED_TP_ENABLED = os.getenv("SCALPING_COMBINED_TP_ENABLED", "true").lower() in ("true", "1", "yes")
COMBINED_TP_PCT = float(os.getenv("SCALPING_COMBINED_TP_PCT", "0.15"))  # +15% combined profit
DCA_TARGET_TP_PCT = float(os.getenv("V3_DCA_COMBINED_TP", "0.20"))  # DCA/FJ target TP (higher than sell trigger)
# V3.11: Minimum total cost (USD) to check COMBINED_TP — skip small positions where % swings are noisy
COMBINED_TP_MIN_COST_USD = float(os.getenv("SCALPING_COMBINED_TP_MIN_COST_USD", "50.0"))
# V3.67: Combined TP Trailing Stop — trail instead of selling immediately at TP threshold
V3_COMBINED_TSL_ENABLED = os.getenv("V3_COMBINED_TSL_ENABLED", "false").lower() in ("true", "1", "yes")
V3_COMBINED_TSL_STEP = float(os.getenv("V3_COMBINED_TSL_STEP", "0.08"))  # 8% trailing step
# V3.9: If enabled + prediction confidence > threshold after PRED_CUT, skip COMBINED_TP (hold for resolution)
V3_PREDICTION_CONFIDENCE_IGNORE_COMBINED_TP_ENABLED = os.getenv("V3_PREDICTION_CONFIDENCE_IGNORE_COMBINED_TP_ENABLED", "true").lower() in ("true", "1", "yes")
V3_PREDICTION_CONFIDENCE_IGNORE_COMBINED_TP = float(os.getenv("V3_PREDICTION_CONFIDENCE_IGNORE_COMBINED_TP", "0.70"))
# V3.16: Cooldown after buy timeout before allowing combined TP (ghost fill propagation delay)
V3_GHOST_FILL_COOLDOWN_SEC = float(os.getenv("V3_GHOST_FILL_COOLDOWN_SEC", "15"))

# V3-LEGACY: removed in DCA-to-winner refactor (rebalance sell, dynamic rebalance, resolve equalize)
# REBALANCE_SELL_ENABLED, REBALANCE_SELL_THRESHOLD, REBALANCE_BUY_THRESHOLD, REBALANCE_SELL_PCT
# MAX_REBALANCE_CYCLES, REBALANCE_COOLDOWN_TICKS, REBALANCE_SELL_PROFIT_PCT, REBALANCE_SELL_LOSS_PCT
# DYNAMIC_REBALANCE_ENABLED, TRENDING_*, VOLATILE_*
# REBALANCE_BUY_ENABLED, REBALANCE_BUY_MIN_LOSS
# RESOLVE_EQUALIZE_ENABLED, RESOLVE_EQUALIZE_MAX_PRICE, RESOLVE_EQUALIZE_MIN_LOSS

# ============ Multi-Market Configuration ============
PARALLEL_MARKETS = os.getenv("SCALPING_PARALLEL_MARKETS", "true").lower() in ("true", "1", "yes")
MAX_CAPITAL_PER_MARKET_PCT = float(os.getenv("SCALPING_MAX_CAPITAL_PER_MARKET_PCT", "0.30"))
MAX_TOTAL_EXPOSURE_PCT = float(os.getenv("SCALPING_MAX_TOTAL_EXPOSURE_PCT", "0.80"))

# ============ Capital Protection ============
# Stop new entries if total account value drops below threshold (0 = disabled)
BOT_STOP_THRESHOLD = float(os.getenv("BOT_STOP_THRESHOLD", "0"))

# ============ Dynamic Chunk Configuration ============
DYNAMIC_CHUNKS_ENABLED = os.getenv("SCALPING_DYNAMIC_CHUNKS", "true").lower() in ("true", "1", "yes")
CHUNK_MIN_TOKENS = float(os.getenv("SCALPING_CHUNK_MIN_TOKENS", "5"))
CHUNK_MAX_TOKENS = float(os.getenv("SCALPING_CHUNK_MAX_TOKENS", "20"))
CHUNK_LIQUIDITY_PCT = float(os.getenv("SCALPING_CHUNK_LIQUIDITY_PCT", "0.30"))

# ============ Legacy Features (DISABLED - kept for compatibility) ============
# These are disabled by default but kept so existing code references don't break
# TAKE_PROFIT_PCT removed in V3.3 - now using only DCA sell via DCA_EXIT_MIN_PROFIT_PCT
# V3.8: Fixed stale API curPrice - PnL now calculated from live orderbook only
STOP_LOSS_PCT = float(os.getenv("SCALPING_STOP_LOSS_PCT", "-0.30"))
SINGLE_SIDE_MAX_LOSS_PCT = 0  # DISABLED - let positions end naturally
# V3-LEGACY: removed in DCA-to-winner refactor
# DCA_INTO_WINNERS = os.getenv("SCALPING_DCA_INTO_WINNERS", "false").lower() in ("true", "1", "yes")
# DCA_BUY_RISE_PCT = float(os.getenv("SCALPING_DCA_BUY_RISE_PCT", "0.05"))
DCA_EXIT_MIN_PROFIT_PCT = 0.05  # kept: referenced in dashboard init + config logging

# TSL - DISABLED
STEPPED_TSL_ENABLED = False
STEPPED_TSL_STEP = 0.04

# Chunked TP - DISABLED (use combined TP only)
CHUNKED_TP_ENABLED = os.getenv("V3_CHUNKED_TP_ENABLED", "false").lower() in ("true", "1", "yes")
CHUNKED_TP_TIER1_PCT = float(os.getenv("V3_CHUNKED_TP_TIER1_PCT", "0.05"))
CHUNKED_TP_TIER1_SELL = float(os.getenv("V3_CHUNKED_TP_TIER1_SELL", "0.30"))
CHUNKED_TP_TIER2_PCT = float(os.getenv("V3_CHUNKED_TP_TIER2_PCT", "0.10"))
CHUNKED_TP_TIER2_SELL = float(os.getenv("V3_CHUNKED_TP_TIER2_SELL", "0.40"))
CHUNKED_TP_TIER3_PCT = float(os.getenv("V3_CHUNKED_TP_TIER3_PCT", "0.15"))
HEDGE_TP_MIN_LOSER_PRICE = 0.50
HEDGE_TRIGGER_PCT = 0.0

# Session profit target - DISABLED (use combined TP)
SESSION_PROFIT_TARGET_ENABLED = False
SESSION_PROFIT_TARGET_PCT = 0.20

# Cycling - DISABLED
CYCLING_ENABLED = False
MAX_CYCLES_PER_WINDOW = 0
CYCLE_COOLDOWN_SEC = 30

# Pre-resolution exit - DISABLED (let positions resolve)
PRE_RESOLUTION_EXIT_ENABLED = False
PRE_RESOLUTION_EXIT_MINUTES = 14.0
PRE_RESOLUTION_EXIT_MINUTES_1H = 59.0
PRE_RESOLUTION_EXIT_MIN_LOSS_PCT = 0

# TIER3 reentry - DISABLED
TIER3_REENTRY_COOLDOWN_SEC = 0

# Momentum/Redis
REDIS_ENABLED = os.getenv("REDIS_ENABLED", "false").lower() in ("true", "1", "yes")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MOMENTUM_ENABLED = False
MARKET_TO_CEX_SYMBOL: dict = {}  # Populated from market configs in __init__
# Per-market momentum threshold: "btc:0.08,eth:0.12" or single value "0.10" for all
_mtp_raw = os.getenv("V3_MARKET_THRESHOLD_PCT", "btc:0.08,eth:0.12,xrp:0.1,sol:0.2")
MOMENTUM_THRESHOLD: dict[str, float] = {}
_MTP_DEFAULT = 0.10
for _part in _mtp_raw.split(","):
    _part = _part.strip()
    if ":" in _part:
        _sym, _val = _part.split(":", 1)
        MOMENTUM_THRESHOLD[_sym.strip().upper()] = float(_val.strip())
    elif _part:
        _MTP_DEFAULT = float(_part)
MOMENTUM_THRESHOLD_DEFAULT = _MTP_DEFAULT
MOMENTUM_LOOKBACK_TICKS = 20
MOMENTUM_MIN_TICKS = 5
MOMENTUM_COOLDOWN_SEC = 30.0

# ============ V3: Volatility Gate (OKX strike distance) ============
# Block entries when OKX price is too close to Polymarket priceToBeat (flat market).
# V3_VOLATILITY_ENABLED=gate → block flat entries; "false"/empty → disabled
_v3_vg_raw = os.getenv("V3_VOLATILITY_ENABLED", os.getenv("V3_VOLATILITY_GATE_ENABLED", "false")).lower().strip()
V3_VOLATILITY_GATE_ENABLED = _v3_vg_raw in ("true", "1", "yes", "gate")
# Per-symbol volatility thresholds (% distance from priceToBeat to allow entry)
_v3_vt_raw = os.getenv("V3_VOLATILITY_THRESHOLD", "")
_VT_DEFAULT = 0.08
V3_VOLATILITY_THRESHOLD: dict[str, float] = {}
for _pair in _v3_vt_raw.split(","):
    if ":" in _pair:
        _sym, _val = _pair.split(":", 1)
        V3_VOLATILITY_THRESHOLD[_sym.strip().upper()] = float(_val.strip())
V3_VOLATILITY_THRESHOLD_DEFAULT = _VT_DEFAULT

# ============ V3: Prediction Boost (buy predicted winning side after X min) ============
PREDICTION_BOOST_ENABLED = os.getenv("V3_PREDICTION_BOOST_ENABLED", "false").lower() in ("true", "1", "yes")
PREDICTION_BOOST_MINUTE = float(os.getenv("V3_PREDICTION_BOOST_MINUTE", "7"))
PREDICTION_BOOST_MIN_CONFIDENCE = float(os.getenv("V3_PREDICTION_BOOST_MIN_CONFIDENCE", "0.65"))
PREDICTION_BOOST_API_URL = (
    os.getenv("V5_PREDICTION_API_URL", "") if V3_WINDOW_MINUTES == 15
    else os.getenv("V7_PREDICTION_API_URL", "")
)
PREDICTION_BOOST_USERNAME = os.getenv("PREDICTION_USERNAME", "")
PREDICTION_BOOST_PASSWORD = os.getenv("PREDICTION_PASSWORD", "")

# ============ V3: Prediction Source (WS zero-latency / HTTP polling) ============
# "http" = current HTTP polling (default), "ws" = WebSocket zero-latency stream
V3_PREDICTION_SOURCE = os.getenv("V3_PREDICTION_SOURCE", "http").lower().strip()
V3_PREDICTION_WS_URL = os.getenv("V3_PREDICTION_WS_URL", os.getenv("V7_PREDICTION_WS_URL", ""))

# ============ V3: PM WS Orderbook Feed ============
# Real-time orderbook prices via Polymarket CLOB WebSocket (vs REST polling)
V3_PM_WS_ENABLED = os.getenv("V3_PM_WS_ENABLED", "true").lower() in ("true", "1", "yes")
V3_PM_WS_STALE_SEC = float(os.getenv("V3_PM_WS_STALE_SEC", "10"))  # WS data older than this = stale, fallback to REST

# ============ V3: Lightweight Midpoint Scan ============
# Use /midpoint REST endpoint (~100ms) instead of full orderbook (~5s) when no position open
V3_MIDPOINT_SCAN_ENABLED = os.getenv("V3_MIDPOINT_SCAN_ENABLED", "true").lower() in ("true", "1", "yes")

# ============ V3: Pre-discover Next Window ============
# Pre-fetch next window's market data N seconds before current window ends
V3_PRE_DISCOVER_SEC = float(os.getenv("V3_PRE_DISCOVER_SEC", "10"))

# ============ V3: Regime Detection (TREND/CHOP from v7) ============
# 4-signal regime: trend confidence, flip rate, noise, spread
V3_REGIME_CHOP_TP = float(os.getenv("V3_REGIME_CHOP_TP", "0"))  # 0=OFF, e.g. 0.03 for 3% TP in CHOP
V3_CHOP_DCA_AMOUNT_USD = float(os.getenv("V3_CHOP_DCA_AMOUNT_USD", "0"))  # 0=OFF; DCA amount in CHOP regime (reduced vs TREND)
V3_CHOP_DCA_COOLDOWN_SEC = float(os.getenv("V3_CHOP_DCA_COOLDOWN_SEC", "20"))  # Separate cooldown for CHOP DCA (longer than TREND)
V3_MAX_DCA_WIN_PER_WINDOW = int(os.getenv("V3_MAX_DCA_WIN_PER_WINDOW", "2"))  # 0=unlimited; max DCA-to-winner fires per window (prevents cascade)
V3_REGIME_TREND_CONFIDENCE_GATE = float(os.getenv("V3_REGIME_TREND_CONFIDENCE_GATE", "0.60"))
V3_REGIME_TREND_MOMENTUM_GATE = float(os.getenv("V3_REGIME_TREND_MOMENTUM_GATE", "0.05"))
V3_REGIME_FLIP_RATE_THRESHOLD = float(os.getenv("V3_REGIME_FLIP_RATE_THRESHOLD", "3.0"))  # flips/min
V3_REGIME_NOISE_THRESHOLD = float(os.getenv("V3_REGIME_NOISE_THRESHOLD", "0.50"))
V3_REGIME_SPREAD_MULTIPLIER = float(os.getenv("V3_REGIME_SPREAD_MULTIPLIER", "2.0"))
V3_REGIME_DEBOUNCE_COUNT = int(os.getenv("V3_REGIME_DEBOUNCE_COUNT", "3"))

# V3: Early prediction cut — sell losing side before BOOST_MINUTE if price drops below threshold
PREDICTION_EARLY_CUT_MAX_PRICE = float(os.getenv("V3_PREDICTION_EARLY_CUT_MAX_PRICE", "0.10"))

# V3: 2nd Hedge — after cutting, buy back losing side if price drops very cheap
PREDICTION_2ND_HEDGE_MAX_PRICE = float(os.getenv("V3_2ND_HEDGE_MAX_PRICE", "0.07"))
PREDICTION_2ND_HEDGE_PCT = float(os.getenv("V3_2ND_HEDGE_PCT", "0.80"))  # 80% of winning tokens

# API endpoints
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
DATA_HOST = os.getenv("DATA_HOST", "https://data-api.polymarket.com")

# Wallet config
PRIVATE_KEY = os.getenv("PRIVATE_KEY") or os.getenv("PK") or os.getenv("POLYMARKET_PRIVATE_KEY") or ""
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE") or os.getenv("POLYMARKET_SIGNATURE_TYPE") or "0")
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS") or os.getenv("POLYMARKET_FUNDER") or None

# Retry config
MAX_RETRIES = int(os.getenv("MAX_RETRY", "10"))
RETRY_DELAY = float(os.getenv("SCALPING_RETRY_DELAY", "1.0"))

# MongoDB config
MONGODB_URL = os.getenv("MONGODB_URL", "")
MONGODB_DB = os.getenv("MONGODB_DB", "konis_polymarket")

# Logging - use polymarket_konis/logs/ folder (relative to script, not CWD)
_default_log_dir = Path(__file__).parent.parent / "logs"
LOG_DIR = Path(os.getenv("SCALPING_LOG_DIR", str(_default_log_dir)))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# V2.5: Session-based logging (15-minute windows for easier analysis)
SESSION_LOGS_ENABLED = os.getenv("SCALPING_SESSION_LOGS", "true").lower() in ("true", "1", "yes")
LOG_SUFFIX = _get_log_suffix()  # e.g., "-wider-tf" for multi-timeframe mode

def get_session_log_file() -> Path:
    """Get log file path based on current window."""
    now = datetime.now()
    day_dir = LOG_DIR / now.strftime('%Y-%m-%d') / "v3"
    day_dir.mkdir(parents=True, exist_ok=True)
    if SESSION_LOGS_ENABLED:
        # Round down to nearest window
        window_minute = (now.minute // V3_WINDOW_MINUTES) * V3_WINDOW_MINUTES
        window_time = now.replace(minute=window_minute, second=0, microsecond=0)
        return day_dir / f"{now.strftime('%Y-%m-%d')}-{window_time.strftime('%H%M')}-dca-v3{LOG_SUFFIX}.log"
    else:
        return day_dir / f"{now.strftime('%Y-%m-%d')}-dca-v3{LOG_SUFFIX}.log"

LOG_FILE = get_session_log_file()

# File-only logging (no console - dashboard handles display)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
logger = logging.getLogger("scalping_v3")

def rotate_log_file_if_needed():
    """Check and rotate to new session log file if window changed."""
    global LOG_FILE
    if not SESSION_LOGS_ENABLED:
        return
    new_log_file = get_session_log_file()
    if new_log_file != LOG_FILE:
        LOG_FILE = new_log_file
        # Remove old FILE handlers and add new one (preserve StreamHandler for headless stdout)
        for handler in logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                logger.removeHandler(handler)
        new_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        new_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(new_handler)
        logger.info(f"=== NEW SESSION LOG: {LOG_FILE.name} ===")

# Suppress noisy HTTP library logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Markets config: use CLI --markets if provided, else default
if _early_args.markets:
    MARKETS_CONFIG_FILE = Path(_early_args.markets)
else:
    MARKETS_CONFIG_FILE = Path(__file__).parent / f"scalping_markets_{V3_WINDOW_MINUTES}m.json"


@dataclass
class MarketConfig:
    slug_pattern: str
    name: str
    enabled: bool = True
    notes: str = ""
    timeframe: str = f"{V3_WINDOW_MINUTES}m"  # e.g. "15m" or "5m"
    cex_pair: str = ""  # e.g. "SOL-USDT-SWAP" — used to extract coin for prediction API


@dataclass
class MomentumSignal:
    """Tracks momentum signal for a CEX symbol"""
    symbol: str
    direction: Optional[str] = None  # "UP", "DOWN", or None
    change_pct: float = 0.0
    latest_price: float = 0.0
    signal_time: float = 0.0
    confidence: float = 0.0


@dataclass
class Position:
    market_slug: str
    condition_id: str
    entry_time: float

    # V3.80: YES/NO token model — track both sides directly, no main/hedge distinction
    yes_token_id: str = ""
    yes_tokens: float = 0.0
    yes_entry_price: float = 0.0
    yes_sold_tokens: float = 0.0
    yes_cost: float = 0.0
    yes_price: float = 0.0  # live price from orderbook

    no_token_id: str = ""
    no_tokens: float = 0.0
    no_entry_price: float = 0.0
    no_sold_tokens: float = 0.0
    no_cost: float = 0.0
    no_price: float = 0.0  # live price from orderbook

    chunks_bought: int = 1
    chunks_sold: int = 0

    # cooldown / last action to avoid spam
    last_action_ts: float = 0.0
    last_dca_win_ts: float = 0.0  # V3.35: DCA-WIN cooldown to prevent rapid repeat buying
    dca_win_count: int = 0  # V3.97: DCA-to-winner counter per window (for MAX_DCA_WIN_PER_WINDOW cap)

    # V3: Session cost tracking for combined PnL
    session_total_cost: float = 0.0
    session_allocated_capital: float = 0.0
    session_realized_profit: float = 0.0
    session_sell_proceeds: float = 0.0  # V3.37: total USDC received from all sells
    session_recycled_cost: float = 0.0  # V3.8: rebalance rebuy cost (recycled, not fresh capital)

    # V3.30: Rebalance cycle tracking
    rebalance_count: int = 0
    cheap_loser_dca_count: int = 0  # V3.88: Cheap loser DCA cycles used this session
    rebalance_last_tick_ts: float = 0.0

    # V3: Prediction flags
    prediction_boost_done: bool = False
    prediction_cut_side: str = ""  # "YES" or "NO" — which side was sold
    prediction_confidence: float = 0.0
    prediction_2nd_hedge_done: bool = False
    insurance_done: bool = False
    flip_follow: bool = False  # V3.99: FLIP_FOLLOW mode — reversed sizing, 2x TP
    lottery_ticket_done: bool = False
    final_justification_done: bool = False

    # V3: Sell/TP tracking
    last_rebalance_sell_ts: float = 0.0  # V3.48: Grace period for API sync
    winner_peak_price: float = 0.0
    winner_peak_side: str = ""  # "YES" or "NO"
    trailing_stop_done: bool = False
    rebalance_rebuy_blocked: bool = False
    unconfirmed_buy_cost: float = 0.0
    last_buy_error_ts: float = 0.0
    combined_tp_peak_pnl: float = 0.0  # V3.67: Peak combined PnL for trailing stop
    combined_tp_floor: float = -1.0    # V3.67: Trailing floor (-1 = not armed)

    # --- V3.80 Accessor helpers ---
    def get_tokens(self, side: str) -> float:
        return self.yes_tokens if side == "YES" else self.no_tokens

    def set_tokens(self, side: str, val: float):
        if side == "YES": self.yes_tokens = val
        else: self.no_tokens = val

    def add_tokens(self, side: str, val: float):
        if side == "YES": self.yes_tokens += val
        else: self.no_tokens += val

    def get_entry_price(self, side: str) -> float:
        return self.yes_entry_price if side == "YES" else self.no_entry_price

    def set_entry_price(self, side: str, val: float):
        if side == "YES": self.yes_entry_price = val
        else: self.no_entry_price = val

    def get_token_id(self, side: str) -> str:
        return self.yes_token_id if side == "YES" else self.no_token_id

    def set_token_id(self, side: str, val: str):
        if side == "YES": self.yes_token_id = val
        else: self.no_token_id = val

    def get_sold_tokens(self, side: str) -> float:
        return self.yes_sold_tokens if side == "YES" else self.no_sold_tokens

    def set_sold_tokens(self, side: str, val: float):
        if side == "YES": self.yes_sold_tokens = val
        else: self.no_sold_tokens = val

    def add_sold_tokens(self, side: str, val: float):
        if side == "YES": self.yes_sold_tokens += val
        else: self.no_sold_tokens += val

    def get_cost(self, side: str) -> float:
        return self.yes_cost if side == "YES" else self.no_cost

    def add_cost(self, side: str, val: float):
        if side == "YES": self.yes_cost += val
        else: self.no_cost += val
        self.session_total_cost += val

    def get_price(self, side: str) -> float:
        return self.yes_price if side == "YES" else self.no_price

    def set_price(self, side: str, val: float):
        if side == "YES": self.yes_price = val
        else: self.no_price = val

    def get_avail(self, side: str) -> float:
        return self.get_tokens(side) - self.get_sold_tokens(side)

    def winner_side(self) -> str:
        return "YES" if self.yes_price >= self.no_price else "NO"

    def loser_side(self) -> str:
        return "NO" if self.yes_price >= self.no_price else "YES"

    def has_tokens(self, side: str) -> bool:
        return self.get_tokens(side) > 0

    def total_tokens(self) -> float:
        return self.yes_tokens + self.no_tokens



@dataclass
class BotState:
    positions: Dict[str, Position] = field(default_factory=dict)  # condition_id -> Position
    total_pnl: float = 0.0
    last_window_ts: int = 0
    # V2.12: Track recently closed tokens to avoid re-adding as phantom hedge
    recently_closed_tokens: Dict[str, float] = field(default_factory=dict)  # token_id -> close_time
    # V2.17: Track pending order tokens to prevent over-buying during concurrent scan cycles
    pending_buy_tokens: Dict[str, float] = field(default_factory=dict)
    pending_buy_timestamps: Dict[str, float] = field(default_factory=dict)  # condition_id -> start_time
    # V3: Session stats (reset on new window)
    session_total_cost: float = 0.0
    session_cheap_loser_total_usd: float = 0.0  # V3.99: Total USD spent on cheap loser DCA across all windows
    session_total_value: float = 0.0
    session_start_balance: float = 0.0
    # Legacy fields (kept for compatibility)
    window_cycles: Dict[str, int] = field(default_factory=dict)
    window_exits: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    last_tier3_close_ts: Dict[str, float] = field(default_factory=dict)


def load_markets_config() -> List[MarketConfig]:
    if not MARKETS_CONFIG_FILE.exists():
        logger.warning(f"Markets config not found: {MARKETS_CONFIG_FILE}")
        return [MarketConfig(f"btc-updown-{V3_WINDOW_MINUTES}m", f"Bitcoin {V3_WINDOW_MINUTES}m", True)]

    try:
        with open(MARKETS_CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        markets: List[MarketConfig] = []
        for m in data.get("markets", []):
            markets.append(
                MarketConfig(
                    slug_pattern=m.get("slug_pattern", ""),
                    name=m.get("name", ""),
                    enabled=m.get("enabled", True),
                    notes=m.get("notes", ""),
                    timeframe=m.get("timeframe", f"{V3_WINDOW_MINUTES}m"),
                    cex_pair=m.get("cex_pair", ""),
                )
            )

        enabled = [m for m in markets if m.enabled]
        # Filter by V3_ENABLED_MARKETS env var (same as v5)
        if V3_ENABLED_MARKETS:
            enabled = [m for m in enabled
                       if any(m.slug_pattern.startswith(e + "-")
                              for e in V3_ENABLED_MARKETS)]
        logger.info(f"Loaded {len(enabled)}/{len(markets)} enabled markets"
                    + (f" (filter: {','.join(V3_ENABLED_MARKETS)})" if V3_ENABLED_MARKETS else ""))
        return enabled or [MarketConfig(f"btc-updown-{V3_WINDOW_MINUTES}m", f"Bitcoin {V3_WINDOW_MINUTES}m", True)]
    except Exception as e:
        logger.error(f"Failed to load markets config: {e}")
        return [MarketConfig(f"btc-updown-{V3_WINDOW_MINUTES}m", f"Bitcoin {V3_WINDOW_MINUTES}m", True)]


## V2.8: Removed save_observed_market - was only for debugging/tracking, not used in trading logic


def construct_1h_market_slug(base_pattern: str, target_time: Optional[datetime] = None) -> str:
    """Construct 1h market slug based on current ET time.

    1h markets use date-based slugs like: bitcoin-up-or-down-february-1-10am-et

    Args:
        base_pattern: e.g., "bitcoin-up-or-down"
        target_time: Optional datetime (UTC). If None, uses current time.

    Returns:
        Full slug like "bitcoin-up-or-down-february-1-10am-et"
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type: ignore

    # Get current time in ET
    if target_time is None:
        target_time = datetime.now(timezone.utc)

    et_tz = ZoneInfo("America/New_York")
    et_time = target_time.astimezone(et_tz)

    # Round to current hour (floor)
    et_hour = et_time.replace(minute=0, second=0, microsecond=0)

    # Format month name
    month_names = ["january", "february", "march", "april", "may", "june",
                   "july", "august", "september", "october", "november", "december"]
    month = month_names[et_hour.month - 1]

    # Format hour (12-hour format with am/pm)
    hour_12 = et_hour.hour % 12
    if hour_12 == 0:
        hour_12 = 12
    ampm = "am" if et_hour.hour < 12 else "pm"

    # Construct slug: bitcoin-up-or-down-february-1-10am-et
    slug = f"{base_pattern}-{month}-{et_hour.day}-{hour_12}{ampm}-et"

    return slug


class ScalpingBotV3:
    def __init__(
        self,
        sl_pct: float = STOP_LOSS_PCT,
        chunk_usd: float = TRADE_CHUNK_USD,
        max_position: float = MAX_POSITION_USD,
        dry_run: bool = DRY_RUN,
        headless: bool = False,
    ):
        self.sl_pct = sl_pct
        self.chunk_usd = chunk_usd
        self.max_position = max_position
        self.dry_run = dry_run
        self.headless = headless

        self.state = BotState()
        self.markets = load_markets_config()
        self.running = True

        # Build MARKET_TO_CEX_SYMBOL from loaded configs (used by momentum/Redis)
        global MARKET_TO_CEX_SYMBOL
        MARKET_TO_CEX_SYMBOL = {m.slug_pattern: m.cex_pair for m in self.markets if m.cex_pair}

        self.trader = None
        self.current_balance = SIMULATED_BALANCE if dry_run else 0.0

        # V3.10: Parallel sell processor - wraps blocking I/O in threads
        # so sells on different markets execute concurrently
        self.sell_processor = MarketSellProcessor(max_workers=max(len(self.markets) * 2, 4))

        mode_suffix = "DRY-RUN" if dry_run else "LIVE"
        self.dashboard = ScalpingDashboard(
            instance_id=f"V3-{mode_suffix}",
            entry_price=0.0,
            exit_price=0.0,
            take_profit_pct=DCA_EXIT_MIN_PROFIT_PCT,  # V3.3: Use DCA exit threshold for display
            stop_loss_pct=sl_pct,
            trade_unit=chunk_usd,
            max_logs=20,
            max_trades=20,
            max_candles=4,
        )
        if self.headless:
            self.dashboard.headless = True

        # dashboard display
        self.current_yes_price = 0.0
        self.current_no_price = 0.0

        # if allowance/balance wrong, block live trading attempts
        self._live_trading_blocked_reason: Optional[str] = None

        # ============ Capital Protection ============
        self._capital_ok: bool = True

        # ============ Volatility Gate (OKX strike distance) ============
        self._okx_feed = None
        self._okx_ws_task = None
        self._known_price_to_beat: Dict[str, float] = {}  # {symbol: ptb}
        self._last_ptb_retry: Dict[str, float] = {}  # {symbol: last_attempt_ts}
        self._fetch_err_logged: set = set()  # rate-limit market fetch error logs

        # ============ PM WS Orderbook Feed (real-time prices from Polymarket CLOB) ============
        self._pm_feed = None  # PolymarketOrderbookFeed instance
        self._pm_ws_task = None  # asyncio task running the WS feed

        # ============ WS Prediction Client (zero-latency prediction stream) ============
        self._ws_pred_client = None  # WsPredictionClient instance

        # ============ ML Model for DCA (late mode: 150→300s) ============
        self._ml_models: dict = {}  # symbol -> PredictionModel or None
        self._ml_tick_buffers: Dict[str, deque] = {}  # symbol -> deque of tick dicts for tick-level models

        # ============ Pre-discover Next Window ============
        self._next_window_market_cache: Dict[str, dict] = {}  # slug -> market data
        self._next_window_ts: int = 0  # which window was pre-discovered

        # ============ Regime Detection (TREND/CHOP from v7) ============
        self._price_ticks: Dict[str, deque] = {}  # market_symbol -> deque of (ts, yes_mid)
        self._spread_ticks: Dict[str, deque] = {}  # market_symbol -> deque of (ts, spread_pct)
        self._last_regime: Dict[str, str] = {}  # market_symbol -> "TREND" / "CHOP"
        self._regime_pending: Dict[str, str] = {}  # pending regime switch
        self._regime_pending_count: Dict[str, int] = {}  # consecutive count for debounce
        self._trend_confirm: Dict[str, Tuple[str, int]] = {}  # market_sym -> (pred_direction, consecutive_count)

        # ============ Persistent HTTP Session (avoids new TLS handshake per request) ============
        self._http_client: Optional[httpx.AsyncClient] = None

        # ============ Momentum Signal Tracking ============
        self.redis_client: Optional[redis.Redis] = None
        self.momentum_signals: Dict[str, MomentumSignal] = {}  # CEX symbol -> signal
        self.price_history: Dict[str, deque] = {}  # CEX symbol -> deque of (timestamp, price)
        self.last_signal_time: Dict[str, float] = {}  # CEX symbol -> last signal timestamp
        # V3.28: Direction history for market regime detection
        self._direction_history: Dict[str, list] = {}  # cex_symbol -> last N directions
        self._last_direction_window: Dict[str, int] = {}  # cex_symbol -> last window_ts recorded
        self._init_redis()

        # ============ MongoDB Persistence ============
        self.mongo: Optional[PolymarketMongoPersistence] = None
        self.session_id = f"v3-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self._init_mongo()

        # ============ Position Sync Tracking ============
        self._last_position_sync_ts: float = 0.0
        self._position_sync_interval: float = 3.0  # Sync every 3 seconds

        # ============ Market End Time Cache ============
        # V2.10: Cache condition_id -> end_time_utc for position filtering
        self._market_end_times: Dict[str, float] = {}  # condition_id -> end timestamp (UTC)
        self._condition_to_slug: Dict[str, str] = {}  # condition_id -> slug_pattern

        # ============ Session TP Tracking ============
        # V3: Track session-level take profit — once hit, stop all trading until next window
        self._session_tp_done: bool = False
        # Use CLI --begin-balance for first window if provided, else auto-set
        self._begin_session_balance: float = _early_args.begin_balance or 0.0
        self._begin_balance_from_cli: bool = _early_args.begin_balance is not None
        self._need_begin_balance_capture: bool = _early_args.begin_balance is None  # Capture after first sync
        self._session_start_pnl: float = 0.0  # Snapshot of total_pnl at window start
        self._unredeemed_positions_value: float = 0.0  # Value of unredeemed/untracked positions from API
        self._begin_unredeemed_value: float = 0.0  # Frozen snapshot at session start (avoids phantom PnL)

        # ============ Dryrun CSV Logging ============
        self._dryrun_csv_path: Optional[Path] = None
        self._dryrun_csv_file = None
        self._dryrun_csv_writer = None
        if self.dry_run:
            self._init_dryrun_csv()

    def _init_dryrun_csv(self):
        """Initialize CSV file for dryrun trade logging."""
        DRYRUNS_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%d%m%Y")
        self._dryrun_csv_path = DRYRUNS_DIR / f"dryrun-session{LOG_SUFFIX}-{date_str}.csv"

        file_exists = self._dryrun_csv_path.exists()
        self._dryrun_csv_file = open(self._dryrun_csv_path, "a", newline="", encoding="utf-8")
        self._dryrun_csv_writer = csv.writer(self._dryrun_csv_file)

        if not file_exists:
            self._dryrun_csv_writer.writerow([
                "timestamp", "session_id", "trade_type", "market_slug", "condition_id",
                "side", "tokens", "price", "usd_value", "pnl_pct", "pnl_usd",
                "balance_after", "total_pnl", "notes"
            ])
            self._dryrun_csv_file.flush()

    def _log_trade_to_csv(
        self,
        trade_type: str,
        market_slug: str,
        condition_id: str,
        side: str,
        tokens: float,
        price: float,
        pnl_pct: float = 0.0,
        pnl_usd: float = 0.0,
        notes: str = ""
    ):
        """Log a trade to the dryrun CSV file."""
        if not self._dryrun_csv_writer:
            return
        try:
            self._dryrun_csv_writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                self.session_id,
                trade_type,
                market_slug,
                condition_id[:16] if condition_id else "",
                side,
                f"{tokens:.2f}",
                f"{price:.4f}",
                f"{tokens * price:.2f}",
                f"{pnl_pct:.4f}",
                f"{pnl_usd:.2f}",
                f"{self.current_balance:.2f}",
                f"{self.state.total_pnl:.2f}",
                notes
            ])
            self._dryrun_csv_file.flush()
        except Exception as e:
            logger.warning(f"CSV log failed: {e}")

    def _init_redis(self):
        """Initialize Redis connection for CEX price data"""
        if not REDIS_ENABLED:
            self._log("Redis DISABLED (REDIS_ENABLED=false) - using orderbook-only mode", "WARN")
            return
        if not MOMENTUM_ENABLED:
            self._log("Momentum signals DISABLED - using orderbook-only mode", "WARN")
            return

        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
            )
            self.redis_client.ping()
            self._log(f"Redis connected: {REDIS_HOST}:{REDIS_PORT}", "INFO")

            # Initialize price history for each CEX symbol
            for cex_symbol in MARKET_TO_CEX_SYMBOL.values():
                self.price_history[cex_symbol] = deque(maxlen=MOMENTUM_LOOKBACK_TICKS * 2)
                self.momentum_signals[cex_symbol] = MomentumSignal(symbol=cex_symbol)
                self.last_signal_time[cex_symbol] = 0.0

        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            self._log(f"Redis FAILED: {e} - falling back to orderbook mode", "ERROR")
            self.redis_client = None

    def _init_mongo(self):
        """Initialize MongoDB persistence - uses _dry suffix for dry run mode"""
        if not MONGODB_URL or PolymarketMongoPersistence is None:
            self._log("MongoDB DISABLED - no MONGODB_URL or persistence module", "WARN")
            return

        try:
            self.mongo = PolymarketMongoPersistence(
                mongodb_url=MONGODB_URL,
                database_name=MONGODB_DB,
            )
            # Override collection name for dry run mode
            if self.dry_run:
                self.mongo.SCALPING_COLLECTION = "polymarket_scalping_dry"
                self._log(f"MongoDB: using DRY RUN collection: {self.mongo.SCALPING_COLLECTION}", "INFO")
            else:
                self._log(f"MongoDB: using LIVE collection: {self.mongo.SCALPING_COLLECTION}", "INFO")

        except Exception as e:
            logger.error(f"MongoDB init failed: {e}")
            self._log(f"MongoDB FAILED: {e}", "ERROR")
            self.mongo = None

    async def _connect_mongo(self):
        """Connect to MongoDB (async) - call after event loop starts"""
        if self.mongo:
            try:
                await self.mongo.connect()
                self._log(f"MongoDB connected: {MONGODB_DB}", "INFO")
            except Exception as e:
                logger.error(f"MongoDB connect failed: {e}")
                self._log(f"MongoDB connect FAILED: {e}", "ERROR")
                self.mongo = None

    def _fetch_cex_prices(self):
        """Fetch latest CEX prices from Redis and update momentum signals"""
        if not self.redis_client or not MOMENTUM_ENABLED:
            return

        now = time.time()

        for cex_symbol in MARKET_TO_CEX_SYMBOL.values():
            try:
                # Try cex_micro_1m_latest first (has aggregated price)
                data_str = self.redis_client.get(f"cex_micro_1m_latest:{cex_symbol}")
                if data_str:
                    data = json.loads(data_str)
                    price = data.get("mid_vw", 0)
                    if price > 0:
                        self.price_history[cex_symbol].append((now, price))
                        self._calculate_momentum(cex_symbol)
                        continue

                # Fallback: get from history
                hist = self.redis_client.lrange(f"history:{cex_symbol}", 0, 0)
                if hist:
                    entry = json.loads(hist[0])
                    price = entry.get("price", 0)
                    if price > 0:
                        self.price_history[cex_symbol].append((now, price))
                        self._calculate_momentum(cex_symbol)

            except Exception as e:
                logger.debug(f"Failed to fetch CEX price for {cex_symbol}: {e}")

    def _calculate_momentum(self, cex_symbol: str):
        """Calculate momentum signal from price history"""
        history = self.price_history.get(cex_symbol)
        if not history:
            return

        signal = self.momentum_signals[cex_symbol]

        # Always update latest price (for dashboard display)
        latest_ts, latest_price = history[-1]
        signal.latest_price = latest_price

        # Need minimum ticks for momentum calculation
        if len(history) < MOMENTUM_MIN_TICKS:
            return

        now = time.time()

        # Get oldest price in lookback window
        oldest_ts, oldest_price = history[0]

        # Use lookback ticks or all available
        lookback_idx = max(0, len(history) - MOMENTUM_LOOKBACK_TICKS)
        oldest_ts, oldest_price = history[lookback_idx]

        if oldest_price <= 0:
            return

        # Calculate price change percentage
        change_pct = ((latest_price - oldest_price) / oldest_price) * 100

        signal.latest_price = latest_price
        signal.change_pct = change_pct

        # Check if we're in cooldown
        last_signal = self.last_signal_time.get(cex_symbol, 0)
        if now - last_signal < MOMENTUM_COOLDOWN_SEC:
            return

        # Per-symbol momentum threshold (e.g. ETH=0.12%, BTC=0.08%)
        base_sym = cex_symbol.replace("USDT", "").replace("USD", "")
        threshold = MOMENTUM_THRESHOLD.get(base_sym, MOMENTUM_THRESHOLD_DEFAULT)

        # Determine direction based on threshold
        if change_pct >= threshold:
            signal.direction = "UP"
            signal.signal_time = now
            signal.confidence = min(1.0, abs(change_pct) / (threshold * 2))
            self.last_signal_time[cex_symbol] = now
            self._log(f"📈 {cex_symbol}: +{change_pct:.3f}% (thresh={threshold:.2f}%) → YES signal", "SIGNAL")
            # V3.28: Record direction per 1m snapshot for regime detection
            self._record_direction(cex_symbol, "UP", int(now // 60))
        elif change_pct <= -threshold:
            signal.direction = "DOWN"
            signal.signal_time = now
            signal.confidence = min(1.0, abs(change_pct) / (threshold * 2))
            self.last_signal_time[cex_symbol] = now
            self._log(f"📉 {cex_symbol}: {change_pct:.3f}% (thresh={threshold:.2f}%) → NO signal", "SIGNAL")
            # V3.28: Record direction per 1m snapshot for regime detection
            self._record_direction(cex_symbol, "DOWN", int(now // 60))
        else:
            # No strong signal - keep previous direction but mark as weak
            signal.direction = None
            signal.confidence = 0.0

    def get_momentum_signal(self, market_slug: str) -> Optional[MomentumSignal]:
        """Get momentum signal for a Polymarket market"""
        cex_symbol = MARKET_TO_CEX_SYMBOL.get(market_slug)
        if not cex_symbol:
            return None
        return self.momentum_signals.get(cex_symbol)

    # ============ V3.28: Market Regime Detection ============
    def _record_direction(self, cex_symbol: str, direction: str, window_ts: int):
        """Record direction for regime detection. Called once per new window."""
        if self._last_direction_window.get(cex_symbol) == window_ts:
            return  # already recorded this window
        self._last_direction_window[cex_symbol] = window_ts
        hist = self._direction_history.setdefault(cex_symbol, [])
        hist.append(direction)
        if len(hist) > 10:  # keep last 10
            hist.pop(0)

    def get_godeye_prediction(self) -> Tuple[Optional[str], float]:
        """Get prediction and confidence from GodEye via Redis polymarket_latest"""
        if not self.redis_client:
            return None, 0.0
        try:
            data_str = self.redis_client.get("polymarket_latest")
            if data_str:
                data = json.loads(data_str)
                cw = data.get("current_window", {})
                prediction = cw.get("prediction")  # "UP" or "DOWN"
                confidence = float(cw.get("confidence", 0) or 0)
                # Convert to YES/NO for dashboard
                if prediction == "UP":
                    return "YES", confidence
                elif prediction == "DOWN":
                    return "NO", confidence
        except Exception as e:
            logger.debug(f"Failed to get GodEye prediction: {e}")
        return None, 0.0

    # ----------------- MongoDB Logging -----------------
    def _log_position_entry(
        self,
        condition_id: str,
        market_config: MarketConfig,
        side: str,
        token_id: str,
        entry_price: float,
        size_tokens: float,
        entry_reason: str,
    ):
        """Log position entry to MongoDB"""
        if not self.mongo:
            return

        window_ts = self.get_current_window_ts()
        amount_usdc = size_tokens * entry_price

        # Get momentum signal info
        momentum = self.get_momentum_signal(market_config.slug_pattern)
        config_snapshot = {
            "dca_exit_profit_pct": DCA_EXIT_MIN_PROFIT_PCT,
            "sl_pct": self.sl_pct,
            "dca_chunk_tokens": DCA_CHUNK_TOKENS,
            "max_position_tokens": MAX_POSITION_TOKENS,
            "momentum_enabled": MOMENTUM_ENABLED,
            "momentum_threshold": MOMENTUM_THRESHOLD,
            "dry_run": self.dry_run,
        }
        if momentum:
            config_snapshot["momentum_change_pct"] = momentum.change_pct
            config_snapshot["cex_price"] = momentum.latest_price

        self.mongo.log_scalping_entry(
            trade_id=f"{condition_id}-{window_ts}-{side}",
            session_id=self.session_id,
            window_ts=window_ts,
            market_slug=market_config.slug_pattern,
            condition_id=condition_id,
            side=side,
            entry_price=entry_price,
            entry_size=size_tokens,
            amount_usdc=amount_usdc,
            token_id=token_id,
            entry_reason=entry_reason,
            config=config_snapshot,
        )

    def _log_position_exit(
        self,
        position: Position,
        exit_price: float,
        pnl_pct: float,
        pnl_usd: float,
        exit_reason: str,
    ):
        """Log position exit to MongoDB"""
        if not self.mongo:
            return

        window_ts = self.get_current_window_ts()
        _side = position.winner_side()
        trade_id = f"{position.condition_id}-{window_ts}-{_side}"

        self.mongo.log_scalping_exit(
            trade_id=trade_id,
            exit_price=exit_price,
            pnl_percent=pnl_pct,
            pnl_cash=pnl_usd,
            exit_reason=exit_reason,
            token_id=position.get_token_id(_side),
        )

    # ----------------- Helpers -----------------
    def _log(self, message: str, level: str = "INFO"):
        self.dashboard.log(message, level)
        # Also write to file logger so messages appear in log files
        lvl = level.upper()
        if lvl in ("WARN", "WARNING"):
            logger.warning(message)
        elif lvl == "ERROR":
            logger.error(message)
        else:
            logger.info(f"[{lvl}] {message}")

    @staticmethod
    def _market_tag(slug: str) -> str:
        """Extract short market tag from slug, e.g. 'sol-updown-15m' -> 'SOL'."""
        prefix = slug.split("-")[0].lower()
        return {"sol": "SOL", "btc": "BTC", "xrp": "XRP", "eth": "ETH",
                "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
                "dogecoin": "DOGE"}.get(prefix, prefix.upper()[:4])

    def _update_candle(self, window_ts: int, status: str, prediction: str = None, confidence: float = 0, reason: str = "", pnl_cash: float = 0):
        self.dashboard.update_candle(
            window_ts,
            {"status": status, "prediction": prediction, "confidence": confidence, "reason": reason, "pnl_cash": pnl_cash},
        )

    def _add_trade_to_dashboard(self, position: Position, exit_price: float, pnl_pct: float, pnl_usd: float, reason: str,
                                   side: str = None, entry_price: float = None):
        """Add trade to dashboard. V3.7: Optional side/entry_price for hedge trades."""
        _side = side if side else position.winner_side()
        _ep = entry_price if entry_price else position.get_entry_price(_side)
        trade = {
            "side": _side,
            "entry_time": position.entry_time,
            "exit_time": time.time(),
            "entry_price": _ep,
            "exit_price": exit_price,
            "pnl_percent": pnl_pct,
            "pnl_cash": pnl_usd,
            "exit_reason": reason,
            "market_slug": position.market_slug,
            "window_ts": getattr(position, 'last_window_ts', 0),
        }
        # V3.7 DEBUG: Log trade being added to dashboard
        logger.debug(f"[DASHBOARD ADD] {reason} pnl_cash=${pnl_usd:+.2f} | Before: total_pnl=${self.dashboard.total_pnl:.2f}")
        self.dashboard.add_trade(trade)
        logger.debug(f"[DASHBOARD ADD] After: total_pnl=${self.dashboard.total_pnl:.2f}")

    def get_current_window_ts(self) -> int:
        now = int(datetime.now(timezone.utc).timestamp())
        return (now // V3_WINDOW_SECONDS) * V3_WINDOW_SECONDS

    def get_market_duration_minutes(self, condition_id: str, market_slug: str = "") -> int:
        """V2.21: Get market duration in minutes from cached end time or slug pattern.
        Returns 15 for 15m markets, 60 for 1h markets, defaults to 15."""
        # Method 1: Detect from slug pattern
        if market_slug:
            slug_lower = market_slug.lower()
            # 1h patterns: "-1h-", "bitcoin-up-or-down-{month}-{day}-{hour}am/pm-et"
            if "-1h-" in slug_lower or "-1h" in slug_lower or "1h-" in slug_lower:
                return 60
            # 1h date-based slugs: bitcoin-up-or-down-february-1-10am-et
            if re.match(r"^(bitcoin|ethereum|solana|xrp|dogecoin)-up-or-down-\w+-\d+-\d+(am|pm)-et$", slug_lower):
                return 60
            if "-15m-" in slug_lower or "-15m" in slug_lower or "15m-" in slug_lower:
                return 15
        # Method 2: Calculate from cached end time (endDate - window_start)
        if condition_id in self._market_end_times:
            market_end_ts = self._market_end_times[condition_id]
            now = time.time()
            # Estimate window start by rounding down to nearest window
            # For 15m: window = 900s, for 1h: window = 3600s
            # Check if duration to end is > 15 minutes (could be 1h market)
            time_to_end = market_end_ts - now
            if time_to_end > V3_WINDOW_SECONDS:  # More than window duration → likely 1h market
                return 60
        return V3_WINDOW_MINUTES  # Default to configured window

    def get_pre_resolution_exit_minutes(self, condition_id: str, market_slug: str = "") -> float:
        """V2.21: Get pre-resolution exit minutes based on market duration."""
        duration = self.get_market_duration_minutes(condition_id, market_slug)
        if duration >= 60:
            return PRE_RESOLUTION_EXIT_MINUTES_1H
        return PRE_RESOLUTION_EXIT_MINUTES

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create persistent HTTP client (reuses TCP/TLS connections)."""
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                timeout=10,
                http2=True,
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            )
        return self._http_client

    async def _http_get_with_retry(self, url: str, params: dict = None, max_retries: int = 2) -> Optional[httpx.Response]:
        """HTTP GET with persistent session and retry on connection errors."""
        client = await self._get_http_client()
        last_err = None
        for attempt in range(max_retries + 1):
            try:
                resp = await client.get(url, params=params)
                return resp
            except (httpx.ConnectError, httpx.RemoteProtocolError) as e:
                last_err = e
                # Recreate client on connection errors (stale connection or TLS reset)
                try:
                    await self._http_client.aclose()
                except Exception:
                    pass
                self._http_client = None
                if attempt < max_retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
            except Exception as e:
                last_err = e
                break
        raise last_err if last_err else Exception("HTTP request failed")

    async def fetch_market_by_slug(self, slug: str) -> Optional[dict]:
        try:
            resp = await self._http_get_with_retry(f"{GAMMA_HOST}/markets", params={"slug": slug})
            data = resp.json()
            if isinstance(data, list) and data:
                # Clear fetch-error flag on success so future errors get logged
                self._fetch_err_logged.discard(slug)
                return data[0]
        except Exception as e:
            # Rate-limit: log each slug's fetch error only once until it succeeds
            if slug not in self._fetch_err_logged:
                self._fetch_err_logged.add(slug)
                logger.warning(f"Market fetch failed for {slug}: {type(e).__name__}: {e}")
        return None

    async def fetch_orderbook(self, token_id: str) -> dict:
        try:
            resp = await self._http_get_with_retry(f"{CLOB_HOST}/book", params={"token_id": token_id})
            return resp.json()
        except Exception as e:
            logger.debug(f"Orderbook fetch failed: {e}")
            return {"bids": [], "asks": []}

    async def fetch_midpoint(self, token_id: str) -> float:
        """Lightweight REST midpoint price (~100ms vs ~5s for full orderbook)."""
        try:
            resp = await self._http_get_with_retry(
                f"{CLOB_HOST}/midpoint", params={"token_id": token_id}, max_retries=1)
            data = resp.json()
            return float(data.get("mid", 0) or 0)
        except Exception as e:
            logger.debug(f"Midpoint fetch failed for {token_id[:12]}: {e}")
            return 0.0

    def get_best_prices(self, orderbook: dict) -> Tuple[float, float]:
        best_bid = 0.0
        best_ask = 1.0
        bids = orderbook.get("bids") or []
        asks = orderbook.get("asks") or []
        if bids:
            # Filter out invalid prices (empty strings, None, etc.)
            valid_bids = [float(b["price"]) for b in bids if b.get("price") and str(b["price"]).strip()]
            if valid_bids:
                best_bid = max(valid_bids)
        if asks:
            # Filter out invalid prices (empty strings, None, etc.)
            valid_asks = [float(a["price"]) for a in asks if a.get("price") and str(a["price"]).strip()]
            if valid_asks:
                best_ask = min(valid_asks)
        return best_bid, best_ask

    def get_best_levels(self, orderbook: dict):
        bids = orderbook.get("bids") or []
        asks = orderbook.get("asks") or []
        best_bid = max(bids, key=lambda x: float(x["price"])) if bids else None
        best_ask = min(asks, key=lambda x: float(x["price"])) if asks else None
        return best_bid, best_ask

    def calculate_combined_pnl(self, position: Position, yes_price: float = 0, no_price: float = 0) -> Tuple[float, float]:
        """V3.80: Calculate combined PnL (YES + NO) in USD and percentage.
        VWAP-free formula — immune to API sync issues.
        net_pnl = remaining_mark_value + sell_proceeds - total_invested

        Args:
            yes_price: current YES price (from orderbook)
            no_price: current NO price (from orderbook)
        Returns:
            (combined_pnl_usd, combined_pnl_pct)
        """
        if position.session_total_cost <= 0:
            return 0.0, 0.0

        # Mark value of remaining tokens on both sides
        yes_avail = position.get_avail("YES")
        no_avail = position.get_avail("NO")
        remaining_value = max(0, yes_avail) * yes_price + max(0, no_avail) * no_price

        # Deduct taker fee from remaining value (what we'd actually receive if sold)
        remaining_value_after_fee = remaining_value * (1 - V3_TAKER_FEE_PCT)

        # VWAP-free PnL — uses actual cash flows, not cost basis
        total_invested = position.session_total_cost + position.unconfirmed_buy_cost
        combined_pnl_usd = remaining_value_after_fee + position.session_sell_proceeds - total_invested
        fresh_capital = total_invested - position.session_recycled_cost
        combined_pnl_pct = combined_pnl_usd / fresh_capital if fresh_capital > 0 else 0.0

        return combined_pnl_usd, combined_pnl_pct

    def _loser_cost_headroom(self, position: "Position", loser_side: str) -> float:
        """V3.98: Max additional USD allowed for loser-side buys.
        Cap: loser_cost <= winner_cost * (1 + COMBINED_TP_PCT).
        If market flips, winner side (former loser) still hits TP.
        Returns 0 if cap reached (no more loser buys allowed).
        """
        winner_side = "NO" if loser_side == "YES" else "YES"
        winner_cost = position.get_cost(winner_side)
        loser_cost = position.get_cost(loser_side)
        max_loser_cost = winner_cost * (1 + COMBINED_TP_PCT)
        headroom = max(0, max_loser_cost - loser_cost)
        return headroom

    def _calc_bid_vwap(self, orderbook, token_count: float):
        """V3.10: Walk bid levels to calculate VWAP for selling token_count tokens.
        Returns (vwap, total_available) or (None, 0) if no valid bids.
        """
        bids = orderbook.get('bids', None) if isinstance(orderbook, dict) else getattr(orderbook, 'bids', None)
        bids = bids or []
        valid = []
        for b in bids:
            try:
                px = float(b.price if hasattr(b, 'price') else b.get('price', 0))
                sz = float(b.size if hasattr(b, 'size') else b.get('size', 0))
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

    def _calc_ask_vwap(self, orderbook, token_count: float):
        """V3.13: Walk ask levels to calculate VWAP for buying token_count tokens.
        Returns (vwap, total_available) or (None, 0) if no valid asks.
        Used by FJ to verify orderbook depth before committing.
        """
        asks = orderbook.get('asks', None) if isinstance(orderbook, dict) else getattr(orderbook, 'asks', None)
        asks = asks or []
        valid = []
        for a in asks:
            try:
                px = float(a.price if hasattr(a, 'price') else a.get('price', 0))
                sz = float(a.size if hasattr(a, 'size') else a.get('size', 0))
                if 0.01 < px < 0.99 and sz > 0:
                    valid.append((px, sz))
            except (ValueError, TypeError):
                continue
        if not valid:
            return None, 0.0
        valid.sort(key=lambda x: x[0])  # ascending — cheapest asks first
        fill = 0.0
        cost = 0.0
        for px, sz in valid:
            take = min(sz, token_count - fill)
            fill += take
            cost += take * px
            if fill >= token_count:
                break
        return (cost / fill if fill > 0 else None), fill

    def _calc_session_total_pnl(self) -> tuple:
        """V3.10: Calculate aggregate session PnL across ALL positions.
        Returns (total_pnl_usd, total_cost, pnl_pct).
        PnL = sum(unrealized + realized) per position.
        Cost = sum(remaining_tokens * entry_price) per position.
        Uses stored current_price/hedge_current_price (updated each scan cycle).
        """
        total_pnl = 0.0
        total_cost = 0.0
        total_value = 0.0
        total_tokens = 0.0
        total_sell_proceeds = 0.0
        for pos in self.state.positions.values():
            if pos.condition_id not in self._condition_to_slug:
                continue
            # V3.80: Calculate value from YES + NO sides
            yes_rem = pos.get_avail("YES")
            no_rem = pos.get_avail("NO")
            pos_value = yes_rem * pos.yes_price + no_rem * pos.no_price
            total_tokens += yes_rem + no_rem
            invested = pos.session_total_cost if pos.session_total_cost > 0 else pos_value
            pos_pnl = pos_value * (1 - V3_TAKER_FEE_PCT) + pos.session_sell_proceeds - invested
            total_pnl += pos_pnl
            total_cost += invested
            total_value += pos_value
            total_sell_proceeds += pos.session_sell_proceeds
        pnl_pct = total_pnl / total_cost if total_cost > 0 else 0.0
        return total_pnl, total_cost, pnl_pct, total_value, total_tokens, total_sell_proceeds

    def _round_tokens(self, x: float) -> float:
        # V2.9: Polymarket supports 4 decimal places for token amounts (taker_amount)
        # USDC (maker_amount) requires max 2 decimals
        # Round down to avoid "invalid amounts" API errors
        return math.floor(x * 10000) / 10000

    def _enforce_order_minimums(self, tokens: float, price: float) -> tuple:
        """V3: Enforce Polymarket minimums: 5 tokens, $1 USD.

        If calculated order is below minimums, bump it up.
        Returns: (adjusted_tokens, adjusted_usd)
        """
        result_tokens = tokens
        result_usd = tokens * price

        # Bump USD to $1 minimum if needed
        if result_usd < POLYMARKET_MIN_USD and price > 0:
            result_tokens = math.ceil(POLYMARKET_MIN_USD / price)
            result_usd = result_tokens * price
            logger.info(f"[MIN ORDER] Bumped from {tokens:.1f} tokens (${tokens*price:.2f}) to {result_tokens} tokens (${result_usd:.2f})")

        # Ensure minimum tokens
        if result_tokens < POLYMARKET_MIN_TOKENS:
            result_tokens = POLYMARKET_MIN_TOKENS
            result_usd = result_tokens * price
            logger.info(f"[MIN ORDER] Bumped to minimum {POLYMARKET_MIN_TOKENS} tokens (${result_usd:.2f})")

        return self._round_tokens(result_tokens), round(result_usd, 2)

    def _update_position_after_buy(self, pos, side: str, token_id: str, tokens: float, price: float):
        """V3.80: Route filled tokens to YES or NO fields directly."""
        buy_cost = tokens * price
        old_tokens = pos.get_tokens(side)
        old_price = pos.get_entry_price(side)
        if old_tokens > 0 and old_price > 0:
            # VWAP update
            total = old_tokens + tokens
            new_vwap = ((old_price * old_tokens) + (price * tokens)) / total
            pos.set_tokens(side, total)
            pos.set_entry_price(side, new_vwap)
        else:
            # First buy on this side
            pos.set_tokens(side, tokens)
            pos.set_entry_price(side, price)
            pos.set_token_id(side, token_id)
        pos.chunks_bought += 1
        pos.last_action_ts = time.time()
        self._track_buy_cost(pos, side, buy_cost)

    def _track_buy_cost(self, pos: 'Position', side: str, cost: float):
        """V3.80: Track buy cost on combined + per-side counters using YES/NO directly."""
        pos.session_total_cost += cost
        if side == "YES":
            pos.yes_cost += cost
        else:
            pos.no_cost += cost

    def _position_current_cost(self, position) -> float:
        """Returns total capital deployed into this position (session_total_cost).

        V3.52 FIX: Use session_total_cost (actual cash spent) instead of tokens * VWAP.
        Old method was vulnerable to API sync overwriting local token counts with stale data,
        making the budget appear much larger than reality and allowing massive overspend.
        Sell proceeds are NOT subtracted — budget tracks total deployment, not net exposure.
        """
        return position.session_total_cost

    def _position_budget_remaining(self, position, target_side: str = None) -> float:
        """Returns remaining USD budget for this position (0 if exhausted).

        V3.10: Per-side budget — each side capped at MAX_POSITION_COST_USD independently.
        Pass target_side (YES/NO) to get per-side budget. None = combined (legacy).
        """
        if MAX_POSITION_COST_USD <= 0:
            return float('inf')
        if target_side:
            # V3.80: Per-side cap using YES/NO cost directly
            return max(0, MAX_POSITION_COST_USD - position.get_cost(target_side))
        # Combined: both sides get MAX each
        is_dual = position.yes_tokens > 0 and position.no_tokens > 0
        budget_limit = MAX_POSITION_COST_USD * 2 if is_dual else MAX_POSITION_COST_USD
        return max(0, budget_limit - self._position_current_cost(position))

    def _cap_buy_to_budget(self, position, buy_tokens: float, price: float, target_side: str = None) -> tuple:
        """Cap buy order to remaining position budget. Returns (tokens, usd) or (0, 0) if exhausted."""
        budget = self._position_budget_remaining(position, target_side)
        buy_usd = buy_tokens * price
        if buy_usd <= budget:
            return buy_tokens, buy_usd
        if budget < POLYMARKET_MIN_USD:
            return 0, 0
        capped_tokens = self._round_tokens(budget / price)
        if capped_tokens < POLYMARKET_MIN_TOKENS:
            return 0, 0
        # Log with per-side info when available
        if target_side:
            side_cost = position.get_cost(target_side)
            self._log(f"BUDGET CAP: ${buy_usd:.2f} → ${capped_tokens * price:.2f} ({target_side} spent=${side_cost:.2f}/{MAX_POSITION_COST_USD:.0f})", "WARN")
        else:
            opp = position.loser_side()
            is_dual = position.get_tokens(opp) > 0 or position.get_entry_price(opp) > 0
            budget_limit = MAX_POSITION_COST_USD * 2 if is_dual else MAX_POSITION_COST_USD
            self._log(f"BUDGET CAP: ${buy_usd:.2f} → ${capped_tokens * price:.2f} (spent=${position.session_total_cost:.2f}/{budget_limit:.0f})", "WARN")
        return capped_tokens, capped_tokens * price

    def _generate_chunk_id(self) -> str:
        """Generate unique chunk ID."""
        import uuid
        return f"chunk-{uuid.uuid4().hex[:12]}"

    # ----------------- Dynamic Chunk Sizing -----------------
    def calculate_chunk_size(self, orderbook: dict, current_position_tokens: float, max_position_tokens: float) -> float:
        """Calculate optimal chunk size based on liquidity and position state.

        V2.5: Enforces MAX_POSITION_TOKENS cap - never returns a chunk that would exceed limit.
        """
        # V2.5: Check remaining room before max position
        remaining_room = max_position_tokens - current_position_tokens
        if remaining_room <= 0:
            logger.info(f"[CHUNK] At max position ({current_position_tokens:.0f}/{max_position_tokens:.0f} tokens)")
            return 0

        if not DYNAMIC_CHUNKS_ENABLED:
            # V2.5: Cap at remaining room
            return min(DCA_CHUNK_TOKENS, remaining_room)

        asks = orderbook.get("asks", [])
        # Use appropriate price band based on entry mode
        price_ceiling = FAVORED_MAX_PRICE if ENTRY_MODE == "favored" else BUY_BAND_HIGH
        good_asks = [a for a in asks if float(a["price"]) <= price_ceiling]
        available_liquidity = sum(float(a["size"]) for a in good_asks[:5])

        if available_liquidity <= 0:
            return min(CHUNK_MIN_TOKENS, remaining_room)

        position_ratio = current_position_tokens / max_position_tokens if max_position_tokens > 0 else 0

        if position_ratio < 0.3:
            base_chunk = min(30, available_liquidity * 0.4)
        elif position_ratio < 0.6:
            base_chunk = min(25, available_liquidity * 0.3)
        elif position_ratio < 0.8:
            base_chunk = min(15, available_liquidity * 0.2)
        else:
            base_chunk = CHUNK_MIN_TOKENS

        max_from_liquidity = available_liquidity * CHUNK_LIQUIDITY_PCT
        chunk = max(CHUNK_MIN_TOKENS, min(CHUNK_MAX_TOKENS, base_chunk, max_from_liquidity))

        # V2.5: Enforce hard cap at remaining room
        chunk = min(chunk, remaining_room)

        # V2.12: Use global POLYMARKET_MIN_TOKENS constant
        if chunk < POLYMARKET_MIN_TOKENS and remaining_room >= POLYMARKET_MIN_TOKENS:
            chunk = POLYMARKET_MIN_TOKENS

        return self._round_tokens(chunk)

    # ----------------- V3: Break-Even Profit Reconciliation -----------------
    # V3.35: Use $0.98 target to sell before resolution (avoid unnecessary redeems)
    # Loser side stays $0.00 — we sell early or it resolves worthless
    RESOLVE_WIN_PRICE = V3_RESOLVE_TARGET_PRICE
    RESOLVE_LOSE_PRICE = 0.00

    def calculate_breakeven_opposite_tokens(
        self,
        main_tokens: float,
        main_entry_price: float,
        opposite_entry_price: float,
    ) -> float:
        """V3.1: Calculate opposite tokens for break-even using REALISTIC resolution prices.

        Market resolution prices (sellable before full resolve):
        - Winner: $0.99 (not $1.00)
        - Loser: $0.01 (not $0.00)

        Strategy: Ensure NET PnL ≈ 0 when hedge wins (main loses)

        If HEDGE wins (main loses):
          main_value = main_tokens × 0.01
          hedge_value = hedge_tokens × 0.99
          total_cost = main_cost + hedge_cost

          For break-even: main_value + hedge_value = total_cost
          → main_tokens × 0.01 + hedge_tokens × 0.99 = main_tokens × main_entry + hedge_tokens × hedge_entry
          → hedge_tokens × (0.99 - hedge_entry) = main_tokens × (main_entry - 0.01)
          → hedge_tokens = main_tokens × (main_entry - 0.01) / (0.99 - hedge_entry)

        Example:
        - Main: 65.76 NO @ $0.4661 = $30.65 cost
        - Hedge entry: $0.80
        - hedge_tokens = 65.76 × (0.4661 - 0.01) / (0.99 - 0.80)
        - hedge_tokens = 65.76 × 0.4561 / 0.19 = 157.9 tokens
        - hedge_cost = 157.9 × 0.80 = $126.32

        If YES wins (hedge wins):
          Main: 65.76 × 0.01 = $0.66
          Hedge: 157.9 × 0.99 = $156.32
          Total return: $156.98
          Total cost: $30.65 + $126.32 = $156.97
          Net: +$0.01 ≈ break-even ✓

        If NO wins (main wins):
          Main: 65.76 × 0.99 = $65.10
          Hedge: 157.9 × 0.01 = $1.58
          Total return: $66.68
          Total cost: $156.97
          Net: -$90.29 (big loss, but this is the "good" outcome we expected to win)

        Returns: Number of opposite tokens to buy
        """
        if main_entry_price >= 1.0 or opposite_entry_price >= 1.0:
            return 0.0
        if opposite_entry_price <= 0 or opposite_entry_price >= self.RESOLVE_WIN_PRICE:
            return 0.0

        # For hedge to cover main loss at resolution:
        # hedge_tokens × (WIN_PRICE - hedge_entry) = main_tokens × (main_entry - LOSE_PRICE)
        numerator = main_tokens * (main_entry_price - self.RESOLVE_LOSE_PRICE)
        denominator = self.RESOLVE_WIN_PRICE - opposite_entry_price

        if denominator <= 0:
            logger.warning(f"[BREAKEVEN] Cannot hedge: opposite_entry {opposite_entry_price} >= {self.RESOLVE_WIN_PRICE}")
            return 0.0

        opposite_tokens = numerator / denominator

        # Calculate costs and expected outcomes
        main_cost = main_tokens * main_entry_price
        hedge_cost = opposite_tokens * opposite_entry_price
        total_cost = main_cost + hedge_cost

        # If MAIN wins
        main_win_value = main_tokens * self.RESOLVE_WIN_PRICE + opposite_tokens * self.RESOLVE_LOSE_PRICE
        main_win_pnl = main_win_value - total_cost

        # If HEDGE wins
        hedge_win_value = main_tokens * self.RESOLVE_LOSE_PRICE + opposite_tokens * self.RESOLVE_WIN_PRICE
        hedge_win_pnl = hedge_win_value - total_cost

        logger.info(f"[BREAKEVEN_V3.1] Main: {main_tokens:.2f} @ ${main_entry_price:.4f} = cost ${main_cost:.2f}")
        logger.info(f"[BREAKEVEN_V3.1] Hedge @ ${opposite_entry_price:.4f}: {opposite_tokens:.2f} tokens, cost ${hedge_cost:.2f}")
        logger.info(f"[BREAKEVEN_V3.1] Total cost: ${total_cost:.2f}")
        logger.info(f"[BREAKEVEN_V3.1] Resolve: MAIN_WIN ${main_win_pnl:+.2f} | HEDGE_WIN ${hedge_win_pnl:+.2f}")

        return self._round_tokens(opposite_tokens)

    def get_hedge_budget(self, position: Position) -> Tuple[float, float, float]:
        """V3.6: Calculate remaining hedge budget based on HEDGE_MAX_COST_PCT cap.

        Prevents over-hedging where hedge cost exceeds main position cost.

        Returns:
            (max_hedge_cost, current_hedge_cost, remaining_budget)

        Example with HEDGE_MAX_COST_PCT=1.0 (100%):
        - Main: 10 tokens @ $0.70 = $7.00 cost
        - Max hedge: $7.00 (100% of main)
        - Current hedge: 5 tokens @ $0.30 = $1.50
        - Remaining budget: $5.50

        Example with HEDGE_MAX_COST_PCT=0.5 (50%):
        - Main: 10 tokens @ $0.70 = $7.00 cost
        - Max hedge: $3.50 (50% of main)
        - Current hedge: 5 tokens @ $0.30 = $1.50
        - Remaining budget: $2.00
        """
        w = position.winner_side()
        l = position.loser_side()
        main_cost = position.get_tokens(w) * position.get_entry_price(w)
        max_hedge_cost = main_cost * HEDGE_MAX_COST_PCT
        current_hedge_cost = position.get_tokens(l) * position.get_entry_price(l) if position.get_tokens(l) > 0 else 0
        remaining_budget = max(0, max_hedge_cost - current_hedge_cost)

        return max_hedge_cost, current_hedge_cost, remaining_budget

    def check_hedge_exceeds_main(self, position: Position) -> bool:
        """V3.6: Check if hedge position has become larger than main (indicating position flip).

        When hedge cost > main cost, the position has effectively flipped sides.
        This should trigger a warning and potentially swap main/hedge designation.

        Returns True if hedge exceeds main (anomaly state).
        """
        w = position.winner_side()
        l = position.loser_side()
        main_cost = position.get_tokens(w) * position.get_entry_price(w)
        hedge_cost = position.get_tokens(l) * position.get_entry_price(l) if position.get_tokens(l) > 0 else 0

        if hedge_cost > main_cost:
            logger.warning(f"[HEDGE EXCEEDS MAIN] Hedge ${hedge_cost:.2f} > Main ${main_cost:.2f} - position flipped!")
            return True
        return False

    def calculate_resolve_scenarios(self, position: Position) -> Tuple[float, float, float, float]:
        """V3.2: Calculate PnL for both resolve scenarios using Polymarket prices.

        Returns:
            (yes_win_pnl, no_win_pnl, yes_tokens_needed, no_tokens_needed)

        Uses total USD spent (size_usd/hedge_size_usd) as cost basis, not
        current_tokens * entry_price, so realized losses from sold tokens
        are properly accounted for.
        """
        # Get REMAINING holdings + TOTAL cost (including sold tokens)
        yes_tokens = position.get_avail("YES")
        no_tokens = position.get_avail("NO")

        # Use session_total_cost (total USD ever spent) and session_sell_proceeds (actual USDC received)
        # to match VWAP-free PnL formula — immune to size_usd shrinking on sells
        # V7: Always include lottery ticket reserve in cost (pre-budgeted from start)
        lottery_reserve = V3_LOTTERY_TICKET_USD if not position.lottery_ticket_done else 0
        total_cost = position.session_total_cost + lottery_reserve

        # Resolve payouts from REMAINING tokens — uses $1.00 (actual resolution payout)
        # Note: RESOLVE_WIN_PRICE is the DCA sell target, not the resolution payout
        RESOLUTION_PAYOUT = 0.99
        yes_win_payout = yes_tokens * RESOLUTION_PAYOUT + no_tokens * self.RESOLVE_LOSE_PRICE
        no_win_payout = no_tokens * RESOLUTION_PAYOUT + yes_tokens * self.RESOLVE_LOSE_PRICE

        # PnL = resolve payout + sell proceeds already received - total cost
        proceeds = position.session_sell_proceeds
        yes_win_pnl = yes_win_payout + proceeds - total_cost
        no_win_pnl = no_win_payout + proceeds - total_cost

        # Calculate tokens needed to reach break-even at actual resolution ($1.00)
        yes_tokens_needed = max(0, -yes_win_pnl / RESOLUTION_PAYOUT) if yes_win_pnl < 0 else 0
        no_tokens_needed = max(0, -no_win_pnl / RESOLUTION_PAYOUT) if no_win_pnl < 0 else 0

        logger.info(f"[RESOLVE] YES: {yes_tokens:.0f} tokens | NO: {no_tokens:.0f} tokens | cost=${total_cost:.2f} proceeds=${proceeds:+.2f}")
        logger.info(f"[RESOLVE] Cost: ${total_cost:.2f} | YES wins: ${yes_win_pnl:+.2f} | NO wins: ${no_win_pnl:+.2f}")
        if yes_tokens_needed > 0 or no_tokens_needed > 0:
            logger.info(f"[RESOLVE] NEED: YES +{yes_tokens_needed:.0f} tokens | NO +{no_tokens_needed:.0f} tokens")

        return yes_win_pnl, no_win_pnl, yes_tokens_needed, no_tokens_needed

    def _calc_resolution_dca_amount(self, position: Position, winner_side_label: str, winner_price: float) -> tuple:
        """V3.35: Calculate DCA amount using token-matching logic.

        Core idea: buy enough winner tokens so that winner_tokens >= loser_tokens * (1 + TP%).
        This ensures resolution profit >= TP target when winner side resolves to $1.
        Returns (usd_amount, skip_reason) — skip_reason is None when DCA should proceed.
        """
        # V3.41: Cap at real breakeven — buy price must be below sell payout after fee
        DCA_MAX_PRICE = self.RESOLVE_WIN_PRICE * (1 - V3_TAKER_FEE_PCT)
        if winner_price >= DCA_MAX_PRICE:
            reason = f"price ${winner_price:.2f} >= breakeven ${DCA_MAX_PRICE:.2f} (DCA net-negative)"
            logger.info(f"[DCA RESOLVE] Skip: {reason}")
            return 0, reason

        # Get current token counts for winner and loser sides
        loser_side_label = "NO" if winner_side_label == "YES" else "YES"
        winner_tokens = position.get_avail(winner_side_label)
        loser_tokens = position.get_avail(loser_side_label)

        # V7: Include lottery ticket reserve in cost (pre-budgeted from start)
        lottery_reserve = V3_LOTTERY_TICKET_USD if not position.lottery_ticket_done else 0
        total_cost = position.session_total_cost + lottery_reserve
        proceeds = position.session_sell_proceeds

        # V3.42: Upfront calculation — solve for N tokens including cost of buying N tokens.
        # After buying N at price P: new_cost = total_cost + N*P
        # Target: (winner_tokens + N) * RESOLVE >= new_cost*(1+TP%) - proceeds
        # Solving: N = (total_cost*(1+TP%) - proceeds - winner_tokens*RESOLVE) / (RESOLVE - P*(1+TP%))
        tp_target = DCA_TARGET_TP_PCT
        denominator = self.RESOLVE_WIN_PRICE - winner_price * (1 + tp_target)
        if denominator <= 0:
            # Salvage mode: if position is underwater, relax TP to breakeven (0%)
            # Better to DCA for breakeven than sit and lose everything
            combined_pnl = (winner_tokens * winner_price + loser_tokens * (1 - winner_price)
                           + proceeds - total_cost)
            if combined_pnl < 0:
                tp_target = 0.0  # aim for breakeven
                denominator = self.RESOLVE_WIN_PRICE - winner_price * (1 + tp_target)
                if denominator > 0:
                    logger.info(f"[DCA RESOLVE] SALVAGE MODE: price ${winner_price:.2f} too high for {DCA_TARGET_TP_PCT*100:.0f}% TP, "
                                f"relaxing to breakeven (combined PnL=${combined_pnl:.2f})")
                else:
                    reason = f"price ${winner_price:.2f} exceeds breakeven max ${self.RESOLVE_WIN_PRICE:.2f} (salvage impossible)"
                    logger.info(f"[DCA RESOLVE] Skip: {reason}")
                    return 0, reason
            else:
                reason = f"price ${winner_price:.2f} exceeds max ${self.RESOLVE_WIN_PRICE / (1 + DCA_TARGET_TP_PCT):.2f} for {DCA_TARGET_TP_PCT*100:.0f}% TP (position profitable, no DCA needed)"
                logger.info(f"[DCA RESOLVE] Skip: {reason}")
                return 0, reason

        numerator = total_cost * (1 + tp_target) - proceeds - winner_tokens * self.RESOLVE_WIN_PRICE
        tokens_to_buy = numerator / denominator

        if tokens_to_buy <= 0:
            tokens_needed_total = winner_tokens
            reason = f"already matched: {winner_tokens:.0f} winner >= {loser_tokens:.0f} loser (resolution profitable)"
            logger.info(f"[DCA RESOLVE] Skip: {reason}")
            return 0, reason

        tokens_needed_total = winner_tokens + tokens_to_buy
        dca_usd = tokens_to_buy * winner_price

        # V3.52 FIX: Cap per-cycle DCA for ALL modes including salvage.
        # Previously salvage had NO cap, causing $276+ single orders blowing past budget.
        # Salvage gets 2x normal DCA to recover faster while staying controlled.
        is_salvage = tp_target < DCA_TARGET_TP_PCT
        if not is_salvage:
            dca_usd = min(dca_usd, V3_DCA_AMOUNT_USD)
        else:
            dca_usd = min(dca_usd, V3_DCA_AMOUNT_USD * 2)

        # V3.10: Per-side budget cap — DCA winner only uses winner side's budget
        budget_remaining = self._position_budget_remaining(position, winner_side_label)
        dca_usd = min(dca_usd, budget_remaining)

        mode_label = "SALVAGE" if is_salvage else "DCA"
        logger.info(
            f"[DCA RESOLVE] [{mode_label}] winner={winner_tokens:.0f} loser={loser_tokens:.0f} "
            f"need={tokens_needed_total:.0f} tokens (buy {tokens_to_buy:.0f} over cycles) "
            f"@ ${winner_price:.2f} → DCA=${dca_usd:.2f} (budget=${budget_remaining:.2f} {winner_side_label})")

        if dca_usd < POLYMARKET_MIN_USD:
            reason = f"budget exhausted: ${budget_remaining:.2f} remaining, DCA=${dca_usd:.2f} < min ${POLYMARKET_MIN_USD:.2f} (need {tokens_to_buy:.0f} tokens but capped by budget)"
            return 0, reason
        return dca_usd, None

    # ----------------- Hedge Mode -----------------
    def should_hedge(self, position: Position) -> bool:
        """Check if we should add hedge to position.

        V2.6: Delayed hedge - only hedge when main PnL goes negative.
        Trust the prediction, hedge only on failure.
        V3.1: Allow hedge top-up if current hedge is below break-even target.
        V3.6: Respect HEDGE_MAX_COST_PCT cap to prevent over-hedging.
        """
        if not HEDGE_ENABLED:
            logger.debug(f"[HEDGE CHECK] Skip: HEDGE_ENABLED=False")
            return False

        # V3: After prediction cut, don't rebuild the intentionally removed hedge
        if position.prediction_cut_side:
            logger.debug(f"[HEDGE CHECK] Skip: Prediction cut active (cut={position.prediction_cut_side})")
            return False

        # V3: Hedge cooldown - prevent re-hedging spam when API sync clears hedge state
        # If we hedged recently (within cooldown), don't re-hedge even if state cleared
        last_hedge_ts = getattr(position, 'last_hedge_ts', 0)
        if last_hedge_ts > 0 and (time.time() - last_hedge_ts) < HEDGE_COOLDOWN_SEC:
            logger.debug(f"[HEDGE CHECK] Skip: Hedge cooldown ({time.time() - last_hedge_ts:.0f}s < {HEDGE_COOLDOWN_SEC:.0f}s)")
            return False

        w = position.winner_side()
        l = position.loser_side()
        w_tokens = position.get_tokens(w)
        l_tokens = position.get_tokens(l)
        w_ep = position.get_entry_price(w)
        l_ep = position.get_entry_price(l)
        w_price = position.get_price(w)

        if w_tokens < HEDGE_MIN_TOKENS:
            logger.debug(f"[HEDGE CHECK] Skip: Position too small ({w_tokens} < {HEDGE_MIN_TOKENS})")
            return False

        # V3.6: Check hedge budget cap - don't over-hedge
        max_hedge, current_hedge, remaining = self.get_hedge_budget(position)
        if remaining < POLYMARKET_MIN_USD:
            logger.info(f"[HEDGE CHECK] Skip: Hedge budget exhausted (${current_hedge:.2f} / ${max_hedge:.2f} = {current_hedge/max_hedge*100:.0f}%)")
            return False

        # V3.6: Warn if hedge is approaching or exceeding main
        if self.check_hedge_exceeds_main(position):
            logger.warning(f"[HEDGE CHECK] Skip: Hedge already exceeds main - position has flipped")
            return False

        # V3.1: Check if existing hedge is sufficient for break-even
        if l_tokens > 0 and l_ep > 0:
            breakeven_tokens = self.calculate_breakeven_opposite_tokens(
                main_tokens=w_tokens,
                main_entry_price=w_ep,
                opposite_entry_price=l_ep,  # Use actual entry price
            )
            if l_tokens >= breakeven_tokens * 0.95:  # Within 5% of target
                logger.debug(f"[HEDGE CHECK] Skip: At break-even ({l_tokens:.0f} >= {breakeven_tokens*0.95:.0f})")
                return False
            # Under break-even - allow topping up if losing
            logger.info(f"[HEDGE CHECK] Under break-even: have {l_tokens:.0f}, need {breakeven_tokens:.0f} (shortfall: {breakeven_tokens - l_tokens:.0f})")

        # V2.6: Only hedge when main PnL is negative (delayed hedge strategy)
        # Trust prediction, hedge only when position is losing
        # V2.16: Configurable threshold - default 0 = hedge at any loss
        # Set HEDGE_TRIGGER_PCT=-0.05 to wait until -5% loss before hedging
        pnl_pct = (w_price - w_ep) / w_ep if w_ep > 0 else 0
        if pnl_pct >= HEDGE_TRIGGER_PCT:
            logger.debug(f"[HEDGE CHECK] Skip: PnL {pnl_pct*100:+.1f}% >= trigger {HEDGE_TRIGGER_PCT*100:.1f}%")
            return False
        logger.info(f"[HEDGE CHECK] Should hedge: {w_tokens} tokens @ ${w_price:.4f} (PnL: {pnl_pct*100:+.1f}% < {HEDGE_TRIGGER_PCT*100:.1f}%), budget: ${remaining:.2f} remaining")
        return True

    # ----------------- V3.30: Price-Threshold Rebalance Sell -----------------
    # ----------------- Position Cycling -----------------
    def _reset_window_state_if_needed(self, window_ts: int):
        """Reset session stats and clear stale positions when window changes (15m markets)"""
        if self.state.last_window_ts != window_ts:
            old_window = self.state.last_window_ts
            self._log(f"NEW WINDOW: {old_window} -> {window_ts}", "INFO")

            # V3.27: Clean up old session state files
            self._cleanup_old_session_states(window_ts)

            # V3.3: Clear old positions from previous window - they're now stale
            # V3.29: Redeem winning tokens at $1, lose losing tokens at $0
            if self.state.positions:
                stale_count = len(self.state.positions)
                total_redemption = 0.0
                total_loss = 0.0
                for cid, pos in self.state.positions.items():
                    # Determine winning side by price
                    yes_wins = pos.yes_price > 0.50 if pos.yes_price > 0 else False
                    if yes_wins:
                        # YES wins → redeem YES tokens at $1
                        redemption = pos.yes_tokens * 1.0
                        total_redemption += redemption
                        loss = pos.no_tokens * pos.no_entry_price if pos.no_tokens > 0 else 0.0
                        total_loss += loss
                        # Record trade: winner side redeemed at $1
                        win_cost = pos.yes_tokens * pos.yes_entry_price
                        net_pnl = redemption - win_cost - loss
                        pnl_pct = net_pnl / (win_cost + loss) if (win_cost + loss) > 0 else 0
                        self._add_trade_to_dashboard(pos, 1.0, pnl_pct, net_pnl, "REDEEMED",
                                                     side="YES", entry_price=pos.yes_entry_price)
                    else:
                        # NO wins → redeem NO tokens at $1
                        if pos.no_tokens > 0:
                            redemption = pos.no_tokens * 1.0
                            total_redemption += redemption
                        else:
                            redemption = 0.0
                        # YES side loses → $0
                        loss = pos.yes_tokens * pos.yes_entry_price
                        total_loss += loss
                        # Record trade: winner side redeemed at $1
                        win_cost = pos.no_tokens * pos.no_entry_price if pos.no_tokens > 0 else 0.0
                        net_pnl = redemption - win_cost - loss
                        pnl_pct = net_pnl / (win_cost + loss) if (win_cost + loss) > 0 else 0
                        self._add_trade_to_dashboard(pos, 1.0, pnl_pct, net_pnl, "REDEEMED",
                                                     side="NO", entry_price=pos.no_entry_price)
                if total_redemption > 0:
                    self.current_balance += total_redemption
                self._log(f"CLEARING {stale_count} stale positions from window {old_window} | redeemed=${total_redemption:.2f} lost=${total_loss:.2f}", "WARN")
                self.state.positions.clear()
                self._market_end_times.clear()  # Clear old market end times too
                self._condition_to_slug.clear()  # V3.58 FIX: Clear old condition→slug mappings to prevent stale positions inflating SESSION TOTAL cost

            # Clear rate-limit flags for new window
            self._fetch_err_logged.clear()
            # Reset cached prices so dashboard doesn't show stale resolved prices
            self.current_yes_price = 0
            self.current_no_price = 0

            # Clear PM WS stale subscriptions for new window (prevent token accumulation)
            if self._pm_feed and V3_PM_WS_ENABLED:
                self._pm_feed.clear_subscriptions()

            # Clear regime detection buffers for new window
            self._price_ticks.clear()
            self._spread_ticks.clear()
            self._last_regime.clear()
            self._regime_pending.clear()
            self._regime_pending_count.clear()
            self._trend_confirm.clear()

            # V3: Reset session stats for new window
            self.state.session_total_cost = 0.0
            self.state.session_total_value = 0.0
            self.state.session_start_balance = self.current_balance
            self.state.last_window_ts = window_ts
            self.state.window_cycles.clear()  # Reset cycle counters
            # V3: Reset session TP flag, snapshot total_pnl
            self._session_tp_done = False
            self._session_start_pnl = self.state.total_pnl
            if self._begin_balance_from_cli:
                # First window only: use CLI --begin-balance override
                self._log(f"BEGIN_SESSION_BALANCE: ${self._begin_session_balance:.2f} (from --begin-balance)", "INFO")
                self._begin_balance_from_cli = False
                self._need_begin_balance_capture = False
            elif self._begin_session_balance > 0:
                # Persistent begin balance already loaded — preserve across windows
                pass
            else:
                # Defer: capture after first sync so positions are included
                self._need_begin_balance_capture = True

    # ============ V3.27: Session State Persistence ============
    def _session_state_path(self, window_ts: int = 0) -> Path:
        """Get path for session state file: trading/session_state_{window_ts}.json"""
        ts = window_ts or self.state.last_window_ts
        return SESSION_STATE_DIR / f"session_state_{ts}.json"

    def _save_session_state(self):
        """Write session state to disk for crash recovery. Called every sync cycle."""
        if not self.state.last_window_ts:
            return
        try:
            positions_data = {}
            for cid, pos in self.state.positions.items():
                positions_data[cid] = {
                    # V3.80: YES/NO token model
                    "yes_tokens": pos.yes_tokens,
                    "yes_entry_price": pos.yes_entry_price,
                    "yes_cost": pos.yes_cost,
                    "no_tokens": pos.no_tokens,
                    "no_entry_price": pos.no_entry_price,
                    "no_cost": pos.no_cost,
                    "session_total_cost": pos.session_total_cost,
                    "session_realized_profit": pos.session_realized_profit,
                    "session_sell_proceeds": pos.session_sell_proceeds,
                    "session_allocated_capital": pos.session_allocated_capital,
                    "prediction_boost_done": pos.prediction_boost_done,
                    "prediction_cut_side": pos.prediction_cut_side,
                    "prediction_confidence": pos.prediction_confidence,
                    "prediction_2nd_hedge_done": pos.prediction_2nd_hedge_done,
                    "flip_follow": pos.flip_follow,
                    "chunks_bought": pos.chunks_bought,
                    "chunks_sold": pos.chunks_sold,
                    "winner_peak_price": pos.winner_peak_price,
                    "winner_peak_side": pos.winner_peak_side,
                    "trailing_stop_done": pos.trailing_stop_done,
                    "cheap_loser_dca_count": pos.cheap_loser_dca_count,
                }
            state = {
                "window_ts": self.state.last_window_ts,
                "begin_session_balance": self._begin_session_balance,
                "total_pnl": self.state.total_pnl,
                "session_tp_done": self._session_tp_done,
                "session_start_pnl": self._session_start_pnl,
                "positions": positions_data,
                "direction_history": self._direction_history,  # V3.28
                "last_direction_window": self._last_direction_window,  # V3.28
                "saved_at": time.time(),
            }
            path = self._session_state_path()
            path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[STATE] Save failed: {e}")

    def _load_session_state(self, window_ts: int) -> bool:
        """Load session state from disk if file exists for current window.
        Returns True if state was loaded, False otherwise."""
        path = self._session_state_path(window_ts)
        if not path.exists():
            return False
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
            if state.get("window_ts") != window_ts:
                return False

            self._begin_session_balance = state.get("begin_session_balance", 0.0)
            self.state.total_pnl = state.get("total_pnl", 0.0)
            self._session_tp_done = state.get("session_tp_done", False)
            self._session_start_pnl = state.get("session_start_pnl", 0.0)
            # V3.28: Restore direction history + dedup guard for regime detection
            self._direction_history = state.get("direction_history", {})
            self._last_direction_window = state.get("last_direction_window", {})
            if self._begin_session_balance > 0:
                self._need_begin_balance_capture = False

            # V3.80: Restore per-position session fields after API sync
            # API is source of truth for token counts. Session state restores accounting only.
            positions_data = state.get("positions", {})
            restored = 0
            for cid, pos in self.state.positions.items():
                if cid in positions_data:
                    pd = positions_data[cid]
                    # V3.80: Restore YES/NO costs (or migrate from old format)
                    if "yes_cost" in pd:
                        pos.yes_cost = pd.get("yes_cost", 0.0)
                        pos.no_cost = pd.get("no_cost", 0.0)
                    else:
                        # Migration: old session_main_cost/session_hedge_cost → yes/no
                        # Determine which side was "main" from saved data
                        old_main_cost = pd.get("session_main_cost", 0.0)
                        old_hedge_cost = pd.get("session_hedge_cost", 0.0)
                        # Best guess: if YES tokens > NO tokens, YES was likely main
                        if pos.yes_tokens >= pos.no_tokens:
                            pos.yes_cost = old_main_cost
                            pos.no_cost = old_hedge_cost
                        else:
                            pos.no_cost = old_main_cost
                            pos.yes_cost = old_hedge_cost
                    pos.session_total_cost = pd.get("session_total_cost", 0.0)
                    pos.session_realized_profit = pd.get("session_realized_profit", 0.0)
                    pos.session_sell_proceeds = pd.get("session_sell_proceeds", 0.0)
                    pos.session_allocated_capital = pd.get("session_allocated_capital", 0.0)
                    pos.prediction_boost_done = pd.get("prediction_boost_done", False)
                    pos.prediction_cut_side = pd.get("prediction_cut_side", "")
                    pos.prediction_confidence = pd.get("prediction_confidence", 0.0)
                    pos.prediction_2nd_hedge_done = pd.get("prediction_2nd_hedge_done", False)
                    pos.flip_follow = pd.get("flip_follow", False)
                    pos.chunks_bought = pd.get("chunks_bought", 1)
                    pos.chunks_sold = pd.get("chunks_sold", 0)
                    pos.winner_peak_price = pd.get("winner_peak_price", 0.0)
                    pos.winner_peak_side = pd.get("winner_peak_side", "")
                    pos.trailing_stop_done = pd.get("trailing_stop_done", False)
                    pos.cheap_loser_dca_count = pd.get("cheap_loser_dca_count", 0)
                    # Ghost cost detection: compare saved vs current API token counts
                    saved_yes = pd.get("yes_tokens", pd.get("size_tokens", 0))
                    saved_no = pd.get("no_tokens", pd.get("hedge_size_tokens", 0))
                    ghost_yes = max(0, pos.yes_tokens - saved_yes)
                    ghost_no = max(0, pos.no_tokens - saved_no)
                    if ghost_yes > 0.5 or ghost_no > 0.5:
                        api_yes_cost = pos.yes_tokens * pos.yes_entry_price if pos.yes_entry_price > 0 else 0
                        api_no_cost = pos.no_tokens * pos.no_entry_price if pos.no_entry_price > 0 else 0
                        api_total_cost = api_yes_cost + api_no_cost
                        ghost_cost = max(0, api_total_cost - pos.session_total_cost)
                        if ghost_cost > 0.5:
                            ghost_yes_cost = max(0, api_yes_cost - pos.yes_cost) if ghost_yes > 0.5 else 0
                            ghost_no_cost = max(0, api_no_cost - pos.no_cost) if ghost_no > 0.5 else 0
                            pos.session_total_cost += ghost_cost
                            pos.yes_cost += ghost_yes_cost
                            pos.no_cost += ghost_no_cost
                            self._log(
                                f"GHOST COST: YES +{ghost_yes:.0f} NO +{ghost_no:.0f} = "
                                f"${ghost_cost:.2f} added (total=${pos.session_total_cost:.2f} "
                                f"API=${api_total_cost:.2f})", "WARN")
                        else:
                            self._log(
                                f"GHOST TOKENS: YES +{ghost_yes:.0f} NO +{ghost_no:.0f} "
                                f"(cost already tracked: ${pos.session_total_cost:.2f} >= API ${api_total_cost:.2f})", "INFO")
                    restored += 1

            self._log(f"SESSION STATE LOADED: {restored}/{len(positions_data)} positions restored | "
                      f"begin=${self._begin_session_balance:.2f} pnl=${self.state.total_pnl:+.2f} tp_done={self._session_tp_done}", "SUCCESS")
            return True
        except Exception as e:
            logger.warning(f"[STATE] Load failed: {e}")
            return False

    def _cleanup_old_session_states(self, current_window_ts: int):
        """Delete session state files from previous windows."""
        try:
            for f in SESSION_STATE_DIR.glob("session_state_*.json"):
                # Extract window_ts from filename
                ts_str = f.stem.replace("session_state_", "")
                if ts_str.isdigit() and int(ts_str) != current_window_ts:
                    f.unlink()
                    logger.debug(f"[STATE] Cleaned up old state file: {f.name}")
        except Exception as e:
            logger.debug(f"[STATE] Cleanup failed: {e}")

    # ============ Persistent Begin Balance (across windows & restarts) ============
    def _get_config_snapshot(self) -> str:
        """Collect all bot-relevant env vars as sorted key=value string for change detection."""
        critical_keys = {
            "DRY_RUN", "POLYMARKET_FUNDER", "PRIVATE_KEY", "BOT_STOP_THRESHOLD",
            "POLYGON_RPC_URL", "BOT_ID",
        }
        items = sorted(
            (k, v) for k, v in os.environ.items()
            if k.startswith("V3_") or k.startswith("SCALPING_") or k in critical_keys
        )
        # Hash PRIVATE_KEY instead of storing it in plaintext
        return "\n".join(
            f"{k}={hashlib.sha256(v.encode()).hexdigest()[:16]}" if k == "PRIVATE_KEY"
            else f"{k}={v}"
            for k, v in items
        )

    def _save_persistent_balance(self):
        """Save begin_session_balance + config snapshot for cross-restart persistence."""
        if self._begin_session_balance <= 0:
            return
        try:
            data = {"begin_session_balance": self._begin_session_balance, "saved_at": time.time()}
            PERSISTENT_BALANCE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
            LAST_CONFIGS_PATH.write_text(self._get_config_snapshot(), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[PERSIST] Save failed: {e}")

    def _load_persistent_balance(self) -> bool:
        """Load persistent begin_balance if configs unchanged. Returns True if loaded."""
        if self._begin_balance_from_cli:
            return False  # CLI override takes priority
        if not PERSISTENT_BALANCE_PATH.exists() or not LAST_CONFIGS_PATH.exists():
            return False
        try:
            data = json.loads(PERSISTENT_BALANCE_PATH.read_text(encoding="utf-8"))
            saved_bal = data.get("begin_session_balance", 0)
            if saved_bal <= 0:
                return False
            saved_configs = LAST_CONFIGS_PATH.read_text(encoding="utf-8").strip()
            current_configs = self._get_config_snapshot().strip()
            if saved_configs != current_configs:
                self._log("CONFIG CHANGED — resetting begin_session_balance", "WARN")
                PERSISTENT_BALANCE_PATH.unlink(missing_ok=True)
                LAST_CONFIGS_PATH.write_text(current_configs, encoding="utf-8")
                return False
            self._begin_session_balance = saved_bal
            self._need_begin_balance_capture = False
            self._log(f"PERSISTENT BEGIN_BALANCE LOADED: ${saved_bal:.2f} (configs unchanged)", "SUCCESS")
            return True
        except Exception as e:
            logger.warning(f"[PERSIST] Load failed: {e}")
            return False

    def can_reenter(self, condition_id: str) -> bool:
        """Check if we can re-enter after exiting in same window.

        V3.32: Always block re-entry after COMBINED_TP/SESSION_TP in same window.
        When CYCLING_ENABLED, uses cooldown timing for reentry control.
        """
        # V3.32: Always check window_exits — block re-entry after TP close regardless of cycling setting
        if condition_id in self.state.window_exits:
            if not CYCLING_ENABLED:
                return False  # No cycling = no re-entry after exit
            exit_price, exit_time = self.state.window_exits[condition_id]
            cycles = self.state.window_cycles.get(condition_id, 0)
            if cycles >= MAX_CYCLES_PER_WINDOW:
                return False
            if time.time() - exit_time < CYCLE_COOLDOWN_SEC:
                return False  # Still in cooldown

        return True

    # ----------------- Multi-Market Capital Management -----------------
    def get_positions_value(self) -> float:
        """Get total value of all open positions"""
        return sum(
            p.get_avail("YES") * p.yes_price + p.get_avail("NO") * p.no_price
            for p in self.state.positions.values())

    def get_available_capital_for_market(self, market_slug: str) -> float:
        """Get remaining capital available for a specific market"""
        total_capital = self.current_balance + self.get_positions_value()
        max_per_market = total_capital * MAX_CAPITAL_PER_MARKET_PCT

        market_exposure = sum(
            (p.yes_tokens * p.yes_entry_price + p.no_tokens * p.no_entry_price)
            for p in self.state.positions.values()
            if p.market_slug == market_slug
        )

        return max(0, max_per_market - market_exposure)

    def get_total_exposure(self) -> float:
        """Get total capital exposed in all positions"""
        return sum(
            (p.yes_tokens * p.yes_entry_price + p.no_tokens * p.no_entry_price)
            for p in self.state.positions.values())

    def _get_all_positions_value(self) -> float:
        """Get combined value of all open positions using live prices."""
        total = 0.0
        for p in self.state.positions.values():
            total += p.get_avail("YES") * p.yes_price + p.get_avail("NO") * p.no_price
        return total

    def _get_all_positions_unrealized_pnl(self) -> float:
        """Get unrealized PnL of all open positions (value - cost)."""
        pnl = 0.0
        for p in self.state.positions.values():
            yes_avail = p.get_avail("YES")
            no_avail = p.get_avail("NO")
            pnl += yes_avail * (p.yes_price - p.yes_entry_price) if p.yes_entry_price > 0 else 0
            pnl += no_avail * (p.no_price - p.no_entry_price) if p.no_entry_price > 0 else 0
        return pnl

    def _get_session_profit(self) -> Tuple[float, float]:
        """Get total session profit using portfolio value vs begin balance.

        Uses real API balance (current_balance) + live position values + live unredeemed value.
        Must match dashboard Total formula to avoid phantom PnL when unredeemed
        positions get reclassified as tracked mid-session.
        """
        if self._begin_session_balance <= 0:
            return 0.0, 0.0
        portfolio_value = self.current_balance + self._get_all_positions_value() + self._unredeemed_positions_value
        total_profit = portfolio_value - self._begin_session_balance
        profit_pct = total_profit / self._begin_session_balance
        return total_profit, profit_pct

    async def _close_all_positions_session_tp(self):
        """Close all open positions (main + hedge) for session-level TP, then block re-entry."""
        # Set flag FIRST to prevent parallel manage_position calls from re-triggering
        self._session_tp_done = True
        # Retry loop: close_position may return on partial fill, so keep trying
        for retry_round in range(MAX_RETRIES):
            # Always sync from API first to get accurate state (catches positions popped but still on-chain)
            self._sync_positions_from_api()
            if retry_round > 0:
                await asyncio.sleep(RETRY_DELAY)
            # Check BOTH main and hedge tokens for remaining positions
            remaining = [(cid, pos) for cid, pos in self.state.positions.items()
                         if pos.get_avail("YES") > 0.1 or pos.get_avail("NO") > 0.1]
            if not remaining:
                break
            # Skip positions on the predicted winning side — let them resolve at $1.00
            to_close = []
            kept = []
            for cid, pos in remaining:
                if pos.prediction_cut_side:
                    predicted_winner = "YES" if pos.prediction_cut_side == "NO" else "NO"
                    w = pos.winner_side()
                    if w == predicted_winner:
                        kept.append(f"{w}(pred={predicted_winner})")
                        continue
                to_close.append((cid, pos))
            if kept:
                self._log(f"SESSION TP: Keeping {len(kept)} predicted-winning positions: {', '.join(kept)}", "INFO")
            if not to_close:
                break
            self._log(f"SESSION TP round {retry_round+1}/{MAX_RETRIES}: {len(to_close)} positions to close ({len(kept)} kept)", "WARN")
            # V3.28: Sell all sides in parallel (main + hedge concurrently)
            close_tasks = []
            for cid, pos in to_close:
                if self.state.positions.get(cid):
                    close_tasks.append(self.close_position(pos, "SESSION_TP"))
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
        # Final verification
        self._sync_positions_from_api()
        final_remaining = [(cid, pos) for cid, pos in self.state.positions.items()
                           if pos.get_avail("YES") > 0.1 or pos.get_avail("NO") > 0.1]
        if final_remaining:
            names = [f"YES {p.get_avail('YES'):.0f}+NO {p.get_avail('NO'):.0f}" for _, p in final_remaining]
            self._log(f"SESSION TP WARNING: {len(final_remaining)} positions could not be fully closed: {names}", "ERROR")
        else:
            self._log("SESSION TP DONE — all positions closed", "SUCCESS")

    def can_enter_market(self, market_slug: str, amount_usd: float) -> bool:
        """Check if we can enter a market with given amount"""
        total_capital = self.current_balance + self.get_positions_value()

        if self.get_available_capital_for_market(market_slug) < amount_usd:
            return False

        if (self.get_total_exposure() + amount_usd) > (total_capital * MAX_TOTAL_EXPOSURE_PCT):
            return False

        return True

    # ----------------- LIVE Preconditions -----------------
    def _init_trader(self) -> bool:
        if self.dry_run:
            return True
        if self.trader:
            return True
        if not PRIVATE_KEY:
            self._live_trading_blocked_reason = "PRIVATE_KEY missing"
            logger.error("PRIVATE_KEY not set - trading disabled")
            return False

        try:
            # polymarket_bot_main is in same trading/ folder
            from lib.polymarket_bot_main import PolymarketTrader

            self.trader = PolymarketTrader(
                private_key=PRIVATE_KEY,
                signature_type=SIGNATURE_TYPE,
                funder_address=FUNDER_ADDRESS,
                clob_host=CLOB_HOST,
                gamma_host=GAMMA_HOST,
                data_host=DATA_HOST,
            )
            logger.info(f"Trader initialized: {self.trader.trading_address}")
            self._refresh_balance_and_allowance_block_if_needed()
            return self._live_trading_blocked_reason is None
        except Exception as e:
            self._live_trading_blocked_reason = f"init trader failed: {e}"
            logger.error(f"Failed to init trader: {e}")
            return False

    def _refresh_balance_and_allowance_block_if_needed(self):
        """Refresh USDC balance from API. Match v1 behavior - only check balance, not allowance."""
        if self.dry_run or not self.trader:
            return
        try:
            bal = self.trader.get_usdc_balance_and_allowance()
            balance = float(bal.get("balance", 0)) / 1e6
            self.current_balance = balance

            if balance <= 0.5:
                self._live_trading_blocked_reason = "USDC balance too low"
                self._log(f"⚠️ Balance too low: ${balance:.2f}. Deposit USDC first.", "ERROR")
            else:
                self._live_trading_blocked_reason = None
        except Exception as e:
            self._live_trading_blocked_reason = f"balance check failed: {e}"
            logger.warning(f"Balance refresh failed: {e}")

    def _sync_positions_from_api(self, force: bool = False):
        """Sync positions from Data API. Catches positions opened externally or from previous sessions.
        force=True bypasses V3.39 active-session guard (used for ghost fill detection)."""
        if self.dry_run or not self.trader:
            return

        try:
            positions = fetch_positions(self.trader.trading_address, limit=100)
            logger.debug(f"[SYNC] API returned {len(positions)} positions")

            # Get current window timestamp for filtering stale positions
            current_window_ts = self.get_current_window_ts()

            # Filter: size > 0, value > 0, and NOT resolved (price not near 0 or 1)
            active = []
            unredeemed_value = 0.0
            for p in positions:
                size = float(p.get("size", 0))
                value = float(p.get("currentValue", 0))
                cur_price = float(p.get("curPrice", 0))
                avg_price = float(p.get("avgPrice", 0))
                slug = p.get("slug", "")
                outcome = p.get("outcome", "")
                title = p.get("title", "")[:50]
                cost = size * avg_price
                pnl_pct_api = float(p.get("percentPnl", 0))

                if size <= 0 or value <= 0:
                    logger.debug(f"[SYNC] Skip (size/value 0): {slug}")
                    continue

                # V3.3 FIX: Filter by market window using condition_id FIRST
                # Don't rely on cur_price — API sometimes returns 0 for active positions
                condition_id = p.get("conditionId", "")
                if not condition_id:
                    logger.debug(f"[SYNC] Skip (no condition_id): {title}")
                    continue

                is_current_window = False
                if condition_id not in self._market_end_times:
                    # V3.40: If _market_end_times is empty (cold start, market fetch failed),
                    # accept ALL positions — better to load duplicates than re-enter and double-buy
                    if len(self._market_end_times) == 0:
                        logger.warning(f"[SYNC] No market end times available — accepting position: {outcome} {size:.0f} | {title}")
                        is_current_window = True
                    else:
                        # Position from another window — check if resolved
                        if cur_price >= 0.999:
                            unredeemed_value += size * 1.0
                        logger.debug(f"[SYNC] Skip STALE position (unknown window): {outcome} {size:.0f} | {title}")
                        continue

                elif condition_id in self._market_end_times:
                    market_end_ts = self._market_end_times[condition_id]
                    position_window_ts = int(market_end_ts) - V3_WINDOW_SECONDS  # window start = end - window duration
                    if position_window_ts != current_window_ts:
                        # Position from different window
                        if cur_price >= 0.999:
                            unredeemed_value += size * 1.0
                        logger.debug(f"[SYNC] Skip STALE position (window {position_window_ts} != {current_window_ts}): {outcome} {size:.0f} | {title}")
                        continue
                    else:
                        is_current_window = True

                # Only skip resolved positions if NOT current window
                # Current window positions are always kept (API may return cur_price=0 for active positions)
                if not is_current_window and (cur_price <= 0.001 or cur_price >= 0.999):
                    if cur_price >= 0.999:
                        unredeemed_value += size * 1.0
                    continue

                logger.debug(f"[SYNC API] {outcome} {size:.2f} @ ${avg_price:.4f} (cur=${cur_price:.4f}, pnl={pnl_pct_api:+.1f}%) = ${cost:.2f} | {title}")
                active.append(p)

            self._unredeemed_positions_value = unredeemed_value
            if unredeemed_value > 0:
                logger.debug(f"[SYNC] Unredeemed/untracked positions value: ${unredeemed_value:.2f}")
            # V3.27: Refresh USDC balance in sync with unredeemed changes
            # When redeem_service converts positions to USDC, both must update together
            # to avoid phantom loss in Portfolio TP / dashboard calculations
            self._refresh_balance_and_allowance_block_if_needed()

            # Group active positions by conditionId (no title keyword matching — conditionId is reliable)
            positions_by_condition: Dict[str, list] = {}
            for dp in active:
                condition_id = dp.get("conditionId", "")
                if condition_id:
                    positions_by_condition.setdefault(condition_id, []).append(dp)

            # Process each condition_id group
            for condition_id, group in positions_by_condition.items():
                # Check if we already have a position for this condition_id
                existing_pos = self.state.positions.get(condition_id)

                # V3.80: Match API positions by outcome (YES/NO) directly — no main/hedge distinction
                if existing_pos:
                    sell_grace_active = (existing_pos.last_rebalance_sell_ts > 0
                                         and time.time() - existing_pos.last_rebalance_sell_ts < 30)
                    # Match each API position to YES or NO by outcome
                    yes_dp = None
                    no_dp = None
                    for dp in group:
                        outcome = dp.get("outcome", "").lower()
                        api_side = "YES" if outcome in ("yes", "up") else "NO"
                        token_id = dp.get("asset", "")
                        # Skip recently closed tokens (phantom prevention)
                        if token_id in self.state.recently_closed_tokens:
                            closed_time = self.state.recently_closed_tokens[token_id]
                            if time.time() - closed_time < 60:
                                logger.debug(f"[SYNC] Skipping recently closed token: {token_id[:16]}...")
                                continue
                            else:
                                del self.state.recently_closed_tokens[token_id]
                        if api_side == "YES":
                            yes_dp = dp
                        else:
                            no_dp = dp

                    # Sync each side independently
                    for side_label, side_dp in [("YES", yes_dp), ("NO", no_dp)]:
                        if side_dp:
                            api_size = float(side_dp.get("size", 0))
                            api_avg = float(side_dp.get("avgPrice", 0))
                            api_cur = float(side_dp.get("curPrice", 0))
                            api_token = side_dp.get("asset", "")

                            # Set token_id if not yet known
                            if not existing_pos.get_token_id(side_label):
                                existing_pos.set_token_id(side_label, api_token)

                            # Sync token count (skip during sell grace)
                            if sell_grace_active:
                                logger.debug(f"[SYNC] Skipping {side_label} token overwrite "
                                             f"(sell grace: {time.time() - existing_pos.last_rebalance_sell_ts:.0f}s ago)")
                            else:
                                existing_pos.set_tokens(side_label, api_size)
                                existing_pos.set_sold_tokens(side_label, 0.0)

                            # Sync entry price only on first sync (no local cost yet)
                            if api_avg > 0 and existing_pos.get_cost(side_label) <= 0:
                                existing_pos.set_entry_price(side_label, api_avg)

                            # V3.92: Always sync cost when API shows higher cost than locally tracked.
                            # Prevents phantom profit from prior-window ghost tokens whose cost
                            # wasn't in session_total_cost (e.g. bot restarted mid-window).
                            # Safe: if API avg lags after a buy, api_cost < local_cost → no-op.
                            if api_avg > 0 and api_size > 0:
                                api_cost = api_size * api_avg
                                local_cost = existing_pos.get_cost(side_label)
                                if local_cost < api_cost - 1.0:
                                    cost_gap = api_cost - local_cost
                                    old_tracked = local_cost
                                    self._track_buy_cost(existing_pos, side_label, cost_gap)
                                    if existing_pos.unconfirmed_buy_cost > 0:
                                        existing_pos.unconfirmed_buy_cost = max(0, existing_pos.unconfirmed_buy_cost - cost_gap)
                                    self._log(f"SYNC COST FIX: {side_label} cost gap ${cost_gap:.2f} "
                                              f"(was=${old_tracked:.2f} now=${existing_pos.get_cost(side_label):.2f} "
                                              f"API=${api_cost:.2f})", "WARNING")

                            # Set initial price from API (until orderbook updates)
                            if existing_pos.get_price(side_label) == 0 and api_cur > 0:
                                existing_pos.set_price(side_label, api_cur)
                                logger.debug(f"[SYNC INIT] {side_label} initial price from API: ${api_cur:.4f}")

                            # Log first time seeing this side
                            if existing_pos.get_tokens(side_label) > 0 and not existing_pos.get_token_id(side_label):
                                self._log(f"SYNCED {side_label}: {api_size:.2f} @ ${api_avg:.4f}", "SUCCESS")
                        else:
                            # Side not in API — check if we expected it
                            local_tokens = existing_pos.get_tokens(side_label)
                            if local_tokens > 0:
                                buy_age = time.time() - existing_pos.last_action_ts if existing_pos.last_action_ts > 0 else 999
                                local_side_cost = existing_pos.get_cost(side_label)
                                # V3.86: Don't remove sides with tracked cost (confirmed fills)
                                # API can temporarily not return positions due to propagation lag
                                if buy_age > 30 and local_side_cost <= 0:
                                    self._log(f"SYNCED: {side_label} {local_tokens:.0f} tokens SOLD (not in API)", "WARN")
                                    existing_pos.set_tokens(side_label, 0.0)
                                    existing_pos.set_sold_tokens(side_label, 0.0)
                                    existing_pos.set_entry_price(side_label, 0.0)
                                    existing_pos.set_token_id(side_label, "")
                                else:
                                    logger.debug(f"[SYNC] {side_label} not in API yet (bought {buy_age:.0f}s ago) — keeping local")

                    continue

                # V3.32: Skip creating position for condition_ids that were TP'd in this window
                if condition_id in self.state.window_exits:
                    logger.debug(f"[SYNC] Skip NEW position {condition_id[:16]}... — already exited this window")
                    continue

                # V3.80: NEW position — parse YES/NO from API directly
                first_dp = group[0]
                first_title = first_dp.get("title", "")

                # Find matching market slug
                market_slug = self._condition_to_slug.get(condition_id, "")
                if not market_slug:
                    _coin_abbrev = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana",
                                    "xrp": "xrp", "doge": "dogecoin", "ada": "cardano"}
                    title_lower = first_title.lower()
                    for m in self.markets:
                        slug_coin = m.slug_pattern.split("-")[0]
                        name_first = m.name.split()[0].lower()
                        full_name = _coin_abbrev.get(slug_coin, slug_coin)
                        if slug_coin in title_lower or name_first in title_lower or full_name in title_lower:
                            market_slug = m.slug_pattern
                            break
                if not market_slug:
                    market_slug = "unknown"
                    logger.debug(f"[SYNC] Unknown market slug for: {first_title}")

                total_balance = self.current_balance + sum(
                    (p.yes_tokens * p.yes_entry_price + p.no_tokens * p.no_entry_price)
                    for p in self.state.positions.values())
                allocated_capital = total_balance * MAX_CAPITAL_PER_MARKET_PCT

                new_pos = Position(
                    market_slug=market_slug,
                    condition_id=condition_id,
                    entry_time=time.time(),
                    session_allocated_capital=allocated_capital,
                )

                # Populate YES/NO sides from API
                total_cost = 0.0
                sides_log = []
                for dp in group:
                    outcome = dp.get("outcome", "").lower()
                    api_side = "YES" if outcome in ("yes", "up") else "NO"
                    api_token = dp.get("asset", "")
                    api_size = float(dp.get("size", 0))
                    api_avg = float(dp.get("avgPrice", 0))
                    api_cur = float(dp.get("curPrice", 0))
                    new_pos.set_token_id(api_side, api_token)
                    new_pos.set_tokens(api_side, api_size)
                    new_pos.set_entry_price(api_side, api_avg)
                    new_pos.set_price(api_side, api_cur)
                    side_cost = api_size * api_avg
                    if api_side == "YES":
                        new_pos.yes_cost = side_cost
                    else:
                        new_pos.no_cost = side_cost
                    total_cost += side_cost
                    sides_log.append(f"{api_side} {api_size:.2f}")

                new_pos.session_total_cost = total_cost
                self._log(f"SYNCED: {' + '.join(sides_log)} ({first_title[:30]}...)", "SUCCESS")
                self.state.positions[condition_id] = new_pos

            # Remove positions confirmed resolved (price at 0 or 1)
            # V3.58 FIX: Also remove stale positions from previous windows that are
            # no longer in API and not in current window's condition_to_slug mapping.
            # Old policy "never remove based on API absence" caused ghost positions
            # whose session_total_cost inflated SESSION TOTAL and exhausted DCA budget.
            to_remove = []
            for cid, pos in self.state.positions.items():
                w = pos.winner_side()
                w_price = pos.get_price(w)
                if w_price <= 0.001 or w_price >= 0.999:
                    self._log(f"SYNCED: {w} position resolved (price={w_price:.4f})", "WARN")
                    to_remove.append(cid)
                elif cid not in positions_by_condition and cid not in self._condition_to_slug:
                    # Not in API AND not in current window's markets — stale from old window
                    self._log(f"SYNCED: removing stale position {w} {pos.get_tokens(w):.0f} (not in API, not in current window)", "WARN")
                    to_remove.append(cid)

            for cid in to_remove:
                self.state.positions.pop(cid, None)

        except Exception as e:
            logger.debug(f"Position sync failed: {e}")  # Silent fail - will retry next cycle

    # ----------------- Decision Logic -----------------
    def should_dca_winner_when_loser_maxed(self, position: Position) -> bool:
        """V2.9: When loser side is maxed, DCA the winner side to capture profit.

        Conditions:
        1. Main position is at max tokens (loser is maxed)
        2. Main position is losing (loser side)
        3. Hedge exists
        4. Hedge is not already maxed
        """
        if position is None:
            return False
        # V3: After prediction cut, don't DCA — position is winding down
        if position.prediction_cut_side:
            logger.debug(f"[DCA WINNER] Skip: Prediction cut active (cut={position.prediction_cut_side})")
            return False

        w = position.winner_side()
        l = position.loser_side()
        w_tokens = position.get_tokens(w)
        l_tokens = position.get_tokens(l)
        w_ep = position.get_entry_price(w)
        w_price = position.get_price(w)
        w_pnl = (w_price - w_ep) / w_ep if w_ep > 0 else 0

        # Main (winner) must be at max (loser maxed out)
        if w_tokens < MAX_POSITION_TOKENS:
            logger.debug(f"[DCA WINNER] Main not maxed: {w_tokens:.2f} < {MAX_POSITION_TOKENS:.2f}")
            return False

        # Main must be losing
        if w_pnl >= 0:
            logger.debug(f"[DCA WINNER] Main not losing: {w_pnl*100:.1f}% >= 0")
            return False

        # Hedge must exist
        if l_tokens <= 0:
            logger.debug(f"[DCA WINNER] No hedge: {l_tokens:.2f}")
            return False

        # Hedge must not be maxed (use same limit for both sides)
        if l_tokens >= MAX_POSITION_TOKENS:
            logger.info(f"[DCA WINNER SKIP] Hedge also maxed: {l_tokens:.0f}/{MAX_POSITION_TOKENS:.0f}")
            return False

        logger.info(f"[DCA WINNER READY] main={w_tokens:.0f}/{MAX_POSITION_TOKENS:.0f} main_pnl={w_pnl*100:.1f}% hedge={l_tokens:.0f}")
        return True

    def should_dca_sell(self, position: Position) -> bool:
        # V3.29: DCA SELL disabled — only exit via COMBINED_TP or REBALANCE cut-loss
        return False

    def should_stop_loss(self, position: Position) -> bool:
        # V2 Strategy: NO traditional stop loss
        # Hedge protects downside - let positions resolve naturally
        # Traditional SL exits prematurely and locks in losses
        # The hedge guarantees partial return regardless of outcome
        return False

    def _update_stepped_tsl(self, position: Position) -> Tuple[int, float, bool]:
        """V2.20: Update stepped trailing stop loss - RATCHET UP ONLY, never down.

        Logic: Floor increases when PnL crosses 50% past each step boundary.
        With STEP=4% (half_step=2%):
        - Level 1: SKIPPED (no break-even floor at 0%)
        - Level 2: PnL ≥ 6% (1.5×STEP) → floor = 4%
        - Level 3: PnL ≥ 10% (2.5×STEP) → floor = 8%
        - Level 4: PnL ≥ 14% (3.5×STEP) → floor = 12%

        IMPORTANT: Floor can ONLY go UP, never down.
        - If profit hits Level 3 (floor +8%), floor stays at +8% forever
        - If profit drops below floor → EXIT immediately
        - This protects gains by locking in the highest floor reached

        Returns:
            Tuple of (new_level, new_floor, level_changed)
        """
        if not STEPPED_TSL_ENABLED:
            return position.tsl_level, position.tsl_floor, False

        w = position.winner_side()
        w_ep = position.get_entry_price(w)
        w_price = position.get_price(w)
        pnl_percent = (w_price - w_ep) / w_ep if w_ep > 0 else 0
        current_level = position.tsl_level
        current_floor = position.tsl_floor

        step = STEPPED_TSL_STEP  # e.g., 0.04 = 4%
        half_step = step / 2  # 50% trigger offset (e.g., 0.02 = 2%)

        # Calculate target level using 50% trigger (same as v1)
        # Level N triggers at (N - 0.5) × STEP, floor = (N - 1) × STEP
        if pnl_percent >= half_step:
            target_level = int((pnl_percent + half_step) / step)
        else:
            target_level = 0

        # Calculate floor for target level: floor = (level - 1) × step
        target_floor = max(0.0, (target_level - 1) * step)

        # V2.20: Skip Level 1 (break-even) - TSL only activates at Level 2+
        # Level 1 floor=0% is too tight, let SINGLE_SIDE_MAX_LOSS_PCT handle losses
        if target_level <= 1:
            return current_level, current_floor, False  # Don't activate TSL yet

        # RATCHET UP ONLY: Floor can only increase, never decrease
        if target_level > current_level:
            return target_level, target_floor, True

        # Keep current level/floor (no step down allowed)
        return current_level, current_floor, False

    def should_tsl_exit(self, position: Position) -> bool:
        """V2.20: Check if stepped TSL should trigger exit.

        Returns True if:
        - TSL is enabled
        - Position has reached at least level 1 (floor >= 0)
        - Current PnL is still positive (TSL protects GAINS, not losses)
        - Current PnL has dropped below the floor

        If PnL goes negative, TSL does NOT trigger - let SINGLE_SIDE_MAX_LOSS_PCT handle it.
        This prevents TSL from acting as a tight stop-loss on positions that never profited.
        """
        if not STEPPED_TSL_ENABLED:
            return False
        if position.tsl_floor < 0:  # TSL not yet activated
            return False
        # V2.20 FIX: Only trigger TSL if position is still profitable
        # If PnL < 0, let SINGLE_SIDE_MAX_LOSS_PCT handle it instead
        w = position.winner_side()
        w_ep = position.get_entry_price(w)
        w_price = position.get_price(w)
        pnl = (w_price - w_ep) / w_ep if w_ep > 0 else 0
        if pnl < 0:
            return False
        return pnl < position.tsl_floor

    # V3.3: Removed should_take_profit and execute_profit_cycle
    # Now using only DCA sell via should_dca_sell (DCA_EXIT_MIN_PROFIT_PCT)

    def should_close_session_profit_target(self, position: Position) -> bool:
        """V2.3: Check if session profit target reached - close all positions.

        Returns True if realized profit >= SESSION_PROFIT_TARGET_PCT of allocated capital.
        Allocated capital = balance × MAX_CAPITAL_PER_MARKET_PCT at session start.
        """
        if not SESSION_PROFIT_TARGET_ENABLED:
            return False

        # Use allocated capital as the base for profit calculation
        # If not set (legacy), fall back to session_total_cost
        base_capital = position.session_allocated_capital if position.session_allocated_capital > 0 else position.session_total_cost
        if base_capital <= 0:
            return False

        profit_pct = position.session_realized_profit / base_capital
        if profit_pct >= SESSION_PROFIT_TARGET_PCT:
            logger.info(f"[SESSION PROFIT TARGET] Realized ${position.session_realized_profit:.2f} = {profit_pct*100:.1f}% of ${base_capital:.2f} allocated >= {SESSION_PROFIT_TARGET_PCT*100:.0f}% target")
            return True

        return False

    def should_combined_tp(self, position: Position, combined_pnl_pct: float, combined_pnl_usd: float) -> bool:
        """V2.7: Check if combined (main+hedge) profit target reached.

        Uses UNREALIZED combined PnL to trigger take profit.
        Works for both hedged and single-side positions.
        """
        if not COMBINED_TP_ENABLED:
            return False

        # V3.11: Skip COMBINED_TP for small positions — % swings are noisy
        # V3.37: Use session_total_cost directly (VWAP-free, immune to API sync issues)
        total_cost = position.session_total_cost
        if total_cost < COMBINED_TP_MIN_COST_USD:
            logger.debug(f"[COMBINED TP] Skip: total cost ${total_cost:.2f} < ${COMBINED_TP_MIN_COST_USD:.0f} threshold")
            return False

        # V3.9: High-confidence prediction → hold for resolution, skip COMBINED_TP (only if enabled)
        if V3_PREDICTION_CONFIDENCE_IGNORE_COMBINED_TP_ENABLED and position.prediction_boost_done and position.prediction_confidence > V3_PREDICTION_CONFIDENCE_IGNORE_COMBINED_TP:
            logger.debug(f"[COMBINED TP] Skip: Prediction confidence {position.prediction_confidence:.0%} > {V3_PREDICTION_CONFIDENCE_IGNORE_COMBINED_TP:.0%} — hold for resolution")
            return False

        # V3.16: Block combined TP during ghost fill cooldown — API may not have propagated yet
        if position.last_buy_error_ts > 0:
            elapsed = time.time() - position.last_buy_error_ts
            if elapsed < V3_GHOST_FILL_COOLDOWN_SEC:
                self._log(f"COMBINED TP BLOCKED: ghost fill cooldown {elapsed:.0f}s / {V3_GHOST_FILL_COOLDOWN_SEC:.0f}s "
                         f"(unconfirmed=${position.unconfirmed_buy_cost:.2f})", "WARNING")
                return False

        # V3.76: TSL floor-breach check runs even when PnL drops below TP threshold
        # V3.79: Only fire TSL if PnL > 0 — when negative, let DCA/FJ/rebalance recover
        # V3.95 FIX: Check peak >= TP (TSL was activated), not floor >= TP.
        # After DCA buys inflate cost, floor (peak - step) can drop below TP threshold
        # even though TSL was legitimately activated. Old check caused TSL dead zone.
        if V3_COMBINED_TSL_ENABLED and position.combined_tp_peak_pnl >= COMBINED_TP_PCT and combined_pnl_pct < position.combined_tp_floor:
            if combined_pnl_pct > 0:
                self._log(
                    f"COMBINED TP TSL EXIT: PnL {combined_pnl_pct*100:+.1f}% dropped below floor "
                    f"{position.combined_tp_floor*100:+.1f}% (peak {position.combined_tp_peak_pnl*100:+.1f}%) - CLOSE ALL", "SIGNAL")
                return True
            else:
                # PnL negative — reset TSL state so recovery isn't capped by stale floor
                old_floor = position.combined_tp_floor
                position.combined_tp_peak_pnl = 0.0
                position.combined_tp_floor = 0.0
                self._log(
                    f"COMBINED TP TSL RESET: PnL {combined_pnl_pct*100:+.1f}% went negative "
                    f"(was floor {old_floor*100:+.1f}%) — reset TSL, let DCA/FJ recover", "WARNING")
                return False

        # Regime-aware TP: use CHOP_TP (lower) when in CHOP regime for faster exits
        # V3.99: FLIP_FOLLOW positions use 2x TP (betting on market flip)
        _effective_tp = COMBINED_TP_PCT * 2 if position.flip_follow else COMBINED_TP_PCT
        if V3_REGIME_CHOP_TP > 0:
            _market_sym = (position.market_slug or "").split("-")[0].upper()
            _cur_regime = self._last_regime.get(_market_sym, "TREND")
            if _cur_regime == "CHOP" and V3_REGIME_CHOP_TP < COMBINED_TP_PCT:
                _effective_tp = V3_REGIME_CHOP_TP
                logger.debug(f"[REGIME] {_market_sym} CHOP → TP lowered to {_effective_tp*100:.0f}%")

        if combined_pnl_pct >= _effective_tp:
            # V3.67: Combined TP Trailing Stop — trail instead of selling immediately
            if V3_COMBINED_TSL_ENABLED:
                # Update peak (ratchet up only)
                if combined_pnl_pct > position.combined_tp_peak_pnl:
                    position.combined_tp_peak_pnl = combined_pnl_pct
                # Calculate floor = peak - step, ratchet up only
                new_floor = position.combined_tp_peak_pnl - V3_COMBINED_TSL_STEP
                if new_floor > position.combined_tp_floor:
                    position.combined_tp_floor = new_floor
                self._log(
                    f"COMBINED TP TSL Trailing: PnL {combined_pnl_pct*100:+.1f}% (${combined_pnl_usd:+.2f}) | "
                    f"peak {position.combined_tp_peak_pnl*100:+.1f}% | floor {position.combined_tp_floor*100:+.1f}%", "INFO")
                return False
            else:
                self._log(f"COMBINED TP EXIT: PnL {combined_pnl_pct*100:+.1f}% (${combined_pnl_usd:+.2f}) >= +{_effective_tp*100:.0f}% target - CLOSE ALL", "SIGNAL")
                return True

        return False

    # ----------------- Core Loop -----------------
    def _cleanup_stale_pending_orders(self):
        """V2.17: Remove pending orders that have timed out (unfilled after PENDING_ORDER_TIMEOUT seconds)"""
        now = time.time()
        stale_keys = []
        for condition_id, start_time in list(self.state.pending_buy_timestamps.items()):
            if now - start_time > PENDING_ORDER_TIMEOUT:
                pending_tokens = self.state.pending_buy_tokens.get(condition_id, 0)
                if pending_tokens > 0:
                    logger.warning(f"[PENDING TIMEOUT] Clearing {pending_tokens:.0f} stale pending tokens for {condition_id[:16]}... (age: {now - start_time:.1f}s)")
                    stale_keys.append(condition_id)

        for condition_id in stale_keys:
            self.state.pending_buy_tokens.pop(condition_id, None)
            self.state.pending_buy_timestamps.pop(condition_id, None)

    async def scan_markets(self):
        window_ts = self.get_current_window_ts()

        # V2.5: Rotate log file if session window changed
        rotate_log_file_if_needed()

        # V2.17: Clean up stale pending orders (timed out)
        self._cleanup_stale_pending_orders()

        # V2.8: Sync positions from API periodically (every 3s) to catch external changes
        now = time.time()
        if now - self._last_position_sync_ts >= self._position_sync_interval:
            self._sync_positions_from_api()
            self._last_position_sync_ts = now

        # Reset window state if window changed (for position cycling)
        self._reset_window_state_if_needed(window_ts)

        # OKX WS health check — restart if task died
        if self._okx_ws_task and self._okx_ws_task.done():
            logger.warning("[OKX-WS] Task died, restarting...")
            self._okx_ws_task = asyncio.create_task(self._okx_feed.run())

        # PM WS health check — restart if task died
        if self._pm_ws_task and self._pm_ws_task.done():
            logger.warning("[PM-WS] Task died, restarting...")
            self._pm_ws_task = asyncio.create_task(self._pm_feed.run())

        # Pre-discover next window's market ~N seconds before window ends
        if V3_PRE_DISCOVER_SEC > 0:
            now_utc = int(datetime.now(timezone.utc).timestamp())
            window_end_ts = window_ts + V3_WINDOW_SECONDS
            secs_remaining = window_end_ts - now_utc
            if 0 < secs_remaining <= V3_PRE_DISCOVER_SEC:
                await self._pre_discover_next_window()

        # Clear priceToBeat cache on window change so new window gets fresh strike
        if V3_VOLATILITY_GATE_ENABLED and hasattr(self, '_last_ptb_window'):
            if window_ts != self._last_ptb_window:
                self._known_price_to_beat.clear()
                self._last_ptb_retry.clear()
        self._last_ptb_window = window_ts

        # Capital protection check
        self._check_capital()

        # V3: Capture begin balance AFTER sync so positions are included in portfolio
        if self._need_begin_balance_capture:
            tracked_value = self._get_all_positions_value()
            self._begin_unredeemed_value = self._unredeemed_positions_value  # Freeze snapshot
            self._begin_session_balance = self.current_balance + tracked_value + self._begin_unredeemed_value
            self._log(f"BEGIN_SESSION_BALANCE: ${self._begin_session_balance:.2f} (balance=${self.current_balance:.2f} + positions=${tracked_value:.2f} + unredeemed=${self._begin_unredeemed_value:.2f})", "INFO")
            self._need_begin_balance_capture = False
            self._save_persistent_balance()  # Persist across restarts

        # V3.27: Save session state to disk every sync cycle for crash recovery
        self._save_session_state()

        # V3: Session-level TP — if all positions closed, skip scanning
        # If some positions were kept (prediction-based skip), continue managing them
        if self._session_tp_done and not self.state.positions:
            return

        # Fetch latest CEX prices and calculate momentum signals
        self._fetch_cex_prices()

        if PARALLEL_MARKETS:
            # Parallel: scan all markets concurrently
            tasks = [self._scan_single_market(mc, window_ts) for mc in self.markets]
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            # Sequential: scan one by one
            for market_config in self.markets:
                await self._scan_single_market(market_config, window_ts)

    async def _scan_single_market(self, market_config: MarketConfig, window_ts: int):
        """Scan a single market - extracted for parallel execution"""
        try:
            # Construct slug based on timeframe
            if market_config.timeframe == "1h":
                # 1h markets use date-based slugs: bitcoin-up-or-down-february-1-10am-et
                slug = construct_1h_market_slug(market_config.slug_pattern)
            else:
                # 15m markets use timestamp-based slugs: btc-updown-15m-{timestamp}
                slug = f"{market_config.slug_pattern}-{window_ts}"

            # Try pre-discovered cache first (only after window actually transitions)
            market = None
            if window_ts == self._next_window_ts and market_config.slug_pattern in self._next_window_market_cache:
                market = self._next_window_market_cache.pop(market_config.slug_pattern, None)
            if not market:
                market = await self.fetch_market_by_slug(slug)
            if not market:
                # Log once per slug to avoid spam (every 2s scan × 15m window = 450 lines)
                notfound_key = f"_notfound_{slug}"
                if not getattr(self, notfound_key, False):
                    setattr(self, notfound_key, True)
                    self._log(f"[WAIT] {market_config.name}: market not found yet ({slug})", "INFO")
                return

            condition_id = market.get("conditionId", "")

            # Volatility gate: refresh priceToBeat if needed
            if V3_VOLATILITY_GATE_ENABLED:
                await self._refresh_price_to_beat_for_market(market_config, market)

            # V2.10: Cache market end time and condition→slug mapping
            if condition_id:
                self._condition_to_slug[condition_id] = market_config.slug_pattern
            end_date_str = market.get("endDate") or market.get("end_date")
            if end_date_str and condition_id:
                try:
                    # Parse ISO format: "2026-01-28T05:30:00Z"
                    from datetime import datetime, timezone
                    end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                    self._market_end_times[condition_id] = end_dt.timestamp()
                except Exception:
                    pass

            clob_tokens = market.get("clobTokenIds", [])
            if isinstance(clob_tokens, str):
                clob_tokens = json.loads(clob_tokens)
            if len(clob_tokens) < 2:
                return

            # V3: Map token IDs to YES/NO based on outcomes field
            # outcomes array corresponds to clobTokenIds array
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    outcomes = []

            # Default: assume [0]=YES, [1]=NO
            yes_token, no_token = clob_tokens[0], clob_tokens[1]

            # If outcomes available, map correctly
            if len(outcomes) >= 2:
                for i, outcome in enumerate(outcomes):
                    outcome_lower = outcome.lower() if outcome else ""
                    if outcome_lower in ("yes", "up"):
                        yes_token = clob_tokens[i]
                    elif outcome_lower in ("no", "down"):
                        no_token = clob_tokens[i]

            # Subscribe PM WS feed to these tokens (idempotent, no-op if already subscribed)
            if self._pm_feed and V3_PM_WS_ENABLED:
                await self._pm_feed.subscribe([yes_token, no_token])

            pos = self.state.positions.get(condition_id)
            has_position = pos is not None

            # --- Price source priority: WS cache → midpoint REST → full orderbook ---
            yes_bid, yes_ask, yes_mid = 0.0, 0.0, 0.0
            no_bid, no_ask, no_mid = 0.0, 0.0, 0.0
            yes_book, no_book = {"bids": [], "asks": []}, {"bids": [], "asks": []}
            _price_source = "REST"
            _need_full_book = has_position  # always need full book for position management

            # Try 1: PM WS cache (0ms latency)
            if self._pm_feed and V3_PM_WS_ENABLED:
                _ws_yes = self._pm_feed.get_prices(yes_token)
                _ws_no = self._pm_feed.get_prices(no_token)
                # Check staleness of BOTH tokens (use min timestamp)
                _ws_yes_ts = self._pm_feed.get_last_update_ts(yes_token)
                _ws_no_ts = self._pm_feed.get_last_update_ts(no_token)
                _ws_min_ts = min(_ws_yes_ts, _ws_no_ts) if (_ws_yes_ts > 0 and _ws_no_ts > 0) else 0
                _ws_stale = (time.monotonic() - _ws_min_ts > V3_PM_WS_STALE_SEC) if _ws_min_ts > 0 else True
                if _ws_yes[2] > 0 and _ws_no[2] > 0 and not _ws_stale:
                    yes_bid, yes_ask, yes_mid = _ws_yes
                    no_bid, no_ask, no_mid = _ws_no
                    _price_source = "WS"
                    # WS has prices — but entry needs full orderbook for accurate ask/bid
                    if not has_position and BUY_BAND_LOW <= yes_mid <= BUY_BAND_HIGH and BUY_BAND_LOW <= no_mid <= BUY_BAND_HIGH:
                        _need_full_book = True  # upgrade to full orderbook for entry

            # Try 2: Midpoint REST (~100ms) — only when no position and WS missed
            if _price_source != "WS" and not has_position and V3_MIDPOINT_SCAN_ENABLED:
                yes_mid, no_mid = await asyncio.gather(
                    self.fetch_midpoint(yes_token), self.fetch_midpoint(no_token))
                if yes_mid > 0 and no_mid > 0:
                    # Midpoint can be stale during resolution transitions — distrust extreme prices
                    _mid_stale = yes_mid < 0.02 or yes_mid > 0.98 or no_mid < 0.02 or no_mid > 0.98
                    if _mid_stale:
                        # Extreme midpoint likely stale from resolved market — fall through to full orderbook
                        _need_full_book = True
                    else:
                        yes_bid, yes_ask = yes_mid, yes_mid  # approximate
                        no_bid, no_ask = no_mid, no_mid
                        _price_source = "MID"
                        # Check if midpoint is in entry zone — if so, upgrade to full orderbook
                        if BUY_BAND_LOW <= yes_mid <= BUY_BAND_HIGH and BUY_BAND_LOW <= no_mid <= BUY_BAND_HIGH:
                            _need_full_book = True

            # Try 3: Full orderbook (always for positions, or when entry zone confirmed)
            if _price_source == "REST" or _need_full_book:
                yes_book, no_book = await asyncio.gather(
                    self.fetch_orderbook(yes_token), self.fetch_orderbook(no_token))
                yes_bid, yes_ask = self.get_best_prices(yes_book)
                no_bid, no_ask = self.get_best_prices(no_book)
                yes_mid = (yes_bid + yes_ask) / 2 if yes_bid and yes_ask < 1 else (yes_bid or yes_ask)
                no_mid = (no_bid + no_ask) / 2 if no_bid and no_ask < 1 else (no_bid or no_ask)
                _price_source = "REST"

            self.current_yes_price = yes_mid
            self.current_no_price = no_mid

            # --- Regime detection: record price/spread tick + classify ---
            if V3_REGIME_CHOP_TP > 0 and yes_mid > 0:
                _market_sym = market_config.slug_pattern.split("-")[0].upper()
                _spread_pct = (yes_ask - yes_bid) / yes_mid if yes_mid > 0 and yes_ask > yes_bid else 0.0
                self._record_price_tick(_market_sym, yes_mid, _spread_pct)
                # Read prediction for regime signals (need confidence, momentum, noise)
                _, _reg_conf, _, _, _, _reg_mom, _reg_noise = self._read_prediction(_market_sym)
                _regime, _chop_cnt, _ = self._detect_regime_4sig(_market_sym, _reg_conf, _reg_mom, _reg_noise)
                self._update_regime_debounced(_market_sym, _regime)

            if has_position:
                # V3.80: Set YES/NO prices directly from orderbook
                pos.yes_price = yes_mid
                pos.no_price = no_mid
                await self.manage_position(pos, market_config, yes_token, no_token, yes_book, no_book, market)
            else:
                await self.check_entry(market_config, condition_id, yes_token, no_token, yes_book, no_book, market)
        except Exception as e:
            logger.error(f"Error scanning {market_config.slug_pattern}: {e}")

    # ============ Regime Detection (TREND/CHOP — ported from v7) ============

    def _record_price_tick(self, market_symbol: str, yes_mid: float, spread_pct: float):
        """Record price + spread for local regime detection (ring buffer, ~5min at 2s ticks)."""
        now = time.time()
        buf = self._price_ticks.setdefault(market_symbol, deque(maxlen=300))
        buf.append((now, yes_mid))
        sbuf = self._spread_ticks.setdefault(market_symbol, deque(maxlen=300))
        sbuf.append((now, spread_pct))

    def _compute_flip_rate(self, market_symbol: str, lookback_sec: float = 180.0,
                           interval_sec: float = 10.0) -> float:
        """Compute direction flips per minute from price history at 10s intervals."""
        buf = self._price_ticks.get(market_symbol)
        if not buf or len(buf) < 10:
            return 0.0
        now = time.time()
        cutoff = now - lookback_sec
        # Sample at interval_sec resolution
        samples = []
        last_sample_ts = 0.0
        for ts, price in buf:
            if ts < cutoff:
                continue
            if ts - last_sample_ts >= interval_sec:
                samples.append(price)
                last_sample_ts = ts
        if len(samples) < 3:
            return 0.0
        flips = 0
        prev_dir = 0
        for i in range(1, len(samples)):
            d = 1 if samples[i] > samples[i - 1] else (-1 if samples[i] < samples[i - 1] else 0)
            if d != 0 and prev_dir != 0 and d != prev_dir:
                flips += 1
            if d != 0:
                prev_dir = d
        duration_min = (samples[-1] if len(samples) > 0 else lookback_sec) and lookback_sec / 60.0
        return flips / duration_min if duration_min > 0 else 0.0

    def _detect_regime_4sig(self, market_symbol: str, pred_confidence: float,
                           pred_momentum: float, pred_noise: float) -> Tuple[str, int, dict]:
        """4-signal regime detection (from v7). Returns (regime, chop_signal_count, details)."""
        # Signal 1: Trend confidence + momentum (from prediction API)
        sig1_trend = (pred_confidence >= V3_REGIME_TREND_CONFIDENCE_GATE and
                      abs(pred_momentum) >= V3_REGIME_TREND_MOMENTUM_GATE)
        # Signal 2: Flip rate (direction changes per minute from local price history)
        flip_rate = self._compute_flip_rate(market_symbol)
        sig2_choppy = flip_rate > V3_REGIME_FLIP_RATE_THRESHOLD
        # Signal 3: Noise (small candle ratio from prediction API)
        sig3_noisy = pred_noise > V3_REGIME_NOISE_THRESHOLD
        # Signal 4: Spread vs average (wide spread = low liquidity = choppy)
        sbuf = self._spread_ticks.get(market_symbol)
        avg_spread = 0.0
        cur_spread = 0.0
        if sbuf and len(sbuf) >= 5:
            spreads = [s for _, s in sbuf]
            avg_spread = sum(spreads) / len(spreads) if spreads else 0.0
            cur_spread = spreads[-1] if spreads else 0.0
        spread_ratio = cur_spread / avg_spread if avg_spread > 0 else 0.0
        sig4_wide_spread = spread_ratio > V3_REGIME_SPREAD_MULTIPLIER

        chop_cnt = sum([not sig1_trend, sig2_choppy, sig3_noisy, sig4_wide_spread])
        regime = "CHOP" if chop_cnt >= 3 else "TREND"

        details = {
            "sig1_trend": sig1_trend, "sig2_flip_rate": flip_rate,
            "sig3_noise": pred_noise, "sig4_spread_ratio": spread_ratio,
        }
        return regime, chop_cnt, details

    def _update_regime_debounced(self, market_symbol: str, regime: str):
        """Debounce regime switch: require N consecutive same readings before changing."""
        prev = self._last_regime.get(market_symbol)
        if regime != prev and prev:
            pend = self._regime_pending.get(market_symbol)
            if regime == pend:
                self._regime_pending_count[market_symbol] = self._regime_pending_count.get(market_symbol, 0) + 1
            else:
                self._regime_pending[market_symbol] = regime
                self._regime_pending_count[market_symbol] = 1
            if self._regime_pending_count.get(market_symbol, 0) >= V3_REGIME_DEBOUNCE_COUNT:
                self._last_regime[market_symbol] = regime
                self._regime_pending.pop(market_symbol, None)
                self._regime_pending_count.pop(market_symbol, None)
        else:
            if not prev:
                self._last_regime[market_symbol] = regime
            self._regime_pending.pop(market_symbol, None)
            self._regime_pending_count.pop(market_symbol, None)

    # ============ Pre-discover Next Window ============

    async def _pre_discover_next_window(self):
        """Pre-discover next window's market ~10s before current window ends. Cache for instant use."""
        window_ts = self.get_current_window_ts()
        next_ts = window_ts + V3_WINDOW_SECONDS
        if self._next_window_ts == next_ts:
            return  # already pre-discovered
        self._next_window_market_cache.clear()
        for mc in self.markets:
            try:
                slug = f"{mc.slug_pattern}-{next_ts}"
                market = await self.fetch_market_by_slug(slug)
                if market:
                    self._next_window_market_cache[mc.slug_pattern] = market
                    self._log(f"[PRE-DISC] Found next market: {slug}", "INFO")
            except Exception as e:
                logger.warning(f"[PRE-DISC] {mc.name}: {e}")
        # Always mark as attempted to prevent retries every 2s
        self._next_window_ts = next_ts
        if self._next_window_market_cache:
            # Pre-subscribe PM WS tokens for next window so prices flow immediately at :00
            if self._pm_feed and V3_PM_WS_ENABLED:
                token_ids = []
                for mkt in self._next_window_market_cache.values():
                    clob_tokens = mkt.get("clobTokenIds", [])
                    if isinstance(clob_tokens, str):
                        clob_tokens = json.loads(clob_tokens)
                    token_ids.extend(clob_tokens)
                if token_ids:
                    await self._pm_feed.subscribe(token_ids)
                    self._log(f"[PRE-DISC] Pre-subscribed {len(token_ids)} token(s) for next window", "INFO")

    # ============ Capital Protection ============

    def _check_capital(self) -> bool:
        """Check if total account value is above stop threshold. Skip if not configured."""
        if BOT_STOP_THRESHOLD <= 0:
            return True
        total = self.current_balance + self._get_all_positions_value() + self._unredeemed_positions_value
        was_ok = self._capital_ok
        self._capital_ok = total >= BOT_STOP_THRESHOLD
        if not self._capital_ok and was_ok:
            self._log(
                f"[CAPITAL] BELOW THRESHOLD: ${total:.2f} < ${BOT_STOP_THRESHOLD:.2f} "
                f"— pausing new entries", "WARNING")
        elif self._capital_ok and not was_ok:
            self._log(
                f"[CAPITAL] Recovered: ${total:.2f} >= ${BOT_STOP_THRESHOLD:.2f} "
                f"— resuming new entries", "INFO")
        return self._capital_ok

    # ============ Volatility Gate (OKX strike distance) ============

    def _get_symbol_from_slug(self, slug_pattern: str) -> str:
        """Extract symbol from slug pattern: 'btc-updown-15m' → 'BTC'."""
        parts = slug_pattern.split("-")
        return parts[0].upper() if parts and parts[0] else ""

    async def _fetch_price_to_beat(self, slug: str, symbol: str = "BTC") -> float:
        """Fetch priceToBeat from Polymarket via headless browser (Playwright).
        The value is rendered client-side from Chainlink data, not in raw HTML."""
        if not slug:
            return 0.0
        try:
            from playwright.async_api import async_playwright
            url = f"https://polymarket.com/event/{slug}"
            # Playwright tmp dir — same path as V7 (ReadWritePaths in systemd service)
            pw_tmp = str(Path(__file__).resolve().parent / ".playwright-tmp")
            os.makedirs(pw_tmp, exist_ok=True)
            os.environ["TMPDIR"] = pw_tmp
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                loc = page.locator(
                    'xpath=//*[@id="price-chart-container"]'
                    '/div/div/div[1]/div/div[1]/div[1]/span')
                text = await loc.text_content(timeout=30000)
                await browser.close()
                if text:
                    ptb = float(text.replace("$", "").replace(",", ""))
                    self._log(f"[VOLATILITY] {symbol} priceToBeat=${ptb:,.2f} (browser)", "INFO")
                    return ptb
        except Exception as e:
            logger.warning(f"[VOLATILITY] Failed to fetch priceToBeat for {slug}: {e}")
        return 0.0

    def _check_volatility_gate(self, market_config: MarketConfig) -> bool:
        """Block entry when market is too flat (OKX price near priceToBeat).
        Returns True if entry allowed, False if blocked."""
        if not self._okx_feed:
            return True  # no feed — allow entry
        symbol = self._get_symbol_from_slug(market_config.slug_pattern)
        if not symbol:
            return True
        price = self._okx_feed.get_price(symbol)
        if price <= 0:
            return False  # no OKX data — block entry
        threshold = V3_VOLATILITY_THRESHOLD.get(symbol, V3_VOLATILITY_THRESHOLD_DEFAULT)
        ptb = self._known_price_to_beat.get(symbol, 0.0)
        if ptb > 0:
            dist_pct = abs(price - ptb) / ptb * 100
            # Block when too flat (distance < threshold), allow when volatile enough
            allowed = dist_pct > threshold
            tag = "ALLOW" if allowed else "BLOCK (flat)"
            self._log(
                f"[VOLATILITY] {symbol} move={dist_pct:.3f}% "
                f"{'>' if allowed else '<='} {threshold}% → {tag} "
                f"(okx=${price:,.2f} ptb=${ptb:,.2f})", "INFO")
            return allowed
        # priceToBeat unavailable — block entry (can't assess volatility)
        cache_key = f"_vol_noptb_{symbol}"
        if not getattr(self, cache_key, False):
            setattr(self, cache_key, True)
            self._log(f"[VOLATILITY] {symbol} no priceToBeat — BLOCK entry (okx=${price:,.1f})", "WARN")
        return False

    async def _refresh_price_to_beat_for_market(self, market_config: MarketConfig, market_obj: dict):
        """Fetch priceToBeat — Gamma API eventMetadata first, then Playwright browser.
        Ported from V7's _discover_markets + _scan_market retry pattern."""
        symbol = self._get_symbol_from_slug(market_config.slug_pattern)
        if not symbol:
            return
        # Skip if already cached for this window
        if self._known_price_to_beat.get(symbol, 0) > 0:
            return
        # Rate-limit retries (every 10s per symbol, like V7)
        now = time.time()
        if now - self._last_ptb_retry.get(symbol, 0) < 10:
            return
        self._last_ptb_retry[symbol] = now
        slug = market_obj.get("slug", "")
        # Try Gamma API eventMetadata first (re-fetch market with events)
        ptb = 0.0
        try:
            resp = await self._http_get_with_retry(f"{GAMMA_HOST}/markets", params={"slug": slug})
            data = resp.json()
            if isinstance(data, list) and data:
                mkt = data[0]
                for evt in (mkt.get("events") or []):
                    em = evt.get("eventMetadata") or {}
                    if "priceToBeat" in em:
                        ptb = float(em["priceToBeat"])
                        break
        except Exception:
            pass
        # Fallback: browser fetch (Playwright) — same as V7
        if ptb <= 0:
            ptb = await self._fetch_price_to_beat(slug, symbol)
        if ptb > 0:
            self._known_price_to_beat[symbol] = ptb
            # Clear "no ptb" log flag so state-change logging works
            cache_key = f"_vol_noptb_{symbol}"
            if getattr(self, cache_key, False):
                setattr(self, cache_key, False)
            self._log(f"[VOLATILITY] {symbol} priceToBeat=${ptb:,.2f}", "INFO")

    async def check_entry(self, market_config: MarketConfig, condition_id: str, yes_token: str, no_token: str, yes_book: dict, no_book: dict, market_obj: dict):
        """DCA-to-winner: Buy $X YES + $X NO simultaneously at entry."""
        # Guards
        if self._session_tp_done:
            return
        if not self._capital_ok:
            return

        # V3.99: Entry cutoff — don't enter near window end (position gets wiped on clear)
        if V3_ENTRY_CUTOFF_MINUTES > 0:
            elapsed_min = (time.time() - self.get_current_window_ts()) / 60
            remaining_min = V3_WINDOW_MINUTES - elapsed_min
            if remaining_min <= V3_ENTRY_CUTOFF_MINUTES:
                self._log(f"ENTRY CUTOFF: {remaining_min:.1f}m left < {V3_ENTRY_CUTOFF_MINUTES:.0f}m cutoff", "WARNING")
                return

        # Volatility gate: block flat entries (OKX price vs priceToBeat)
        if V3_VOLATILITY_GATE_ENABLED and not self._check_volatility_gate(market_config):
            return

        # V3.99: Prediction quality gate + direction for asymmetric entry
        _entry_sym = self._get_symbol_from_slug(market_config.slug_pattern) or "BTC"
        _pred, _pred_conf, _, _pred_qs, _, _pred_mom, _ = self._read_prediction(_entry_sym)
        if V3_ENTRY_MIN_CONFIDENCE > 0 and _pred_conf < V3_ENTRY_MIN_CONFIDENCE:
            self._log(f"ENTRY QUALITY GATE: {market_config.name} conf={_pred_conf:.1%} < {V3_ENTRY_MIN_CONFIDENCE:.0%}", "DEBUG")
            return
        if V3_ENTRY_MIN_QUALITY > 0 and _pred_qs < V3_ENTRY_MIN_QUALITY:
            self._log(f"ENTRY QUALITY GATE: {market_config.name} qs={_pred_qs:.2f} < {V3_ENTRY_MIN_QUALITY:.2f}", "DEBUG")
            return
        # V3.100: Momentum strength gate — only enter when momentum is strong enough
        if V3_ENTRY_MIN_MOMENTUM > 0 and abs(_pred_mom) < V3_ENTRY_MIN_MOMENTUM:
            self._log(f"ENTRY MOMENTUM GATE: {market_config.name} mom={abs(_pred_mom):.3f} < {V3_ENTRY_MIN_MOMENTUM}", "DEBUG")
            return

        # Estimate cost for capital check: both sides (asymmetric entry)
        entry_cost_estimate = V3_ENTRY_AMOUNT_USD * (V3_ENTRY_LEADER_MULT + V3_ENTRY_TRAILER_MULT)
        if not self.can_enter_market(market_config.slug_pattern, entry_cost_estimate):
            self._log(f"[SKIP] {market_config.name}: capital limit reached", "DEBUG")
            return

        yes_bid, yes_ask = self.get_best_prices(yes_book)
        no_bid, no_ask = self.get_best_prices(no_book)

        if not self.can_reenter(condition_id):
            self._log(f"[SKIP] {market_config.name}: cycling cooldown active", "DEBUG")
            return

        # Buy band: leader must be within BUY_BAND, trailer gets wider ceiling (TRAILER_BAND_HIGH)
        if not yes_ask or not no_ask:
            return
        cheap_ask, expensive_ask = min(yes_ask, no_ask), max(yes_ask, no_ask)
        if cheap_ask < BUY_BAND_LOW or cheap_ask > BUY_BAND_HIGH:
            self._log(f"[SKIP] {market_config.name}: YES=${yes_ask:.2f} NO=${no_ask:.2f} — leader outside band ${BUY_BAND_LOW:.2f}-${BUY_BAND_HIGH:.2f}", "DEBUG")
            return
        if expensive_ask > TRAILER_BAND_HIGH:
            self._log(f"[SKIP] {market_config.name}: YES=${yes_ask:.2f} NO=${no_ask:.2f} — trailer above ceiling ${TRAILER_BAND_HIGH:.2f}", "DEBUG")
            return

        # V3.99: Prediction drives leader side — source configurable via V3_ENTRY_PREDICTION_SOURCE
        _pred_side = None
        if V3_ENTRY_PREDICTION_SOURCE == "ml" and V3_ML_ENTRY_ENABLED:
            # ML XGBoost model — uses tick-level features
            _ml_allowed, _pred_side = self._ml_check_entry(_entry_sym, yes_ask, no_ask)
            if not _ml_allowed:
                return
        elif _pred in ("UP", "DOWN"):
            # Prediction API (godeye) — UP→YES leader, DOWN→NO leader
            _pred_side = "YES" if _pred == "UP" else "NO"

        # Determine leader/trailer sides — prediction overrides price when available
        if _pred_side == "YES":
            winner_side, winner_token, winner_book = "YES", yes_token, yes_book
            loser_side, loser_token, loser_book = "NO", no_token, no_book
            winner_price, loser_price = yes_ask, no_ask
        elif _pred_side == "NO":
            winner_side, winner_token, winner_book = "NO", no_token, no_book
            loser_side, loser_token, loser_book = "YES", yes_token, yes_book
            winner_price, loser_price = no_ask, yes_ask
        elif yes_ask >= no_ask:
            winner_side, winner_token, winner_book = "YES", yes_token, yes_book
            loser_side, loser_token, loser_book = "NO", no_token, no_book
            winner_price, loser_price = yes_ask, no_ask
        else:
            winner_side, winner_token, winner_book = "NO", no_token, no_book
            loser_side, loser_token, loser_book = "YES", yes_token, yes_book
            winner_price, loser_price = no_ask, yes_ask

        # Asymmetric entry — leader gets more $, trailer gets less
        winner_usd = V3_ENTRY_AMOUNT_USD * V3_ENTRY_LEADER_MULT
        loser_usd = V3_ENTRY_AMOUNT_USD * V3_ENTRY_TRAILER_MULT

        # V3.99: FLIP_FOLLOW — when loser price is near 0.50 (market undecided),
        # reverse sizing: bet more on loser side (likely to flip), use 2x TP
        _is_flip = False
        if V3_FLIP_FOLLOW_ENABLED and V3_FLIP_FOLLOW_MIN_PRICE <= loser_price <= V3_FLIP_FOLLOW_MAX_PRICE:
            winner_usd = V3_ENTRY_AMOUNT_USD * V3_ENTRY_TRAILER_MULT  # leader gets less
            loser_usd = V3_ENTRY_AMOUNT_USD * V3_ENTRY_LEADER_MULT    # trailer gets more
            _is_flip = True

        self._log(
            f"ENTRY: {market_config.name} — YES=${yes_ask:.4f} NO=${no_ask:.4f} "
            f"— leader=${winner_usd:.1f} trailer=${loser_usd:.1f} "
            f"(winner {winner_usd/winner_price:.0f} + loser {loser_usd/loser_price:.0f} tokens = ${winner_usd + loser_usd:.1f})"
            f"{f' [PRED:{_pred}({_pred_conf:.0%})]' if _pred else ''}"
            f"{' [FLIP_FOLLOW]' if _is_flip else ''}",
            "SIGNAL")

        # Buy winner side first (higher price, lock it before it moves)
        await self.dca_buy(market_config, condition_id, winner_token, winner_side, winner_book, market_obj, usd_amount=winner_usd)

        # Buy loser side — retry with fresh book until filled
        # Skip trailer entirely when TRAILER_MULT=0 (single-side mode)
        pos = self.state.positions.get(condition_id)
        if loser_usd <= 0:
            self._log(f"SINGLE-SIDE ENTRY: trailer disabled (TRAILER_MULT=0)", "INFO")
        elif pos and pos.get_tokens(loser_side) > 0:
            self._log(
                f"DUAL ENTRY SKIP: {loser_side} already has {pos.get_tokens(loser_side):.0f} tokens "
                f"(likely ghost fill) — skipping loser buy", "WARNING")
        elif pos:
            for opp_attempt in range(1, MAX_RETRIES + 1):
                await self.dca_buy(market_config, condition_id, loser_token, loser_side, loser_book, market_obj, is_opposite=True, usd_amount=loser_usd)
                # Check if hedge side was filled
                pos = self.state.positions.get(condition_id)
                if pos and pos.get_tokens(loser_side) > 0:
                    break  # opposite side filled successfully
                # Refresh orderbook and retry
                self._log(f"DUAL ENTRY RETRY {opp_attempt}/{MAX_RETRIES}: {loser_side} not filled, refreshing book", "WARN")
                await asyncio.sleep(RETRY_DELAY)
                try:
                    loser_book = await self.fetch_orderbook(loser_token)
                except Exception as e:
                    self._log(f"DUAL ENTRY RETRY: Failed to refresh book: {e}", "ERROR")
            else:
                # All retries exhausted — log but continue (position will be one-sided)
                self._log(f"DUAL ENTRY FAILED: {loser_side} not filled after {MAX_RETRIES} attempts", "ERROR")

        # V3.99: Set flip_follow flag on position for 2x TP
        if _is_flip:
            pos = self.state.positions.get(condition_id)
            if pos:
                pos.flip_follow = True
                self._log(f"FLIP_FOLLOW: loser=${loser_price:.2f} in [{V3_FLIP_FOLLOW_MIN_PRICE}-{V3_FLIP_FOLLOW_MAX_PRICE}] — 2x TP active", "INFO")

    def _detect_regime(self, confidence: float, momentum: float) -> str:
        """V3.8: Classify market regime from prediction signals.

        TREND: Strong directional move — DCA to winner (pyramid).
        CHOP: Oscillating/ranging — rebalance sell+rebuy to lock profit.
        """
        abs_mom = abs(momentum)
        if confidence >= V3_REGIME_TREND_CONFIDENCE and abs_mom >= V3_REGIME_TREND_MOMENTUM:
            return "TREND"
        return "CHOP"

    def _get_ml_model(self, symbol: str, timing_mode: str = "late"):
        """Lazy-load ML model for symbol. Returns model or None.
        Cache key includes timing_mode so entry (early) and DCA (late) models coexist."""
        cache_key = f"{symbol}_{timing_mode}"
        if cache_key not in self._ml_models:
            try:
                import sys as _sys
                _proj = Path(__file__).resolve().parent.parent
                if str(_proj) not in _sys.path:
                    _sys.path.insert(0, str(_proj))
                from ai_prediction.inference import PredictionModel
                model = PredictionModel(symbol, timing_mode=timing_mode)
                self._ml_models[cache_key] = model if model.is_loaded else None
                if model.is_loaded:
                    self._log(f"[ML] Loaded {timing_mode} model for {symbol}", "INFO")
                else:
                    self._log(f"[ML] No model available for {symbol} ({timing_mode})", "WARNING")
            except Exception as e:
                self._log(f"[ML] Failed to load model for {symbol}: {e}", "ERROR")
                self._ml_models[cache_key] = None
        return self._ml_models.get(cache_key)

    def _build_ml_tick_dict(self, coin: str, yes_price: float, no_price: float) -> dict:
        """Build tick dict for ML model from current prediction API data."""
        import time as _time
        _wts = int(_time.time()) // V3_WINDOW_SECONDS * V3_WINDOW_SECONDS
        elapsed = int(_time.time()) - _wts
        prediction, confidence, _, quality, cross_pairs, momentum, _ = self._read_prediction(coin)
        return {
            "confidence": confidence,
            "quality_score": quality,
            "cross_pair_agreement": cross_pairs,
            "weighted_signal": 0.0,
            "price_change_pct": 0.0,
            "momentum_accumulated": momentum,
            "momentum_signal": 0.0,
            "orderbook_imbalance_score": 0.0,
            "orderbook_sweep_score": 0.0,
            "candle_size_factor": 0.0,
            "cex_leader_score": 0.0,
            "exchange_consensus_score": 0.0,
            "persistence_score": 0.0,
            "whale_flow_accumulated": 0.0,
            "polymarket_yes_price": yes_price,
            "polymarket_no_price": no_price,
            "window_start_ts": _wts,
            "elapsed_sec": elapsed,
            "prediction": prediction,
        }

    def _append_ml_tick(self, coin: str, tick_dict: dict):
        """Append tick to ML buffer for tick-level models."""
        if coin not in self._ml_tick_buffers:
            self._ml_tick_buffers[coin] = deque(maxlen=60)
        self._ml_tick_buffers[coin].append(tick_dict)

    def _get_recent_ticks(self, coin: str) -> list:
        """Get recent tick buffer for tick-level models."""
        return list(self._ml_tick_buffers.get(coin, []))

    def _ml_check_entry(self, coin: str, yes_price: float, no_price: float):
        """ML entry gate: returns (allowed, predicted_side) where predicted_side is 'YES'|'NO'|None.
        When allowed and side known, caller applies asymmetric multipliers."""
        if not V3_ML_ENTRY_ENABLED:
            return True, None
        ml_model = self._get_ml_model(coin, timing_mode=V3_ML_ENTRY_MODE)
        if not ml_model:
            return True, None  # no model → allow (graceful fallback)
        tick_dict = self._build_ml_tick_dict(coin, yes_price, no_price)
        self._append_ml_tick(coin, tick_dict)
        recent_ticks = self._get_recent_ticks(coin)
        proba = ml_model.predict_proba(tick_dict, recent_ticks=recent_ticks)
        prediction = tick_dict["prediction"]
        confidence = tick_dict["confidence"]
        momentum = tick_dict["momentum_accumulated"]
        if proba < V3_ML_ENTRY_MIN_PROBA:
            self._log(
                f"[ML] ENTRY BLOCKED: {coin} proba={proba:.2f}<{V3_ML_ENTRY_MIN_PROBA:.2f} "
                f"(conf={confidence:.0%} mom={momentum:+.3f})", "WARNING")
            return False, None
        # ML says enter — determine predicted winner side from prediction direction
        pred_side = "YES" if prediction == "UP" else ("NO" if prediction == "DOWN" else None)
        self._log(f"[ML] ENTRY OK: {coin} proba={proba:.2f} side={pred_side}", "INFO")
        return True, pred_side

    def _ml_should_allow_dca(self, coin: str, prediction: str, confidence: float,
                              quality: float, cross_pairs: int, momentum: float,
                              yes_price: float, no_price: float) -> bool:
        """Check ML model to see if DCA should proceed. Returns True if allowed.
        Uses tick_cross model by default (same as entry), falls back to early/late."""
        if not V3_ML_DCA_ENABLED:
            return True
        # Use same mode as entry for tick-level, otherwise auto-select early/late
        if V3_ML_ENTRY_MODE.startswith("tick"):
            _dca_mode = V3_ML_ENTRY_MODE
        else:
            import time as _time
            now = int(_time.time())
            _wts = now // V3_WINDOW_SECONDS * V3_WINDOW_SECONDS
            elapsed = now - _wts
            _dca_mode = "early" if elapsed < V3_WINDOW_SECONDS / 2 else "late"
        ml_model = self._get_ml_model(coin, timing_mode=_dca_mode)
        if not ml_model:
            return True  # no model → allow (graceful fallback)
        tick_dict = self._build_ml_tick_dict(coin, yes_price, no_price)
        self._append_ml_tick(coin, tick_dict)
        recent_ticks = self._get_recent_ticks(coin)
        proba = ml_model.predict_proba(tick_dict, recent_ticks=recent_ticks)
        if proba < V3_ML_MIN_PROBA:
            self._log(
                f"[ML] DCA BLOCKED: {coin} proba={proba:.2f}<{V3_ML_MIN_PROBA:.2f} "
                f"({_dca_mode} conf={confidence:.0%} mom={momentum:+.3f})", "WARNING")
            return False
        self._log(f"[ML] DCA OK: {coin} proba={proba:.2f} ({_dca_mode})", "DEBUG")
        return True

    def _read_prediction(self, coin: str = "BTC") -> Tuple[str, float, float, float, int, float, float]:
        """Read prediction signal. Returns (prediction, confidence, elapsed, quality, cross_pairs, momentum, noise).
        Supports WS (zero-latency cache) with HTTP fallback, or HTTP-only mode."""
        _empty = ("", 0.0, 0.0, 0.0, 0, 0.0, 0.0)

        # WS prediction source: read from cache (0ms), fallback to HTTP if miss
        if V3_PREDICTION_SOURCE == "ws" and self._ws_pred_client:
            result = self._ws_pred_client.get_prediction(coin)
            if result[0]:  # cache hit
                return result
            # WS cache miss — fall through to HTTP if URL configured
            if not PREDICTION_BOOST_API_URL:
                return _empty

        # HTTP prediction (default or WS fallback)
        if not PREDICTION_BOOST_API_URL:
            return _empty
        try:
            url = f"{PREDICTION_BOOST_API_URL}/{coin}"
            resp = httpx.get(url,
                             auth=(PREDICTION_BOOST_USERNAME, PREDICTION_BOOST_PASSWORD),
                             timeout=5.0)
            resp.raise_for_status()
            data = resp.json()
            if coin in data:
                data = data[coin]
            if data.get("status") in ("stale", "no_data"):
                return _empty
            prediction = (data.get("prediction") or "").upper()
            if prediction not in ("UP", "DOWN"):
                return _empty
            confidence = float(data.get("confidence", 0))
            elapsed = float(data.get("elapsed_minutes", 0))
            raw = data.get("raw", {})
            cw = raw.get("current_window", {})
            # Reject stale predictions from previous window (race condition at boundary)
            api_window_ts = int(cw.get("window_start_ts", 0))
            bot_window_ts = self.get_current_window_ts()
            if api_window_ts > 0 and api_window_ts != bot_window_ts:
                return _empty
            quality = float(cw.get("quality_score", 0))
            cross_pairs = int(cw.get("cross_pair_agreement", 0))
            momentum = float(cw.get("momentum_accumulated", 0))
            noise = float(cw.get("small_candle_ratio", 0))
            return prediction, confidence, elapsed, quality, cross_pairs, momentum, noise
        except Exception as e:
            logger.warning(f"[PRED] Failed to read prediction: {e}")
            return _empty

    async def _execute_prediction_boost(self, position: Position, winning_side: str,
                                         tokens_needed: float,
                                         yes_token: str, no_token: str,
                                         yes_book: dict, no_book: dict,
                                         market_config: MarketConfig) -> bool:
        """V3: Buy tokens on predicted winning side to cover losing side sell loss."""
        token_id = yes_token if winning_side == "YES" else no_token
        book = yes_book if winning_side == "YES" else no_book
        best_bid, best_ask = self.get_best_prices(book)

        if not best_ask or best_ask <= 0:
            self._log(f"PRED BOOST SKIP: Invalid ask for {winning_side}", "WARN")
            return False

        buy_tokens = tokens_needed
        buy_tokens, buy_usd = self._enforce_order_minimums(buy_tokens, best_ask)

        # V3.8: PRED_BOOST ignores V3_MAX_POSITION_COST_USD — high-conviction signal

        self._log(f"PRED BOOST BUY: {winning_side} {buy_tokens:.0f} @ ${best_ask:.4f} (${buy_tokens * best_ask:.2f})", "ENTRY")

        if self.dry_run:
            exec_price = best_ask
            self.current_balance -= buy_tokens * exec_price
            self._update_position_tokens(position, winning_side, token_id, buy_tokens, exec_price)
            self._track_buy_cost(position, winning_side, buy_tokens * exec_price)
            position.last_action_ts = time.time()
            self._log_trade_to_csv("PRED_BOOST", market_config.slug_pattern, position.condition_id,
                                   winning_side, buy_tokens, exec_price, notes="prediction_boost")
            if self.mongo:
                self.mongo.log_trade({
                    "type": "V3_PRED_BOOST", "market_slug": market_config.slug_pattern,
                    "side": winning_side, "tokens": buy_tokens, "price": exec_price,
                    "cost": buy_tokens * exec_price, "timestamp": time.time(),
                })
            return True

        # LIVE
        if not self._init_trader() or self._live_trading_blocked_reason:
            return False
        try:
            amount_usdc = max(round(buy_tokens * best_ask, 2), POLYMARKET_MIN_USD)
            try:
                from py_clob_client.clob_types import OrderType
                order_type = OrderType.GTC
            except Exception:
                order_type = None

            result = self.trader.buy_by_amount_usdc(
                token_id=token_id, amount_usdc=amount_usdc,
                order_type=order_type, neg_risk=False, tick_size=0.01)

            if result and result.get("success", False):
                exec_data = result.get("executions", [{}])[0] if result.get("executions") else {}
                actual_tokens = float(exec_data.get("matchedAmount", buy_tokens))
                actual_price = float(exec_data.get("matchPrice", best_ask))
                self._update_position_tokens(position, winning_side, token_id, actual_tokens, actual_price)
                self._track_buy_cost(position, winning_side, actual_tokens * actual_price)
                position.last_action_ts = time.time()
                self._refresh_balance_and_allowance_block_if_needed()
                self._log(f"PRED BOOST OK: {winning_side} {actual_tokens:.0f} @ ${actual_price:.4f}", "SUCCESS")
                if self.mongo:
                    self.mongo.log_trade({
                        "type": "V3_PRED_BOOST", "market_slug": market_config.slug_pattern,
                        "side": winning_side, "tokens": actual_tokens, "price": actual_price,
                        "cost": actual_tokens * actual_price, "timestamp": time.time(),
                    })
                return True
            else:
                err = result.get("errorMsg", str(result)) if result else "No result"
                self._log(f"PRED BOOST FAIL: {err}", "ERROR")
        except Exception as e:
            logger.error(f"[PRED BOOST ERROR] {e}")
            self._log(f"PRED BOOST ERROR: {e}", "ERROR")
        return False

    def _update_position_tokens(self, position: Position, side: str,
                                 token_id: str, tokens: float, price: float):
        """V3.80: Update position YES/NO tokens after a buy."""
        buy_cost = tokens * price
        old_tokens = position.get_tokens(side)
        old_price = position.get_entry_price(side)
        if old_tokens > 0 and old_price > 0:
            old_cost = old_tokens * old_price
            position.set_tokens(side, old_tokens + tokens)
            position.set_entry_price(side, (old_cost + buy_cost) / (old_tokens + tokens))
        else:
            position.set_token_id(side, token_id)
            position.set_tokens(side, tokens)
            position.set_entry_price(side, price)

    async def _execute_prediction_cut(self, position: Position, losing_side: str,
                                       yes_token: str, no_token: str,
                                       yes_book: dict, no_book: dict,
                                       market_config: MarketConfig):
        """V3: Sell ALL tokens on losing side with retry loop (like close_position)."""
        token_id = (yes_token if losing_side == "YES" else no_token)
        entry_px = position.get_entry_price(losing_side)
        remaining = position.get_avail(losing_side)

        remaining = self._round_tokens(remaining)
        if remaining < POLYMARKET_MIN_TOKENS:
            self._log(f"PRED CUT SKIP: {losing_side} only {remaining:.0f} tokens", "WARN")
            return

        total_sold = 0.0
        total_proceeds = 0.0
        self._log(f"PRED CUT START: Sell {losing_side} {remaining:.0f} tokens (entry=${entry_px:.4f})", "EXIT")

        if self.dry_run:
            book = yes_book if losing_side == "YES" else no_book
            best_bid, _ = self.get_best_prices(book)
            if best_bid and best_bid > 0:
                pnl = remaining * (best_bid - entry_px)
                await self.execute_rebalance_sell_unified(
                    position, losing_side, remaining,
                    (best_bid - entry_px) / entry_px if entry_px > 0 else 0,
                    book, market_config)
                total_sold = remaining
            position.prediction_cut_side = losing_side
            position.last_action_ts = time.time()
            return

        if not self.trader or self._live_trading_blocked_reason:
            return

        # V3.6: Sweep all bid levels per cycle, retry with delay if tokens remain
        PRED_CUT_MAX_CYCLES = 5
        PRED_CUT_RETRY_DELAY = 3  # seconds between sweep cycles

        for cycle in range(1, PRED_CUT_MAX_CYCLES + 1):
            # Check actual remaining tokens via RPC
            # V3.10: blocking I/O runs in thread pool for parallel execution
            try:
                rpc_bal = await self.sell_processor.run_io(verify_position_balance, self.trader.trading_address, token_id)
                if rpc_bal is not None and rpc_bal <= 0.5:
                    self._log(f"PRED CUT DONE: {losing_side} fully sold (RPC={rpc_bal:.1f})", "SUCCESS")
                    break
                if rpc_bal is not None:
                    remaining = float(rpc_bal)
            except Exception:
                pass

            remaining = self._round_tokens(remaining)
            if remaining < POLYMARKET_MIN_TOKENS:
                break

            # Fresh orderbook — sweep ALL bid levels
            try:
                await self.sell_processor.run_io(self.trader.refresh_api_creds)
                orderbook = await self.sell_processor.run_io(self.trader.clob.get_order_book, token_id)
                if not orderbook or not orderbook.bids:
                    self._log(f"PRED CUT: No bids for {losing_side} (cycle {cycle})", "WARN")
                    if cycle < PRED_CUT_MAX_CYCLES:
                        await asyncio.sleep(PRED_CUT_RETRY_DELAY)
                        continue
                    break
            except Exception as e:
                logger.warning(f"[PRED CUT] Orderbook error: {e}")
                if cycle < PRED_CUT_MAX_CYCLES:
                    await asyncio.sleep(PRED_CUT_RETRY_DELAY)
                    continue
                break

            # Sort bids by price descending (best first), filter valid
            valid_bids = [b for b in orderbook.bids
                          if b.price and str(b.price).strip()
                          and 0.01 < float(b.price) < 0.99]
            sorted_bids = sorted(valid_bids, key=lambda x: float(x.price), reverse=True)

            if not sorted_bids:
                self._log(f"PRED CUT: No valid bids for {losing_side} (cycle {cycle})", "WARN")
                if cycle < PRED_CUT_MAX_CYCLES:
                    await asyncio.sleep(PRED_CUT_RETRY_DELAY)
                    continue
                break

            # V3.9: Single FAK sell for ALL remaining at best bid price
            # Old per-bid sweep failed: sell_position re-reads book, caps to
            # available_size → order value < Polymarket $1 min → API rejects.
            # Single order for full remaining = higher value, FAK fills what's available.
            from py_clob_client.clob_types import OrderArgs, OrderType as _OT, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import SELL as _SELL

            best_price = float(sorted_bids[0].price)
            sell_size = float(remaining)
            raw_usdc = sell_size * best_price
            target_usdc = math.floor(raw_usdc * 100) / 100
            if target_usdc > 0 and best_price > 0:
                sell_size = math.floor(target_usdc / best_price * 10000) / 10000

            if sell_size < 5 or target_usdc < 0.50:
                self._log(f"PRED CUT: Dust ({remaining:.0f} tokens ~${target_usdc:.2f})", "WARN")
                break

            try:
                order_args = OrderArgs(
                    token_id=token_id, price=best_price,
                    size=sell_size, side=_SELL)
                options = PartialCreateOrderOptions(neg_risk=False, tick_size="0.01")
                # V3.10: blocking I/O in thread pool
                signed = await self.sell_processor.run_io(self.trader.clob.create_order, order_args, options)
                result = await self.sell_processor.run_io(self.trader.clob.post_order, signed, _OT.FAK)

                if result and result.get("success"):
                    # V3.9 FIX: For SELL orders, makingAmount = tokens sold, takingAmount = USDC received
                    making = float(result.get("makingAmount", 0) or 0)
                    taking = float(result.get("takingAmount", 0) or 0)
                    sold = making if making > 0 else (taking / best_price if taking > 0 and best_price > 0 else 0)
                    if sold > 0:
                        proceeds = taking if taking > 0 else sold * best_price
                        total_sold += sold
                        total_proceeds += proceeds
                        pnl = sold * (best_price - entry_px)
                        position.add_sold_tokens(losing_side, sold)
                        position.session_realized_profit += pnl
                        position.session_sell_proceeds += proceeds  # V3.37
                        self.state.total_pnl += pnl
                        self.current_balance += proceeds
                        remaining -= sold
                        self._log(f"PRED CUT: {losing_side} {sold:.0f}/{remaining:.0f} @ ${best_price:.4f} (${pnl:+.2f}) [c{cycle}]", "EXIT")
                    else:
                        self._log(f"PRED CUT: No fill @ ${best_price:.4f} (cycle {cycle})", "WARN")
                else:
                    self._log(f"PRED CUT: Sell failed @ ${best_price:.4f} (cycle {cycle})", "WARN")
            except Exception as e:
                logger.warning(f"[PRED CUT] Sell error @ ${best_price}: {e}")
                await asyncio.sleep(0.5)

            # Retry if tokens remain
            if remaining >= POLYMARKET_MIN_TOKENS and cycle < PRED_CUT_MAX_CYCLES:
                self._log(f"PRED CUT: {remaining:.0f} tokens remain, retry in {PRED_CUT_RETRY_DELAY}s (cycle {cycle})", "WARN")
                await asyncio.sleep(PRED_CUT_RETRY_DELAY)

        # Log summary
        total_pnl = total_proceeds - (total_sold * entry_px)
        self._log(f"[{self._market_tag(market_config.slug_pattern)}] PRED CUT DONE: {losing_side} sold {total_sold:.0f} tokens, proceeds=${total_proceeds:.2f}, PnL=${total_pnl:+.2f}", "EXIT")
        self._log_trade_to_csv("PRED_CUT", market_config.slug_pattern, position.condition_id,
                               losing_side, total_sold, total_proceeds / total_sold if total_sold > 0 else 0,
                               notes=f"pred_cut pnl=${total_pnl:.2f}")

        # FIX: Add PRED_CUT trade to dashboard for accurate Total PnL display
        # Previously missing — dashboard showed inflated PnL (all wins, no PRED_CUT losses)
        if total_sold > 0:
            avg_exit = total_proceeds / total_sold
            pnl_pct = (avg_exit - entry_px) / entry_px if entry_px > 0 else 0
            self._add_trade_to_dashboard(position, avg_exit, pnl_pct, total_pnl, "PRED_CUT",
                                          side=losing_side, entry_price=entry_px)

        position.prediction_cut_side = losing_side
        position.last_action_ts = time.time()

    async def _execute_prediction_2nd_hedge(self, position: Position, cut_side: str,
                                             token_id: str, ask_price: float,
                                             market_config: MarketConfig,
                                             buy_tokens: float = 0):
        """V3: Buy back cut side at cheap price — sized as % of winning tokens."""
        if ask_price <= 0:
            return
        if buy_tokens <= 0:
            return
        buy_tokens, buy_usd = self._enforce_order_minimums(buy_tokens, ask_price)

        # V3: Cap to position cost budget (per-side)
        buy_tokens, buy_usd = self._cap_buy_to_budget(position, buy_tokens, ask_price, target_side=cut_side)
        if buy_tokens < POLYMARKET_MIN_TOKENS:
            position.prediction_2nd_hedge_done = True
            self._log(f"[{self._market_tag(market_config.slug_pattern)}] PRED 2ND HEDGE SKIP: Budget exhausted", "WARN")
            return

        self._log(f"[{self._market_tag(market_config.slug_pattern)}] PRED 2ND HEDGE BUY: {cut_side} {buy_tokens:.0f} @ ${ask_price:.4f} (${buy_usd:.2f})", "ENTRY")

        if self.dry_run:
            self._update_position_tokens(position, cut_side, token_id, buy_tokens, ask_price)
            self._track_buy_cost(position, cut_side, buy_tokens * ask_price)
            self.current_balance -= buy_tokens * ask_price
            position.prediction_2nd_hedge_done = True
            position.last_action_ts = time.time()
            self._log_trade_to_csv("PRED_2ND_HEDGE", market_config.slug_pattern,
                                   position.condition_id, cut_side, buy_tokens, ask_price,
                                   notes="prediction_2nd_hedge")
            if self.mongo:
                self.mongo.log_trade({
                    "type": "V3_PRED_2ND_HEDGE", "market_slug": market_config.slug_pattern,
                    "side": cut_side, "tokens": buy_tokens, "price": ask_price,
                    "cost": buy_usd, "timestamp": time.time(),
                })
            return

        # LIVE
        if not self._init_trader() or self._live_trading_blocked_reason:
            return
        try:
            from py_clob_client.clob_types import OrderType
            order_type = OrderType.GTC
        except Exception:
            order_type = None
        try:
            result = self.trader.buy_by_amount_usdc(
                token_id=token_id, amount_usdc=max(round(buy_usd, 2), POLYMARKET_MIN_USD),
                order_type=order_type, neg_risk=False, tick_size=0.01)
            if result and result.get("success", False):
                exec_data = result.get("executions", [{}])[0] if result.get("executions") else {}
                actual_tokens = float(exec_data.get("matchedAmount", buy_tokens))
                actual_price = float(exec_data.get("matchPrice", ask_price))
                self._update_position_tokens(position, cut_side, token_id, actual_tokens, actual_price)
                self._track_buy_cost(position, cut_side, actual_tokens * actual_price)
                position.prediction_2nd_hedge_done = True
                position.last_action_ts = time.time()
                self._refresh_balance_and_allowance_block_if_needed()
                self._log(f"PRED 2ND HEDGE OK: {cut_side} {actual_tokens:.0f} @ ${actual_price:.4f}", "SUCCESS")
                if self.mongo:
                    self.mongo.log_trade({
                        "type": "V3_PRED_2ND_HEDGE", "market_slug": market_config.slug_pattern,
                        "side": cut_side, "tokens": actual_tokens, "price": actual_price,
                        "cost": actual_tokens * actual_price, "timestamp": time.time(),
                    })
            else:
                err = result.get("errorMsg", str(result)) if result else "No result"
                self._log(f"PRED 2ND HEDGE FAIL: {err}", "ERROR")
        except Exception as e:
            logger.error(f"[PRED 2ND HEDGE ERROR] {e}")
            self._log(f"PRED 2ND HEDGE ERROR: {e}", "ERROR")

    async def manage_position(self, position: Position, market_config: MarketConfig, yes_token: str, no_token: str, yes_book: dict, no_book: dict, market_obj: dict):
        """DCA-to-winner: identify winner/loser by VWAP, DCA winner, sell loser, insure reversals."""
        # --- 1. Calculate + log combined PnL ---
        combined_pnl_usd, combined_pnl_pct = self.calculate_combined_pnl(
            position, position.yes_price, position.no_price)
        logger.debug(f"[{position.market_slug}] Combined: {combined_pnl_pct*100:+.1f}% (${combined_pnl_usd:+.2f}) | YES: {position.yes_tokens:.0f} NO: {position.no_tokens:.0f}")

        # --- 2. Session TP — DISABLED (unreliable: uses mark price not bid VWAP, causes premature exits at loss) ---
        # Per-market COMBINED_TP below handles TP correctly using bid VWAP.
        # if COMBINED_TP_ENABLED and not self._session_tp_done:
        #     st_pnl, st_cost, st_pct, st_value, st_tokens = self._calc_session_total_pnl()
        #     if st_cost >= COMBINED_TP_MIN_COST_USD and st_pct >= COMBINED_TP_PCT:
        #         self._log(
        #             f"SESSION TP HIT: realized+unrealized=${st_pnl:+.2f} / cost=${st_cost:.2f} = {st_pct*100:+.1f}% "
        #             f"(>= +{COMBINED_TP_PCT*100:.0f}%) value=${st_value:.2f} tokens={st_tokens:.0f} — CLOSING ALL", "SUCCESS")
        #         await self._close_all_positions_session_tp()
        #         return

        # --- 3a. V3.68: Near-resolution trigger — sell at 0.99 instead of waiting for redeem (~0.97) ---
        # V3.102: Near-resolution trigger at $0.99 — sell instead of waiting for redeem (~$0.97)
        winner_price = max(position.yes_price, position.no_price)
        if winner_price >= 0.99 and position.session_total_cost >= COMBINED_TP_MIN_COST_USD:
            self._log(
                f"NEAR-RESOLUTION CLOSE: winner ${winner_price:.4f} >= $0.99 — closing position "
                f"(PnL {combined_pnl_pct*100:+.1f}%, ${combined_pnl_usd:+.2f})", "SUCCESS")
            await self.close_position(position, "COMBINED_TP")
            return

        # --- 3b. Per-position COMBINED_TP via fresh VWAP ---
        # V3.58: Use mid-market PnL as cheap pre-filter, then fetch FRESH orderbooks
        # for authoritative VWAP check. Stale cached books caused false TP triggers
        # (e.g. +46.8% mid-market vs -27.7% actual VWAP).
        # V3.100: Skip combined TP if any side has tokens but $0 cost (phantom PnL from sync)
        _zero_cost_side = next((s for s in ("YES", "NO") if position.get_avail(s) > 0 and position.get_cost(s) <= 0), None)
        if _zero_cost_side:
            self._log(f"COMBINED TP SKIP: {_zero_cost_side} has {position.get_avail(_zero_cost_side):.0f} tokens but $0 cost — waiting for cost sync", "WARNING")
        elif self.should_combined_tp(position, combined_pnl_pct, combined_pnl_usd):
            # V3.80: Verify with fresh VWAP on both sides
            tp_yes_price = position.yes_price
            tp_no_price = position.no_price
            for tp_side in ("YES", "NO"):
                tp_tokens = position.get_avail(tp_side)
                tp_tid = position.get_token_id(tp_side)
                if tp_tokens > 0 and tp_tid:
                    fresh_book = await self.fetch_orderbook(tp_tid)
                    vwap_bid, _ = self._calc_bid_vwap(fresh_book, tp_tokens)
                    if vwap_bid is not None:
                        if tp_side == "YES":
                            tp_yes_price = vwap_bid
                        else:
                            tp_no_price = vwap_bid

            tp_pnl_usd, tp_pnl_pct = self.calculate_combined_pnl(position, tp_yes_price, tp_no_price)

            # V3.99: REMOVED winner-only PnL override (was V3.81).
            # When loser is cheap, winner-only PnL ignores loser loss → triggers TP at net negative.
            # Now: always use combined VWAP PnL. If loser is near-zero, hold for resolution instead.
            # The near-resolution trigger at line ~4113 handles winner >= $0.95 exits.

            if tp_pnl_pct < COMBINED_TP_PCT:
                self._log(
                    f"COMBINED TP VWAP BLOCK: mid-market {combined_pnl_pct*100:+.1f}% but VWAP {tp_pnl_pct*100:+.1f}% "
                    f"< +{COMBINED_TP_PCT*100:.0f}% (YES VWAP=${tp_yes_price:.4f} NO=${tp_no_price:.4f})", "WARN")
            else:
                # Skip sell if winner in dead zone (0.93-0.98) — let it resolve for $0.97
                # But SELL at >= 0.99 — better than $0.97 resolve value
                winner_price_now = max(position.yes_price, position.no_price)
                if winner_price_now >= V3_RESOLVE_SKIP_SELL_PRICE and winner_price_now < 0.99:
                    self._log(
                        f"COMBINED TP SKIP SELL: winner ${winner_price_now:.4f} in [{V3_RESOLVE_SKIP_SELL_PRICE:.2f}, 0.99) — "
                        f"let resolve (VWAP PnL {tp_pnl_pct*100:+.1f}%)", "INFO")
                else:
                    vwap_info = f"VWAP YES=${tp_yes_price:.4f} NO=${tp_no_price:.4f}"
                    self._log(
                        f"COMBINED TP: VWAP PnL {tp_pnl_pct*100:+.1f}% (${tp_pnl_usd:+.2f}) >= +{COMBINED_TP_PCT*100:.0f}% target "
                        f"({vwap_info}) - CLOSING ALL", "SUCCESS")
                    await self.close_position(position, "COMBINED_TP")
                    return

        # V3.14: FJ no longer freezes — DCA/combined TP continue after FJ

        # --- 4. Determine winner/loser by current price ---
        # V3.80: Need both sides to have tokens for DCA logic
        if position.yes_tokens <= 0 or position.no_tokens <= 0:
            return  # incomplete dual-side, wait for entry to complete

        # V3.80: Calculate gain for each side using YES/NO directly
        yes_vwap = position.yes_entry_price if position.yes_entry_price > 0 else 1.0
        no_vwap = position.no_entry_price if position.no_entry_price > 0 else 1.0
        yes_price = position.yes_price
        no_price = position.no_price

        yes_gain = (yes_price - yes_vwap) / yes_vwap if yes_vwap > 0 else 0
        no_gain = (no_price - no_vwap) / no_vwap if no_vwap > 0 else 0

        # Alias for logging (uses YES/NO directly, no main/hedge concept)
        main_gain = yes_gain
        hedge_gain = no_gain

        # Winner = side with higher price
        leading_price = max(yes_price, no_price)
        if leading_price < V3_DCA_WINNER_MIN_PRICE:
            if V3_DCA_MODE != "rebalance":
                return
        winner_side_label = "YES" if yes_price > no_price else "NO"
        loser_side_label = "NO" if winner_side_label == "YES" else "YES"
        winner_price_now = position.get_price(winner_side_label)
        loser_price = position.get_price(loser_side_label)
        winner_gain = yes_gain if winner_side_label == "YES" else no_gain
        # V3.80: winner/loser resolved from YES/NO prices directly

        # V3.83: Colorful log — green=positive gain, red=negative, cyan=winner label
        _mg_c = "\033[32m" if main_gain >= 0 else "\033[31m"
        _hg_c = "\033[32m" if hedge_gain >= 0 else "\033[31m"
        _wg_c = "\033[32m" if winner_gain >= 0 else "\033[31m"
        _wl_c = "\033[96m"  # cyan for winner label
        logger.info(
            f"[DCA-WIN] {position.market_slug}: main_gain={_mg_c}{main_gain*100:+.1f}%\033[0m "
            f"hedge_gain={_hg_c}{hedge_gain*100:+.1f}%\033[0m "
            f"\u2192 winner={_wl_c}{winner_side_label}\033[0m ${winner_price_now:.2f} "
            f"gain={_wg_c}{winner_gain*100:+.1f}%\033[0m | loser_price=${loser_price:.4f}")

        # V3.53: Track peak winner price for trailing stop (reset on winner side flip)
        if position.winner_peak_side != winner_side_label:
            position.winner_peak_price = winner_price_now
            position.winner_peak_side = winner_side_label
        elif winner_price_now > position.winner_peak_price:
            position.winner_peak_price = winner_price_now

        # Cooldown check
        now = time.time()
        if now - position.last_action_ts < MARKET_ACTION_COOLDOWN_SEC:
            return

        # --- 4b. Loss cut: sell portion of side that dropped >= V3_DCA_LOSS_CUT_PCT from VWAP ---
        # V3.62: Defer loss cut when FJ should fire OR has already fired (insurance tokens held for resolution)
        fj_should_fire = (V3_FJ_TRIGGER_PRICE > 0 and not position.final_justification_done
                          and loser_price <= V3_FJ_TRIGGER_PRICE and loser_price > 0)
        fj_active = fj_should_fire or position.final_justification_done
        # V3.90: Decouple CHOP freeze from FJ eligibility — only freeze when FJ is actually
        # about to execute (in last N minutes AND solver found solution). fj_eligible means loser
        # hit trigger but FJ hasn't fired yet (CHOP can continue earning). fj_executing means
        # FJ is imminent (freeze CHOP to preserve winner tokens for FJ solver).
        _elapsed_min_fj = (time.time() - self.get_current_window_ts()) / 60
        _remaining_min_fj = V3_WINDOW_MINUTES - _elapsed_min_fj
        _winner_enforce_fj = V3_FJ_ENFORCE_PRICE > 0 and winner_price_now >= V3_FJ_ENFORCE_PRICE
        fj_imminent = fj_should_fire and (V3_FJ_LAST_MIN <= 0 or _remaining_min_fj <= V3_FJ_LAST_MIN or _winner_enforce_fj)
        if V3_DCA_LOSS_CUT_PCT > 0 and not fj_active:
            # V3.80: Loss cut uses YES/NO directly
            for side_label, side_gain, side_vwap, side_price in [
                ("YES", yes_gain, yes_vwap, yes_price),
                ("NO", no_gain, no_vwap, no_price),
            ]:
                if side_gain <= -V3_DCA_LOSS_CUT_PCT:
                    avail = position.get_avail(side_label)
                    sell_tokens = self._round_tokens(avail * V3_DCA_LOSS_CUT_SELL_PCT)
                    if sell_tokens >= POLYMARKET_MIN_TOKENS:
                        cut_book = yes_book if side_label == "YES" else no_book
                        pnl_pct = (side_price - side_vwap) / side_vwap if side_vwap > 0 else 0
                        self._log(
                            f"DCA LOSS CUT: {side_label} dropped {side_gain*100:+.1f}% from VWAP ${side_vwap:.4f} "
                            f"→ sell {V3_DCA_LOSS_CUT_SELL_PCT*100:.0f}% ({sell_tokens:.0f} tokens) @ ${side_price:.4f}", "EXIT")
                        sold = await self.execute_rebalance_sell_unified(
                            position, side_label, sell_tokens, pnl_pct, cut_book, market_config,
                            skip_min_price=True)  # V3.91: loss cuts intentionally sell below VWAP
                        if sold:
                            return  # sell succeeded — one action per tick
                        # V3.90: sell failed — fall through to DCA


        # --- 5. DCA — mode-dependent: "winner", "loser", or "rebalance" (hybrid regime) ---
        _coin = self._get_symbol_from_slug(market_config.slug_pattern)
        prediction, pred_conf, _, pred_quality, _, pred_momentum, _pred_noise = self._read_prediction(_coin)
        pred_side = "YES" if prediction == "UP" else ("NO" if prediction == "DOWN" else "")

        # ML model gate — check before any DCA buy (late model: 150→300s window)
        _ml_dca_allowed = self._ml_should_allow_dca(
            _coin, prediction, pred_conf, pred_quality, 0, pred_momentum,
            position.yes_price, position.no_price) if V3_ML_DCA_ENABLED else True

        # Shared timing gates
        elapsed_min = (time.time() - self.get_current_window_ts()) / 60
        remaining_min = V3_WINDOW_MINUTES - elapsed_min
        dca_in_cooldown = position.last_dca_win_ts > 0 and (time.time() - position.last_dca_win_ts) < V3_DCA_COOLDOWN_SEC

        if V3_DCA_MODE == "loser":
            # V3.7 COUNTER-TREND: DCA into loser side (buy cheap, profit on mean-reversion)
            if loser_price > V3_DCA_LOSER_MAX_PRICE:
                self._log(f"DCA-LOSER SKIP: loser ${loser_price:.4f} > max ${V3_DCA_LOSER_MAX_PRICE:.2f}", "INFO")
            elif V3_DCA_DELAY_MINUTES > 0 and elapsed_min < V3_DCA_DELAY_MINUTES:
                self._log(f"DCA-LOSER DELAY: {elapsed_min:.1f}m < {V3_DCA_DELAY_MINUTES:.1f}m delay", "INFO")
            elif V3_DCA_CUTOFF_MINUTES > 0 and remaining_min <= V3_DCA_CUTOFF_MINUTES:
                self._log(f"DCA-LOSER CUTOFF: {remaining_min:.1f}m left < {V3_DCA_CUTOFF_MINUTES:.0f}m cutoff", "INFO")
            elif dca_in_cooldown:
                cd_remaining = V3_DCA_COOLDOWN_SEC - (time.time() - position.last_dca_win_ts)
                self._log(f"DCA-LOSER COOLDOWN: {cd_remaining:.0f}s remaining", "INFO")
            elif not _ml_dca_allowed:
                pass  # ML gate already logged the skip
            else:
                # V3.10: Per-side budget check — loser side capped at MAX_POSITION_COST_USD
                budget_remaining = self._position_budget_remaining(position, loser_side_label)
                dca_usd = min(V3_DCA_AMOUNT_USD, budget_remaining)
                if dca_usd < 1.0:
                    self._log(f"DCA-LOSER SKIP: {loser_side_label} budget exhausted (${budget_remaining:.2f} remaining)", "INFO")
                else:
                    # Buy loser side
                    loser_token_id = yes_token if loser_side_label == "YES" else no_token
                    loser_book = yes_book if loser_side_label == "YES" else no_book

                    self._log(
                        f"DCA-LOSER TRIGGER: {loser_side_label} @ ${loser_price:.4f} <= ${V3_DCA_LOSER_MAX_PRICE:.2f} "
                        f"→ DCA ${dca_usd:.2f} into loser (counter-trend)", "SIGNAL")
                    await self.dca_buy(
                        market_config, position.condition_id, loser_token_id,
                        loser_side_label, loser_book, market_obj,
                        usd_amount=dca_usd)
                    position.last_dca_win_ts = time.time()

        elif V3_DCA_MODE == "rebalance":
            # V3.8 HYBRID: Detect regime → TREND: DCA to winner, CHOP: rebalance sell+rebuy
            regime = self._detect_regime(pred_conf, pred_momentum)
            has_winner = leading_price >= V3_DCA_WINNER_MIN_PRICE

            # V3.95: Prediction confirmation — track consecutive same-direction ticks
            # Prevents TREND DCA on transient prediction flips (ref: V7 CROSS_MARKET_PRED_CONFIRM_TICKS)
            _market_sym = self._get_symbol_from_slug(market_config.slug_pattern) or _coin
            _trend_confirmed = True  # default: pass if disabled
            if V3_PRED_CONFIRM_TICKS > 0 and regime == "TREND" and prediction:
                _prev_dir, _prev_cnt = self._trend_confirm.get(_market_sym, (None, 0))
                if prediction == _prev_dir:
                    _cnt = _prev_cnt + 1
                else:
                    _cnt = 1  # direction changed — restart
                self._trend_confirm[_market_sym] = (prediction, _cnt)
                if _cnt < V3_PRED_CONFIRM_TICKS:
                    _trend_confirmed = False
                    self._log(
                        f"TREND CONFIRM: {prediction} {_cnt}/{V3_PRED_CONFIRM_TICKS} "
                        f"(mom={pred_momentum:+.3f} conf={pred_conf:.0%})", "INFO")
            elif regime != "TREND":
                # Reset confirm counter when not in TREND
                self._trend_confirm.pop(_market_sym, None)

            # Regime-aware cooldown: CHOP uses longer cooldown to slow down DCA
            _cd_sec = V3_CHOP_DCA_COOLDOWN_SEC if regime == "CHOP" else V3_DCA_COOLDOWN_SEC
            _rebal_in_cooldown = position.last_dca_win_ts > 0 and (time.time() - position.last_dca_win_ts) < _cd_sec

            if V3_DCA_CUTOFF_MINUTES > 0 and remaining_min <= V3_DCA_CUTOFF_MINUTES:
                self._log(f"REBALANCE CUTOFF: {remaining_min:.1f}m left < {V3_DCA_CUTOFF_MINUTES:.0f}m cutoff", "INFO")
            elif _rebal_in_cooldown:
                cd_remaining = _cd_sec - (time.time() - position.last_dca_win_ts)
                self._log(f"REBALANCE COOLDOWN: {cd_remaining:.0f}s remaining (regime={regime}, cd={_cd_sec:.0f}s)", "INFO")
            elif has_winner and not fj_should_fire and (
                (regime == "TREND" and _trend_confirmed) or
                (regime == "CHOP" and V3_CHOP_DCA_AMOUNT_USD > 0)
            ):
                # TREND: DCA to winner (pyramid into winning side)
                # CHOP: DCA to winner with reduced amount (V3_CHOP_DCA_AMOUNT_USD)
                # V3.62: Skip DCA when FJ should fire — avoid inflating solver with new winner tokens
                # V3.63: Skip DCA if winner price is dropping from peak (reversal guard)
                # V3.91: Skip TREND DCA after FJ fires — FJ is terminal, more winner buys bloat cost & trigger FJ reset deadlock
                _is_chop_dca = regime == "CHOP"
                _base_dca_amt = V3_CHOP_DCA_AMOUNT_USD if _is_chop_dca else V3_DCA_AMOUNT_USD
                _regime_label = "\033[33mCHOP\033[0m" if _is_chop_dca else "\033[32mTREND\033[0m"
                _peak_drop = 0.0
                if position.winner_peak_price > 0:
                    _peak_drop = (position.winner_peak_price - winner_price_now) / position.winner_peak_price
                # V3.81: Reset peak when price drops below threshold — prevents stuck peak blocking DCA forever
                if (V3_REBALANCE_PEAK_RESET_PRICE > 0
                        and winner_price_now < V3_REBALANCE_PEAK_RESET_PRICE
                        and position.winner_peak_price > V3_REBALANCE_PEAK_RESET_PRICE):
                    self._log(
                        f"REBALANCE-{regime} PEAK RESET: {winner_side_label} ${winner_price_now:.2f} < "
                        f"${V3_REBALANCE_PEAK_RESET_PRICE:.2f} threshold (was ${position.winner_peak_price:.2f})", "WARNING")
                    position.winner_peak_price = V3_REBALANCE_PEAK_RESET_PRICE
                    _peak_drop = 0.0  # allow DCA this tick
                if _peak_drop >= 0.05:
                    self._log(
                        f"REBALANCE-{regime} SKIP: winner {winner_side_label} reversing "
                        f"${position.winner_peak_price:.2f}→${winner_price_now:.2f} "
                        f"(-{_peak_drop*100:.1f}% from peak)", "WARNING")
                else:
                    win_token_id = yes_token if winner_side_label == "YES" else no_token
                    win_book = yes_book if winner_side_label == "YES" else no_book

                    # V3.66: Skip DCA if winner price too high for combined TP to be reachable
                    marginal_return = (V3_RESOLVE_TARGET_PRICE - winner_price_now) / winner_price_now if winner_price_now > 0 else 0
                    if marginal_return < COMBINED_TP_PCT:
                        self._log(
                            f"REBALANCE-{regime} SKIP: {winner_side_label} price ${winner_price_now:.2f} too high — "
                            f"marginal return {marginal_return*100:.1f}% < TP {COMBINED_TP_PCT*100:.0f}%", "WARNING")
                    elif V3_MAX_DCA_WIN_PER_WINDOW > 0 and position.dca_win_count >= V3_MAX_DCA_WIN_PER_WINDOW:
                        self._log(
                            f"REBALANCE-{regime} SKIP: DCA cap reached ({position.dca_win_count}/{V3_MAX_DCA_WIN_PER_WINDOW})", "WARNING")
                    elif not _ml_dca_allowed:
                        pass  # ML gate already logged the skip
                    else:
                        budget_remaining = self._position_budget_remaining(position, winner_side_label)
                        dca_usd = min(_base_dca_amt, budget_remaining)
                        if dca_usd < 1.0:
                            self._log(f"REBALANCE-{regime} SKIP: {winner_side_label} budget exhausted (${budget_remaining:.2f})", "INFO")
                        else:
                            _tc_dir, _tc_cnt = self._trend_confirm.get(_market_sym, ("", 0))
                            _tc_fire = f" confirm={_tc_dir} {_tc_cnt}/{V3_PRED_CONFIRM_TICKS}" if V3_PRED_CONFIRM_TICKS > 0 else ""
                            self._log(
                                f"REBALANCE-{_regime_label}: conf={pred_conf:.0%} mom={pred_momentum:+.3f}{_tc_fire} "
                                f"→ DCA ${dca_usd:.2f} into winner \033[96m{winner_side_label}\033[0m "
                                f"({position.dca_win_count + 1}/{V3_MAX_DCA_WIN_PER_WINDOW or '∞'})", "SIGNAL")
                            await self.dca_buy(
                                market_config, position.condition_id, win_token_id,
                                winner_side_label, win_book, market_obj,
                                usd_amount=dca_usd)
                            position.last_dca_win_ts = time.time()
                            position.dca_win_count += 1
                            # V3.82: Reset FJ after DCA-TREND creates token imbalance
                            # so FJ can re-fire to buy cheap loser insurance
                            if position.final_justification_done:
                                w_avail = position.get_avail(winner_side_label)
                                l_avail = position.get_avail(loser_side_label)
                                if w_avail > l_avail * (1 + V3_FJ_MAX_LOSS_PCT):  # imbalance exceeds FJ loss target
                                    position.final_justification_done = False
                                    self._log(
                                        f"FJ RESET: DCA-TREND imbalance {winner_side_label}={w_avail:.0f} vs "
                                        f"{loser_side_label}={l_avail:.0f} — re-enable FJ for loser insurance", "WARNING")
            else:
                # CHOP: Sell winner profit → buy loser for breakeven hedge
                _rc = "\033[33m"  # yellow for CHOP
                _tc_dir, _tc_cnt = self._trend_confirm.get(_market_sym, ("", 0))
                _tc_str = f" | confirm={_tc_dir} {_tc_cnt}/{V3_PRED_CONFIRM_TICKS}" if V3_PRED_CONFIRM_TICKS > 0 and _tc_cnt > 0 else ""
                self._log(
                    f"REBALANCE-{_rc}CHOP\033[0m: conf={pred_conf:.0%} mom={pred_momentum:+.3f} "
                    f"| main_gain={_mg_c}{main_gain*100:+.1f}%\033[0m hedge_gain={_hg_c}{hedge_gain*100:+.1f}%\033[0m{_tc_str}", "INFO")
                # V3.90: Only freeze CHOP when FJ is imminent (in last N minutes), not just eligible
                # CHOP continues earning while FJ waits for time gate. Prevents 13+ min of idle CHOP.
                if fj_imminent:
                    self._log(
                        f"REBALANCE \033[31mFROZEN\033[0m: FJ imminent ({_remaining_min_fj:.1f}m left, loser ${loser_price:.4f}) "
                        f"— hold winner tokens for FJ solver", "WARNING")
                elif fj_should_fire:
                    self._log(
                        f"REBALANCE \033[33mFJ-ELIGIBLE\033[0m: loser ${loser_price:.4f} <= trigger ${V3_FJ_TRIGGER_PRICE} "
                        f"but {_remaining_min_fj:.1f}m left > {V3_FJ_LAST_MIN:.0f}m gate — CHOP continues", "INFO")
                # V3.85: Freeze rebalance when Combined TP TSL is actively trailing
                # Selling winner during TSL destroys the profit TSL is trying to capture.
                tsl_active = position.combined_tp_floor > 0  # V3.88 FIX: was >= COMBINED_TP_PCT, missed early TSL phase
                if tsl_active:
                    self._log(
                        f"REBALANCE \033[31mFROZEN\033[0m: Combined TP TSL active "
                        f"(floor={position.combined_tp_floor*100:+.1f}%, peak={position.combined_tp_peak_pnl*100:+.1f}%) "
                        f"— hold position for TSL exit", "WARNING")
                # V3.74: Cap rebalance cycles to prevent VWAP inflation from endless sell+rebuy
                # V3.90: Use fj_imminent instead of fj_active — CHOP allowed during FJ-eligible
                # V3.94: Removed fj_done freeze — FJ freeze was preventing winner DCA, causing net-negative sessions
                fj_done = position.final_justification_done
                rebalance_allowed = not fj_imminent and not tsl_active and (V3_MAX_REBALANCE_CYCLES <= 0 or position.rebalance_count < V3_MAX_REBALANCE_CYCLES)
                if not rebalance_allowed and not fj_imminent and not tsl_active:
                    self._log(
                        f"REBALANCE SKIP: cycle {position.rebalance_count}/{V3_MAX_REBALANCE_CYCLES} — max reached, hold for resolution", "WARNING")
                # V3.80: Iterate over YES/NO directly
                for side_label, side_vwap, side_price in [
                    ("YES", yes_vwap, yes_price),
                    ("NO", no_vwap, no_price),
                ]:
                    side_name = side_label  # V3.80: side_name is now YES/NO
                    if not rebalance_allowed:
                        break
                    if side_vwap <= 0 or side_price <= 0.01:
                        continue
                    # Check gain after taker fee
                    net_price = side_price * (1 - V3_TAKER_FEE_PCT)
                    gain = (net_price - side_vwap) / side_vwap
                    if gain < V3_REBALANCE_GAIN_PCT:
                        continue
                    if position.rebalance_rebuy_blocked:
                        continue
                    # V3.80: Resolve side book/token/avail directly from YES/NO
                    avail = position.get_avail(side_label)
                    side_book = yes_book if side_label == "YES" else no_book
                    side_token = yes_token if side_label == "YES" else no_token
                    # V3.83: Pre-check OPPOSITE side ask depth — we buy loser, not rebuy winner
                    opp_book_check = no_book if side_label == "YES" else yes_book
                    opp_label_check = "NO" if side_label == "YES" else "YES"
                    _, opp_best_ask = self.get_best_prices(opp_book_check)
                    if opp_best_ask <= 0 or opp_best_ask > BUY_BAND_HIGH:
                        self._log(
                            f"REBALANCE SKIP: {opp_label_check} ask=${opp_best_ask:.4f} outside band (>${BUY_BAND_HIGH}) — can't hedge", "WARNING")
                        continue
                    sell_tokens = self._round_tokens(avail * V3_REBALANCE_SELL_PCT)
                    if sell_tokens < POLYMARKET_MIN_TOKENS:
                        continue
                    # V3.85: Winner floor guard — don't sell below the minimum tokens needed
                    # to keep winner-wins loss within V3_REBALANCE_MAX_LOSS_PCT at resolution.
                    # Uses separate threshold from FJ (7%) because hedge buys inflate cost;
                    # FJ's tight target would kill rebalance entirely. Default 40% = max -$60 on $150 pos.
                    if V3_REBALANCE_MAX_LOSS_PCT > 0:
                        effective_cost = position.session_total_cost
                        proceeds = position.session_sell_proceeds
                        R = 0.99
                        # min_winner = tokens needed so winner_wins >= -(max_loss%)
                        # winner_value = min_winner * R + proceeds >= (1 - X) * effective_cost
                        min_winner_tokens = max(0, ((1 - V3_REBALANCE_MAX_LOSS_PCT) * effective_cost - proceeds) / R) if effective_cost > 0 else 0
                        remaining_after_sell = avail - sell_tokens
                        if remaining_after_sell < min_winner_tokens:
                            # Reduce sell qty to stay above floor
                            max_sellable = max(0, self._round_tokens(avail - min_winner_tokens))
                            if max_sellable < POLYMARKET_MIN_TOKENS:
                                self._log(
                                    f"REBALANCE \033[31mFLOOR\033[0m: {side_label} {avail:.0f} tokens, "
                                    f"floor={min_winner_tokens:.0f} (need for -{V3_REBALANCE_MAX_LOSS_PCT*100:.0f}% max loss) "
                                    f"— sell would breach, skipping", "WARNING")
                                continue
                            sell_tokens = max_sellable
                            self._log(
                                f"REBALANCE FLOOR CAP: {side_label} sell reduced to {sell_tokens:.0f} "
                                f"(floor={min_winner_tokens:.0f}, avail={avail:.0f})", "WARNING")
                    # V3.60: VWAP pre-check — verify actual sell fill is profitable AND rebuy is feasible
                    # Without this, bot sells into thin bids (fill worse than best_bid) and can't rebuy cheaply
                    bid_vwap, bid_fill = self._calc_bid_vwap(side_book, sell_tokens)
                    if not bid_vwap or bid_fill < sell_tokens * 0.8:
                        self._log(
                            f"REBALANCE SKIP: {side_label} bid depth insufficient — "
                            f"need {sell_tokens:.0f} tokens, only {bid_fill:.0f} available", "WARNING")
                        continue
                    # Check if actual fill VWAP (after fee) is still profitable vs entry
                    bid_vwap_net = bid_vwap * (1 - V3_TAKER_FEE_PCT)
                    vwap_gain = (bid_vwap_net - side_vwap) / side_vwap if side_vwap > 0 else 0
                    if vwap_gain < V3_REBALANCE_GAIN_PCT:
                        self._log(
                            f"REBALANCE SKIP: {side_label} bid VWAP ${bid_vwap:.4f} (net ${bid_vwap_net:.4f}) "
                            f"vs entry ${side_vwap:.4f} → gain {vwap_gain*100:+.1f}% < {V3_REBALANCE_GAIN_PCT*100:.0f}% target", "WARNING")
                        continue
                    # V3.83: Removed V3.72 same-side TP feasibility check — no longer rebuying same side
                    # Sell profitable portion
                    self._log(
                        f"\033[33mREBALANCE SELL\033[0m: \033[96m{side_label}\033[0m {sell_tokens:.0f}/{avail:.0f} tokens "
                        f"@ ${side_price:.4f} (vwap=${side_vwap:.4f}, gain=\033[32m{gain*100:+.1f}%\033[0m)", "SIGNAL")
                    sold = await self.execute_rebalance_sell_unified(
                        position, side_name, sell_tokens, gain, side_book, market_config)
                    if sold:
                        position.rebalance_count += 1  # V3.74: Track cycle count
                        # V3.15: Sleep between sell and rebuy to avoid API rate-limit timeout
                        await asyncio.sleep(2)
                        # V3.83: Buy OPPOSITE (loser) side with proceeds for breakeven hedge
                        # Instead of rebuying same side (VWAP inflation), buy loser to cover both outcomes
                        opp_label = "NO" if side_label == "YES" else "YES"
                        opp_book = no_book if side_label == "YES" else yes_book
                        opp_token = no_token if side_label == "YES" else yes_token
                        opp_price = no_price if side_label == "YES" else yes_price
                        rebuy_usd = sell_tokens * net_price * V3_REBALANCE_HEDGE_PCT  # V3.87: Configurable hedge % of proceeds
                        # Calculate breakeven status: loser needs tokens >= total_cost to break even on resolution
                        opp_tokens_now = position.get_avail(opp_label)
                        total_cost = position.session_total_cost
                        tokens_deficit = max(0, total_cost - opp_tokens_now)
                        self._log(
                            f"REBALANCE HEDGE: sell {side_label} profit → buy {opp_label} ${rebuy_usd:.2f} "
                            f"@ ${opp_price:.4f} (loser has {opp_tokens_now:.0f} tokens, "
                            f"need {total_cost:.0f} for breakeven, deficit={tokens_deficit:.0f})", "SIGNAL")
                        # V3.98: Cap loser buy cost so if market flips, position still hits TP
                        if rebuy_usd > 0:
                            headroom = self._loser_cost_headroom(position, opp_label)
                            if rebuy_usd > headroom:
                                self._log(
                                    f"REBALANCE HEDGE CAP: ${rebuy_usd:.2f} → ${headroom:.2f} "
                                    f"(loser cost cap=winner_cost*(1+TP))", "WARNING")
                                rebuy_usd = headroom
                        # V3.90 FIX: Skip hedge buy when hedge_pct=0 — dca_buy with usd=0 falls through
                        # to legacy chunk sizing, buying ~30 tokens unintentionally
                        # V3.93: Skip hedge buy when loser price < FJ trigger — loser is near-certain to expire worthless
                        if rebuy_usd > 0 and opp_price < V3_FJ_TRIGGER_PRICE:
                            self._log(
                                f"REBALANCE HEDGE SKIP: loser {opp_label} ${opp_price:.4f} < FJ trigger ${V3_FJ_TRIGGER_PRICE} "
                                f"— not buying near-worthless tokens", "WARNING")
                            rebuy_usd = 0
                        if rebuy_usd > 0:
                            # V3.59 FIX: Reset cooldown so dca_buy doesn't silently skip the rebuy
                            position.last_action_ts = 0
                            # Track tokens before buy to detect failure
                            tokens_before = position.get_tokens(opp_label)
                            await self.dca_buy(
                                market_config, position.condition_id, opp_token,
                                opp_label, opp_book, market_obj,
                                usd_amount=rebuy_usd, skip_budget_cap=True)
                            tokens_after = position.get_tokens(opp_label)
                            if tokens_after > tokens_before:
                                position.session_recycled_cost += rebuy_usd
                                position.last_dca_win_ts = time.time()
                                position.rebalance_rebuy_blocked = False
                                bought = tokens_after - tokens_before
                                new_deficit = max(0, total_cost - tokens_after)
                                self._log(
                                    f"REBALANCE HEDGE OK: bought {bought:.0f} {opp_label} tokens "
                                    f"(now {tokens_after:.0f}, deficit={new_deficit:.0f})", "SUCCESS")
                            else:
                                position.rebalance_rebuy_blocked = True
                                position.last_dca_win_ts = time.time()
                                self._log(
                                    f"REBALANCE HEDGE FAILED: {opp_label} — sold {sell_tokens:.0f} {side_label} but "
                                    f"buy {opp_label} did not execute. Rebalance BLOCKED.", "ERROR")
                        else:
                            self._log(
                                f"REBALANCE SELL-ONLY: hedge_pct=0, no loser buy "
                                f"(loser {opp_tokens_now:.0f} tokens, deficit={tokens_deficit:.0f})", "INFO")
                        break  # one rebalance action per tick

                # V3.88: Cheap loser DCA — buy cheap loser tokens as flip insurance in CHOP
                # When loser drops significantly from VWAP, buy small amount (50% of entry)
                # If market flips → cheap tokens win big. If not → FJ covers winner, small loss.
                # V3.99: Session-level dynamic cap: total cheap loser spend <= session_total_cost * (1 + COMBINED_TP_PCT)
                _session_cheap_max = self.state.session_total_cost * (1 + COMBINED_TP_PCT)
                _session_cheap_remaining = max(0, _session_cheap_max - self.state.session_cheap_loser_total_usd)
                if (V3_CHEAP_LOSER_DCA_MAX > 0
                        and position.cheap_loser_dca_count < V3_CHEAP_LOSER_DCA_MAX
                        and _session_cheap_remaining >= 1.0
                        and not _rebal_in_cooldown
                        and not fj_active
                        and not tsl_active):
                    loser_vwap = position.get_entry_price(loser_side_label)
                    if loser_vwap > 0 and loser_price > 0:
                        loser_drop = (loser_vwap - loser_price) / loser_vwap
                        if loser_drop >= V3_CHEAP_LOSER_DCA_DROP_PCT:
                            cheap_dca_usd = V3_ENTRY_AMOUNT_USD * V3_CHEAP_LOSER_DCA_AMOUNT_PCT
                            budget_remaining = self._position_budget_remaining(position, loser_side_label)
                            # V3.98: Per-position cap — loser_cost <= winner_cost * (1 + TP)
                            headroom = self._loser_cost_headroom(position, loser_side_label)
                            # V3.99: Session-level cap — total cheap loser <= session_cost * (1 + TP)
                            cheap_dca_usd = min(cheap_dca_usd, budget_remaining, headroom, _session_cheap_remaining)
                            if cheap_dca_usd >= 1.0:
                                loser_token_id = yes_token if loser_side_label == "YES" else no_token
                                loser_book = yes_book if loser_side_label == "YES" else no_book
                                self._log(
                                    f"\033[36mCHEAP LOSER DCA\033[0m: {loser_side_label} @ ${loser_price:.4f} "
                                    f"(vwap=${loser_vwap:.4f}, drop={loser_drop*100:+.1f}% >= {V3_CHEAP_LOSER_DCA_DROP_PCT*100:.0f}%) "
                                    f"→ buy ${cheap_dca_usd:.2f} ({position.cheap_loser_dca_count+1}/{V3_CHEAP_LOSER_DCA_MAX}) "
                                    f"[session ${self.state.session_cheap_loser_total_usd:.1f}/${_session_cheap_max:.1f}]", "SIGNAL")
                                tokens_before = position.get_tokens(loser_side_label)
                                await self.dca_buy(
                                    market_config, position.condition_id, loser_token_id,
                                    loser_side_label, loser_book, market_obj,
                                    usd_amount=cheap_dca_usd)
                                if position.get_tokens(loser_side_label) > tokens_before:
                                    position.cheap_loser_dca_count += 1
                                    position.last_dca_win_ts = time.time()
                                    self.state.session_cheap_loser_total_usd += cheap_dca_usd
                                    self._log(
                                        f"CHEAP LOSER DCA OK: {loser_side_label} "
                                        f"({position.cheap_loser_dca_count}/{V3_CHEAP_LOSER_DCA_MAX}) "
                                        f"[session ${self.state.session_cheap_loser_total_usd:.1f}/${_session_cheap_max:.1f}]", "SUCCESS")

        else:
            # LEGACY MODE: DCA into winner + sell loser (V3_DCA_MODE="winner")
            if not pred_side:
                self._log(f"DCA-WIN SKIP: no prediction available for {_coin}", "WARNING")
            elif pred_side != winner_side_label:
                self._log(
                    f"DCA-WIN SKIP: pred={prediction}({pred_conf:.0%} q={pred_quality:.2f}) → {pred_side} ≠ winner {winner_side_label}", "WARNING")
            elif pred_conf < PREDICTION_BOOST_MIN_CONFIDENCE:
                self._log(
                    f"DCA-WIN SKIP: pred={prediction}({pred_conf:.0%} q={pred_quality:.2f}) below min confidence {PREDICTION_BOOST_MIN_CONFIDENCE:.0%}", "WARNING")
            elif V3_DCA_MIN_QUALITY_SCORE > 0 and pred_quality < V3_DCA_MIN_QUALITY_SCORE:
                self._log(
                    f"DCA-WIN SKIP: pred={prediction}({pred_conf:.0%} q={pred_quality:.2f}) below min quality {V3_DCA_MIN_QUALITY_SCORE:.2f}", "WARNING")
            _quality_ok = V3_DCA_MIN_QUALITY_SCORE <= 0 or pred_quality >= V3_DCA_MIN_QUALITY_SCORE
            if pred_side == winner_side_label and pred_conf >= PREDICTION_BOOST_MIN_CONFIDENCE and _quality_ok and not fj_should_fire:
                # V3.62: Skip DCA-to-winner when FJ should fire — avoid inflating solver
                if V3_DCA_DELAY_MINUTES > 0 and elapsed_min < V3_DCA_DELAY_MINUTES:
                    self._log(f"DCA-WIN DELAY: {elapsed_min:.1f}m < {V3_DCA_DELAY_MINUTES:.1f}m delay", "WARNING")
                    return
                if V3_DCA_CUTOFF_MINUTES > 0 and remaining_min <= V3_DCA_CUTOFF_MINUTES:
                    self._log(f"DCA-WIN CUTOFF: {remaining_min:.1f}m left < {V3_DCA_CUTOFF_MINUTES:.0f}m cutoff", "WARNING")
                    return

                if dca_in_cooldown:
                    cd_remaining = V3_DCA_COOLDOWN_SEC - (time.time() - position.last_dca_win_ts)
                    self._log(f"DCA-WIN COOLDOWN: {cd_remaining:.0f}s remaining", "WARNING")
                else:
                    win_token_id = yes_token if winner_side_label == "YES" else no_token
                    win_book = yes_book if winner_side_label == "YES" else no_book

                    dca_usd, skip_reason = self._calc_resolution_dca_amount(position, winner_side_label, winner_price_now)

                    if dca_usd <= 0:
                        self._log(
                            f"DCA-WIN SKIP: {winner_side_label} pred={prediction}({pred_conf:.0%} q={pred_quality:.2f}) — {skip_reason}", "WARNING")
                    else:
                        self._log(
                            f"DCA-WIN TRIGGER: {winner_side_label} pred={prediction}({pred_conf:.0%} q={pred_quality:.2f}) "
                            f"\u2192 DCA ${dca_usd:.2f} into winner (resolution-targeted), sell loser", "SIGNAL")
                        await self.dca_buy(
                            market_config, position.condition_id, win_token_id,
                            winner_side_label, win_book, market_obj,
                            usd_amount=dca_usd)
                        position.last_dca_win_ts = time.time()

                # Sell 30% of loser — keep min 5 tokens for resolve scenario calc
                MIN_HEDGE_KEEP = 5
                if not dca_in_cooldown:
                    if loser_price >= V3_SELL_LOSER_MIN_PRICE:
                        available_tokens = position.get_avail(loser_side_label)
                        sellable = max(0, available_tokens - MIN_HEDGE_KEEP)
                        sell_tokens = self._round_tokens(sellable * V3_SELL_LOSER_PCT)

                        if sell_tokens >= POLYMARKET_MIN_TOKENS:
                            loser_book = yes_book if loser_side_label == "YES" else no_book
                            loser_entry = position.get_entry_price(loser_side_label)
                            pnl_pct_sell = (loser_price - loser_entry) / loser_entry if loser_entry > 0 else 0
                            self._log(
                                f"DCA-WIN SELL LOSER: {loser_side_label} {sell_tokens:.0f} tokens @ ${loser_price:.4f} "
                                f"(entry=${loser_entry:.4f}, pnl={pnl_pct_sell*100:+.1f}%)", "EXIT")
                            await self.execute_rebalance_sell_unified(
                                position, loser_side_label, sell_tokens, pnl_pct_sell, loser_book, market_config)
                    else:
                        self._log(
                            f"DCA-WIN SKIP SELL: loser ${loser_price:.4f} < floor ${V3_SELL_LOSER_MIN_PRICE:.2f}", "WARNING")

        # --- 6. Lottery Ticket: buy ultra-cheap loser tokens (one-shot, cost pre-reserved) ---
        if not position.lottery_ticket_done and loser_price <= V3_LOTTERY_TICKET_PRICE and loser_price > 0:
            lottery_tokens = self._round_tokens(V3_LOTTERY_TICKET_USD / loser_price)
            if lottery_tokens >= POLYMARKET_MIN_TOKENS:
                self._log(
                    f"LOTTERY TICKET: {loser_side_label} @ ${loser_price:.4f} <= ${V3_LOTTERY_TICKET_PRICE} "
                    f"→ {lottery_tokens:.0f} tokens for ${V3_LOTTERY_TICKET_USD:.2f} (pre-reserved)", "SIGNAL")
                await self.execute_rebalance_buy(
                    position, loser_side_label, lottery_tokens,
                    yes_token, no_token, yes_book, no_book, market_config,
                    skip_budget_cap=True)
                position.lottery_ticket_done = True
                await asyncio.sleep(2)  # V3.15: Avoid API rate-limit before FJ

        # --- 6b. V3.90 Progressive Recoup: sell cheap loser + buy winner for early partial de-risk ---
        # Instead of waiting for FJ one-shot at loser=0.15, progressively recoup when winner is strong.
        # Sell loser at 0.10-0.18 (partial value recovery) and buy winner to lock in guaranteed profit.
        # Triggers when: winner >= 0.80 AND loser <= FJ_TRIGGER AND not in cooldown AND FJ not yet done
        V3_RECOUP_WINNER_MIN = 0.80  # winner must be strong for recoup
        if (V3_PROGRESSIVE_RECOUP_ENABLED
                and not position.final_justification_done
                and loser_price > 0 and loser_price <= V3_FJ_TRIGGER_PRICE
                and winner_price_now >= V3_RECOUP_WINNER_MIN
                and not dca_in_cooldown):
            loser_avail = position.get_avail(loser_side_label)
            # Sell up to 30% of loser tokens — recover partial value before they go to 0
            recoup_sell = self._round_tokens(loser_avail * 0.30)
            if recoup_sell >= POLYMARKET_MIN_TOKENS:
                loser_book_recoup = yes_book if loser_side_label == "YES" else no_book
                loser_entry = position.get_entry_price(loser_side_label)
                loser_pnl = (loser_price - loser_entry) / loser_entry if loser_entry > 0 else 0
                # Check loser bid depth
                bid_vwap_r, bid_fill_r = self._calc_bid_vwap(loser_book_recoup, recoup_sell)
                if bid_vwap_r and bid_fill_r >= recoup_sell * 0.5:
                    recoup_proceeds_est = recoup_sell * bid_vwap_r * (1 - V3_TAKER_FEE_PCT)
                    # Buy winner with proceeds — guaranteed profit if winner resolves
                    marginal_win = (V3_RESOLVE_TARGET_PRICE - winner_price_now) / winner_price_now if winner_price_now > 0 else 0
                    if marginal_win >= 0.05:  # at least 5% upside to resolution
                        self._log(
                            f"PROGRESSIVE RECOUP: sell {recoup_sell:.0f} {loser_side_label} @ ${loser_price:.4f} "
                            f"(entry=${loser_entry:.4f}, pnl={loser_pnl*100:+.1f}%) → buy {winner_side_label} "
                            f"${recoup_proceeds_est:.2f} @ ${winner_price_now:.4f} "
                            f"(marginal +{marginal_win*100:.0f}% to resolve)", "SIGNAL")
                        sold = await self.execute_rebalance_sell_unified(
                            position, loser_side_label, recoup_sell, loser_pnl,
                            loser_book_recoup, market_config, skip_min_price=True)
                        if sold:
                            await asyncio.sleep(2)
                            win_token_r = yes_token if winner_side_label == "YES" else no_token
                            win_book_r = yes_book if winner_side_label == "YES" else no_book
                            position.last_action_ts = 0  # reset cooldown for rebuy
                            await self.dca_buy(
                                market_config, position.condition_id, win_token_r,
                                winner_side_label, win_book_r, market_obj,
                                usd_amount=recoup_proceeds_est, skip_budget_cap=True)
                            position.last_dca_win_ts = time.time()

        # --- 7. Final Justification: buy winner tokens to guarantee winner-wins >= MIN_WIN% ---
        # V3.61: FJ with loser insurance — buy both winner top-up AND loser insurance, no freeze, DCA continues
        if V3_FJ_TRIGGER_PRICE > 0 and not position.final_justification_done and loser_price <= V3_FJ_TRIGGER_PRICE and loser_price > 0:
            effective_cost = position.session_total_cost + (V3_LOTTERY_TICKET_USD if not position.lottery_ticket_done else 0)
            R = 0.99  # resolution payout with 1% safety buffer
            proceeds = position.session_sell_proceeds
            # V3.90 Phase 2: Book-aware FJ target — if CHOP already generated realized proceeds,
            # reduce the effective loss FJ needs to cover. FJ buys fewer tokens when CHOP has de-risked.
            fj_effective_cost = max(0, effective_cost - proceeds)  # net exposure after realized proceeds
            X = V3_FJ_MAX_LOSS_PCT   # loser-wins >= -X (e.g., -10%)
            Y = V3_FJ_MIN_WIN_PCT    # winner-wins >= +Y (e.g., +5%)
            # V3.69: Use ask VWAP for loser cost — mid-market price underestimates actual buy cost
            loser_book = no_book if winner_side_label == "YES" else yes_book
            loser_ask_vwap, _ = self._calc_ask_vwap(loser_book, 100)  # estimate for ~100 tokens
            lp = loser_ask_vwap if loser_ask_vwap is not None else loser_price
            wp = winner_price_now

            # Token counts
            winner_tokens = position.get_avail(winner_side_label)
            loser_tokens = position.get_avail(loser_side_label)

            # Pre-FJ PnL for both outcomes
            delta = winner_tokens * R + proceeds
            loser_value = loser_tokens * R + proceeds
            w_pnl_before = (delta - effective_cost) / effective_cost if effective_cost > 0 else 0
            l_pnl_before = (loser_value - effective_cost) / effective_cost if effective_cost > 0 else 0

            # Quick check — if both scenarios already satisfy targets, skip FJ
            if l_pnl_before >= -X and w_pnl_before >= Y:
                pass  # both outcomes OK, no FJ needed
            else:
                # V3.7: Time gate — ALWAYS wait until last N minutes
                elapsed_min = (time.time() - self.get_current_window_ts()) / 60
                remaining_min = V3_WINDOW_MINUTES - elapsed_min
                # V3.12: Price-enforce gate — bypass time gate when winner price is high enough
                winner_enforce = V3_FJ_ENFORCE_PRICE > 0 and winner_price_now >= V3_FJ_ENFORCE_PRICE
                in_last_minutes = V3_FJ_LAST_MIN <= 0 or remaining_min <= V3_FJ_LAST_MIN or winner_enforce

                skip_fj = True  # Default: skip unless solver finds affordable level
                Ny, Nw = 0, 0
                if not in_last_minutes:
                    self._log(
                        f"FINAL JUSTIFICATION WAIT: {remaining_min:.1f}m left > {V3_FJ_LAST_MIN:.0f}m gate "
                        f"(winner-wins {w_pnl_before*100:+.1f}%, loser-wins {l_pnl_before*100:+.1f}%) — keep DCA'ing", "WARNING")
                else:
                    if winner_enforce and remaining_min > V3_FJ_LAST_MIN:
                        self._log(
                            f"FJ PRICE-ENFORCE: winner {winner_price_now:.3f} >= {V3_FJ_ENFORCE_PRICE} — "
                            f"bypassing time gate ({remaining_min:.1f}m left)", "WARNING")
                    # V3.88: PnL-target FJ solver — buy winner for TP, cap loser loss
                    # Strategy: buy winner tokens to hit winner-wins >= Y (V3_FJ_MIN_WIN_PCT),
                    # then buy loser tokens to cap loser-wins >= -X (V3_FJ_MAX_LOSS_PCT).
                    # Single-pass: Nw/Ny coupling means iteration diverges at high winner prices.
                    # V3.90: Use fj_effective_cost (net of realized proceeds) for solver targets.
                    # If CHOP already de-risked via realized proceeds, FJ needs fewer tokens.
                    fj_solver_cost = fj_effective_cost  # net exposure = total_cost - realized_proceeds
                    # V3.97 FIX: FJ budget — use wallet balance as fallback when position budget exhausted.
                    # DCA can exhaust per-side budget, leaving FJ unable to buy insurance tokens.
                    # FJ is a terminal action so it should use wallet balance, not just position budget.
                    fj_budget_cap = effective_cost
                    _pos_budget = self._position_budget_remaining(position)
                    _wallet_balance = getattr(self, 'current_balance', 0.0)
                    raw_budget = min(max(_pos_budget, _wallet_balance), fj_budget_cap)
                    if _pos_budget < 1.0 and _wallet_balance > 1.0:
                        self._log(
                            f"FJ BUDGET: position budget exhausted (${_pos_budget:.2f}), "
                            f"using wallet balance ${_wallet_balance:.2f}", "WARNING")
                    remaining_budget = raw_budget
                    if proceeds > 0:
                        self._log(
                            f"FJ BOOK-AWARE: realized proceeds ${proceeds:.2f} reduce solver target "
                            f"${effective_cost:.2f} → ${fj_solver_cost:.2f}", "INFO")

                    chosen_Ny, chosen_Nw = 0, 0
                    skip_fj = False
                    # V3.95 FIX: Use effective_cost (gross) for solver — V3.90 book-aware
                    # double-counted proceeds (subtracted from cost AND added to revenue),
                    # making solver think Nw=0 when actually 100+ tokens needed.
                    W_resolve = winner_tokens * R + proceeds
                    L_resolve = loser_tokens * R + proceeds

                    # V3.88: PnL-based solver (single pass — coupled system has fixed PnL ratio)
                    denom_w = R - (1 + Y) * wp  # marginal winner-wins gain per winner token
                    denom_l = R - (1 - X) * lp  # marginal loser-wins gain per loser token
                    marginal_return = (R - wp) / wp if wp > 0 else 0

                    if denom_w > 0.001 and marginal_return >= COMBINED_TP_PCT:
                        # Step 1: Nw for winner-wins >= Y (assuming Ny=0)
                        # V3.95 FIX: Use effective_cost (gross) — fj_solver_cost double-counted proceeds
                        _nw_need = (1 + Y) * effective_cost - W_resolve
                        chosen_Nw = max(0, math.ceil(_nw_need / denom_w)) if _nw_need > 0 else 0

                        # Step 2: Ny for loser-wins >= -X (given Nw from step 1)
                        if denom_l > 0.001:
                            _cost_with_nw = effective_cost + chosen_Nw * wp
                            _ny_need = (1 - X) * _cost_with_nw - L_resolve
                            chosen_Ny = max(0, math.ceil(_ny_need / denom_l)) if _ny_need > 0 else 0

                        # Budget check — prioritize winner tokens (main profit driver)
                        total_fj_cost = chosen_Nw * wp + chosen_Ny * lp
                        if total_fj_cost > remaining_budget:
                            max_nw_budget = math.floor(remaining_budget / wp) if wp > 0.001 else 0
                            chosen_Nw = min(chosen_Nw, max_nw_budget)
                            leftover = remaining_budget - chosen_Nw * wp
                            max_ny_leftover = math.floor(leftover / lp) if lp > 0.001 else 0
                            chosen_Ny = min(chosen_Ny, max_ny_leftover)
                    elif marginal_return < COMBINED_TP_PCT:
                        # Winner price too high for TP — skip (near resolution will handle)
                        skip_fj = True

                    # V3.98→V3.99: FJ is exempt from loser cost headroom cap.
                    # FJ is insurance — it MUST be allowed to buy loser tokens even when
                    # the loser side is overweight (e.g. from DCA that flipped sides).
                    # The headroom cap still applies to cheap loser DCA and other buys.

                    # Bump loser to minimum order size if > 0 but below min
                    if 0 < chosen_Ny < POLYMARKET_MIN_TOKENS:
                        chosen_Ny = POLYMARKET_MIN_TOKENS

                    # Compute post-FJ PnL for both outcomes
                    total_fj_cost_check = chosen_Ny * lp + chosen_Nw * wp
                    new_cost_check = effective_cost + total_fj_cost_check
                    w_pnl_post = ((winner_tokens + chosen_Nw) * R + proceeds - new_cost_check) / new_cost_check if new_cost_check > 0 else 0
                    l_pnl_post = ((loser_tokens + chosen_Ny) * R + proceeds - new_cost_check) / new_cost_check if new_cost_check > 0 else 0

                    if chosen_Ny > 0 or chosen_Nw > 0:
                        # V3.88: Quality gate — strict targets first, then relaxed fallback
                        # At high winner prices, Nw/Ny coupling makes strict targets unreachable.
                        # Relaxed: winner-wins >= 0% (break-even) AND loser-wins >= -X is acceptable
                        # to avoid FJ deadlock (FJ blocks DCA but can never fire).
                        strict_ok = w_pnl_post >= Y and l_pnl_post >= -X
                        relaxed_ok = w_pnl_post >= 0 and l_pnl_post >= -X
                        # V3.95: Allow capped-loss FJ — cap BOTH sides at -X (e.g. -25%).
                        # Without FJ, resolve exits lose ~$14. With capped-loss FJ,
                        # worst case is -X% (~$5) on either outcome — much better.
                        capped_ok = w_pnl_post >= -X and l_pnl_post >= -X
                        if not strict_ok and not relaxed_ok and not capped_ok:
                            skip_fj = True
                            self._log(
                                f"FJ WAIT: solution win={w_pnl_post*100:+.1f}% loss={l_pnl_post*100:+.1f}% "
                                f"misses targets (win>={Y*100:+.0f}% loss>={-X*100:+.0f}%) — waiting for cheaper loser", "WARNING")
                        else:
                            _gate = "STRICT" if strict_ok else ("RELAXED" if relaxed_ok else "CAPPED-LOSS")
                            self._log(
                                f"FJ {_gate}: {chosen_Nw:.0f} {winner_side_label} @ ${wp:.4f} = ${chosen_Nw*wp:.2f}"
                                f"{f' + {chosen_Ny:.0f} {loser_side_label} @ ${lp:.4f} = ${chosen_Ny*lp:.2f}' if chosen_Ny > 0 else ''}"
                                f" | budget ${remaining_budget:.2f} | winner-wins: {w_pnl_post*100:+.1f}% loser-wins: {l_pnl_post*100:+.1f}%", "SIGNAL")
                    else:
                        skip_fj = True
                        self._log(
                            f"FINAL JUSTIFICATION SKIP: no affordable solution "
                            f"(Nw={chosen_Nw} Ny={chosen_Ny} budget=${remaining_budget:.0f} denom_w={denom_w:.4f})", "WARNING")

                    # V3.13: Orderbook depth pre-check — verify market can fill FJ tokens
                    if not skip_fj:
                        Ny, Nw = chosen_Ny, chosen_Nw
                        winner_book = yes_book if winner_side_label == "YES" else no_book
                        loser_book = no_book if winner_side_label == "YES" else yes_book
                        real_cost = 0.0
                        depth_ok = True
                        if Nw > 0:
                            w_vwap, w_avail = self._calc_ask_vwap(winner_book, Nw)
                            if w_vwap is None or w_avail < Nw * 0.5:
                                depth_ok = False
                            else:
                                real_cost += w_vwap * min(Nw, w_avail)
                        if Ny > 0:
                            l_vwap, l_avail = self._calc_ask_vwap(loser_book, Ny)
                            if l_vwap is None or l_avail < Ny * 0.5:
                                depth_ok = False
                            else:
                                real_cost += l_vwap * min(Ny, l_avail)
                        if not depth_ok or real_cost > remaining_budget:
                            skip_fj = True
                            self._log(
                                f"FINAL JUSTIFICATION SKIP: orderbook depth insufficient or "
                                f"cost ${real_cost:.0f} > budget ${remaining_budget:.0f}", "WARNING")

                # V3.88: Execute FJ — buy loser insurance first (cheap), then winner top-up
                if not skip_fj:
                    Ny, Nw = chosen_Ny, chosen_Nw
                    self._log(
                        f"FINAL JUSTIFICATION: buy {Ny:.0f} {loser_side_label} @ ${lp:.4f} = ${Ny*lp:.2f}"
                        f"{f' + {Nw:.0f} {winner_side_label} @ ${wp:.4f} = ${Nw*wp:.2f}' if Nw > 0 else ''} | "
                        f"winner-wins: {w_pnl_post*100:+.1f}% | loser-wins: {l_pnl_post*100:+.1f}%", "SIGNAL")

                    actual_ny = 0
                    actual_nw = 0
                    # Buy loser FIRST (cheap insurance priority)
                    if Ny >= POLYMARKET_MIN_TOKENS:
                        actual_ny = await self.execute_rebalance_buy(
                            position, loser_side_label, Ny,
                            yes_token, no_token, yes_book, no_book, market_config,
                            skip_budget_cap=True)
                    if Nw >= POLYMARKET_MIN_TOKENS:
                        actual_nw = await self.execute_rebalance_buy(
                            position, winner_side_label, Nw,
                            yes_token, no_token, yes_book, no_book, market_config,
                            skip_budget_cap=True)

                    if actual_ny > 0 or actual_nw > 0:
                        position.final_justification_done = True  # One-shot: don't fire again (DCA still continues)
                        parts = []
                        if actual_ny > 0:
                            parts.append(f"{actual_ny:.0f} {loser_side_label} insurance")
                        if actual_nw > 0:
                            parts.append(f"{actual_nw:.0f} {winner_side_label} top-up")
                        self._log(
                            f"FINAL JUSTIFICATION DONE: bought {' + '.join(parts)}, "
                            f"DCA continues (winner≥{w_pnl_post*100:+.1f}% loser≥{l_pnl_post*100:+.1f}%)", "SUCCESS")
                    else:
                        self._log(
                            f"FINAL JUSTIFICATION SKIP: token amounts below minimum or fill failed "
                            f"(Ny={Ny:.0f} Nw={Nw:.0f}, min={POLYMARKET_MIN_TOKENS})", "WARNING")

    # ----------------- Trading -----------------
    async def dca_buy(self, market_config: MarketConfig, condition_id: str, token_id: str, side: str, book: dict, market_obj: dict, is_opposite: bool = False, usd_amount: float = 0, skip_budget_cap: bool = False):
        """
        Buy tokens on a side.
        - usd_amount > 0: DCA-to-winner mode — calculate tokens = usd_amount / ask_price.
        - usd_amount = 0: legacy chunk-based sizing (DCA_CHUNK_TOKENS / dynamic chunks).
        - is_opposite=True: hedge/dual-side buy — always taker at best_ask.
        - skip_budget_cap=True: rebalance rebuy — skip per-side budget cap (recycling sell proceeds).
        """
        MIN_ORDER_USD = 1.0

        best_bid_level, best_ask_level = self.get_best_levels(book)
        best_bid = float(best_bid_level["price"]) if best_bid_level else 0.0
        best_ask = float(best_ask_level["price"]) if best_ask_level else 1.0
        ask_size = float(best_ask_level.get("size", 0)) if best_ask_level else 0.0

        # DCA-to-winner + opposite buys: always taker at best ask
        # V3.70: Enforce buy band ceiling — trailer side gets wider ceiling (TRAILER_BAND_HIGH)
        _ceiling = TRAILER_BAND_HIGH if is_opposite else BUY_BAND_HIGH
        if (usd_amount > 0 or is_opposite) and best_ask > _ceiling:
            self._log(f"DCA SKIP: {side} ask ${best_ask:.4f} > band ceiling ${_ceiling:.2f}", "WARNING")
            return
        if usd_amount > 0 or is_opposite:
            target_take = True
            intended_price = best_ask
        else:
            # Legacy band-based logic (fallback, shouldn't be reached in new flow)
            if best_ask > BUY_BAND_HIGH:
                logger.debug(f"[SKIP] Market settling: ask ${best_ask:.4f} > ENTRY_MAX ${BUY_BAND_HIGH:.2f}")
                return
            target_take = best_ask <= BUY_BAND_HIGH
            intended_price = min(BUY_BAND_HIGH, BUY_BAND_HIGH)

        # Dynamic chunk sizing
        existing = self.state.positions.get(condition_id)
        current_tokens = existing.get_tokens(side) if existing else 0

        if existing and not self.dry_run:
            self._sync_positions_from_api()
            existing = self.state.positions.get(condition_id)
            current_tokens = existing.get_tokens(side) if existing else 0

        pending_tokens = self.state.pending_buy_tokens.get(condition_id, 0)
        effective_tokens = current_tokens + pending_tokens

        if effective_tokens >= MAX_POSITION_TOKENS:
            logger.info(f"[DCA SKIP] At max position: {current_tokens:.0f}+{pending_tokens:.0f} pending/{MAX_POSITION_TOKENS:.0f} tokens")
            return

        # Calculate buy_tokens: USD-override or legacy chunk
        if usd_amount > 0 and best_ask > 0:
            buy_tokens = self._round_tokens(usd_amount / best_ask)
        else:
            buy_tokens = self.calculate_chunk_size(book, effective_tokens, MAX_POSITION_TOKENS)

        if buy_tokens <= 0:
            logger.info(f"[DCA SKIP] Chunk is 0 (position: {effective_tokens:.0f}/{MAX_POSITION_TOKENS:.0f})")
            return

        notional_price = best_ask if target_take else intended_price

        buy_tokens, buy_usd = self._enforce_order_minimums(buy_tokens, notional_price)

        remaining_room = MAX_POSITION_TOKENS - effective_tokens
        if buy_tokens > remaining_room:
            if remaining_room < POLYMARKET_MIN_TOKENS:
                logger.info(f"[DCA SKIP] Remaining room {remaining_room:.0f} < minimum {POLYMARKET_MIN_TOKENS}")
                return
            buy_tokens = self._round_tokens(remaining_room)
            buy_usd = buy_tokens * notional_price

        existing = self.state.positions.get(condition_id)
        if existing and not skip_budget_cap:
            # V3.10: Per-side budget cap — pass target side for independent per-side limits
            buy_tokens, buy_usd = self._cap_buy_to_budget(existing, buy_tokens, notional_price, target_side=side)
            if buy_tokens <= 0:
                side_cost = existing.get_cost(side)
                logger.info(f"[DCA SKIP] {side} budget exhausted: spent=${side_cost:.2f}/{MAX_POSITION_COST_USD:.0f}")
                return

        if existing and (time.time() - existing.last_action_ts < MARKET_ACTION_COOLDOWN_SEC):
            # Skip cooldown for entry opposite-side buys (both sides must buy together)
            if not (is_opposite and usd_amount > 0):
                return

        # V2.18: Check TIER3 reentry cooldown for NEW entries (no existing position)
        if not existing and TIER3_REENTRY_COOLDOWN_SEC > 0:
            last_tier3_ts = self.state.last_tier3_close_ts.get(condition_id, 0)
            elapsed = time.time() - last_tier3_ts
            if elapsed < TIER3_REENTRY_COOLDOWN_SEC:
                remaining = TIER3_REENTRY_COOLDOWN_SEC - elapsed
                logger.info(f"[TIER3 COOLDOWN] Skipping entry - {remaining:.0f}s remaining after TIER3 close")
                return

        # ------------ DRY RUN ------------
        if self.dry_run:
            exec_price = best_ask if target_take else intended_price
            action = "TAKER BUY" if target_take else "MAKER BID"
            self._log(f"[{self._market_tag(market_config.slug_pattern)}] {action} {side} {buy_tokens:.2f} @ ${exec_price:.4f} (${buy_tokens*exec_price:.2f})", "ENTRY" if target_take else "INFO")

            # simulate immediate fill (good enough for backtesting / UI)
            self.current_balance -= buy_tokens * exec_price

            # V3: Log chunk to MongoDB
            chunk_id = self._generate_chunk_id()
            if self.mongo:
                self.mongo.log_chunk_entry(
                    chunk_id=chunk_id,
                    session_id=self.session_id,
                    market_slug=market_config.slug_pattern,
                    condition_id=condition_id,
                    side=side,
                    token_id=token_id,
                    entry_price=exec_price,
                    entry_tokens=buy_tokens,
                    entry_usd=buy_tokens * exec_price,
                )

            if condition_id in self.state.positions:
                pos = self.state.positions[condition_id]
                self._update_position_after_buy(pos, side, token_id, buy_tokens, exec_price)
                # CSV: DCA_BUY
                self._log_trade_to_csv("DCA_BUY", market_config.slug_pattern, condition_id, side, buy_tokens, exec_price, notes=f"chunk#{pos.chunks_bought}")
            else:
                # V3.80: Create position with YES/NO fields directly
                total_balance = self.current_balance + sum(
                    (p.yes_tokens * p.yes_entry_price + p.no_tokens * p.no_entry_price)
                    for p in self.state.positions.values())
                allocated_capital = total_balance * MAX_CAPITAL_PER_MARKET_PCT

                entry_cost = buy_tokens * exec_price
                new_pos = Position(
                    market_slug=market_config.slug_pattern,
                    condition_id=condition_id,
                    entry_time=time.time(),
                    last_action_ts=time.time(),
                    session_allocated_capital=allocated_capital,
                    session_total_cost=entry_cost,
                )
                new_pos.set_token_id(side, token_id)
                new_pos.set_tokens(side, buy_tokens)
                new_pos.set_entry_price(side, exec_price)
                new_pos.set_price(side, exec_price)
                if side == "YES":
                    new_pos.yes_cost = entry_cost
                else:
                    new_pos.no_cost = entry_cost
                self.state.positions[condition_id] = new_pos
                # V2.21: Log position tracking
                self._log(f"[POS+] Added {market_config.slug_pattern} {side} | Total positions: {len(self.state.positions)}", "DEBUG")
                self._update_candle(self.get_current_window_ts(), "ENTERED", side, 0, f"BUY @ ${exec_price:.4f}")

                # Log to MongoDB
                self._log_position_entry(condition_id, market_config, side, token_id, exec_price, buy_tokens, "DRY_RUN")
                # CSV: ENTRY
                self._log_trade_to_csv("ENTRY", market_config.slug_pattern, condition_id, side, buy_tokens, exec_price, notes="initial")
            # Don't return here - let hedge execute in check_entry()
            return

        # ------------ LIVE ------------
        if not self._init_trader():
            return
        if self._live_trading_blocked_reason:
            self._log(f"LIVE TRADING BLOCKED: {self._live_trading_blocked_reason}", "ERROR")
            return

        # Get market info for neg_risk and tick_size
        try:
            slug = f"{market_config.slug_pattern}-{self.get_current_window_ts()}"
            m = self.trader.get_market_by_slug(slug)
            neg_risk = bool(getattr(m, "neg_risk", False))
            tick_size = float(getattr(m, "min_tick_size", 0.01)) or 0.01
        except Exception:
            neg_risk, tick_size = False, 0.01

        # If taking, ensure some liquidity exists at ask
        # Skip top-of-book cap when usd_amount is pre-determined (entry/DCA buys)
        # — buy_by_amount_usdc fills across multiple price levels automatically
        if target_take and ask_size > 0 and usd_amount <= 0:
            buy_tokens = min(buy_tokens, ask_size)
            buy_tokens = self._round_tokens(buy_tokens)
            if buy_tokens * best_ask < MIN_ORDER_USD:
                target_take = False  # too small at ask; go maker

        try:
            from py_clob_client.clob_types import OrderType  # type: ignore
        except Exception:
            OrderType = None  # fallback

        # V2.17: Add to pending orders before attempting buy
        # This prevents concurrent scan cycles from over-buying
        self.state.pending_buy_tokens[condition_id] = self.state.pending_buy_tokens.get(condition_id, 0) + buy_tokens
        self.state.pending_buy_timestamps[condition_id] = time.time()  # Track when pending started
        logger.info(f"[PENDING] Added {buy_tokens:.0f} tokens to pending for {condition_id[:16]}... (total pending: {self.state.pending_buy_tokens[condition_id]:.0f})")

        total_filled_tokens = 0  # V2.18: Track total tokens filled across all attempts
        total_filled_usd = 0     # V2.18: Track total USD spent across all attempts

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # V2.18: Re-check position limit before each retry (in case partial fills accumulated)
                pos = self.state.positions.get(condition_id)
                current_tokens = pos.get_tokens(side) if pos else 0
                if current_tokens + total_filled_tokens >= MAX_POSITION_TOKENS:
                    logger.info(f"[RETRY STOP] Position limit reached during retries: {current_tokens:.0f} + {total_filled_tokens:.0f} filled >= {MAX_POSITION_TOKENS:.0f}")
                    # Clear pending and exit
                    self.state.pending_buy_tokens[condition_id] = max(0, self.state.pending_buy_tokens.get(condition_id, 0) - buy_tokens)
                    if self.state.pending_buy_tokens.get(condition_id, 0) <= 0:
                        self.state.pending_buy_timestamps.pop(condition_id, None)
                    return

                self.trader.refresh_api_creds()

                if target_take:
                    # V2.9: Round USDC to 2 decimals to avoid floating point precision issues
                    # When usd_amount specified, send full amount (buy_by_amount_usdc fills across levels)
                    amount_usdc = round(usd_amount if usd_amount > 0 else buy_tokens * best_ask, 2)
                    # Enforce Polymarket $1 minimum
                    if amount_usdc < 1.0:
                        amount_usdc = 1.0
                    logger.info(f"[TAKER BUY] {market_config.name} {side} ~{buy_tokens:.2f} @ {best_ask:.4f} (${amount_usdc:.2f}) attempt {attempt}/{MAX_RETRIES}")

                    result = self.trader.buy_by_amount_usdc(
                        token_id=token_id,
                        amount_usdc=amount_usdc,
                        order_type=OrderType.FOK if OrderType else None,
                        neg_risk=neg_risk,
                        tick_size=tick_size,
                        max_retries=1,
                        retry_delay=0,
                        max_fill_price=_ceiling,  # V3.75: Prevent fills above band ceiling (wider for trailer)
                    )
                    limit_price = best_ask
                else:
                    desired_price = intended_price
                    desired_price = math.floor(desired_price / tick_size) * tick_size
                    desired_price = max(desired_price, tick_size)
                    improved = min(desired_price, best_bid + tick_size) if best_bid > 0 else desired_price
                    limit_price = round(min(desired_price, improved), 6)

                    # Enforce buy band: don't place bids below BUY_BAND_LOW
                    if limit_price < BUY_BAND_LOW:
                        logger.debug(f"[SKIP BID] Price ${limit_price:.4f} below band ${BUY_BAND_LOW:.2f}-${BUY_BAND_HIGH:.2f}")
                        return

                    logger.info(f"[MAKER BID] {market_config.name} {side} {buy_tokens:.2f} @ {limit_price:.4f} attempt {attempt}/{MAX_RETRIES}")

                    if hasattr(self.trader, "buy_limit_order"):
                        result = self.trader.buy_limit_order(
                            token_id=token_id,
                            price=float(limit_price),
                            size=float(buy_tokens),
                            neg_risk=neg_risk,
                            tick_size=tick_size,
                            max_retries=1,
                        )
                    else:
                        # Fallback: try GTC if wrapper supports it; otherwise it will fail and we log the error.
                        # V2.9: Round USDC to 2 decimals
                        amount_usdc = round(buy_tokens * limit_price, 2)
                        # Enforce Polymarket $1 minimum
                        if amount_usdc < 1.0:
                            amount_usdc = 1.0
                        gtc = getattr(OrderType, "GTC", None) if OrderType else None
                        result = self.trader.buy_by_amount_usdc(
                            token_id=token_id,
                            amount_usdc=amount_usdc,
                            order_type=gtc,
                            neg_risk=neg_risk,
                            tick_size=tick_size,
                            max_retries=1,
                            retry_delay=0,
                        )

                if result.get("success"):
                    taking_amount = float(result.get("takingAmount", 0) or 0)
                    making_amount = float(result.get("makingAmount", 0) or 0)

                    if taking_amount > 0 and making_amount > 0:
                        actual_price = making_amount / taking_amount
                        actual_tokens = taking_amount
                        filled_note = "FILLED"

                        # V2.4: Safety check - reject fills way below intended price
                        # This protects against filling at extreme prices near market resolution
                        if actual_price < BUY_BAND_LOW:
                            self._log(f"REJECTED FILL: ${actual_price:.4f} < BUY_BAND_LOW ${BUY_BAND_LOW:.2f}", "ERROR")
                            logger.error(f"[REJECT] Fill price ${actual_price:.4f} below band, not recording position")
                            # V2.17: Clear pending on rejected fill
                            self.state.pending_buy_tokens[condition_id] = max(0, self.state.pending_buy_tokens.get(condition_id, 0) - buy_tokens)
                            if self.state.pending_buy_tokens.get(condition_id, 0) <= 0:
                                self.state.pending_buy_timestamps.pop(condition_id, None)
                            return  # Don't record this position
                    else:
                        actual_price = float(limit_price)
                        actual_tokens = float(buy_tokens)
                        filled_note = "PLACED"

                    self._log(f"[{self._market_tag(market_config.slug_pattern)}] {filled_note} BUY {side} {actual_tokens:.2f} @ ${actual_price:.4f}", "ENTRY")

                    # V3: Log chunk to MongoDB
                    chunk_id = self._generate_chunk_id()
                    if self.mongo:
                        self.mongo.log_chunk_entry(
                            chunk_id=chunk_id,
                            session_id=self.session_id,
                            market_slug=market_config.slug_pattern,
                            condition_id=condition_id,
                            side=side,
                            token_id=token_id,
                            entry_price=actual_price,
                            entry_tokens=actual_tokens,
                            entry_usd=actual_tokens * actual_price,
                        )

                    if condition_id in self.state.positions:
                        pos = self.state.positions[condition_id]
                        self._update_position_after_buy(pos, side, token_id, actual_tokens, actual_price)
                    else:
                        # V3.80: Create position with YES/NO fields directly
                        total_balance = self.current_balance + sum(
                            (p.yes_tokens * p.yes_entry_price + p.no_tokens * p.no_entry_price)
                            for p in self.state.positions.values())
                        allocated_capital = total_balance * MAX_CAPITAL_PER_MARKET_PCT

                        entry_cost = actual_tokens * actual_price
                        new_pos = Position(
                            market_slug=market_config.slug_pattern,
                            condition_id=condition_id,
                            entry_time=time.time(),
                            last_action_ts=time.time(),
                            session_allocated_capital=allocated_capital,
                            session_total_cost=entry_cost,
                        )
                        new_pos.set_token_id(side, token_id)
                        new_pos.set_tokens(side, actual_tokens)
                        new_pos.set_entry_price(side, actual_price)
                        new_pos.set_price(side, actual_price)
                        if side == "YES":
                            new_pos.yes_cost = entry_cost
                        else:
                            new_pos.no_cost = entry_cost
                        self.state.positions[condition_id] = new_pos
                        self._update_candle(self.get_current_window_ts(), "ENTERED", side, 0, f"BUY @ ${actual_price:.4f}")

                        # Log to MongoDB
                        self._log_position_entry(condition_id, market_config, side, token_id, actual_price, actual_tokens, "LIVE")

                    # V3.11: Track filled USD and retry if partial fill
                    filled_usd = actual_tokens * actual_price
                    total_filled_tokens += actual_tokens
                    total_filled_usd += filled_usd

                    # Check if we still need to fill more (partial fill retry)
                    if usd_amount > 0:
                        remaining_usd = usd_amount - total_filled_usd
                        if remaining_usd > 1.0:
                            logger.info(f"[PARTIAL FILL RETRY] Filled ${total_filled_usd:.2f} of ${usd_amount:.2f}, ${remaining_usd:.2f} remaining — retrying")
                            usd_amount = remaining_usd  # Update for next iteration
                            buy_tokens = self._round_tokens(remaining_usd / best_ask) if best_ask > 0 else 0
                            await asyncio.sleep(0.5)  # Brief pause before retry
                            continue  # Continue retry loop to fill remainder

                    # V2.17: Clear pending on successful fill
                    self.state.pending_buy_tokens[condition_id] = max(0, self.state.pending_buy_tokens.get(condition_id, 0) - buy_tokens)
                    if self.state.pending_buy_tokens.get(condition_id, 0) <= 0:
                        self.state.pending_buy_timestamps.pop(condition_id, None)
                    logger.info(f"[PENDING] Cleared {buy_tokens:.0f} tokens from pending (remaining: {self.state.pending_buy_tokens.get(condition_id, 0):.0f})")

                    self._refresh_balance_and_allowance_block_if_needed()
                    return

                # V2.18: Check for partial fill even when success=False
                # The wrapper may return success=False when Remaining > 0, but tokens were still bought
                taking_amount = float(result.get("takingAmount", 0) or 0)
                making_amount = float(result.get("makingAmount", 0) or 0)

                if taking_amount > 0 and making_amount > 0:
                    # PARTIAL FILL: Tokens were bought even though success=False
                    actual_price = making_amount / taking_amount
                    actual_tokens = taking_amount
                    total_filled_tokens += actual_tokens
                    total_filled_usd += making_amount

                    logger.warning(f"[PARTIAL FILL] Attempt {attempt}: Got {actual_tokens:.2f} tokens @ ${actual_price:.4f} despite success=False")

                    # Record this partial fill to position
                    if actual_price >= BUY_BAND_LOW:  # Safety check
                        # V3: Log chunk to MongoDB for partial fill
                        chunk_id = self._generate_chunk_id()
                        if self.mongo:
                            self.mongo.log_chunk_entry(
                                chunk_id=chunk_id,
                                session_id=self.session_id,
                                market_slug=market_config.slug_pattern,
                                condition_id=condition_id,
                                side=side,
                                token_id=token_id,
                                entry_price=actual_price,
                                entry_tokens=actual_tokens,
                                entry_usd=making_amount,
                            )

                        if condition_id in self.state.positions:
                            pos = self.state.positions[condition_id]
                            self._update_position_after_buy(pos, side, token_id, actual_tokens, actual_price)
                        else:
                            # V3.80: Create position with YES/NO fields
                            total_balance = self.current_balance + sum(
                                (p.yes_tokens * p.yes_entry_price + p.no_tokens * p.no_entry_price)
                                for p in self.state.positions.values())
                            allocated_capital = total_balance * MAX_CAPITAL_PER_MARKET_PCT
                            new_pos = Position(
                                market_slug=market_config.slug_pattern,
                                condition_id=condition_id,
                                entry_time=time.time(),
                                last_action_ts=time.time(),
                                session_allocated_capital=allocated_capital,
                                session_total_cost=making_amount,
                            )
                            new_pos.set_token_id(side, token_id)
                            new_pos.set_tokens(side, actual_tokens)
                            new_pos.set_entry_price(side, actual_price)
                            new_pos.set_price(side, actual_price)
                            if side == "YES":
                                new_pos.yes_cost = making_amount
                            else:
                                new_pos.no_cost = making_amount
                            self.state.positions[condition_id] = new_pos
                            self._update_candle(self.get_current_window_ts(), "ENTERED", side, 0, f"BUY @ ${actual_price:.4f}")
                            self._log_position_entry(condition_id, market_config, side, token_id, actual_price, actual_tokens, "LIVE")

                        self._log(f"PARTIAL BUY {side} {actual_tokens:.2f} @ ${actual_price:.4f} (total filled: {total_filled_tokens:.2f})", "ENTRY")

                    # V3.11: Check if we need to fill more (partial fill retry)
                    if usd_amount > 0:
                        remaining_usd = usd_amount - total_filled_usd
                        if remaining_usd > 1.0:
                            logger.info(f"[PARTIAL FILL RETRY] Filled ${total_filled_usd:.2f} of ${usd_amount:.2f}, ${remaining_usd:.2f} remaining — retrying")
                            usd_amount = remaining_usd
                            buy_tokens = self._round_tokens(remaining_usd / best_ask) if best_ask > 0 else 0
                            await asyncio.sleep(0.5)
                            continue  # Continue retry loop to fill remainder

                    # Fully filled or no usd_amount target — stop
                    logger.info(f"[STOP RETRY] Partial fill complete, {total_filled_tokens:.2f} tokens filled")
                    # Clear pending and exit
                    self.state.pending_buy_tokens[condition_id] = max(0, self.state.pending_buy_tokens.get(condition_id, 0) - buy_tokens)
                    if self.state.pending_buy_tokens.get(condition_id, 0) <= 0:
                        self.state.pending_buy_timestamps.pop(condition_id, None)
                    self._refresh_balance_and_allowance_block_if_needed()
                    return

                err = result.get("errorMsg", result.get("error", str(result)))
                logger.error(f"[BUY FAILED] Attempt {attempt}: {err}")
                if "allowance" in str(err).lower():
                    self._refresh_balance_and_allowance_block_if_needed()

            except Exception as e:
                logger.error(f"[BUY ERROR] Attempt {attempt}: {e}")
                # V3.16: Wait for API propagation before ghost fill check — immediate sync
                # misses ghost fills because Polymarket API lags 5-20s behind on-chain state
                await asyncio.sleep(V3_GHOST_FILL_COOLDOWN_SEC)
                # V3.45: Ghost fill detection — API may have filled the order despite the exception
                try:
                    pos = self.state.positions.get(condition_id)
                    pre_yes = pos.yes_tokens if pos else 0
                    pre_no = pos.no_tokens if pos else 0
                    self._sync_positions_from_api(force=True)
                    pos = self.state.positions.get(condition_id)
                    if pos:
                        new_yes = pos.yes_tokens
                        new_no = pos.no_tokens
                        ghost_tokens = (new_yes + new_no) - (pre_yes + pre_no)
                        if ghost_tokens > 0.5:
                            self._log(
                                f"GHOST FILL DETECTED: +{ghost_tokens:.0f} tokens on-chain after exception "
                                f"(YES {pre_yes:.0f}→{new_yes:.0f}, NO {pre_no:.0f}→{new_no:.0f})",
                                "WARN")
                            # Update session cost for the ghost-filled tokens (per-side)
                            ghost_yes_delta = max(0, new_yes - pre_yes)
                            ghost_no_delta = max(0, new_no - pre_no)
                            if ghost_yes_delta > 0:
                                self._track_buy_cost(pos, "YES", ghost_yes_delta * best_ask)
                            if ghost_no_delta > 0:
                                self._track_buy_cost(pos, "NO", ghost_no_delta * best_ask)
                            # Clear pending and stop retrying
                            self.state.pending_buy_tokens[condition_id] = max(0, self.state.pending_buy_tokens.get(condition_id, 0) - buy_tokens)
                            if self.state.pending_buy_tokens.get(condition_id, 0) <= 0:
                                self.state.pending_buy_timestamps.pop(condition_id, None)
                            self._refresh_balance_and_allowance_block_if_needed()
                            return
                except Exception as sync_err:
                    logger.warning(f"[GHOST FILL CHECK] Sync failed: {sync_err}")
                # V3.16: Already waited for ghost fill cooldown, no additional retry delay needed
                continue

        # V2.17: Clear pending on all attempts exhausted
        self.state.pending_buy_tokens[condition_id] = max(0, self.state.pending_buy_tokens.get(condition_id, 0) - buy_tokens)
        if self.state.pending_buy_tokens.get(condition_id, 0) <= 0:
            self.state.pending_buy_timestamps.pop(condition_id, None)
        logger.error(f"[BUY FAILED] All {MAX_RETRIES} attempts exhausted (cleared {buy_tokens:.0f} from pending)")

    async def execute_hedge(self, position: Position, opposite_token_id: str, opposite_book: dict, market_config: MarketConfig):
        """Execute hedge position on opposite side.

        V3: Uses break-even profit reconciliation - calculates opposite tokens
        to match main position's max profit, guaranteeing break-even worst case.
        """
        best_bid, best_ask = self.get_best_prices(opposite_book)
        hedge_side = position.loser_side()

        # Validate prices before proceeding
        if not best_ask or best_ask <= 0:
            self._log(f"HEDGE SKIP: Invalid ask price ({best_ask})", "WARN")
            logger.warning(f"[HEDGE SKIP] No valid ask price for hedge: best_ask={best_ask}")
            return

        # V3: No max price limit - always hedge for break-even protection
        # (removed HEDGE_MAX_PRICE check - break-even is more important than price limit)

        # V3.1: Calculate additional hedge tokens needed considering existing hedge
        # Formula for top-up: N2 = [main × (entry - 0.01) - N1 × (0.99 - P1)] / (0.99 - P2)
        # Where N1=existing hedge, P1=existing entry, P2=new ask
        w = position.winner_side()
        existing_hedge = position.get_tokens(hedge_side) if position.get_tokens(hedge_side) > 0 else 0
        existing_entry = position.get_entry_price(hedge_side) if position.get_entry_price(hedge_side) > 0 else best_ask

        # Calculate main's loss at resolution (if hedge wins)
        main_loss_value = position.get_tokens(w) * (position.get_entry_price(w) - self.RESOLVE_LOSE_PRICE)

        # Calculate existing hedge's profit at resolution
        existing_profit = existing_hedge * (self.RESOLVE_WIN_PRICE - existing_entry) if existing_hedge > 0 else 0

        # Shortfall that new tokens need to cover
        shortfall = main_loss_value - existing_profit

        if shortfall <= 0:
            logger.info(f"[HEDGE SKIP] Already at break-even: main_loss=${main_loss_value:.2f}, hedge_profit=${existing_profit:.2f}")
            return

        # Calculate tokens needed at current ask price
        profit_per_token = self.RESOLVE_WIN_PRICE - best_ask
        if profit_per_token <= 0:
            logger.warning(f"[HEDGE SKIP] Cannot hedge: ask ${best_ask:.4f} >= resolve ${self.RESOLVE_WIN_PRICE}")
            return

        breakeven_tokens = shortfall / profit_per_token
        logger.info(f"[HEDGE CALC] Main loss: ${main_loss_value:.2f}, Existing profit: ${existing_profit:.2f}, Shortfall: ${shortfall:.2f}, Need: {breakeven_tokens:.0f} tokens @ ${best_ask:.4f}")

        # Apply HEDGE_RATIO as a scaling factor (1.0 = full break-even, 0.5 = half)
        hedge_tokens = breakeven_tokens * HEDGE_RATIO
        hedge_tokens = max(hedge_tokens, HEDGE_MIN_TOKENS)
        hedge_tokens = self._round_tokens(hedge_tokens)

        logger.info(f"[HEDGE PREP] breakeven={breakeven_tokens:.0f}, ratio={HEDGE_RATIO}, final={hedge_tokens:.0f} tokens, ask=${best_ask:.4f}")

        # V3.6: Cap hedge tokens by remaining budget to prevent over-hedging
        max_hedge, current_hedge, remaining_budget = self.get_hedge_budget(position)
        max_tokens_by_budget = remaining_budget / best_ask if best_ask > 0 else 0
        if hedge_tokens > max_tokens_by_budget:
            original_tokens = hedge_tokens
            hedge_tokens = self._round_tokens(max_tokens_by_budget)
            logger.info(f"[HEDGE CAP] Reduced {original_tokens:.0f} → {hedge_tokens:.0f} tokens (budget: ${remaining_budget:.2f}, max_hedge: ${max_hedge:.2f})")

        if hedge_tokens < HEDGE_MIN_TOKENS:
            self._log(f"HEDGE SKIP: After budget cap, {hedge_tokens:.0f} tokens below minimum (budget: ${remaining_budget:.2f})", "WARN")
            return

        # V3: Enforce order minimums (bump to $1 if needed)
        hedge_tokens, hedge_usd = self._enforce_order_minimums(hedge_tokens, best_ask)

        # V3: Cap to position cost budget (per-side: hedge side)
        hedge_tokens, hedge_usd = self._cap_buy_to_budget(position, hedge_tokens, best_ask, target_side=hedge_side)
        if hedge_tokens < POLYMARKET_MIN_TOKENS:
            side_cost = position.get_cost(hedge_side)
            self._log(f"HEDGE SKIP: {hedge_side} budget exhausted (spent=${side_cost:.2f}/{MAX_POSITION_COST_USD:.0f})", "WARN")
            return

        # DRY RUN
        if self.dry_run:
            exec_price = best_ask
            self._log(f"[{self._market_tag(market_config.slug_pattern)}] HEDGE {hedge_side} {hedge_tokens:.2f} @ ${exec_price:.4f} (${hedge_tokens*exec_price:.2f})", "HEDGE")

            self.current_balance -= hedge_tokens * exec_price
            position.set_token_id(hedge_side, opposite_token_id)
            position.set_tokens(hedge_side, hedge_tokens)
            position.set_entry_price(hedge_side, exec_price)
            position.last_hedge_ts = time.time()  # V3: Set hedge cooldown
            self._track_buy_cost(position, hedge_side, hedge_tokens * exec_price)
            # CSV: HEDGE_ENTRY
            self._log_trade_to_csv("HEDGE_ENTRY", market_config.slug_pattern, position.condition_id, hedge_side, hedge_tokens, exec_price, notes=f"main={w}")
            return

        # LIVE
        if not self._init_trader():
            self._log("HEDGE SKIP: Trader not initialized", "WARN")
            return
        if self._live_trading_blocked_reason:
            self._log(f"HEDGE SKIP: {self._live_trading_blocked_reason}", "WARN")
            return

        try:
            # V2.12: Use global POLYMARKET_MIN_TOKENS constant
            min_usdc_for_5_tokens = round(POLYMARKET_MIN_TOKENS * best_ask, 2)
            amount_usdc = round(hedge_tokens * best_ask, 2)
            amount_usdc = max(amount_usdc, min_usdc_for_5_tokens, POLYMARKET_MIN_USD)

            # Recalculate actual tokens that will be bought
            actual_hedge_tokens = amount_usdc / best_ask
            logger.info(f"[HEDGE ATTEMPT] {hedge_side} ${amount_usdc:.2f} (~{actual_hedge_tokens:.0f} tokens @ ${best_ask:.4f}) token_id={opposite_token_id[:16]}...")
            self._log(f"HEDGE ATTEMPT: {hedge_side} ${amount_usdc:.2f} (~{actual_hedge_tokens:.0f} tokens @ ${best_ask:.4f})", "INFO")

            # Use GTC order instead of FOK to avoid "order couldn't be fully filled" errors
            # Hedge is small and can wait in the book if not immediately filled
            try:
                from py_clob_client.clob_types import OrderType
                order_type = OrderType.GTC
            except Exception:
                order_type = None

            result = self.trader.buy_by_amount_usdc(
                token_id=opposite_token_id,
                amount_usdc=amount_usdc,
                order_type=order_type,
                neg_risk=False,
                tick_size=0.01,
                max_retries=MAX_RETRIES,
                retry_delay=RETRY_DELAY,
            )

            logger.info(f"[HEDGE RESULT] raw={result}")

            if result.get("success"):
                taking_amount = float(result.get("takingAmount", 0) or 0)
                making_amount = float(result.get("makingAmount", 0) or 0)

                # V3.4 FIX: Only update local state if order was ACTUALLY filled
                # taking_amount = 0 means GTC order went to book but got no fill
                if taking_amount <= 0:
                    self._log(f"HEDGE ORDER PLACED: {hedge_side} (GTC in book, awaiting fill)", "INFO")
                    position.set_token_id(hedge_side, opposite_token_id)
                    position.last_hedge_ts = time.time()
                    # DON'T update tokens - API sync will pick it up when filled
                else:
                    actual_price = making_amount / taking_amount
                    actual_tokens = taking_amount

                    logger.info(f"[HEDGE SUCCESS] {hedge_side} {actual_tokens:.2f} tokens @ ${actual_price:.4f}")
                    self._log(f"[{self._market_tag(market_config.slug_pattern)}] HEDGE SUCCESS: {hedge_side} {actual_tokens:.2f} @ ${actual_price:.4f}", "HEDGE")
                    position.set_token_id(hedge_side, opposite_token_id)
                    position.last_hedge_ts = time.time()  # V3: Set hedge cooldown
                    self._track_buy_cost(position, hedge_side, making_amount)
                    # V3.4: Sync immediately to get actual filled amount from API
                    self._sync_positions_from_api()
            else:
                err = result.get("errorMsg", result.get("error", str(result)))
                logger.error(f"[HEDGE FAILED] err={err}, full_result={result}")
                self._log(f"HEDGE FAILED: {err}", "ERROR")

        except Exception as e:
            import traceback
            logger.error(f"[HEDGE ERROR] {e}\n{traceback.format_exc()}")
            self._log(f"HEDGE ERROR: {e}", "ERROR")

    async def execute_rebalance_sell_unified(self, position: Position, side: str, sell_tokens: float, pnl_pct: float, book: dict, market_config: MarketConfig, skip_min_price: bool = False) -> bool:
        """V3.4: Unified sell for either main or hedge position. Returns True on success.
        skip_min_price: V3.91 — bypass entry-VWAP floor (for loss cuts that intentionally sell below VWAP)."""
        best_bid, _ = self.get_best_prices(book)

        if not best_bid or best_bid <= 0:
            self._log(f"REBALANCE SKIP: Invalid bid price", "WARN")
            return False

        # V3.31: Skip sell when price >= 0.99 — Polymarket API rejects it (max: 0.99)
        # Position will resolve at $1.00 soon anyway, no need to sell at ceiling
        if best_bid >= 0.99:
            self._log(f"REBALANCE SKIP: Price ${best_bid:.4f} at ceiling (>= $0.99), let it resolve", "WARN")
            return False

        sell_tokens = self._round_tokens(sell_tokens)
        if sell_tokens < POLYMARKET_MIN_TOKENS:
            return False

        # V3.80: side is "YES" or "NO" directly (no more "main"/"hedge")
        # Backward compat: accept "main"/"hedge" and convert
        if side == "main":
            side = position.winner_side()
        elif side == "hedge":
            side = position.loser_side()
        token_id = position.get_token_id(side)
        side_label = side
        entry_price = position.get_entry_price(side)

        expected_pnl = sell_tokens * (best_bid - entry_price)
        pnl_pct_actual = (best_bid - entry_price) / entry_price if entry_price > 0 else 0
        # V3.56: Use actual market PnL for label, not caller's pnl_pct (which can use stale VWAP)
        action = "PROFIT" if pnl_pct_actual > 0 else "LOSS"

        logger.info(f"[REBALANCE {action}] Sell {sell_tokens:.0f} {side_label} @ ${best_bid:.4f} (entry=${entry_price:.4f}, pnl={pnl_pct_actual*100:+.1f}%, ${expected_pnl:+.2f})")

        if self.dry_run:
            self._log(f"[{self._market_tag(market_config.slug_pattern)}] REBALANCE {action}: {side_label} {sell_tokens:.0f} @ ${best_bid:.4f} (${expected_pnl:+.2f})", "SUCCESS")
            # V3.80: Update YES/NO tokens directly
            position.add_tokens(side, -sell_tokens)
            # V3.41: apply taker fee to dry-run sell proceeds for realistic simulation
            dry_proceeds = sell_tokens * best_bid * (1 - V3_TAKER_FEE_PCT)
            self.current_balance += dry_proceeds
            expected_pnl = sell_tokens * (best_bid * (1 - V3_TAKER_FEE_PCT) - entry_price)
            # V3.5 FIX: Update both session_realized_profit AND state.total_pnl for dry_run
            position.session_realized_profit += expected_pnl
            position.session_sell_proceeds += dry_proceeds  # V3.37
            self.state.total_pnl += expected_pnl
            position.last_action_ts = time.time()
            position.last_rebalance_sell_ts = time.time()  # V3.48: Grace period for sync
            # V3.7 FIX: Add trade to dashboard with correct side/entry_price for hedge trades
            self._add_trade_to_dashboard(position, best_bid, pnl_pct_actual, expected_pnl, f"REBALANCE_{action}",
                                         side=side_label, entry_price=entry_price)
            self._log_trade_to_csv(f"REBALANCE_{action}", market_config.slug_pattern, position.condition_id,
                                   side_label, sell_tokens, best_bid, notes=f"pnl=${expected_pnl:.2f}")
            return True

        if not self._init_trader():
            return False
        if self._live_trading_blocked_reason:
            return False

        try:
            # V3.19: Cancel pending buy orders for this token before selling — prevents stale GTC bids
            # from filling after rebalance sell (creating unwanted re-entry)
            try:
                cancelled = await self.sell_processor.run_io(
                    self.trader.clob.cancel_market_orders, asset_id=token_id
                )
                if cancelled:
                    self._log(f"[{self._market_tag(market_config.slug_pattern)}] Cancelled pending orders for {side_label} before rebalance", "WARN")
            except Exception as e:
                logger.warning(f"[REBALANCE] Cancel pending orders failed (non-fatal): {e}")

            # V3.17: Record pre-sell tokens for ghost sell detection
            pre_sell_tokens = position.get_tokens(side)

            # V3.10: sell_position runs in thread pool — other markets can sell concurrently
            result = await self.sell_processor.run_io(
                self.trader.sell_position,
                token_id=token_id,
                size=float(sell_tokens),
                neg_risk=False,
                tick_size=0.01,
                max_retries=MAX_RETRIES,
                min_price=0 if skip_min_price else entry_price,  # V3.91: loss cuts bypass floor; rebalance keeps V3.73 guard
            )

            if result and result.get("success"):
                # V3.9 FIX: For SELL orders, makingAmount = tokens sold, takingAmount = USDC received
                making_amount = float(result.get("makingAmount", 0) or 0)
                taking_amount = float(result.get("takingAmount", 0) or 0)
                # V3.96 FIX: Don't assume sold=sell_tokens when both amounts are 0 (phantom fill)
                if making_amount <= 0 and taking_amount <= 0:
                    self._log(f"REBALANCE SELL PHANTOM: {side_label} API success but amounts=0 — no actual fill", "ERROR")
                    return False
                actual_sold = making_amount if making_amount > 0 else (taking_amount / best_bid if taking_amount > 0 and best_bid > 0 else sell_tokens)
                actual_proceeds = taking_amount if taking_amount > 0 else actual_sold * best_bid

                # V3.58 FIX: Use actual proceeds (takingAmount) for PnL, not trigger price
                actual_fill_price = actual_proceeds / actual_sold if actual_sold > 0 else best_bid
                actual_pnl = actual_proceeds - (actual_sold * entry_price)
                pnl_pct_actual = (actual_fill_price - entry_price) / entry_price if entry_price > 0 else 0

                # Log partial fill if applicable
                if actual_sold < sell_tokens:
                    self._log(f"[{self._market_tag(market_config.slug_pattern)}] REBALANCE {action} PARTIAL: {side_label} {actual_sold:.0f}/{sell_tokens:.0f} @ ${actual_fill_price:.4f} (${actual_pnl:+.2f})", "WARN")
                else:
                    self._log(f"[{self._market_tag(market_config.slug_pattern)}] REBALANCE {action}: {side_label} {actual_sold:.0f} @ ${actual_fill_price:.4f} (${actual_pnl:+.2f})", "SUCCESS")

                # V3.80: Update YES/NO tokens directly
                position.add_tokens(side, -actual_sold)
                self.current_balance += actual_proceeds
                # V3.5 FIX: Update both session_realized_profit AND state.total_pnl
                position.session_realized_profit += actual_pnl
                position.session_sell_proceeds += actual_proceeds  # V3.37
                self.state.total_pnl += actual_pnl
                position.last_action_ts = time.time()
                # V3.7 FIX: Add trade to dashboard with correct side/entry_price for hedge trades
                self._add_trade_to_dashboard(position, actual_fill_price, pnl_pct_actual, actual_pnl, f"REBALANCE_{action}",
                                             side=side_label, entry_price=entry_price)

                self._log_trade_to_csv(f"REBALANCE_{action}", market_config.slug_pattern, position.condition_id,
                                       side_label, actual_sold, actual_fill_price, notes=f"pnl=${actual_pnl:.2f}")
                # V3.48: Set grace period — prevent immediate sync from overwriting size_tokens
                # before Polymarket API propagates the sell (causes double-counting in combined TP)
                position.last_rebalance_sell_ts = time.time()
                return True
            else:
                err = result.get("errorMsg", result.get("error", str(result))) if result else "No result"
                self._log(f"REBALANCE FAILED: {err}", "ERROR")
                # V3.17: Ghost sell detection — timeout may have sold tokens on-chain
                err_lower = str(err).lower()
                if "request exception" in err_lower or "timed out" in err_lower or "timeout" in err_lower:
                    await self._detect_ghost_sell(
                        position, side, side_label, pre_sell_tokens, best_bid, entry_price)
                else:
                    self._sync_positions_from_api()
                return False

        except Exception as e:
            import traceback
            logger.error(f"[REBALANCE ERROR] {e}\n{traceback.format_exc()}")
            self._log(f"REBALANCE ERROR: {e}", "ERROR")
            # V3.17: Ghost sell detection for exceptions too
            err_lower = str(e).lower()
            if "request exception" in err_lower or "timed out" in err_lower or "timeout" in err_lower:
                await self._detect_ghost_sell(
                    position, side, side_label, pre_sell_tokens, best_bid, entry_price)
            return False

    async def _detect_ghost_sell(self, position: Position, side: str, side_label: str,
                                 pre_sell_tokens: float, sell_price: float, entry_price: float):
        """V3.17: Detect ghost sells — timeout may have sold tokens on-chain.
        Waits for API propagation, syncs, and tracks proceeds if tokens decreased."""
        position.last_buy_error_ts = time.time()  # Block combined TP during cooldown
        self._log(f"GHOST SELL GUARD: waiting {V3_GHOST_FILL_COOLDOWN_SEC:.0f}s for API propagation", "WARNING")
        await asyncio.sleep(V3_GHOST_FILL_COOLDOWN_SEC)
        self._sync_positions_from_api(force=True)
        post_sell_tokens = position.get_tokens(side)
        ghost_sold = pre_sell_tokens - post_sell_tokens
        if ghost_sold > 0.5:
            ghost_proceeds = ghost_sold * sell_price
            ghost_pnl = ghost_sold * (sell_price - entry_price)
            position.session_sell_proceeds += ghost_proceeds
            position.session_realized_profit += ghost_pnl
            self.state.total_pnl += ghost_pnl
            # V3.80: Token count already updated by API sync above
            position.rebalance_rebuy_blocked = True  # Block further sells — state changed
            self._log(f"GHOST SELL DETECTED: {side_label} {ghost_sold:.0f} tokens @ ${sell_price:.4f} "
                      f"(proceeds=${ghost_proceeds:.2f} pnl=${ghost_pnl:+.2f})", "WARNING")
        else:
            self._log(f"GHOST SELL CHECK: no token decrease detected (pre={pre_sell_tokens:.0f} post={post_sell_tokens:.0f})", "INFO")

    async def execute_rebalance_buy(self, position: Position, side_to_buy: str, tokens_needed: float,
                                     yes_token_id: str, no_token_id: str, yes_book: dict, no_book: dict,
                                     market_config: MarketConfig, skip_budget_cap: bool = False):
        """V3.2: Buy tokens on a specific side to reach break-even on resolve.

        This is triggered when resolve scenario shows negative PnL on one side.
        Different from hedge (which is opposite side) - this buys the SPECIFIC side needed.
        V3.6: Respects hedge budget cap when buying opposite side.
        """
        # Determine which book and token to use
        if side_to_buy == "YES":
            book = yes_book
            token_id = yes_token_id
        else:
            book = no_book
            token_id = no_token_id

        best_bid, best_ask = self.get_best_prices(book)

        if not best_ask or best_ask <= 0:
            self._log(f"REBALANCE BUY SKIP: Invalid ask price ({best_ask})", "WARN")
            return

        buy_tokens = self._round_tokens(tokens_needed * HEDGE_RATIO)  # Apply ratio
        buy_tokens = max(buy_tokens, HEDGE_MIN_TOKENS)

        # V3.6: If buying opposite side (hedge), apply budget cap
        # V3.57: FJ (skip_budget_cap=True) bypasses hedge budget — insurance must fire
        is_buying_hedge = side_to_buy != position.winner_side()
        if is_buying_hedge and not skip_budget_cap:
            max_hedge, current_hedge, remaining_budget = self.get_hedge_budget(position)
            max_tokens_by_budget = remaining_budget / best_ask if best_ask > 0 else 0

            if remaining_budget < POLYMARKET_MIN_USD:
                self._log(f"REBALANCE BUY SKIP: Hedge budget exhausted (${current_hedge:.2f} / ${max_hedge:.2f})", "WARN")
                return False

            if buy_tokens > max_tokens_by_budget:
                original_tokens = buy_tokens
                buy_tokens = self._round_tokens(max_tokens_by_budget)
                logger.info(f"[REBALANCE BUY CAP] Reduced {original_tokens:.0f} → {buy_tokens:.0f} tokens (budget: ${remaining_budget:.2f})")

            if buy_tokens < HEDGE_MIN_TOKENS:
                self._log(f"REBALANCE BUY SKIP: After budget cap, {buy_tokens:.0f} tokens below minimum", "WARN")
                return 0

        buy_tokens, buy_usd = self._enforce_order_minimums(buy_tokens, best_ask)

        # V3: Cap to position cost budget (skip for Final Justification — must buy full amount)
        if not skip_budget_cap:
            buy_tokens, buy_usd = self._cap_buy_to_budget(position, buy_tokens, best_ask, target_side=side_to_buy)
            if buy_tokens < POLYMARKET_MIN_TOKENS:
                side_cost = position.get_cost(side_to_buy)
                self._log(f"REBALANCE BUY SKIP: {side_to_buy} budget exhausted (spent=${side_cost:.2f}/{MAX_POSITION_COST_USD:.0f})", "WARN")
                return 0

        logger.info(f"[REBALANCE BUY] {side_to_buy} {buy_tokens:.0f} @ ${best_ask:.4f} (${buy_usd:.2f}) to fix resolve scenario")

        # DRY RUN
        if self.dry_run:
            exec_price = best_ask
            self._log(f"[{self._market_tag(market_config.slug_pattern)}] REBALANCE BUY: {side_to_buy} {buy_tokens:.0f} @ ${exec_price:.4f} (${buy_tokens*exec_price:.2f})", "HEDGE")

            self.current_balance -= buy_tokens * exec_price

            # Update position based on which side we're buying
            old_tokens = position.get_tokens(side_to_buy)
            if old_tokens > 0:
                old_cost = old_tokens * position.get_entry_price(side_to_buy)
                new_cost = buy_tokens * exec_price
                position.set_tokens(side_to_buy, old_tokens + buy_tokens)
                position.set_entry_price(side_to_buy, (old_cost + new_cost) / (old_tokens + buy_tokens))
            else:
                position.set_token_id(side_to_buy, token_id)
                position.set_tokens(side_to_buy, buy_tokens)
                position.set_entry_price(side_to_buy, exec_price)

            position.last_hedge_ts = time.time()
            self._track_buy_cost(position, side_to_buy, buy_tokens * exec_price)

            self._log_trade_to_csv("REBALANCE_BUY", market_config.slug_pattern, position.condition_id,
                                   side_to_buy, buy_tokens, exec_price, notes="breakeven_fix")
            return buy_tokens  # Return actual count for caller verification

        # LIVE
        if not self._init_trader():
            self._log("REBALANCE BUY SKIP: Trader not initialized", "WARN")
            return 0
        if self._live_trading_blocked_reason:
            self._log(f"REBALANCE BUY SKIP: {self._live_trading_blocked_reason}", "WARN")
            return 0

        # Price band guard — don't buy outside safe range
        # V3.91: FJ (skip_budget_cap=True) bypasses band guard — insurance must buy winner at any price
        if not skip_budget_cap and (best_ask < BUY_BAND_LOW or best_ask > BUY_BAND_HIGH):
            self._log(f"REBALANCE BUY SKIP: price ${best_ask:.4f} outside band ${BUY_BAND_LOW}-${BUY_BAND_HIGH}", "WARNING")
            return 0

        amount_usdc = round(buy_tokens * best_ask, 2)
        amount_usdc = max(amount_usdc, POLYMARKET_MIN_USD)

        try:
            from py_clob_client.clob_types import OrderType
            order_type = OrderType.GTC
        except Exception:
            order_type = None

        # V3.63: Retry loop for timeout errors (FJ/rebalance buys are time-critical)
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"[REBALANCE BUY ATTEMPT] {side_to_buy} ${amount_usdc:.2f} (~{buy_tokens:.0f} tokens @ ${best_ask:.4f}) token_id={token_id[:16]}... attempt {attempt}/{max_attempts}")
                self._log(f"REBALANCE BUY ATTEMPT: {side_to_buy} ${amount_usdc:.2f} (~{buy_tokens:.0f} tokens @ ${best_ask:.4f})", "INFO")

                self.trader.refresh_api_creds()
                result = self.trader.buy_by_amount_usdc(
                    token_id=token_id,
                    amount_usdc=amount_usdc,
                    order_type=order_type,
                    neg_risk=False,
                    tick_size=0.01,
                )

                if result and result.get("success", False):
                    exec_data = result.get("executions", [{}])[0] if result.get("executions") else {}
                    actual_tokens = float(exec_data.get("matchedAmount", buy_tokens))
                    actual_price = float(exec_data.get("matchPrice", best_ask))

                    logger.info(f"[REBALANCE BUY SUCCESS] {side_to_buy} {actual_tokens:.0f} @ ${actual_price:.4f}")
                    self._log(f"[{self._market_tag(market_config.slug_pattern)}] REBALANCE BUY SUCCESS: {side_to_buy} {actual_tokens:.0f} @ ${actual_price:.4f}", "SUCCESS")

                    self.current_balance -= actual_tokens * actual_price

                    old_tokens = position.get_tokens(side_to_buy)
                    if old_tokens > 0:
                        old_cost = old_tokens * position.get_entry_price(side_to_buy)
                        new_cost = actual_tokens * actual_price
                        position.set_tokens(side_to_buy, old_tokens + actual_tokens)
                        position.set_entry_price(side_to_buy, (old_cost + new_cost) / (old_tokens + actual_tokens))
                    else:
                        position.set_token_id(side_to_buy, token_id)
                        position.set_tokens(side_to_buy, actual_tokens)
                        position.set_entry_price(side_to_buy, actual_price)

                    position.last_hedge_ts = time.time()
                    self._track_buy_cost(position, side_to_buy, actual_tokens * actual_price)

                    self._log_trade_to_csv("REBALANCE_BUY", market_config.slug_pattern, position.condition_id,
                                           side_to_buy, actual_tokens, actual_price, notes="breakeven_fix")
                    self._sync_positions_from_api()
                    return actual_tokens  # Return actual count for caller verification
                else:
                    err = result.get("errorMsg", result.get("error", str(result))) if result else "No result"
                    logger.error(f"[REBALANCE BUY FAILED] Attempt {attempt}: {err}")
                    if "allowance" in str(err).lower():
                        self._refresh_balance_and_allowance_block_if_needed()
                    continue  # retry

            except Exception as e:
                import traceback
                logger.error(f"[REBALANCE BUY ERROR] Attempt {attempt}: {e}\n{traceback.format_exc()}")
                self._log(f"REBALANCE BUY ERROR attempt {attempt}/{max_attempts}: {e}", "ERROR")
                # V3.63: Ghost fill detection — wait for API propagation then check on-chain
                pre_yes = position.yes_tokens
                pre_no = position.no_tokens
                await asyncio.sleep(V3_GHOST_FILL_COOLDOWN_SEC)
                self._sync_positions_from_api(force=True)
                ghost_yes = max(0, position.yes_tokens - pre_yes)
                ghost_no = max(0, position.no_tokens - pre_no)
                ghost_tokens = ghost_yes + ghost_no
                if ghost_tokens > 0.5:
                    self._log(
                        f"GHOST FILL DETECTED: +{ghost_tokens:.0f} tokens on-chain after timeout "
                        f"(YES {pre_yes:.0f}→{position.yes_tokens:.0f}, NO {pre_no:.0f}→{position.no_tokens:.0f})",
                        "WARN")
                    if ghost_yes > 0:
                        self._track_buy_cost(position, "YES", ghost_yes * best_ask)
                    if ghost_no > 0:
                        self._track_buy_cost(position, "NO", ghost_no * best_ask)
                    return ghost_tokens
                # No ghost fill — track unconfirmed cost and retry
                position.unconfirmed_buy_cost += amount_usdc
                position.last_buy_error_ts = time.time()
                self._log(f"GHOST FILL GUARD: +${amount_usdc:.2f} unconfirmed cost "
                          f"(total=${position.unconfirmed_buy_cost:.2f})", "WARNING")
                continue  # retry next attempt

        logger.error(f"[REBALANCE BUY FAILED] All {max_attempts} attempts exhausted for {side_to_buy}")
        self._log(f"REBALANCE BUY FAILED: all {max_attempts} attempts exhausted", "ERROR")
        return 0

    # V3.3: Removed execute_profit_cycle (was V3.2)
    # Now using only DCA sell via should_dca_sell (DCA_EXIT_MIN_PROFIT_PCT)

    # V3.80: Removed _close_main_keep_hedge and _close_hedge_only (dead code)

    async def dca_sell(self, position: Position):
        sell_side = position.winner_side()
        sell_amount = min(DCA_CHUNK_TOKENS, position.get_avail(sell_side))
        sell_amount = self._round_tokens(sell_amount)
        if sell_amount < 1.0:
            return

        token_id = position.get_token_id(sell_side)
        sell_ep = position.get_entry_price(sell_side)
        sell_price = position.get_price(sell_side)

        # DRY-RUN — V3.41: use mid-price with taker fee for realistic simulation
        if self.dry_run:
            exit_price = sell_price
            pnl_pct = (exit_price - sell_ep) / sell_ep if sell_ep > 0 else 0
            pnl_usd = sell_amount * (exit_price * (1 - V3_TAKER_FEE_PCT) - sell_ep)

            self._log(f"[{self._market_tag(position.market_slug)}] DCA SELL {sell_side} {sell_amount:.0f} @ ${exit_price:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", "EXIT")
            dca_sell_proceeds = sell_amount * exit_price * (1 - V3_TAKER_FEE_PCT)
            self.current_balance += dca_sell_proceeds

            position.add_sold_tokens(sell_side, sell_amount)
            position.chunks_sold += 1
            self.state.total_pnl += pnl_usd
            position.session_realized_profit += pnl_usd  # V2.3
            position.session_sell_proceeds += dca_sell_proceeds  # V3.37
            self._add_trade_to_dashboard(position, exit_price, pnl_pct, pnl_usd, "DCA_SELL", side=sell_side, entry_price=sell_ep)
            # CSV: DCA_SELL
            self._log_trade_to_csv("DCA_SELL", position.market_slug, position.condition_id, sell_side, sell_amount, exit_price, pnl_pct, pnl_usd, notes=f"chunk#{position.chunks_sold}")
            position.last_action_ts = time.time()
            return

        if not self.trader or self._live_trading_blocked_reason:
            return

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # V3.10: All blocking I/O runs in thread pool for parallel execution across markets
                rpc_balance = await self.sell_processor.run_io(verify_position_balance, self.trader.trading_address, token_id)
                actual_sell = min(sell_amount, rpc_balance) if (rpc_balance is not None and rpc_balance > 0.001) else sell_amount

                await self.sell_processor.run_io(self.trader.refresh_api_creds)

                # V3.1: Re-fetch orderbook on EACH attempt to get fresh price
                orderbook = await self.sell_processor.run_io(self.trader.clob.get_order_book, token_id)
                if not orderbook or not orderbook.bids:
                    logger.warning(f"[DCA SELL] Attempt {attempt}: No bids - skipping")
                    return

                best_bid = max(orderbook.bids, key=lambda x: float(x.price))
                best_price = float(best_bid.price)
                available_size = float(best_bid.size)

                if best_price >= 0.99 or best_price <= 0.01:
                    logger.warning("Market resolved - leaving for redemption")
                    return

                # V3.1: Re-calculate PnL with fresh price BEFORE placing order
                pnl_pct_expected = (best_price - sell_ep) / sell_ep if sell_ep > 0 else 0
                pnl_usd_expected = actual_sell * (best_price - sell_ep)

                # V3.1: Skip if fresh price would result in negative PnL (price dropped since should_dca_sell check)
                if pnl_pct_expected < 0:
                    logger.warning(f"[DCA SELL] Attempt {attempt}: Skip - price dropped! Entry=${sell_ep:.4f} Bid=${best_price:.4f} PnL={pnl_pct_expected:+.1%}")
                    # Wait and retry - price might recover
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    else:
                        logger.warning(f"[DCA SELL] All {MAX_RETRIES} attempts failed due to negative PnL")
                        return

                actual_sell = min(actual_sell, available_size)
                actual_sell = self._round_tokens(actual_sell)
                if actual_sell < 0.1:
                    logger.warning("Dust - skipping")
                    return

                logger.info(f"[DCA SELL] Attempt {attempt}: {actual_sell:.0f} tokens @ ${best_price:.4f} (expected PnL: {pnl_pct_expected:+.1%})")

                # V3.10: sell_position runs in thread pool — other markets can sell concurrently
                result = await self.sell_processor.run_io(
                    self.trader.sell_position,
                    token_id=token_id,
                    size=float(actual_sell),
                    neg_risk=False,
                    tick_size=0.01,
                    max_retries=1,
                )

                if result.get("success"):
                    # V3.9 FIX: For SELL orders, makingAmount = tokens sold, takingAmount = USDC received
                    making_amount = float(result.get("makingAmount", 0) or 0)
                    taking_amount = float(result.get("takingAmount", 0) or 0)
                    tokens_sold = making_amount if making_amount > 0 else (taking_amount / best_price if taking_amount > 0 and best_price > 0 else actual_sell)

                    # Calculate PnL based on ACTUAL sold amount
                    pnl_pct = pnl_pct_expected
                    pnl_usd = tokens_sold * (best_price - sell_ep)

                    # Log partial fill if applicable
                    if tokens_sold < actual_sell:
                        self._log(f"[{self._market_tag(position.market_slug)}] DCA SELL PARTIAL {sell_side} {tokens_sold:.0f}/{actual_sell:.0f} @ ${best_price:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", "WARN")
                    else:
                        self._log(f"[{self._market_tag(position.market_slug)}] DCA SELL {sell_side} {tokens_sold:.0f} @ ${best_price:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", "EXIT")

                    # Update with ACTUAL sold amount
                    live_proceeds = tokens_sold * best_price
                    position.add_sold_tokens(sell_side, tokens_sold)
                    position.chunks_sold += 1
                    self.state.total_pnl += pnl_usd
                    position.session_realized_profit += pnl_usd  # V2.3
                    position.session_sell_proceeds += live_proceeds  # V3.37
                    self._add_trade_to_dashboard(position, best_price, pnl_pct, pnl_usd, "DCA_SELL")
                    position.last_action_ts = time.time()

                    self._refresh_balance_and_allowance_block_if_needed()
                    # V3.5: Sync from API to get accurate remaining position after partial fill
                    self._sync_positions_from_api()
                    return

                err = result.get("errorMsg", result.get("error", str(result)))
                self._log(f"DCA SELL FAILED: attempt {attempt}/{MAX_RETRIES}: {err}", "ERROR")
                self._sync_positions_from_api()  # V2.8: Sync after failed sell

            except Exception as e:
                logger.error(f"[DCA SELL ERROR] Attempt {attempt}: {e}")
                self._sync_positions_from_api()  # V2.8: Sync on error

            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)

    async def close_position(self, position: Position, reason: str):
        close_side = position.winner_side()
        opp_side = position.loser_side()
        token_id = position.get_token_id(close_side)
        sell_size = position.get_avail(close_side)
        sell_size = self._round_tokens(sell_size)
        exit_price = position.get_price(close_side)
        close_ep = position.get_entry_price(close_side)

        # V2.20 FIX: Skip if no tokens remaining (already sold via DCA_SELL)
        if sell_size <= 0:
            # SESSION_TP: Main done but hedge may remain — try to close hedge before popping
            hedge_remaining = position.get_avail(opp_side)
            opp_ep = position.get_entry_price(opp_side)
            opp_tid = position.get_token_id(opp_side)
            if reason == "SESSION_TP" and hedge_remaining > 0.1 and opp_tid and not self.dry_run and self.trader:
                self._log(f"CLOSE: Main done, closing hedge {opp_side} {hedge_remaining:.0f} tokens", "INFO")
                try:
                    # V3.10: sell in thread pool for parallel execution
                    hedge_result = await self.sell_processor.run_io(
                        self.trader.sell_position,
                        token_id=opp_tid, size=float(hedge_remaining),
                        neg_risk=False, tick_size=0.01, max_retries=1,
                    )
                    if hedge_result.get("success"):
                        # V3.9 FIX: makingAmount = tokens sold, takingAmount = USDC received
                        h_making = float(hedge_result.get("makingAmount", 0) or 0)
                        h_taking = float(hedge_result.get("takingAmount", 0) or 0)
                        hedge_sold = h_making if h_making > 0 else (h_taking / opp_ep if h_taking > 0 and opp_ep > 0 else hedge_remaining)
                        position.add_tokens(opp_side, -hedge_sold)
                        position.add_sold_tokens(opp_side, hedge_sold)
                        if hedge_sold < hedge_remaining:
                            self._log(f"HEDGE PARTIAL {opp_side} {hedge_sold:.0f}/{hedge_remaining:.0f}", "WARN")
                            return  # Don't pop — outer retry loop will retry hedge
                        self._log(f"HEDGE CLOSE {opp_side} {hedge_sold:.0f}", "HEDGE")
                    else:
                        self._log(f"HEDGE CLOSE FAIL: {hedge_result.get('error', 'unknown')}", "ERROR")
                        return  # Don't pop — outer retry loop will retry hedge
                except Exception as e:
                    logger.error(f"[HEDGE CLOSE ERROR] {e}")
                    return  # Don't pop — outer retry loop will retry hedge
            self._log(f"CLOSE SKIP: No main tokens remaining - removing position", "WARN")
            self._log(f"[POS-] Removed {position.market_slug} (empty) | Remaining: {len(self.state.positions) - 1}", "DEBUG")
            self.state.positions.pop(position.condition_id, None)
            return

        # V3.29: COMBINED_TP/SESSION_TP — sell both sides, winning first
        # Treats main and hedge as equal; whichever has PnL > 0 or price > 0.51 sells first
        _close_all_reasons = ("COMBINED_TP", "SESSION_TP")
        if reason in _close_all_reasons:
            # V3.80: Build sides from YES/NO directly
            sides = []
            for sell_side in ("YES", "NO"):
                rem = self._round_tokens(position.get_avail(sell_side))
                if rem > 0.1 and position.get_token_id(sell_side):
                    ep = position.get_entry_price(sell_side)
                    cp = position.get_price(sell_side)
                    pnl = (cp - ep) / ep if ep > 0 else 0
                    sides.append({"t": sell_side, "tid": position.get_token_id(sell_side),
                                  "ep": ep, "cp": cp, "sz": rem, "side": sell_side, "pnl": pnl})

            # Sort: winning first (PnL > 0 or price > 0.51 = likely winner)
            sides.sort(key=lambda s: (1 if s["pnl"] > 0 or s["cp"] > 0.51 else 0, s["pnl"]), reverse=True)
            if sides:
                order_str = " -> ".join(f'{s["side"]}({s["t"]},{s["pnl"]*100:+.1f}%)' for s in sides)
                self._log(f"{reason}: Sell order: {order_str}", "INFO")

            # --- DRY-RUN path --- V3.41: apply taker fee for realistic simulation
            if self.dry_run:
                # V3.49 FIX: Pop position + set window_exits FIRST to prevent sell-loop bug.
                # If dashboard/logging throws, position must still be removed to avoid
                # re-triggering COMBINED_TP and inflating session_sell_proceeds.
                self.state.window_exits[position.condition_id] = (sides[0]["cp"] if sides else 0, time.time())
                self.state.window_cycles[position.condition_id] = self.state.window_cycles.get(position.condition_id, 0) + 1
                self._log(f"[POS-] Removed {position.market_slug} | Remaining: {len(self.state.positions) - 1}", "DEBUG")
                self.state.positions.pop(position.condition_id, None)
                for s in sides:
                    sell_price_after_fee = s["cp"] * (1 - V3_TAKER_FEE_PCT)
                    pnl_pct = (s["cp"] - s["ep"]) / s["ep"] if s["ep"] > 0 else 0
                    pnl_usd = s["sz"] * (sell_price_after_fee - s["ep"]) if s["ep"] > 0 else 0
                    tag = reason
                    level = "ERROR" if pnl_usd < 0 else "SUCCESS"
                    self._log(f"[{self._market_tag(position.market_slug)}] {tag} {s['side']} {s['sz']:.0f} @ ${s['cp']:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", level)
                    close_proceeds = s["sz"] * sell_price_after_fee
                    self.current_balance += close_proceeds
                    self.state.total_pnl += pnl_usd
                    position.session_realized_profit += pnl_usd
                    position.session_sell_proceeds += close_proceeds  # V3.37
                    try:
                        self._add_trade_to_dashboard(position, s["cp"], pnl_pct, pnl_usd, tag, side=s["side"], entry_price=s["ep"])
                        self._update_candle(self.get_current_window_ts(), "CLOSED", s["side"], 0, tag, pnl_usd)
                        self._log_position_exit(position, s["cp"], pnl_pct, pnl_usd, tag)
                    except Exception as e:
                        logger.warning(f"[CLOSE] Dashboard/log error (position already removed): {e}")
                    self._log_trade_to_csv(tag, position.market_slug, position.condition_id, s["side"], s["sz"], s["cp"], pnl_pct, pnl_usd, notes=reason)
                return

            # --- LIVE path ---
            if not self.trader or self._live_trading_blocked_reason:
                return
            from py_clob_client.clob_types import OrderType, OrderArgs as _OA, PartialCreateOrderOptions as _PCO
            from py_clob_client.order_builder.constants import SELL as _SELL

            # V3.89: Verify on-chain token balance before sell — position tracker may be stale after rebalances
            verified_sides = []
            for s in sides:
                try:
                    rpc_bal = await self.sell_processor.run_io(verify_position_balance, self.trader.trading_address, s["tid"])
                    if rpc_bal is not None:
                        if rpc_bal <= 0.001:
                            self._log(f"{reason}: {s['side']} skip — on-chain balance=0", "INFO")
                            continue
                        if abs(rpc_bal - s["sz"]) > 1:
                            self._log(f"{reason}: {s['side']} balance corrected: tracker={s['sz']:.0f} → on-chain={rpc_bal:.0f}", "WARNING")
                        s["sz"] = float(rpc_bal)
                except Exception as e:
                    logger.debug(f"RPC balance check {s['side']}: {e}")
                verified_sides.append(s)
            sides = verified_sides

            # V3.29: Pre-fetch VWAP for ALL sides to determine realistic sell order
            for s in sides:
                try:
                    await self.sell_processor.run_io(self.trader.refresh_api_creds)
                    ob = await self.sell_processor.run_io(self.trader.clob.get_order_book, s["tid"])
                    if ob and ob.bids:
                        vb = [b for b in ob.bids if b.price and str(b.price).strip() and 0.01 < float(b.price) < 0.99]
                        sb = sorted(vb, key=lambda x: float(x.price), reverse=True)
                        if sb:
                            ft = 0.0; fc = 0.0
                            for bid in sb:
                                bpx = float(bid.price); bsz = float(bid.size)
                                take = min(bsz, float(s["sz"]) - ft)
                                ft += take; fc += take * bpx
                                if ft >= float(s["sz"]): break
                            s["vwap"] = fc / ft if ft > 0 else float(sb[0].price)
                            s["pnl"] = (s["vwap"] - s["ep"]) / s["ep"] if s["ep"] > 0 else 0
                except Exception as e:
                    logger.debug(f"VWAP pre-check {s['side']}: {e}")

            # Re-sort by VWAP PnL (realistic exit price, not current_price)
            sides.sort(key=lambda s: (1 if s["pnl"] > 0 or s.get("vwap", s["cp"]) > 0.51 else 0, s["pnl"]), reverse=True)
            if sides:
                order_str = " -> ".join(f'{s["side"]}({s["t"]},VWAP={s.get("vwap",0):.4f},PnL={s["pnl"]*100:+.1f}%)' for s in sides)
                self._log(f"{reason}: VWAP sell order: {order_str}", "INFO")

            # Aggregate PnL recheck using VWAP for both sides
            if reason == "SESSION_TP":
                st_pnl, st_cost, st_pct, _, _, _ = self._calc_session_total_pnl()
                if st_cost > 0 and st_pct < COMBINED_TP_PCT:
                    self._log(f"SESSION_TP SKIP: agg PnL {st_pct*100:+.1f}% < +{COMBINED_TP_PCT*100:.0f}%", "WARN")
                    return
            elif reason == "COMBINED_TP" and len(sides) >= 2:
                # V3.68: Skip VWAP PnL guard for near-resolution — sell at 0.99 regardless of combined PnL
                winner_cp = max(position.yes_price, position.no_price)
                if winner_cp >= 0.99:
                    self._log(f"COMBINED_TP: Near-resolution ${winner_cp:.4f} >= $0.99 — bypass VWAP PnL guard", "INFO")
                else:
                    yes_vwap = next((s.get("vwap", s["cp"]) for s in sides if s["side"] == "YES"), position.yes_price)
                    no_vwap = next((s.get("vwap", s["cp"]) for s in sides if s["side"] == "NO"), position.no_price)
                    c_usd, c_pct = self.calculate_combined_pnl(position, yes_vwap, no_vwap)
                    # V3.81: If loser price < FJ trigger, use winner-only PnL (loser is sunk cost)
                    loser_label = position.loser_side()
                    loser_vwap = no_vwap if loser_label == "NO" else yes_vwap
                    if loser_vwap < V3_FJ_TRIGGER_PRICE and c_pct < COMBINED_TP_PCT:
                        winner_label = position.winner_side()
                        w_vwap = yes_vwap if winner_label == "YES" else no_vwap
                        w_avail = position.get_avail(winner_label)
                        w_cost = position.get_cost(winner_label)
                        w_value = w_avail * w_vwap
                        # V3.93 FIX: Subtract recycled cost from proceeds (hedge buys are sunk with loser)
                        w_net_proceeds = position.session_sell_proceeds - position.session_recycled_cost
                        w_pnl = (w_value + w_net_proceeds - w_cost) / w_cost if w_cost > 0 else 0
                        self._log(f"COMBINED_TP: Loser {loser_label} VWAP ${loser_vwap:.4f} < FJ ${V3_FJ_TRIGGER_PRICE:.2f} — winner-only PnL {w_pnl*100:+.1f}%", "INFO")
                        c_pct = w_pnl
                    if c_pct < COMBINED_TP_PCT:
                        self._log(f"COMBINED_TP SKIP: VWAP PnL {c_pct*100:+.1f}%", "WARN")
                        return

            is_first = True
            for s in sides:
                side_label = f"{s['side']}({s['t']})"
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        # RPC balance check
                        try:
                            rpc_bal = await self.sell_processor.run_io(verify_position_balance, self.trader.trading_address, s["tid"])
                            if rpc_bal is not None and rpc_bal <= 0.001:
                                self._log(f"{reason}: {side_label} closed (RPC=0)", "INFO")
                                break
                            s["sz"] = float(rpc_bal) if rpc_bal else s["sz"]
                        except Exception:
                            pass

                        await self.sell_processor.run_io(self.trader.refresh_api_creds)
                        ob = await self.sell_processor.run_io(self.trader.clob.get_order_book, s["tid"])
                        if not ob or not ob.bids:
                            self._log(f"{reason}: No bids for {side_label}", "WARN")
                            break

                        valid_bids = [b for b in ob.bids if b.price and str(b.price).strip() and 0.01 < float(b.price) < 0.99]
                        sorted_bids = sorted(valid_bids, key=lambda x: float(x.price), reverse=True)
                        if not sorted_bids:
                            self._log(f"{reason}: No valid bids for {side_label}", "WARN")
                            break

                        # Walk bids for VWAP + sweep floor
                        fill_tk = 0.0; fill_cost = 0.0; sw_floor = float(sorted_bids[0].price)
                        for bid in sorted_bids:
                            bpx = float(bid.price); bsz = float(bid.size)
                            take = min(bsz, float(s["sz"]) - fill_tk)
                            fill_tk += take; fill_cost += take * bpx; sw_floor = bpx
                            if fill_tk >= float(s["sz"]):
                                break
                        vwap = fill_cost / fill_tk if fill_tk > 0 else float(sorted_bids[0].price)

                        if vwap >= 0.99 or vwap <= 0.01:
                            self._log(f"{reason}: {side_label} resolved — leave for redemption", "INFO")
                            break

                        actual = self._round_tokens(min(float(s["sz"]), fill_tk))
                        if actual < 0.1:
                            self._log(f"{reason}: {side_label} thin book", "WARN")
                            break

                        # Place FOK sell at sweep floor to hit all bid levels
                        raw_usdc = actual * sw_floor
                        target_usdc = math.floor(raw_usdc * 100) / 100
                        final_sz = math.floor(target_usdc / sw_floor * 10000) / 10000 if sw_floor > 0 else actual
                        if final_sz < 5 or target_usdc < 1.0:
                            self._log(f"{reason}: {side_label} too small ({final_sz:.0f}tk ${target_usdc:.2f})", "WARN")
                            break

                        self._log(f"{reason} SELL: {side_label} {final_sz:.0f} @ floor=${sw_floor:.4f} (VWAP=${vwap:.4f})", "INFO")
                        oa = _OA(token_id=s["tid"], price=sw_floor, size=float(final_sz), side=_SELL)
                        opts = _PCO(neg_risk=False, tick_size="0.01")
                        signed = await self.sell_processor.run_io(self.trader.clob.create_order, oa, opts)
                        result = await self.sell_processor.run_io(self.trader.clob.post_order, signed, OrderType.FOK)
                        if not isinstance(result, dict):
                            result = {"success": False, "error": str(result)}

                        if result.get("success"):
                            mk = float(result.get("makingAmount", 0) or 0)
                            tk_amt = float(result.get("takingAmount", 0) or 0)
                            # V3.96 FIX: Don't assume sold=actual when API returns success but
                            # makingAmount=0 and takingAmount=0 — order didn't actually fill
                            if mk <= 0 and tk_amt <= 0:
                                self._log(f"{reason}: {side_label} API success but makingAmount=0 takingAmount=0 — phantom fill, retrying", "ERROR")
                                if attempt < MAX_RETRIES:
                                    await asyncio.sleep(RETRY_DELAY)
                                continue
                            sold = mk if mk > 0 else (tk_amt / vwap if tk_amt > 0 and vwap > 0 else actual)
                            pnl_pct = (vwap - s["ep"]) / s["ep"] if s["ep"] > 0 else 0
                            pnl_usd = sold * (vwap - s["ep"]) if s["ep"] > 0 else 0
                            tag = reason

                            if sold < actual:
                                self._log(f"[{self._market_tag(position.market_slug)}] {tag} PARTIAL {s['side']} {sold:.0f}/{actual:.0f} @ ${vwap:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", "WARN")
                            else:
                                lvl = "ERROR" if pnl_usd < 0 else "SUCCESS"
                                self._log(f"[{self._market_tag(position.market_slug)}] {tag} {s['side']} {sold:.0f} @ ${vwap:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", lvl)

                            live_close_proceeds = tk_amt if tk_amt > 0 else sold * vwap
                            self.state.total_pnl += pnl_usd
                            position.session_realized_profit += pnl_usd
                            position.session_sell_proceeds += live_close_proceeds  # V3.37
                            position.add_tokens(s["side"], -sold)
                            position.add_sold_tokens(s["side"], sold)
                            # V3.49 FIX: Wrap dashboard/logging so sell-loop can't be broken by logging errors
                            try:
                                self._add_trade_to_dashboard(position, vwap, pnl_pct, pnl_usd, tag, side=s["side"], entry_price=s["ep"])
                                self._update_candle(self.get_current_window_ts(), "CLOSED", s["side"], 0, tag, pnl_usd)
                                self._log_position_exit(position, vwap, pnl_pct, pnl_usd, tag)
                            except Exception as e:
                                logger.warning(f"[CLOSE] Dashboard/log error: {e}")
                            self._log_trade_to_csv(tag, position.market_slug, position.condition_id, s["side"], sold, vwap, pnl_pct, pnl_usd, notes=reason)
                            # V3.60 FIX: Set grace period so API sync doesn't restore sold tokens
                            # (API propagation delay → sync sees stale data → restores hedge → re-sell loop)
                            position.last_rebalance_sell_ts = time.time()
                            is_first = False
                            break  # Side done, move to next side

                        err = result.get("errorMsg", result.get("error", str(result)))
                        self._log(f"{reason}: {side_label} fail {attempt}/{MAX_RETRIES}: {err}", "ERROR")

                    except Exception as e:
                        logger.error(f"[{reason} ERROR] {side_label} {attempt}/{MAX_RETRIES}: {e}")

                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)

            # V3.87: Retry selling remaining tokens after combined TP partial
            for _tp_retry in range(3):
                _yes_rem = position.get_avail("YES")
                _no_rem = position.get_avail("NO")
                if _yes_rem <= 0.1 and _no_rem <= 0.1:
                    break
                await asyncio.sleep(2)
                self._sync_positions_from_api()
                _retry_sold_any = False
                for _rs in ("YES", "NO"):
                    _rr = self._round_tokens(position.get_avail(_rs))
                    _tid = position.get_token_id(_rs)
                    if _rr <= 0.1 or not _tid:
                        continue
                    self._log(f"{reason}: Retry sell {_rs} {_rr:.0f} remaining (attempt {_tp_retry+1}/3)", "WARN")
                    try:
                        sell_result = await self.sell_processor.run_io(
                            self.trader.sell_position,
                            token_id=_tid, size=float(_rr),
                            neg_risk=False, tick_size=0.01, max_retries=1,
                        )
                        if sell_result.get("success"):
                            _mk = float(sell_result.get("makingAmount", 0) or 0)
                            _tk = float(sell_result.get("takingAmount", 0) or 0)
                            # V3.96 FIX: Skip phantom fills (success but 0 amounts)
                            if _mk <= 0 and _tk <= 0:
                                self._log(f"{reason}: RETRY {_rs} phantom fill (makingAmount=0) — skipping", "ERROR")
                                continue
                            _sold = _mk if _mk > 0 else _rr
                            _ep = position.get_entry_price(_rs)
                            _vwap = _tk / _sold if _sold > 0 and _tk > 0 else position.get_price(_rs)
                            _pnl_pct = (_vwap - _ep) / _ep if _ep > 0 else 0
                            _pnl_usd = _sold * (_vwap - _ep) if _ep > 0 else 0
                            lvl = "SUCCESS" if _pnl_usd >= 0 else "ERROR"
                            self._log(f"[{self._market_tag(position.market_slug)}] {reason} RETRY {_rs} {_sold:.0f} @ ${_vwap:.4f} PnL: {_pnl_pct:+.1%} (${_pnl_usd:+.2f})", lvl)
                            _proceeds = _tk if _tk > 0 else _sold * _vwap
                            self.state.total_pnl += _pnl_usd
                            position.session_realized_profit += _pnl_usd
                            position.session_sell_proceeds += _proceeds
                            position.add_tokens(_rs, -_sold)
                            position.add_sold_tokens(_rs, _sold)
                            position.last_rebalance_sell_ts = time.time()
                            try:
                                self._add_trade_to_dashboard(position, _vwap, _pnl_pct, _pnl_usd, reason, side=_rs, entry_price=_ep)
                            except Exception:
                                pass
                            self._log_trade_to_csv(reason, position.market_slug, position.condition_id, _rs, _sold, _vwap, _pnl_pct, _pnl_usd, notes=f"{reason}_RETRY")
                            _retry_sold_any = True
                    except Exception as e:
                        logger.error(f"[{reason} RETRY ERROR] {_rs}: {e}")
                if not _retry_sold_any:
                    break  # No progress, stop retrying

            # Cleanup
            self.state.window_exits[position.condition_id] = (sides[0]["cp"] if sides else 0, time.time())
            self.state.window_cycles[position.condition_id] = self.state.window_cycles.get(position.condition_id, 0) + 1
            yes_rem = position.get_avail("YES")
            no_rem = position.get_avail("NO")
            if yes_rem <= 0.1 and no_rem <= 0.1:
                self.state.positions.pop(position.condition_id, None)
            else:
                # V3.81: Write off worthless loser — if one side sold and the remaining side
                # has price < FJ trigger (no liquidity), treat as sunk cost and pop position
                loser_label = position.loser_side()
                loser_rem = position.get_avail(loser_label)
                loser_price = position.get_price(loser_label)
                winner_rem = position.get_avail(position.winner_side())
                if winner_rem <= 0.1 and loser_rem > 0.1 and loser_price < V3_FJ_TRIGGER_PRICE:
                    loser_cost = loser_rem * position.get_entry_price(loser_label)
                    self._log(f"{reason}: Writing off {loser_label} {loser_rem:.0f} tokens @ ${loser_price:.4f} "
                              f"(< FJ ${V3_FJ_TRIGGER_PRICE:.2f}, cost=${loser_cost:.2f} sunk)", "WARN")
                    self.state.positions.pop(position.condition_id, None)
                else:
                    self._log(f"{reason}: Partial — YES {yes_rem:.0f}, NO {no_rem:.0f} remaining", "WARN")
            self._refresh_balance_and_allowance_block_if_needed()
            self._sync_positions_from_api()
            return

        # DRY-RUN
        if self.dry_run:
            pnl_pct = (exit_price - close_ep) / close_ep if close_ep > 0 else 0
            pnl_usd = sell_size * (exit_price - close_ep)

            # Pre-validate: if TAKE_PROFIT intent but price moved against us, SKIP order
            if "PROFIT" in reason and pnl_pct < 0:
                self._log(f"TP SKIP: price dropped entry={close_ep:.3f} → exit={exit_price:.3f} ({pnl_pct:+.1%})", "WARN")
                return  # Don't execute, wait for price recovery

            action = reason
            level = "ERROR" if pnl_usd < 0 else "SUCCESS"
            self._log(f"[{self._market_tag(position.market_slug)}] {action} {close_side} {sell_size:.0f} @ ${exit_price:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", level)

            close_main_proceeds = sell_size * exit_price
            self.current_balance += close_main_proceeds
            self.state.total_pnl += pnl_usd
            position.session_realized_profit += pnl_usd  # V2.3
            position.session_sell_proceeds += close_main_proceeds  # V3.37
            self._add_trade_to_dashboard(position, exit_price, pnl_pct, pnl_usd, action, side=close_side, entry_price=close_ep)
            self._update_candle(self.get_current_window_ts(), "CLOSED", close_side, 0, action, pnl_usd)

            # Log to MongoDB
            self._log_position_exit(position, exit_price, pnl_pct, pnl_usd, action)
            # CSV: CLOSE (main)
            self._log_trade_to_csv(action, position.market_slug, position.condition_id, close_side, sell_size, exit_price, pnl_pct, pnl_usd, notes=reason)

            # V3: Close hedge for session-wide exits (both sides close together)
            # For TAKE_PROFIT/STOP_LOSS on main only, keep hedge - let market resolve naturally
            should_close_hedge = reason in ("COMBINED_TP", "RESOLUTION", "SESSION_TP")
            opp_tokens = position.get_tokens(opp_side)
            opp_ep = position.get_entry_price(opp_side)
            if opp_tokens > 0 and opp_ep > 0 and should_close_hedge:
                # Hedge current price is inverse of main position price
                hedge_exit_price = 1 - exit_price
                hedge_pnl_pct = (hedge_exit_price - opp_ep) / opp_ep
                hedge_pnl_usd = opp_tokens * (hedge_exit_price - opp_ep)

                self.current_balance += opp_tokens * hedge_exit_price
                self.state.total_pnl += hedge_pnl_usd

                level = "SUCCESS" if hedge_pnl_usd >= 0 else "ERROR"
                self._log(f"[{self._market_tag(position.market_slug)}] HEDGE CLOSE {opp_side} {opp_tokens:.0f} @ ${hedge_exit_price:.4f} PnL: {hedge_pnl_pct:+.1%} (${hedge_pnl_usd:+.2f})", level)

                # Add hedge trade to dashboard
                hedge_trade = {
                    "side": opp_side,
                    "entry_time": position.entry_time,
                    "exit_time": time.time(),
                    "entry_price": opp_ep,
                    "exit_price": hedge_exit_price,
                    "pnl_percent": hedge_pnl_pct,
                    "pnl_cash": hedge_pnl_usd,
                    "exit_reason": f"HEDGE_{action}",
                }
                logger.debug(f"[DASHBOARD ADD] HEDGE_{action} (dry) pnl_cash=${hedge_pnl_usd:+.2f}")
                self.dashboard.add_trade(hedge_trade)
                # CSV: HEDGE_CLOSE
                self._log_trade_to_csv(f"HEDGE_{action}", position.market_slug, position.condition_id, opp_side, opp_tokens, hedge_exit_price, hedge_pnl_pct, hedge_pnl_usd, notes=reason)
            elif opp_tokens > 0 and not should_close_hedge:
                # V3: Promote hedge to main position instead of closing it
                self._log(f"PROMOTE HEDGE: {opp_side} {opp_tokens:.0f} @ ${opp_ep:.4f} becomes main", "INFO")

            # Record exit for position cycling
            self.state.window_exits[position.condition_id] = (exit_price, time.time())
            self.state.window_cycles[position.condition_id] = self.state.window_cycles.get(position.condition_id, 0) + 1

            # V2.21: Log position removal
            self._log(f"[POS-] Removed {position.market_slug} {close_side} | Remaining: {len(self.state.positions) - 1}", "DEBUG")
            self.state.positions.pop(position.condition_id, None)
            return

        if not self.trader or self._live_trading_blocked_reason:
            return

        # V3.6: FOK for COMBINED_TP/SESSION_TP to avoid partial-fill cascades
        from py_clob_client.clob_types import OrderType
        _sweep_reasons = ("COMBINED_TP", "SESSION_TP")
        sell_order_type = OrderType.FOK if reason in _sweep_reasons else OrderType.FAK

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # V2.9: Recheck remaining balance every retry (may have partial sells)
                # V3.10: blocking I/O runs in thread pool for parallel execution
                try:
                    rpc_balance = await self.sell_processor.run_io(verify_position_balance, self.trader.trading_address, token_id)
                    if rpc_balance is not None and rpc_balance <= 0.001:
                        logger.info(f"Position closed (RPC balance: {rpc_balance:.6f})")
                        self.state.positions.pop(position.condition_id, None)
                        return
                    sell_size = float(rpc_balance) if rpc_balance else float(sell_size)
                except Exception:
                    pass

                await self.sell_processor.run_io(self.trader.refresh_api_creds)
                orderbook = await self.sell_processor.run_io(self.trader.clob.get_order_book, token_id)
                if not orderbook or not orderbook.bids:
                    if reason == "SESSION_TP":
                        self._log(f"SESSION_TP: No bids for {close_side} — will retry", "WARN")
                        return  # Don't pop — outer retry loop will retry
                    logger.warning("No bids - leaving for redemption")
                    self.state.positions.pop(position.condition_id, None)
                    return

                # V3.10: For COMBINED_TP/SESSION_TP, scan ALL bid levels for VWAP and total liquidity
                if reason in _sweep_reasons:
                    valid_bids = [b for b in orderbook.bids
                                  if b.price and str(b.price).strip()
                                  and 0.01 < float(b.price) < 0.99]
                    sorted_bids = sorted(valid_bids, key=lambda x: float(x.price), reverse=True)
                    if not sorted_bids:
                        if reason == "SESSION_TP":
                            self._log(f"SESSION_TP: No valid bids for {close_side} — will retry", "WARN")
                            return
                        logger.warning(f"{reason}: No valid bids — leaving for redemption")
                        self.state.positions.pop(position.condition_id, None)
                        return

                    # Walk bids to find total available and VWAP for our sell size
                    fill_tokens = 0.0
                    fill_cost = 0.0
                    sweep_floor = float(sorted_bids[0].price)
                    for bid in sorted_bids:
                        bid_px = float(bid.price)
                        bid_sz = float(bid.size)
                        take = min(bid_sz, float(sell_size) - fill_tokens)
                        fill_tokens += take
                        fill_cost += take * bid_px
                        sweep_floor = bid_px
                        if fill_tokens >= float(sell_size):
                            break

                    best_price = fill_cost / fill_tokens if fill_tokens > 0 else float(sorted_bids[0].price)
                    available_size = fill_tokens
                    self._log(
                        f"{reason} SWEEP: {len(sorted_bids)} levels, "
                        f"avail={fill_tokens:.0f}, VWAP=${best_price:.4f}, floor=${sweep_floor:.4f}", "INFO")
                else:
                    best_bid = max(orderbook.bids, key=lambda x: float(x.price))
                    best_price = float(best_bid.price)
                    available_size = float(best_bid.size)
                    sweep_floor = best_price

                if best_price >= 0.99 or best_price <= 0.01:
                    logger.warning("Market resolved - leaving for redemption")
                    self.state.positions.pop(position.condition_id, None)
                    return

                actual_sell = min(float(sell_size), available_size)
                actual_sell = self._round_tokens(actual_sell)
                if actual_sell < 0.1:
                    if reason == "SESSION_TP" and sell_size >= 0.1:
                        self._log(f"SESSION_TP: Thin orderbook (bid size {available_size:.2f}) for {close_side} {sell_size:.0f} tokens — will retry", "WARN")
                        return  # Don't pop — outer retry loop will retry
                    logger.info("Position closed (dust remaining)")
                    self.state.positions.pop(position.condition_id, None)
                    return

                # Pre-validate: if TAKE_PROFIT intent but price moved against us, SKIP order
                if "PROFIT" in reason:
                    expected_pnl_pct = (best_price - close_ep) / close_ep if close_ep > 0 else 0

                    # V3.80: For COMBINED_TP, recheck combined PnL using VWAP
                    if "COMBINED" in reason and (position.yes_tokens > 0 and position.no_tokens > 0):
                        # Use best_price for the side being sold, current price for the other
                        if close_side == "YES":
                            combined_pnl_usd, combined_pnl_pct = self.calculate_combined_pnl(position, best_price, position.no_price)
                        else:
                            combined_pnl_usd, combined_pnl_pct = self.calculate_combined_pnl(position, position.yes_price, best_price)
                        if combined_pnl_pct < COMBINED_TP_PCT:
                            self._log(f"COMBINED_TP SKIP: VWAP PnL {combined_pnl_pct*100:+.1f}% < +{COMBINED_TP_PCT*100:.0f}% threshold", "WARN")
                            return  # Don't execute, wait for price recovery
                    elif expected_pnl_pct < 0:
                        self._log(f"TP SKIP: price dropped entry={close_ep:.3f} → bid={best_price:.3f} ({expected_pnl_pct:+.1%})", "WARN")
                        return  # Don't execute, wait for price recovery

                # V3.10: For SESSION_TP, recheck aggregate session PnL using VWAP
                if reason == "SESSION_TP":
                    st_pnl, st_cost, st_pct, _, _, _ = self._calc_session_total_pnl()
                    if st_cost > 0 and st_pct < COMBINED_TP_PCT:
                        self._log(f"SESSION_TP SKIP: aggregate VWAP PnL {st_pct*100:+.1f}% < +{COMBINED_TP_PCT*100:.0f}% threshold", "WARN")
                        return

                # V3.10: For COMBINED_TP/SESSION_TP, create order directly at sweep_floor to hit all bid levels
                if reason in _sweep_reasons:
                    from py_clob_client.clob_types import OrderArgs as _OA, PartialCreateOrderOptions as _PCO
                    from py_clob_client.order_builder.constants import SELL as _SELL

                    raw_usdc = actual_sell * sweep_floor
                    target_usdc = math.floor(raw_usdc * 100) / 100
                    final_size = math.floor(target_usdc / sweep_floor * 10000) / 10000 if sweep_floor > 0 else actual_sell

                    if final_size < 5 or target_usdc < 1.0:
                        self._log(f"{reason}: Order too small ({final_size:.0f} tokens ${target_usdc:.2f}) — skipping side", "WARN")
                        # V3.28: Skip tiny positions instead of looping — let outer retry handle hedge side
                        position.set_sold_tokens(close_side, position.get_tokens(close_side))  # Mark main as done
                        break

                    self._log(f"{reason} SELL: {final_size:.0f} @ floor=${sweep_floor:.4f} (VWAP=${best_price:.4f})", "INFO")
                    order_args = _OA(token_id=token_id, price=sweep_floor, size=float(final_size), side=_SELL)
                    options = _PCO(neg_risk=False, tick_size="0.01")
                    signed = await self.sell_processor.run_io(self.trader.clob.create_order, order_args, options)
                    result = await self.sell_processor.run_io(self.trader.clob.post_order, signed, sell_order_type)
                    if not isinstance(result, dict):
                        result = {"success": False, "error": str(result)}
                else:
                    # V3.10: sell in thread pool for parallel execution
                    result = await self.sell_processor.run_io(
                        self.trader.sell_position,
                        token_id=token_id,
                        size=float(actual_sell),
                        order_type=sell_order_type,
                        neg_risk=False,
                        tick_size=0.01,
                        max_retries=1,
                    )

                if result.get("success"):
                    # V3.9 FIX: For SELL orders, makingAmount = tokens sold, takingAmount = USDC received
                    making_amount = float(result.get("makingAmount", 0) or 0)
                    taking_amount = float(result.get("takingAmount", 0) or 0)
                    tokens_sold = making_amount if making_amount > 0 else (taking_amount / best_price if taking_amount > 0 and best_price > 0 else actual_sell)

                    pnl_pct = (best_price - close_ep) / close_ep if close_ep > 0 else 0
                    pnl_usd = tokens_sold * (best_price - close_ep)

                    action = reason

                    # V3.5: Log partial fill if applicable
                    if tokens_sold < actual_sell:
                        self._log(f"[{self._market_tag(position.market_slug)}] {action} PARTIAL {close_side} {tokens_sold:.0f}/{actual_sell:.0f} @ ${best_price:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", "WARN")
                    else:
                        level = "ERROR" if pnl_usd < 0 else "SUCCESS"
                        self._log(f"[{self._market_tag(position.market_slug)}] {action} {close_side} {tokens_sold:.0f} @ ${best_price:.4f} PnL: {pnl_pct:+.1%} (${pnl_usd:+.2f})", level)

                    live_main_proceeds = taking_amount if taking_amount > 0 else tokens_sold * best_price
                    self.state.total_pnl += pnl_usd
                    position.session_realized_profit += pnl_usd  # V2.3
                    position.session_sell_proceeds += live_main_proceeds  # V3.37
                    self._add_trade_to_dashboard(position, best_price, pnl_pct, pnl_usd, action, side=close_side, entry_price=close_ep)
                    self._update_candle(self.get_current_window_ts(), "CLOSED", close_side, 0, action, pnl_usd)

                    # Log to MongoDB
                    self._log_position_exit(position, best_price, pnl_pct, pnl_usd, action)

                    # V3.5: Update position size with actual sold (for partial fills)
                    position.add_tokens(close_side, -tokens_sold)
                    position.add_sold_tokens(close_side, tokens_sold)

                    # V3.5: Only fully close position if all tokens sold
                    fully_closed = position.get_tokens(close_side) <= 0.1  # Allow for dust

                    # V3: Close hedge for session-wide exits (both sides close together)
                    # For TAKE_PROFIT/STOP_LOSS on main only, keep hedge - let market resolve naturally
                    # SESSION_TP: attempt hedge close regardless of main fully_closed (outer loop retries)
                    should_close_hedge = reason in ("COMBINED_TP", "RESOLUTION", "SESSION_TP")
                    if reason != "SESSION_TP":
                        should_close_hedge = should_close_hedge and fully_closed
                    opp_tokens = position.get_tokens(opp_side)
                    opp_tid = position.get_token_id(opp_side)
                    opp_ep = position.get_entry_price(opp_side)
                    if opp_tokens > 0 and opp_tid and should_close_hedge:
                        try:
                            hedge_sell_size = float(position.get_avail(opp_side))
                            if hedge_sell_size < 0.1:
                                self._log(f"HEDGE CLOSE SKIP: {opp_side} dust remaining", "HEDGE")
                            else:
                                # V3.10: sell in thread pool for parallel execution
                                hedge_result = await self.sell_processor.run_io(
                                    self.trader.sell_position,
                                    token_id=opp_tid,
                                    size=hedge_sell_size,
                                    order_type=sell_order_type,  # V3.6: FOK for COMBINED_TP
                                    neg_risk=False,
                                    tick_size=0.01,
                                    max_retries=1,
                                )
                                if hedge_result.get("success"):
                                    # V3.9 FIX: makingAmount = tokens sold, takingAmount = USDC received
                                    h_making = float(hedge_result.get("makingAmount", 0) or 0)
                                    h_taking = float(hedge_result.get("takingAmount", 0) or 0)
                                    hedge_sold = h_making if h_making > 0 else (h_taking / best_price if h_taking > 0 and best_price > 0 else hedge_sell_size)
                                    position.add_tokens(opp_side, -hedge_sold)
                                    position.add_sold_tokens(opp_side, hedge_sold)
                                    # V3.9 FIX: Track hedge PnL (was missing in live path)
                                    opp_price = position.get_price(opp_side)
                                    hedge_exit_price = opp_price if opp_price > 0 else (1 - best_price)
                                    hedge_pnl_usd = hedge_sold * (hedge_exit_price - opp_ep)
                                    hedge_pnl_pct = (hedge_exit_price - opp_ep) / opp_ep if opp_ep > 0 else 0
                                    hedge_close_proceeds = h_taking if h_taking > 0 else hedge_sold * hedge_exit_price
                                    self.state.total_pnl += hedge_pnl_usd
                                    position.session_realized_profit += hedge_pnl_usd
                                    position.session_sell_proceeds += hedge_close_proceeds  # V3.37
                                    self._add_trade_to_dashboard(position, hedge_exit_price, hedge_pnl_pct, hedge_pnl_usd, f"HEDGE_{action}",
                                                                 side=opp_side, entry_price=opp_ep)
                                    self._log_trade_to_csv(f"HEDGE_{action}", position.market_slug, position.condition_id,
                                                           opp_side, hedge_sold, hedge_exit_price, hedge_pnl_pct, hedge_pnl_usd, notes=reason)
                                    if hedge_sold < hedge_sell_size:
                                        self._log(f"HEDGE PARTIAL {opp_side} {hedge_sold:.0f}/{hedge_sell_size:.0f} PnL: ${hedge_pnl_usd:+.2f}", "WARN")
                                    else:
                                        self._log(f"HEDGE CLOSE {opp_side} {hedge_sold:.0f} PnL: ${hedge_pnl_usd:+.2f}", "HEDGE")
                                else:
                                    self._log(f"HEDGE CLOSE FAIL: {hedge_result.get('error', 'unknown')}", "ERROR")
                        except Exception as e:
                            logger.error(f"[HEDGE CLOSE ERROR] {e}")
                    elif opp_tokens > 0 and not should_close_hedge:
                        # V3: Hedge stays open - will be synced as new position on next tick
                        self._log(f"HEDGE KEPT: {opp_side} {opp_tokens:.0f} tokens remain open", "INFO")

                    # Record exit for position cycling
                    self.state.window_exits[position.condition_id] = (best_price, time.time())
                    self.state.window_cycles[position.condition_id] = self.state.window_cycles.get(position.condition_id, 0) + 1

                    # V3.5: Only remove from tracking if fully closed (main + hedge)
                    hedge_remaining = position.get_avail(opp_side)
                    main_remaining = position.get_tokens(close_side)
                    if main_remaining <= 0.1 and hedge_remaining <= 0.1:
                        self.state.positions.pop(position.condition_id, None)
                    elif main_remaining <= 0.1 and hedge_remaining > 0.1:
                        self._log(f"PARTIAL CLOSE: main done, hedge {opp_side} {hedge_remaining:.0f} tokens remaining", "WARN")
                    else:
                        self._log(f"PARTIAL CLOSE: {main_remaining:.0f} tokens remaining, will retry", "WARN")

                    self._refresh_balance_and_allowance_block_if_needed()
                    # V3.5: Sync from API to get accurate remaining position after partial fill
                    self._sync_positions_from_api()

                    # V3.10: For COMBINED_TP/SESSION_TP, retry immediately in inner loop
                    # instead of waiting 3-18 sec for next scan cycle
                    if not fully_closed and reason in ("COMBINED_TP", "SESSION_TP"):
                        self._log(f"RETRY IMMEDIATELY: {main_remaining:.0f} tokens remaining", "INFO")
                        continue
                    return

                err = result.get("errorMsg", result.get("error", str(result)))
                self._log(f"CLOSE FAILED: attempt {attempt}/{MAX_RETRIES}: {err}", "ERROR")
                self._sync_positions_from_api()  # V2.8: Sync after failed close

            except Exception as e:
                logger.error(f"[CLOSE ERROR] attempt {attempt}/{MAX_RETRIES}: {e}")
                self._sync_positions_from_api()  # V2.8: Sync on error

            if attempt < MAX_RETRIES:
                logger.info(f"[CLOSE] Retrying in {RETRY_DELAY}s...")
                await asyncio.sleep(RETRY_DELAY)

    # ----------------- UI -----------------
    def _render_dashboard(self):
        # Sync positions from API before rendering (catches external positions)
        self._refresh_balance_and_allowance_block_if_needed()
        self._sync_positions_from_api()

        # V2.21: Debug - log how many positions are tracked
        if self.state.positions:
            markets = [p.market_slug for p in self.state.positions.values()]
            logger.debug(f"[DASHBOARD] Rendering {len(self.state.positions)} positions: {markets}")

        # V3.80: Build positions list from YES/NO directly — both sides equal
        positions_list = []
        for pos in self.state.positions.values():
            if pos.condition_id not in self._condition_to_slug:
                continue
            for dash_side in ("YES", "NO"):
                side_tokens = pos.get_tokens(dash_side)
                if side_tokens > 0:
                    side_ep = pos.get_entry_price(dash_side)
                    side_cp = pos.get_price(dash_side)
                    side_pnl = (side_cp - side_ep) / side_ep if side_ep > 0 else 0
                    positions_list.append({
                        "side": dash_side,
                        "entry_price": side_ep,
                        "size": side_tokens,
                        "current_price": side_cp,
                        "pnl_pct": side_pnl,
                        "tsl_level": 0,
                        "tsl_floor": 0,
                        "duration": time.time() - pos.entry_time,
                        "market": pos.market_slug,
                        "is_hedge": False,
                    })

        status = "OPEN" if positions_list else "WAITING"
        if self._live_trading_blocked_reason and not self.dry_run:
            status = "BLOCKED"

        # V3.10: Single aggregate calculation for combined PnL + session totals
        combined_pnl_usd, session_total_cost, combined_pnl_pct, session_total_value, session_total_tokens, session_sell_proceeds = self._calc_session_total_pnl()
        session_total_profit = combined_pnl_usd
        session_avg_entry = session_total_cost / session_total_tokens if session_total_tokens > 0 else 0.0
        logger.debug(f"[SESSION] TOTAL: cost=${session_total_cost:.2f} value=${session_total_value:.2f} profit=${session_total_profit:+.2f} tokens={session_total_tokens:.0f}")

        # Get prediction from GodEye (Redis polymarket_latest)
        godeye_prediction, godeye_confidence = self.get_godeye_prediction()

        self.dashboard.render(
            status=status,
            window_ts=self.get_current_window_ts(),
            elapsed_minutes=(time.time() - (self.get_current_window_ts())) / 60,
            prediction=godeye_prediction,
            confidence=godeye_confidence,
            current_balance=self.current_balance,
            yes_price=self.current_yes_price,
            no_price=self.current_no_price,
            entry_range=(BUY_BAND_LOW, BUY_BAND_HIGH),
            position_side=positions_list[0]["side"] if positions_list else None,
            position_entry_price=positions_list[0]["entry_price"] if positions_list else 0,
            position_pnl_pct=positions_list[0]["pnl_pct"] if positions_list else 0,
            position_size=positions_list[0]["size"] if positions_list else 0,
            position_current_price=positions_list[0]["current_price"] if positions_list else 0,
            tsl_level=0,
            tsl_floor=0,
            position_duration=positions_list[0]["duration"] if positions_list else 0,
            positions=positions_list,
            combined_pnl_usd=combined_pnl_usd,
            combined_pnl_pct=combined_pnl_pct,
            # V3: Session summary stats
            session_total_cost=session_total_cost,
            session_sell_proceeds=session_sell_proceeds,
            session_total_value=session_total_value,
            session_total_profit=session_total_profit,
            session_avg_entry=session_avg_entry,
            session_total_tokens=session_total_tokens,
            begin_session_balance=self._begin_session_balance,
            portfolio_value=self.current_balance + self._get_all_positions_value() + self._unredeemed_positions_value,
            session_profit=self._get_session_profit()[0],
            session_profit_pct=self._get_session_profit()[1],
            width=160,
            logs_only=False,
            window_minutes=float(V3_WINDOW_MINUTES),
        )

        # Publish TUI snapshot to Redis for web dashboard
        self._publish_tui_to_redis()

    def _publish_tui_to_redis(self):
        """Publish latest TUI snapshot to Redis for web dashboard consumption.
        Uses its own Redis connection (independent of momentum/CEX Redis)."""
        try:
            # Lazy-init dashboard Redis connection
            if not getattr(self, '_dashboard_redis', None):
                self._dashboard_redis = redis.Redis(
                    host=os.getenv("REDIS_HOST", "localhost"),
                    port=int(os.getenv("REDIS_PORT", "6379")),
                    decode_responses=True,
                    socket_timeout=2,
                    socket_connect_timeout=2,
                )
                self._dashboard_redis.ping()
            snapshot = getattr(self.dashboard, '_last_snapshot', None)
            if snapshot:
                iid = self.dashboard.instance_id or "latest"
                self._dashboard_redis.set(f"konis:tui:{iid}", json.dumps(snapshot, default=str))
        except Exception as e:
            logger.warning(f"Redis TUI publish failed: {e}")
            self._dashboard_redis = None  # Reset on failure, retry next cycle

    async def run(self):
        mode = "DRY-RUN" if self.dry_run else "LIVE"
        self._log(f"Starting V3 bot ({mode}) — DCA mode: {V3_DCA_MODE.upper()}", "INFO")
        if V3_DCA_MODE == "rebalance":
            _confirm_str = f", pred_confirm={V3_PRED_CONFIRM_TICKS}" if V3_PRED_CONFIRM_TICKS > 0 else ""
            _entry_str = f"${V3_ENTRY_AMOUNT_USD} x leader={V3_ENTRY_LEADER_MULT:.1f}/trailer={V3_ENTRY_TRAILER_MULT:.1f}"
            self._log(f"Strategy: {_entry_str}, HYBRID REBALANCE (trend_conf>={V3_REGIME_TREND_CONFIDENCE:.0%}, trend_mom>={V3_REGIME_TREND_MOMENTUM:.2f}, rebal_gain>={V3_REBALANCE_GAIN_PCT:.0%}, rebal_sell={V3_REBALANCE_SELL_PCT:.0%}, peak_reset=${V3_REBALANCE_PEAK_RESET_PRICE:.2f}{_confirm_str})", "INFO")
        else:
            _entry_str = f"${V3_ENTRY_AMOUNT_USD} x leader={V3_ENTRY_LEADER_MULT:.1f}/trailer={V3_ENTRY_TRAILER_MULT:.1f}"
            self._log(f"Strategy: {_entry_str}, DCA {V3_DCA_MODE.upper()} (loser_max=${V3_DCA_LOSER_MAX_PRICE:.2f}, conf>={PREDICTION_BOOST_MIN_CONFIDENCE:.0%}, quality>={V3_DCA_MIN_QUALITY_SCORE:.2f})", "INFO")
        _chop_dca_str = f" chop_dca=${V3_CHOP_DCA_AMOUNT_USD:.0f}/cd={V3_CHOP_DCA_COOLDOWN_SEC:.0f}s" if V3_CHOP_DCA_AMOUNT_USD > 0 else ""
        self._log(f"DCA amount: ${V3_DCA_AMOUNT_USD} cd={V3_DCA_COOLDOWN_SEC:.0f}s{_chop_dca_str} | FJ trigger: <=${V3_FJ_TRIGGER_PRICE} cheap-first (win>={V3_FJ_MIN_WIN_PCT*100:+.0f}%) fj-gate={V3_FJ_LAST_MIN:.0f}m enforce={V3_FJ_ENFORCE_PRICE or 'OFF'}", "INFO")
        self._log(f"Buy band: ${BUY_BAND_LOW:.2f}-${BUY_BAND_HIGH:.2f} | Max tokens: {MAX_POSITION_TOKENS:.0f} | Budget: ${MAX_POSITION_COST_USD:.0f}/side (${MAX_POSITION_COST_USD*2:.0f} total)", "INFO")
        tsl_info = f" TSL: step={V3_COMBINED_TSL_STEP*100:.0f}%" if V3_COMBINED_TSL_ENABLED else ""
        self._log(f"Combined TP: {'ON' if COMBINED_TP_ENABLED else 'OFF'} at +{COMBINED_TP_PCT*100:.0f}% (min cost ${COMBINED_TP_MIN_COST_USD}){tsl_info} | DCA target: +{DCA_TARGET_TP_PCT*100:.0f}% | ghost-cooldown={V3_GHOST_FILL_COOLDOWN_SEC:.0f}s", "INFO")
        self._log(f"Loss control: cut={V3_DCA_LOSS_CUT_PCT*100:.0f}% sell={V3_DCA_LOSS_CUT_SELL_PCT*100:.0f}% min_price=${V3_SELL_LOSER_MIN_PRICE:.2f} | Hedge: rebal_hedge={V3_REBALANCE_HEDGE_PCT*100:.0f}%", "INFO")
        if V3_MAX_REBALANCE_CYCLES > 0:
            self._log(f"Rebalance cycles: max {V3_MAX_REBALANCE_CYCLES} per session", "INFO")
        if V3_CHEAP_LOSER_DCA_MAX > 0:
            self._log(f"Cheap loser DCA: max {V3_CHEAP_LOSER_DCA_MAX} buys/window, drop>={V3_CHEAP_LOSER_DCA_DROP_PCT*100:.0f}%, amount={V3_CHEAP_LOSER_DCA_AMOUNT_PCT*100:.0f}% of entry, session cap=cost*(1+{COMBINED_TP_PCT*100:.0f}%)", "INFO")
        self._log(f"PARALLEL: {'ON' if PARALLEL_MARKETS else 'OFF'} | DYNAMIC CHUNKS: {'ON' if DYNAMIC_CHUNKS_ENABLED else 'OFF'} ({CHUNK_MIN_TOKENS:.0f}-{CHUNK_MAX_TOKENS:.0f})", "INFO")

        self._log(f"Markets: {', '.join([m.name for m in self.markets])}", "INFO")

        # Connect to MongoDB
        await self._connect_mongo()

        if not self.dry_run:
            self._init_trader()  # will set blocked reason if needed

            # V3.3: CRITICAL - Fetch current markets FIRST to populate _market_end_times
            # Then sync existing positions from API BEFORE any trading decisions
            self._log("Fetching current market data...", "INFO")
            window_ts = self.get_current_window_ts()
            # V3.58: Clear stale mappings before populating with current window's data
            self._condition_to_slug.clear()
            for market_config in self.markets:
                try:
                    slug = f"{market_config.slug_pattern}-{window_ts}"
                    market = await self.fetch_market_by_slug(slug)
                    if market:
                        # Populate _market_end_times and _condition_to_slug from market data
                        for token in market.get("tokens", []):
                            condition_id = token.get("condition_id", "")
                            if condition_id:
                                self._condition_to_slug[condition_id] = market_config.slug_pattern
                                if market.get("end_date_iso"):
                                    from datetime import datetime
                                    end_dt = datetime.fromisoformat(market["end_date_iso"].replace("Z", "+00:00"))
                                    self._market_end_times[condition_id] = end_dt.timestamp()
                        self._log(f"Found market: {market_config.name}", "INFO")
                except Exception as e:
                    logger.warning(f"Failed to fetch market {market_config.name}: {e}")

            # Now sync positions (with _market_end_times populated)
            self._log("Syncing existing positions from API...", "INFO")
            self._sync_positions_from_api()

            # V3.27: Restore session state from disk if file exists for current window
            self.state.last_window_ts = window_ts
            if self._load_session_state(window_ts):
                self._log("Resumed session from saved state", "SUCCESS")
            # Load persistent begin_balance (survives across windows & restarts, resets on config change)
            self._load_persistent_balance()
            if self.state.positions:
                self._log(f"Found {len(self.state.positions)} existing positions", "SUCCESS")
                for cid, pos in self.state.positions.items():
                    yes_info = f"YES {pos.yes_tokens:.0f} @ ${pos.yes_entry_price:.4f}" if pos.yes_tokens > 0 else ""
                    no_info = f"NO {pos.no_tokens:.0f} @ ${pos.no_entry_price:.4f}" if pos.no_tokens > 0 else ""
                    sep = " + " if yes_info and no_info else ""
                    self._log(f"  {yes_info}{sep}{no_info}", "INFO")
            else:
                self._log("No existing positions found", "INFO")

        # Start OKX WebSocket price feed for volatility gate
        if V3_VOLATILITY_GATE_ENABLED:
            try:
                from okx_ws_price_feed import OkxPriceFeed
            except ImportError:
                # Fallback: import via importlib for kebab-case filename
                import importlib.util as _ilu
                _okx_spec = _ilu.spec_from_file_location(
                    "okx_feed", str(Path(__file__).resolve().parent / "lib" / "okx-ws-price-feed.py"))
                _okx_mod = _ilu.module_from_spec(_okx_spec)
                _okx_spec.loader.exec_module(_okx_mod)
                OkxPriceFeed = _okx_mod.OkxPriceFeed
            symbols = list({mc.slug_pattern.split("-")[0].upper() for mc in self.markets})
            symbols = [s for s in symbols if s]
            self._okx_feed = OkxPriceFeed(symbols=symbols)
            self._okx_ws_task = asyncio.create_task(self._okx_feed.run())
            thresholds = ", ".join(f"{s}:{V3_VOLATILITY_THRESHOLD.get(s, V3_VOLATILITY_THRESHOLD_DEFAULT)}%"
                                   for s in symbols)
            self._log(f"VOLATILITY GATE: ON ({thresholds}) — OKX feed started for {symbols}", "INFO")
        else:
            self._log("VOLATILITY GATE: OFF", "INFO")

        if BOT_STOP_THRESHOLD > 0:
            self._log(f"CAPITAL PROTECTION: stop<${BOT_STOP_THRESHOLD:.0f}", "INFO")

        # Start PM WS orderbook feed for real-time prices (fallback to REST if unavailable)
        if V3_PM_WS_ENABLED and PolymarketOrderbookFeed:
            self._pm_feed = PolymarketOrderbookFeed()
            self._pm_ws_task = asyncio.create_task(self._pm_feed.run())
            self._log("PM WS FEED: ON — real-time orderbook prices", "INFO")
            # Subscribe current window's tokens immediately
            window_ts = self.get_current_window_ts()
            for mc in self.markets:
                slug = f"{mc.slug_pattern}-{window_ts}"
                market = await self.fetch_market_by_slug(slug)
                if market:
                    clob_tokens = market.get("clobTokenIds", [])
                    if isinstance(clob_tokens, str):
                        clob_tokens = json.loads(clob_tokens)
                    if clob_tokens:
                        await self._pm_feed.subscribe(clob_tokens)
        elif V3_PM_WS_ENABLED:
            self._log("PM WS FEED: SKIP — module not available", "WARN")

        # Start WS prediction client for zero-latency predictions
        if V3_PREDICTION_SOURCE == "ws" and WsPredictionClient:
            if V3_PREDICTION_WS_URL and PREDICTION_BOOST_USERNAME and PREDICTION_BOOST_PASSWORD:
                self._ws_pred_client = WsPredictionClient(
                    ws_url=V3_PREDICTION_WS_URL,
                    username=PREDICTION_BOOST_USERNAME,
                    password=PREDICTION_BOOST_PASSWORD,
                )
                self._ws_pred_client.start()
                self._log(f"WS PREDICTION: ON — {V3_PREDICTION_WS_URL}", "INFO")
            else:
                self._log("WS PREDICTION: SKIP — V3_PREDICTION_WS_URL or credentials not set", "WARN")
        elif V3_PREDICTION_SOURCE == "ws":
            self._log("WS PREDICTION: SKIP — module not available", "WARN")

        # Log regime detection status
        if V3_REGIME_CHOP_TP > 0:
            self._log(
                f"REGIME: ON CHOP_TP={V3_REGIME_CHOP_TP:.0%} | "
                f"Conf>={V3_REGIME_TREND_CONFIDENCE_GATE:.0%} Mom>={V3_REGIME_TREND_MOMENTUM_GATE} | "
                f"FlipRate>{V3_REGIME_FLIP_RATE_THRESHOLD}/min | "
                f"Noise>{V3_REGIME_NOISE_THRESHOLD:.0%} | "
                f"Spread>{V3_REGIME_SPREAD_MULTIPLIER:.1f}×avg", "INFO")
        else:
            self._log("REGIME: OFF (V3_REGIME_CHOP_TP=0)", "INFO")

        # Log scan optimization
        self._log(f"SCAN: midpoint={'ON' if V3_MIDPOINT_SCAN_ENABLED else 'OFF'} | "
                  f"pre-discover={V3_PRE_DISCOVER_SEC:.0f}s | "
                  f"PM-WS={'ON' if V3_PM_WS_ENABLED else 'OFF'} | "
                  f"pred-source={V3_PREDICTION_SOURCE.upper()}", "INFO")

        while self.running:
            try:
                await self.scan_markets()
                self._render_dashboard()
            except Exception as e:
                self._log(f"Scan error: {e}", "ERROR")
            await asyncio.sleep(CHECK_INTERVAL)


def list_markets():
    print("\n" + "=" * 60)
    print("CONFIGURED MARKETS")
    print("=" * 60)

    if not MARKETS_CONFIG_FILE.exists():
        print("No markets config found:", MARKETS_CONFIG_FILE)
        return

    with open(MARKETS_CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    for m in data.get("markets", []):
        status = "[ON] " if m.get("enabled") else "[OFF]"
        print(f"  {status} {m.get('name', 'Unknown')} ({m.get('slug_pattern')})")
        if m.get("notes"):
            print(f"       {m.get('notes')}")

    # V2.8: Removed trader_observed_markets display (tracking removed)


def _parse_args():
    """Parse full CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Konis Polymarket Scalping Bot V3 - Dual-Side Momentum Strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python konis-scalping-bot-v3.py
  python konis-scalping-bot-v3.py --list-markets
  python konis-scalping-bot-v3.py -m custom_markets.json
  python konis-scalping-bot-v3.py -e /path/to/.env.production
  python konis-scalping-bot-v3.py -m markets.json -e .env.staging
        """
    )
    parser.add_argument(
        "--markets", "-m",
        type=str,
        default=None,
        help="Path to markets JSON config file (default: scalping_markets.json)"
    )
    parser.add_argument(
        "--env", "-e",
        type=str,
        default=None,
        help="Path to .env config file (default: ../polymarket_konis/.env)"
    )
    parser.add_argument(
        "--list-markets",
        action="store_true",
        help="List configured markets and exit"
    )
    parser.add_argument(
        "--begin-balance",
        type=float,
        default=None,
        help="Override begin session balance for first window (e.g., --begin-balance=416)"
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Disable TUI rendering (logs only, no terminal dashboard)"
    )
    return parser.parse_args()


async def _amain():
    args = _parse_args()

    if args.list_markets:
        list_markets()
        return

    # Print active config paths for transparency
    print(f"[CONFIG] .env: {_env_path}")
    print(f"[CONFIG] Markets: {MARKETS_CONFIG_FILE}")

    # Headless: add stdout handler so logs print to terminal (TUI dashboard suppressed)
    if args.headless:
        import sys
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(stdout_handler)

    bot = ScalpingBotV3(headless=args.headless)
    try:
        await bot.run()
    finally:
        # Clean shutdown of persistent HTTP client
        if bot._http_client and not bot._http_client.is_closed:
            await bot._http_client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        print("\nStopped.")
