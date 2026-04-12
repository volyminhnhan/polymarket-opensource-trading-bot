"""
V7 Bot Config & Logging — Environment variables, logging setup, and market loading.

Centralized config for the V7 prediction hedge bot (5-minute markets).
All env vars are loaded once and exported as module-level constants.
"""

import io
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List

# --- Path setup ---
SCRIPT_DIR = Path(__file__).resolve().parent  # lib/
POLYMARKET_KONIS_DIR = SCRIPT_DIR.parent      # project root
PROJECT_ROOT = POLYMARKET_KONIS_DIR

for _path in [str(PROJECT_ROOT), str(SCRIPT_DIR)]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

# --- Configuration from env ---
BOT_ID = os.getenv("BOT_ID", "")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")
CHECK_INTERVAL = int(os.getenv("SCALPING_CHECK_INTERVAL", "2"))
BG_SYNC_INTERVAL = int(os.getenv("V7_BG_SYNC_INTERVAL", "5"))  # background sync when WS active
WS_STALE_THRESHOLD = float(os.getenv("V7_WS_STALE_THRESHOLD", "3"))  # seconds — REST fallback if WS price stale
SIMULATED_BALANCE = float(os.getenv("SCALPING_SIMULATED_BALANCE", "1000.0"))

# V7 strategy parameters — prediction entry + hold for resolution (5m markets)
POSITION_SIZE_USD = float(os.getenv("V7_POSITION_SIZE_USD", "20"))
ENTRY_MINUTE = float(os.getenv("V7_ENTRY_MINUTE", "1"))
ENTRY_MAX_MINUTE = float(os.getenv("V7_ENTRY_MAX_MINUTE", "0"))  # 0 = disabled; skip entry after this minute
LOCK_PREDICTION_AT_ENTRY_MINUTE = os.getenv("V7_LOCK_PREDICTION_AT_ENTRY_MINUTE", "true").lower() in ("true", "1", "yes")
MIN_CONFIDENCE = float(os.getenv("V7_MIN_CONFIDENCE", "0.70"))
MIN_QUALITY_SCORE = float(os.getenv("V7_MIN_QUALITY_SCORE", "0.55"))  # 0 = disabled; skip entry if quality_score < this
MIN_CROSS_PAIRS_AGREEMENT = int(os.getenv("V7_MIN_CROSS_PAIRS_AGREEMENT", "3"))  # 0 = disabled; skip entry if cross_pair_agreement < this (0-3)
ENTRY_MIN_MOMENTUM = float(os.getenv("V7_ENTRY_MIN_MOMENTUM", "0"))  # 0 = disabled; skip entry if |momentum_accumulated| < this AND direction misaligned
MIN_WEIGHTED_SIGNAL = float(os.getenv("V7_MIN_WEIGHTED_SIGNAL", "0"))  # 0 = disabled; skip entry if abs(weighted_signal) < this
PRED_CONFIRM_TICKS = int(os.getenv("V7_PRED_CONFIRM_TICKS", "3"))  # 0 = disabled; require N consecutive HTTP prediction polls on same side before entry
BUY_BAND_LOW = float(os.getenv("V7_BUY_BAND_LOW", "0.40"))
BUY_BAND_HIGH = float(os.getenv("V7_BUY_BAND_HIGH", "0.65"))
BUY_MAX_FILL_SLIPPAGE = float(os.getenv("V7_BUY_MAX_FILL_SLIPPAGE", "0.05"))  # 5% slippage tolerance above BUY_BAND_HIGH
BUY_MAX_ABOVE_MID = float(os.getenv("V7_BUY_MAX_ABOVE_MID", "0"))  # 0=disabled; cap FAK price at mid + this (e.g. 0.01 = mid + 1 cent)
HEDGE_ENTRY_PRICE_USD = float(os.getenv("V7_HEDGE_ENTRY_PRICE_USD", "20"))
HEDGE_PRICE = float(os.getenv("V7_HEDGE_PRICE", "0"))  # Limit price for immediate entry hedge (0=disabled)
TP_RATIO = float(os.getenv("V7_TP_RATIO", "0"))  # 0 = disabled; 0.20 = sell all at +20%
TP_IGNORE_VWAP = os.getenv("V7_TP_IGNORE_VWAP", "false").lower() in ("true", "1", "yes")  # TP uses mid price instead of VWAP
ENTRY_PRICE = float(os.getenv("V7_ENTRY_PRICE", os.getenv("V7_ENFORCE_PRICE", "0")))  # 0 = disabled; buy when any side mid >= this
FAVOR_PREDICTION = os.getenv("V7_FAVOR_PREDICTION", "true").lower() in ("true", "1", "yes")  # True: prediction drives side, ignore ENTRY_PRICE side selection
SL_RATIO = float(os.getenv("V7_SL_RATIO", "0"))  # 0 = disabled; 0.60 = sell all at -60% loss on main side
# Originals — snapshot at import, used by LOOSEN/TIGHTEN dashboard commands to revert
TP_RATIO_ORIGINAL = TP_RATIO
SL_RATIO_ORIGINAL = SL_RATIO
# Per-pair SL:TP overrides — format: "sol:0.25:0.60,btc:0.30:0.50" (symbol:sl:tp)
# Overrides global TP_RATIO/SL_RATIO for specific pairs. 0 = use global default.
_pair_sl_tp_raw = os.getenv("V7_PAIR_SL_TP", "")
PAIR_SL_TP: dict[str, tuple[float, float]] = {}  # {symbol: (sl_ratio, tp_ratio)}
for _part in _pair_sl_tp_raw.split(","):
    _part = _part.strip()
    if ":" in _part:
        _pieces = _part.split(":")
        if len(_pieces) == 3:
            _sym = _pieces[0].strip().upper()
            PAIR_SL_TP[_sym] = (float(_pieces[1].strip()), float(_pieces[2].strip()))
# Stepped Trailing Stop Loss — ratchets SL floor up as profit grows
TSL_ENABLED = os.getenv("V7_TSL_ENABLED", "true").lower() in ("true", "1", "yes")
TSL_STEP = float(os.getenv("V7_TSL_STEP", "0.08"))  # 8% per step (L1 at 16%, floor=+8%)
TSL_GRACE_SECONDS = int(os.getenv("V7_TSL_GRACE_SECONDS", "10"))  # Skip sell for N seconds after entry
TAKER_FEE_PCT = float(os.getenv("V7_TAKER_FEE_PCT", "0.02"))  # 2% Polymarket taker fee for PnL estimation
# Time stop — exit at current price if held too long without hitting TP (0 = disabled)
TIME_STOP_SEC = int(os.getenv("V7_TIME_STOP_SEC", "0"))  # seconds after entry; 120 = exit after 2min if no TP
TIME_STOP_MIN_PNL = float(os.getenv("V7_TIME_STOP_MIN_PNL", "0"))  # min PnL% to accept time exit (0 = breakeven+)
EXIT_PRICE = float(os.getenv("V7_EXIT_PRICE", "0.95"))  # 0 = disabled; sell entry side when mid >= this price
MIN_LIQUIDITY = float(os.getenv("V7_MIN_LIQUIDITY", "100"))  # Skip entry if total order book depth < this USD
# Dual mode — after enforce-price triggers on one side, also buy opposite side (independent TP/SL)
DUAL_MODE_ENABLED = os.getenv("V7_DUAL_MODE_ENABLED", "false").lower() in ("true", "1", "yes")
DUAL_POSITION_SIZE_USD = float(os.getenv("V7_DUAL_POSITION_SIZE_USD", "10"))
DUAL_TP_RATIO = float(os.getenv("V7_DUAL_TP_RATIO", "0.23"))
DUAL_SL_RATIO = float(os.getenv("V7_DUAL_SL_RATIO", "0.65"))
# Cheap mode — buy any side that drops below CHEAP_ENTRY_PRICE, skip timing/prediction/band
CHEAP_MODE = os.getenv("V7_CHEAP_MODE", "false").lower() in ("true", "1", "yes")
CHEAP_ENTRY_PRICE = float(os.getenv("V7_CHEAP_ENTRY_PRICE", "0.18"))
# Counter-trend: confirm initial trend via prediction, enter when trend flips + price is cheap
CHEAP_COUNTER_TREND = os.getenv("V7_CHEAP_COUNTER_TREND", "false").lower() in ("true", "1", "yes")
# Manage-only mode — sync positions from API + manage SL/TP, no new entries
MANAGE_POSITIONS_ONLY = os.getenv("V7_MANAGE_POSITIONS_ONLY", "false").lower() in ("true", "1", "yes")
# Pace detection: enter when Polymarket price of one side rises > X% in Y seconds
# Bypasses ENTRY_MINUTE, MIN_CONFIDENCE, MIN_CROSS_PAIRS, MIN_QUALITY gates
# Uses raw Polymarket WS price momentum — no prediction/OKX dependency
PACE_DETECT = os.getenv("V7_PACE_DETECT", "false").lower() in ("true", "1", "yes")
PACE_DETECT_PCT = float(os.getenv("V7_PACE_DETECT_PCT", "3.0"))         # min % price increase to trigger
PACE_DETECT_WINDOW_SEC = int(os.getenv("V7_PACE_DETECT_WINDOW_SEC", "7"))  # lookback window in seconds
PACE_DETECT_PRICE_CAP = float(os.getenv("V7_PACE_DETECT_PRICE_CAP", "0.65"))  # max entry price
PACE_DETECT_MAX_SEC = int(os.getenv("V7_PACE_DETECT_MAX_SEC", "45"))     # only trigger in first N seconds
# Hyper prediction — enter based on Hyperliquid trade direction from hyper-watch
# Bypasses ENTRY_MINUTE, MIN_CONFIDENCE, prediction gates. Respects BUY_BAND.
HYPER_PREDICTION = os.getenv("V7_HYPER_PREDICTION", "false").lower() in ("true", "1", "yes")
HYPER_PREDICTION_API_URL = os.getenv("V7_HYPER_PREDICTION_API_URL", "https://godeye.konis.ai/api/v7/hyper-prediction")
HYPER_PREDICTION_ENTRY_SEC = int(os.getenv("V7_HYPER_PREDICTION_ENTRY_SEC", "20"))  # earliest entry (seconds)
HYPER_PREDICTION_MAX_SEC = int(os.getenv("V7_HYPER_PREDICTION_MAX_SEC", "60"))      # latest entry (seconds)
# Hyper boost — if hyper prediction agrees with main prediction AND hyper conf > threshold,
# relax MIN_CONFIDENCE and extend BUY_BAND (relative % adjustment)
HYPER_BOOST_ENABLED = os.getenv("V7_HYPER_BOOST_ENABLED", "false").lower() in ("true", "1", "yes")
HYPER_BOOST_MIN_CONF = float(os.getenv("V7_HYPER_BOOST_MIN_CONF", "0.40"))      # hyper conf threshold
HYPER_BOOST_CONF_RELAX = float(os.getenv("V7_HYPER_BOOST_CONF_RELAX", "0.10"))  # lower MIN_CONFIDENCE by X% (relative)
HYPER_BOOST_BAND_EXTEND = float(os.getenv("V7_HYPER_BOOST_BAND_EXTEND", "0.10"))  # extend BUY_BAND by X% (relative)
HYPER_BOOST_MOMENTUM_RELAX = float(os.getenv("V7_HYPER_BOOST_MOMENTUM_RELAX", "0.70"))  # lower ENTRY_MIN_MOMENTUM by X% (relative)
# Whale trend scalping — enter on trend CHANGE (NEUTRAL->BULLISH/BEARISH)
# Captures initial trend at session start, acts only when it flips
WHALE_TREND_SCALPING = os.getenv("V8_WHALE_TREND_SCALPING", "false").lower() in ("true", "1", "yes")
WHALE_TREND_API_URL = os.getenv("V8_WHALE_TREND_API_URL", "https://godeye.konis.ai/api/v7/whale-trend")
# Oracle gate — CEX sigmoid oracle as early-entry accelerator
# When oracle divergence confirms prediction direction, allows entry before ENTRY_MINUTE
ORACLE_GATE_ENABLED = os.getenv("V7_ORACLE_GATE_ENABLED", "false").lower() in ("true", "1", "yes")
ORACLE_K = float(os.getenv("V7_ORACLE_K", "200"))  # sigmoid steepness (200: 0.58% move -> p=0.76)
ORACLE_MIN_DIFF = float(os.getenv("V7_ORACLE_MIN_DIFF", "0.02"))  # min oracle-vs-poly divergence
ORACLE_EARLY_ENTRY_SEC = int(os.getenv("V7_ORACLE_EARLY_ENTRY_SEC", "30"))  # earliest oracle entry (seconds)
ORACLE_MOMENTUM_WINDOW = int(os.getenv("V7_ORACLE_MOMENTUM_WINDOW", "30"))  # ticks for linear regression
ORACLE_PREDICT_SECONDS = int(os.getenv("V7_ORACLE_PREDICT_SECONDS", "30"))  # extrapolation horizon
WINDOW_MINUTES = int(os.getenv("V7_WINDOW_MINUTES", "5"))  # market window: 5 or 15
WINDOW_SECONDS = WINDOW_MINUTES * 60
SYNC_GRACE_SECONDS = int(os.getenv("V7_SYNC_GRACE_SECONDS", "90"))  # API eventual consistency grace period
# Cross-market mode: use cross-market entry timing (ENTRY_MIN-ENTRY_MAX) + prediction confirmation
# V7_CROSS_MARKET_TRADE_WINDOW overrides WINDOW_MINUTES when set (e.g. 5 for 5m, 15 for 15m)
CROSS_MARKET_TRADE = os.getenv("V7_CROSS_MARKET_TRADE", "false").lower() in ("true", "1", "yes")
_cross_window_raw = os.getenv("V7_CROSS_MARKET_TRADE_WINDOW", "")
if CROSS_MARKET_TRADE and _cross_window_raw:
    WINDOW_MINUTES = int(_cross_window_raw)
    WINDOW_SECONDS = WINDOW_MINUTES * 60
CROSS_MARKET_ENTRY_MIN = float(os.getenv("V7_CROSS_MARKET_ENTRY_MIN", "5.0"))  # earliest entry minute
CROSS_MARKET_ENTRY_MAX = float(os.getenv("V7_CROSS_MARKET_ENTRY_MAX", "10.0"))  # latest entry minute

# Volatility-based OKX WS mode: "hedge" (dynamic hedge), "gate" (block flat entries), or "false"
# Backwards compat: "true"/"1"/"yes" -> "hedge"
_vol_mode_raw = os.getenv("V7_VOLATILITY_HEDGE_ENABLED", "false").lower().strip()
if _vol_mode_raw in ("true", "1", "yes", "hedge"):
    VOLATILITY_MODE = "hedge"
elif _vol_mode_raw == "yolo":
    VOLATILITY_MODE = "yolo"
elif _vol_mode_raw == "gate":
    VOLATILITY_MODE = "gate"
else:
    VOLATILITY_MODE = "off"
VOLATILITY_HEDGE_ENABLED = VOLATILITY_MODE != "off"
VOLATILITY_HEDGE_PRICE = float(os.getenv("V7_VOLATILITY_HEDGE_PRICE", "0.10"))
# Per-market thresholds: "btc:0.05,eth:0.20" or single value "0.15" for all
_vht_raw = os.getenv("V7_VOLATILITY_HEDGE_THRESHOLD", "0.15")
VOLATILITY_HEDGE_THRESHOLD: dict[str, float] = {}
_VHT_DEFAULT = 0.15
for _part in _vht_raw.split(","):
    _part = _part.strip()
    if ":" in _part:
        _sym, _val = _part.split(":", 1)
        VOLATILITY_HEDGE_THRESHOLD[_sym.strip().upper()] = float(_val.strip())
    elif _part:
        _VHT_DEFAULT = float(_part)
VOLATILITY_HEDGE_THRESHOLD_DEFAULT = _VHT_DEFAULT
# PTB source: "okx" uses OKX window-open price (fast), "polymarket" uses Gamma API + browser fallback
PTB_SOURCE = os.getenv("V7_PTB_SOURCE", "okx").lower().strip()

# Choppy market signal file — written by external watchdog, bot skips entries when file exists
CHOPPY_SIGNAL_FILE = os.getenv("V7_CHOPPY_SIGNAL_FILE",
                                str(SCRIPT_DIR / ".choppy_pause"))

# Remote watchdog API (basic auth). When CHOPPY_API_URL is set, bot calls the
# konis-backend endpoint instead of checking the local file. Same creds for
# legacy and container bots — stored centrally in konis-backend .env.
CHOPPY_API_URL = os.getenv("V7_CHOPPY_API_URL", "").strip()
CHOPPY_API_USER = os.getenv("V7_CHOPPY_API_USER", "").strip()
CHOPPY_API_PASS = os.getenv("V7_CHOPPY_API_PASS", "").strip()

# Per-market OKX price change % threshold to ignore normal TP (let position ride momentum)
# Format: "btc:0.11,eth:0.1" or single value "0.15" for all
_itp_raw = os.getenv("V7_IGNORE_TP_PERC", "")
IGNORE_TP_PERC: dict[str, float] = {}
_ITP_DEFAULT = 0.0  # 0 = feature disabled
for _part in _itp_raw.split(","):
    _part = _part.strip()
    if ":" in _part:
        _sym, _val = _part.split(":", 1)
        IGNORE_TP_PERC[_sym.strip().upper()] = float(_val.strip())
    elif _part:
        _ITP_DEFAULT = float(_part)
IGNORE_TP_PERC_DEFAULT = _ITP_DEFAULT
# Pullback from peak profit % to trigger exit while IGNORE_TP is active (0.05 = 5%)
IGNORE_TP_PULLBACK = float(os.getenv("V7_IGNORE_TP_PULLBACK", "0.05"))


# Prediction flip — exit position when fresh prediction flips to opposite with high confidence
EXIT_BY_PREDICTION = os.getenv("V7_EXIT_BY_PREDICTION", "true").lower() in ("true", "1", "yes")
MIN_EXIT_CONFIDENCE = float(os.getenv("V7_MIN_EXIT_CONFIDENCE", "0.94"))

# Maker mode — hybrid entry: maker (GTC limit) for medium signals, taker (FAK) for strong signals
# confidence >= MIN_CONFIDENCE and < MAKER_CONFIDENCE → maker at bid+1tick
# confidence >= MAKER_CONFIDENCE → taker at best ask (current behavior)
# spread < MAKER_MIN_SPREAD → always taker (spread too tight for maker edge)
MAKER_MODE = os.getenv("V7_MAKER_MODE", "false").lower() in ("true", "1", "yes")
MAKER_CONFIDENCE = float(os.getenv("V7_MAKER_CONFIDENCE", "0.80"))
MAKER_MIN_SPREAD = float(os.getenv("V7_MAKER_MIN_SPREAD", "0.03"))
MAKER_OFFSET = float(os.getenv("V7_MAKER_OFFSET", "0.01"))  # bid + offset = maker price (0.01 = 1 tick)

# ML model gate — XGBoost classifier as additional entry filter
# When enabled, prediction API must pass first, then ML model must also approve entry
ML_MODEL_ENABLED = os.getenv("V7_ML_MODEL_ENABLED", "false").lower() in ("true", "1", "yes")
ML_MODEL_MODE = os.getenv("V7_ML_MODEL_MODE", "tick_cross").lower().strip()  # "early"|"late"|"tick"|"tick_cross"
ML_MIN_PROBA = float(os.getenv("V7_ML_MIN_PROBA", "0.50"))  # min ML probability to allow entry (0.0-1.0)

# Enabled markets (comma-separated short names: eth,btc,sol,xrp)
# Empty = all markets in JSON are enabled
ENABLED_MARKETS = [m.strip().lower() for m in
                   os.getenv("V7_ENABLED_MARKETS", "").split(",") if m.strip()]

# Prediction source: "redis", "http", or "ws" (WebSocket for zero-latency)
PREDICTION_SOURCE = os.getenv("V7_PREDICTION_SOURCE",
                              os.getenv("PREDICTION_SOURCE", "http")).lower()
PREDICTION_API_URL = os.getenv("V7_PREDICTION_API_URL", "")
PREDICTION_API_USER = os.getenv("PREDICTION_USERNAME", "")
PREDICTION_API_PASS = os.getenv("PREDICTION_PASSWORD", "")
# WebSocket prediction URL (ws:// or wss://) — used when PREDICTION_SOURCE=ws
PREDICTION_WS_URL = os.getenv("V7_PREDICTION_WS_URL", "")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# Redis prediction key template — supports {symbol} placeholder for multi-symbol
REDIS_PREDICTION_KEY = os.getenv("V7_REDIS_PREDICTION_KEY", "polymarket_latest")

# API endpoints
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
DATA_HOST = os.getenv("DATA_HOST", "https://data-api.polymarket.com")

# Wallet
PRIVATE_KEY = os.getenv("PRIVATE_KEY") or os.getenv("PK") or ""
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE") or "0")
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS") or os.getenv("POLYMARKET_FUNDER") or None

MAX_RETRIES = int(os.getenv("MAX_RETRY", "5"))

# Session state persistence — saved/loaded on each state change to prevent double-entry on restart
STATE_FILE = os.getenv("V7_STATE_FILE", str(SCRIPT_DIR / "v7_session_state.json"))

# Trading window (UTC) — bot only enters new positions within this window
# Format: "HH:MM" (e.g. "03:30") or just hour "3" (= "03:00"). Set "" to disable (trade 24h).
# Example: VN 10:30-15:30 = UTC 03:30-08:30
def _parse_trading_time(raw: str) -> int:
    """Parse time string to minutes since midnight. Returns -1 if empty/disabled."""
    raw = raw.strip()
    if not raw or raw == "-1":
        return -1
    if ":" in raw:
        h, m = raw.split(":")
        return int(h) * 60 + int(m)
    return int(raw) * 60

TRADING_WINDOW_START = _parse_trading_time(os.getenv("TRADING_WINDOW_START_UTC", ""))
TRADING_WINDOW_END = _parse_trading_time(os.getenv("TRADING_WINDOW_END_UTC", ""))
MONGODB_URL = os.getenv("MONGODB_URL", "")
MONGODB_DB = os.getenv("MONGODB_DB", "konis_polymarket")

# Capital protection — stop new entries when account value drops below threshold
BOT_STARTING_CAPITAL = float(os.getenv("BOT_STARTING_CAPITAL", "0"))
BOT_STOP_THRESHOLD = float(os.getenv("BOT_STOP_THRESHOLD", "0"))

# --- Logging ---
LOG_DIR = POLYMARKET_KONIS_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


class TeeStdout(io.TextIOBase):
    """Tee stdout to both terminal and a log file (ANSI codes stripped in file)."""

    def __init__(self, original_stdout, log_path: Path):
        self._orig = original_stdout
        self._file = open(log_path, "a", encoding="utf-8")

    def write(self, s):
        if s:
            self._orig.write(s)
            self._file.write(_ANSI_RE.sub("", s))
        return len(s) if s else 0

    def flush(self):
        self._orig.flush()
        self._file.flush()

    def rotate(self, new_path: Path):
        self._file.close()
        self._file = open(new_path, "a", encoding="utf-8")

    @property
    def encoding(self):
        return self._orig.encoding

    def isatty(self):
        return self._orig.isatty()

    def fileno(self):
        return self._orig.fileno()


# Global reference for rotation
_tee: TeeStdout | None = None


def get_log_file() -> Path:
    now = datetime.now()
    # Window-aligned log rotation
    wm = (now.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    wt = now.replace(minute=wm, second=0, microsecond=0)
    day_dir = LOG_DIR / now.strftime('%Y-%m-%d') / "v7"
    day_dir.mkdir(parents=True, exist_ok=True)
    return day_dir / f"{now.strftime('%Y-%m-%d')}-{wt.strftime('%H%M')}-scalp-v7.log"


def setup_logging() -> logging.Logger:
    global _tee
    log_file = get_log_file()
    handlers = [
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.__stdout__),
    ]
    # Only tee stdout when running as a service (no tty) so dashboard prints go to logs
    if not sys.__stdout__.isatty():
        _tee = TeeStdout(sys.__stdout__, log_file)
        sys.stdout = _tee
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    lgr = logging.getLogger("scalp_v7")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return lgr


def rotate_tee(new_path: Path):
    """Rotate the stdout tee to a new log file."""
    if _tee is not None:
        _tee.rotate(new_path)


def load_markets(markets_file: Path) -> List[dict]:
    """Load markets from JSON, filtered by V7_ENABLED_MARKETS env var.

    V7_ENABLED_MARKETS=eth,btc -> only markets whose slug starts with 'eth-' or 'btc-'.
    Empty env var -> all markets in JSON are loaded.
    """
    if not markets_file.exists():
        return [{"slug_pattern": f"btc-updown-{WINDOW_MINUTES}m", "name": f"BTC {WINDOW_MINUTES}m"}]
    try:
        with open(markets_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        all_markets = data.get("markets", [])
        if ENABLED_MARKETS:
            return [m for m in all_markets
                    if any(m.get("slug_pattern", "").startswith(e + "-")
                           for e in ENABLED_MARKETS)]
        return all_markets
    except Exception as e:
        logging.getLogger("scalp_v7").error(f"Failed to load markets config: {e}")
        return [{"slug_pattern": f"btc-updown-{WINDOW_MINUTES}m", "name": f"BTC {WINDOW_MINUTES}m"}]
