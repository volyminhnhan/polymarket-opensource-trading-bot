#!/usr/bin/env python3
"""
Konis Polymarket Scalping Bot v7 — Prediction Entry + Hold for Resolution (5m markets)

Strategy per 5-minute window:
  1. ENTRY — After ENTRY_MINUTE: enter via price threshold (ENTRY_PRICE) and/or prediction.
  2. HEDGE — Volatility-based hedge via OKX WS pricing.
  3. HOLD — Wait for market resolution. No sells. Winning side -> $1, losing -> $0.
  4. ARCHIVE — At new window: archive PnL, clear positions.

Run: python konis-trading-v7.py --env .env --dry-run
"""

import argparse
import asyncio
import hashlib
import importlib.util
import json
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_lib_dir = _script_dir / "lib"
for _p in [str(_script_dir), str(_lib_dir)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dotenv import load_dotenv

# --- Early CLI args + env loading ---
def _parse_early_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env", "-e", type=str, default=None)
    parser.add_argument("--markets", "-m", type=str, default=None)
    args, _ = parser.parse_known_args()
    return args

_early = _parse_early_args()
load_dotenv(Path(_early.env) if _early.env else _script_dir / ".env")

# --- Import kebab-case v7 modules ---
def _imp(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_cfg = _imp("v7_cfg", str(_lib_dir / "v7-bot-config-and-logging.py"))
_v7_pos = _imp("v7_pos", str(_lib_dir / "v7-hedge-position.py"))
_v7_engine = _imp("v7_engine", str(_lib_dir / "v7-prediction-hedge-strategy-engine.py"))
_v7_actions = _imp("v7_act", str(_lib_dir / "v7-seed-and-cut-strategy-actions.py"))
_v7_dash = _imp("v7_dash", str(_lib_dir / "v7-dashboard-render-helper.py"))
_v7_pace = _imp("v7_pace", str(_lib_dir / "v7-pace-detect-entry-logic.py"))
_v7_sync = _imp("v7_sync", str(_lib_dir / "v7-position-sync-from-api.py"))
_okx_feed = _imp("okx_feed", str(_lib_dir / "binance-ws-price-feed.py"))
_oracle_mod = _imp("oracle_gate", str(_lib_dir / "v7-oracle-gate-computation.py"))
_pm_ws = _imp("pm_ws", str(_lib_dir / "polymarket-ws-orderbook-feed.py"))
_ws_handler_mod = _imp("ws_handler", str(_lib_dir / "v7-ws-price-handler.py"))
_ws_pred_mod = _imp("ws_pred", str(_lib_dir / "v7-ws-prediction-client.py"))
_v7_entry = _imp("v7_entry", str(_lib_dir / "v7-entry-side-selection-logic.py"))
_subgraph = _imp("subgraph_pos", str(_lib_dir / "subgraph_positions.py"))

from terminal_ui import ScalpingDashboard  # type: ignore
try:
    from mongo_persistence import PolymarketMongoPersistence  # type: ignore
except Exception:
    PolymarketMongoPersistence = None  # type: ignore

BotState = _v7_pos.BotState
logger = _cfg.setup_logging()
MARKETS_FILE = Path(_early.markets) if _early.markets else _script_dir / f"scalping_markets_{_cfg.WINDOW_MINUTES}m.json"
REDIS_HEALTH_CHECK_INTERVAL = 30  # seconds between Redis connectivity checks


class ScalpingBotv7:
    """v7 Prediction Entry Bot — main orchestrator."""

    def __init__(self, dry_run: bool = False, headless: bool = False):
        self.dry_run = dry_run
        self.headless = headless
        self.state = BotState()
        self.markets = _cfg.load_markets(MARKETS_FILE)
        self.running = True
        self.trader = None
        self.current_balance = _cfg.SIMULATED_BALANCE if dry_run else 0.0
        self._redis = None
        self._last_window_ts: int = 0
        self._seeded_this_window: set = set()
        self._known_markets: dict = {}
        self._market_cache: dict = {}
        self._redis_healthy: bool = True
        self._last_redis_check: float = 0.0
        self._manual_exit_all: bool = False  # keystroke 'K' flag
        self._manual_entry_cmd: str | None = None  # dashboard manual entry command
        self._unredeemed_value: float = 0.0
        self._capital_ok: bool = True
        # Counter-trend: track confirmed trend per symbol ("UP"/"DOWN"/None)
        self._window_trends: dict[str, str] = {}
        # Locked prediction at entry_minute: {symbol: (prediction, confidence)}
        self._locked_prediction: dict[str, tuple] = {}
        # Prediction confirmation: counts consecutive HTTP polls with same side
        self._pred_confirm: dict[str, tuple] = {}  # symbol -> (side, count)
        # Cached prediction results (written by read_prediction, read by WS handler)
        self._last_prediction: dict[str, tuple] = {}  # symbol -> prediction tuple
        # Pre-discovered next-window markets (populated ~10s before window end)
        self._next_window_markets: dict = {}   # cid -> info
        self._next_window_cache: dict = {}     # slug_pattern -> market
        self._next_window_ts: int = 0          # which window was pre-discovered
        self._okx_feed = None
        self._okx_ws_task = None
        self._pm_feed = None
        self._pm_ws_task = None
        self._ws_handler = None
        self._v7_pace = _v7_pace  # shared module ref for WS handler
        self._ws_pred_client = None  # WS prediction client (zero-latency)
        # Regime detection — local price/spread history buffers (ring buffers, ~5min at 2s ticks)
        self._price_ticks: dict[str, deque] = {}   # market_symbol -> deque of (timestamp, yes_mid)
        self._spread_ticks: dict[str, deque] = {}  # market_symbol -> deque of (timestamp, spread_pct)
        self._last_regime: dict[str, str] = {}     # market_symbol -> "TREND" / "CHOP"
        self._regime_pending: dict[str, str] = {}   # pending regime before debounce confirms
        self._regime_pending_count: dict[str, int] = {}  # consecutive count of pending regime
        self.session_pnl = 0.0
        # Persistent begin_session_balance — survives across windows & restarts
        self._begin_session_balance: float = 0.0
        self._need_begin_balance_capture: bool = True
        self._persistent_balance_path = Path(_cfg.STATE_FILE).parent / "v7_persistent_balance.json"
        self._last_configs_path = Path(_cfg.STATE_FILE).parent / "v7_last_configs.env"
        mode = "DRY-RUN" if dry_run else "LIVE"
        self.dashboard = ScalpingDashboard(instance_id=f"V7-{mode}")
        self.dashboard.headless = headless
        # Config tag for dashboard display
        _tp_tag = f"TP{_cfg.TP_RATIO:.0%}" if _cfg.TP_RATIO > 0 else ""
        _sl_tag = f"SL{_cfg.SL_RATIO:.0%}" if _cfg.SL_RATIO > 0 else ""
        _hb_tag = ""
        if getattr(_cfg, "HYPER_BOOST_ENABLED", False):
            _hb_tag = (f"HB{int(_cfg.HYPER_BOOST_MIN_CONF*100)}/"
                       f"-{int(_cfg.HYPER_BOOST_CONF_RELAX*100)}%/"
                       f"+{int(_cfg.HYPER_BOOST_BAND_EXTEND*100)}% ")
        self.dashboard.config_tag = (
            f"C{int(_cfg.MIN_CONFIDENCE*100)} "
            f"WS{_cfg.MIN_WEIGHTED_SIGNAL:.2f} "
            f"QS{_cfg.MIN_QUALITY_SCORE:.2f} "
            f"E{int(_cfg.ENTRY_MINUTE*60)} "
            f"B{_cfg.BUY_BAND_LOW:.2f}-{_cfg.BUY_BAND_HIGH:.2f} "
            f"CP{_cfg.MIN_CROSS_PAIRS_AGREEMENT} "
            + _hb_tag
            + (f"{_tp_tag} " if _tp_tag else "")
            + (f"{_sl_tag} " if _sl_tag else "")
            + f"${_cfg.POSITION_SIZE_USD}"
        )
        self._dashboard_redis = None  # lazy-init for Redis TUI publish
        self.mongo = None
        if _cfg.MONGODB_URL and PolymarketMongoPersistence:
            try:
                self.mongo = PolymarketMongoPersistence(_cfg.MONGODB_URL, _cfg.MONGODB_DB)
            except Exception as e:
                logger.warning(f"MongoDB init failed: {e}")
        logger.info(f"=== v7 PREDICTION ENTRY BOT ({mode}) ===")
        enabled = [m["slug_pattern"] for m in self.markets]
        logger.info(f"Enabled markets: {enabled}")
        src = _cfg.PREDICTION_SOURCE.upper()
        if src == "WS":
            src = "WS"  # banner shows WS (zero-latency)
        tp_str = f"TP={_cfg.TP_RATIO:.0%}" if _cfg.TP_RATIO > 0 else "TP=OFF"
        hedge_str = (f"Hedge ${_cfg.HEDGE_ENTRY_PRICE_USD}@${_cfg.HEDGE_PRICE}"
                     if _cfg.HEDGE_PRICE > 0 else "Hedge OFF")
        logger.info(f"Prediction: {src} | Size: ${_cfg.POSITION_SIZE_USD} | "
                     f"Entry@min {_cfg.ENTRY_MINUTE} | Conf>={_cfg.MIN_CONFIDENCE} | "
                     f"Band=[${_cfg.BUY_BAND_LOW}-${_cfg.BUY_BAND_HIGH}] | "
                     f"{hedge_str} | {tp_str} | MinLiq=${_cfg.MIN_LIQUIDITY:.0f}" +
                     (f" | QS>={_cfg.MIN_QUALITY_SCORE}" if _cfg.MIN_QUALITY_SCORE > 0 else "") +
                     (f" | WS>={_cfg.MIN_WEIGHTED_SIGNAL}" if _cfg.MIN_WEIGHTED_SIGNAL > 0 else "") +
                     (f" | PredLock=ON" if _cfg.LOCK_PREDICTION_AT_ENTRY_MINUTE else "") +
                     (f" | XPairs>={_cfg.MIN_CROSS_PAIRS_AGREEMENT}" if _cfg.MIN_CROSS_PAIRS_AGREEMENT > 0 else "") +
                     (f" | Mom>={_cfg.ENTRY_MIN_MOMENTUM}" if _cfg.ENTRY_MIN_MOMENTUM > 0 else ""))
        if _cfg.TRADING_WINDOW_START >= 0 and _cfg.TRADING_WINDOW_END >= 0:
            logger.info(f"Trading window: {_cfg.TRADING_WINDOW_START // 60:02d}:{_cfg.TRADING_WINDOW_START % 60:02d}-{_cfg.TRADING_WINDOW_END // 60:02d}:{_cfg.TRADING_WINDOW_END % 60:02d} UTC")
        else:
            logger.info("Trading window: 24h (no restriction)")
        if _cfg.VOLATILITY_HEDGE_ENABLED:
            thresholds = ", ".join(f"{s}:{v}%" for s, v in _cfg.VOLATILITY_HEDGE_THRESHOLD.items())
            if not thresholds:
                thresholds = f"default:{_cfg.VOLATILITY_HEDGE_THRESHOLD_DEFAULT}%"
            else:
                thresholds += f", default:{_cfg.VOLATILITY_HEDGE_THRESHOLD_DEFAULT}%"
            mode_label = "HEDGE" if _cfg.VOLATILITY_MODE == "hedge" else "GATE"
            extra = f" hedge_price=${_cfg.VOLATILITY_HEDGE_PRICE}" if _cfg.VOLATILITY_MODE == "hedge" else " (block flat entries)"
            logger.info(f"Volatility: {mode_label} ({thresholds}{extra})")
        else:
            logger.info("Volatility: OFF")
        if _cfg.CHEAP_MODE:
            ct_label = "counter-trend ON" if _cfg.CHEAP_COUNTER_TREND else "counter-trend OFF"
            logger.info(f"Cheap mode: ON | Buy below ${_cfg.CHEAP_ENTRY_PRICE} | "
                         f"Size: ${_cfg.POSITION_SIZE_USD} | "
                         f"TP={_cfg.TP_RATIO:.0%} | SL={_cfg.SL_RATIO:.0%} | "
                         f"{ct_label}")
        if _cfg.PAIR_SL_TP:
            _pairs = " | ".join(f"{s}: SL={sl:.0%} TP={tp:.0%}" for s, (sl, tp) in _cfg.PAIR_SL_TP.items())
            logger.info(f"Per-pair SL/TP: {_pairs}")
        if _cfg.DUAL_MODE_ENABLED:
            logger.info(f"Dual mode: ON | Size: ${_cfg.DUAL_POSITION_SIZE_USD} | "
                         f"TP={_cfg.DUAL_TP_RATIO:.0%} | SL={_cfg.DUAL_SL_RATIO:.0%}")
        if _cfg.TSL_ENABLED:
            _half = _cfg.TSL_STEP / 2
            logger.info(f"TSL: ON | Step={_cfg.TSL_STEP:.0%} "
                         f"(L1 at +{_cfg.TSL_STEP:.0%} floor=+{_half:.0%}, "
                         f"then +{_half:.0%} per level)")
        if _cfg.BOT_STOP_THRESHOLD > 0:
            logger.info(f"Capital protection: stop<${_cfg.BOT_STOP_THRESHOLD} "
                         f"(starting=${_cfg.BOT_STARTING_CAPITAL})")
        else:
            logger.info("Capital protection: OFF")
        if _cfg.PACE_DETECT:
            logger.info(
                f"Pace detection: ON | "
                f"pct={_cfg.PACE_DETECT_PCT}% in {_cfg.PACE_DETECT_WINDOW_SEC}s | "
                f"cap=${_cfg.PACE_DETECT_PRICE_CAP} | "
                f"max_sec={_cfg.PACE_DETECT_MAX_SEC}s")
        if _cfg.CROSS_MARKET_TRADE:
            logger.info(
                f"Cross-market: ON | {_cfg.WINDOW_MINUTES}m markets | "
                f"Entry window: min {_cfg.CROSS_MARKET_ENTRY_MIN}-{_cfg.CROSS_MARKET_ENTRY_MAX} | "
                f"PredConfirm=\033[33m{_cfg.PRED_CONFIRM_TICKS}\033[0m")
        if _cfg.HYPER_PREDICTION:
            logger.info(
                f"Hyper Prediction: ON | API={_cfg.HYPER_PREDICTION_API_URL} "
                f"entry={_cfg.HYPER_PREDICTION_ENTRY_SEC}-{_cfg.HYPER_PREDICTION_MAX_SEC}s")
        if _cfg.ORACLE_GATE_ENABLED:
            logger.info(
                f"Oracle Gate: ON | K={_cfg.ORACLE_K} minDiff={_cfg.ORACLE_MIN_DIFF} "
                f"earlyEntry={_cfg.ORACLE_EARLY_ENTRY_SEC}s")
        if _cfg.MANAGE_POSITIONS_ONLY:
            logger.info("Mode: MANAGE POSITIONS ONLY (no new entries)")

    async def initialize(self):
        if not self.dry_run:
            from lib.polymarket_bot_main import PolymarketTrader
            self.trader = PolymarketTrader(
                private_key=_cfg.PRIVATE_KEY, signature_type=_cfg.SIGNATURE_TYPE,
                funder_address=_cfg.FUNDER_ADDRESS, clob_host=_cfg.CLOB_HOST,
                gamma_host=_cfg.GAMMA_HOST, data_host=_cfg.DATA_HOST,
            )
            logger.info(f"Trader: {self.trader.trading_address}")
            self._refresh_balance()
        if _cfg.PREDICTION_SOURCE == "none":
            logger.info("[PREDICTION] Source=NONE — trading by ENTRY_PRICE only")
        elif _cfg.PREDICTION_SOURCE == "ws":
            # WebSocket prediction — zero-latency stream from whale_dashboard
            if not _cfg.PREDICTION_WS_URL:
                logger.error("[WS-PRED] V7_PREDICTION_WS_URL not set. Exiting.")
                sys.exit(1)
            if not _cfg.PREDICTION_API_USER or not _cfg.PREDICTION_API_PASS:
                logger.error("[WS-PRED] PREDICTION_USERNAME/PASSWORD not set. Exiting.")
                sys.exit(1)
            self._ws_pred_client = _ws_pred_mod.WsPredictionClient(
                ws_url=_cfg.PREDICTION_WS_URL,
                username=_cfg.PREDICTION_API_USER,
                password=_cfg.PREDICTION_API_PASS,
            )
            self._ws_pred_client.start()
            logger.info(f"[WS-PRED] Prediction stream started: {_cfg.PREDICTION_WS_URL}")
            # Also validate HTTP fallback if URL is set
            if _cfg.PREDICTION_API_URL:
                try:
                    import httpx
                    r = httpx.get(_cfg.PREDICTION_API_URL,
                                  auth=(_cfg.PREDICTION_API_USER, _cfg.PREDICTION_API_PASS),
                                  timeout=5.0)
                    r.raise_for_status()
                    logger.info(f"[WS-PRED] HTTP fallback OK: {_cfg.PREDICTION_API_URL}")
                except Exception as e:
                    logger.warning(f"[WS-PRED] HTTP fallback unavailable: {e}")
        elif _cfg.PREDICTION_SOURCE == "http":
            if not _cfg.PREDICTION_API_URL:
                logger.error("[HTTP] v7_PREDICTION_API_URL not set. Exiting.")
                sys.exit(1)
            if not _cfg.PREDICTION_API_USER or not _cfg.PREDICTION_API_PASS:
                logger.error("[HTTP] PREDICTION_USERNAME/PASSWORD not set. Exiting.")
                sys.exit(1)
            try:
                import httpx
                r = httpx.get(_cfg.PREDICTION_API_URL,
                              auth=(_cfg.PREDICTION_API_USER, _cfg.PREDICTION_API_PASS),
                              timeout=5.0)
                r.raise_for_status()
                logger.info(f"[HTTP] Prediction API OK: {_cfg.PREDICTION_API_URL}")
            except Exception as e:
                logger.error(f"[HTTP] Cannot reach {_cfg.PREDICTION_API_URL} — {e}")
                logger.error("[HTTP] Prediction API is required. Exiting.")
                sys.exit(1)
        else:
            try:
                import redis as _redis_lib
                self._redis = _redis_lib.Redis(
                    host=_cfg.REDIS_HOST, port=_cfg.REDIS_PORT,
                    decode_responses=True, socket_connect_timeout=1, socket_timeout=1)
                self._redis.ping()
                logger.info(f"[REDIS] Connected {_cfg.REDIS_HOST}:{_cfg.REDIS_PORT}")
            except Exception as e:
                logger.error(f"[REDIS] Cannot connect — {e}")
                logger.error("[REDIS] Redis is required for predictions. Exiting.")
                sys.exit(1)
        # Start Binance WebSocket price feed for volatility hedge or oracle gate
        if _cfg.VOLATILITY_HEDGE_ENABLED or _cfg.ORACLE_GATE_ENABLED:
            symbols = [m.get("slug_pattern", "").split("-")[0].upper()
                       for m in self.markets]
            symbols = [s for s in symbols if s]
            self._okx_feed = _okx_feed.BinancePriceFeed(symbols=symbols)
            self._okx_ws_task = asyncio.create_task(self._okx_feed.run())
            logger.info(f"[BINANCE-WS] Price feed started for: {symbols}")
        # Oracle gate — CEX-implied probability as early-entry accelerator
        self._oracle_gate = None
        if _cfg.ORACLE_GATE_ENABLED:
            self._oracle_gate = _oracle_mod.OracleGate(
                k_base=_cfg.ORACLE_K, min_diff=_cfg.ORACLE_MIN_DIFF,
                window_minutes=_cfg.WINDOW_MINUTES,
                momentum_window=_cfg.ORACLE_MOMENTUM_WINDOW,
                predict_seconds=_cfg.ORACLE_PREDICT_SECONDS)
            logger.info(f"[ORACLE] Gate enabled — K={_cfg.ORACLE_K} minDiff={_cfg.ORACLE_MIN_DIFF} "
                        f"earlyEntry={_cfg.ORACLE_EARLY_ENTRY_SEC}s")
        # Start Polymarket WS orderbook feed — used for exit management (TP/SL/hedge)
        self._ws_handler = _ws_handler_mod.WsPriceHandler(self, _cfg)
        self._pm_feed = _pm_ws.PolymarketOrderbookFeed(
            on_price_update=self._ws_handler.on_price_update)
        self._pm_ws_task = asyncio.create_task(self._pm_feed.run())
        logger.info("[PM-WS] Polymarket orderbook feed started (exit management)")

    def _refresh_balance(self) -> bool:
        if self.dry_run or not self.trader:
            return True
        # Try QuikNode RPC first (reliable)
        try:
            addr = getattr(self.trader, "trading_address", None)
            if addr:
                rpc_bal = _subgraph.fetch_usdc_balance(addr)
                if rpc_bal is not None:
                    self.current_balance = rpc_bal
                    self._last_balance_ts = time.time()
                    return True
        except Exception as e:
            logger.warning(f"[BALANCE] RPC failed: {e} — trying CLOB API")
        # Fallback: CLOB API
        try:
            bal = self.trader.get_usdc_balance_and_allowance()
            self.current_balance = float(bal.get("balance", 0)) / 1e6
            self._last_balance_ts = time.time()
            return True
        except Exception as e:
            logger.warning(f"[BALANCE] CLOB API also failed: {e}")
        # Use cached balance if fresh enough (< 30s old)
        if self.current_balance > 0 and hasattr(self, '_last_balance_ts') and time.time() - self._last_balance_ts < 30:
            logger.info(f"[BALANCE] Using cached ${self.current_balance:.2f} (age={time.time() - self._last_balance_ts:.0f}s)")
            return True
        return False

    def _check_prediction_health(self) -> bool:
        """Ping prediction source periodically; cache result for REDIS_HEALTH_CHECK_INTERVAL seconds."""
        if _cfg.PREDICTION_SOURCE == "none":
            return True
        now = time.time()
        if now - self._last_redis_check < REDIS_HEALTH_CHECK_INTERVAL:
            return self._redis_healthy
        self._last_redis_check = now
        src = _cfg.PREDICTION_SOURCE.upper()
        try:
            if _cfg.PREDICTION_SOURCE == "ws":
                # WS health = client connected and has cached data
                if not self._ws_pred_client or not self._ws_pred_client.connected:
                    raise ConnectionError("WS prediction client not connected")
            elif _cfg.PREDICTION_SOURCE == "http":
                import httpx
                t0 = time.monotonic()
                r = httpx.get(_cfg.PREDICTION_API_URL,
                              auth=(_cfg.PREDICTION_API_USER, _cfg.PREDICTION_API_PASS),
                              timeout=5.0)
                latency_ms = (time.monotonic() - t0) * 1000
                r.raise_for_status()
                logger.debug(
                    f"[{src}] Health check OK: {r.status_code} in {latency_ms:.0f}ms")
            else:
                if self._redis is None:
                    raise ConnectionError("Redis client not initialized")
                self._redis.ping()
            if not self._redis_healthy:
                self._log(f"[{src}] Connection restored — resuming new entries", "INFO")
            self._redis_healthy = True
        except Exception as e:
            err_type = type(e).__name__
            if self._redis_healthy:
                self._log(
                    f"[{src}] Health check FAILED ({err_type}): {e} — "
                    f"pausing new entries, managing open positions only", "WARNING")
            else:
                # Already unhealthy — log at debug to avoid spam
                logger.debug(
                    f"[{src}] Health check still failing ({err_type}): {e}")
            self._redis_healthy = False
        return self._redis_healthy

    def _compute_total_account_value(self) -> float:
        """Total account value = USDC balance + open position value + unredeemed winnings."""
        positions_value = sum(p.current_value() for p in self.state.positions.values())
        return self.current_balance + positions_value + self._unredeemed_value

    def _check_capital(self) -> bool:
        """Check if total account value is above stop threshold. Skip if not configured."""
        if _cfg.BOT_STOP_THRESHOLD <= 0:
            return True
        total = self._compute_total_account_value()
        was_ok = self._capital_ok
        self._capital_ok = total >= _cfg.BOT_STOP_THRESHOLD
        if not self._capital_ok and was_ok:
            self._log(
                f"[CAPITAL] BELOW THRESHOLD: ${total:.2f} < ${_cfg.BOT_STOP_THRESHOLD:.2f} "
                f"(bal=${self.current_balance:.2f} pos=${total - self.current_balance - self._unredeemed_value:.2f} "
                f"unredeemed=${self._unredeemed_value:.2f}) — "
                f"pausing new entries, managing open positions only", "WARNING")
        elif self._capital_ok and not was_ok:
            self._log(
                f"[CAPITAL] Recovered: ${total:.2f} >= ${_cfg.BOT_STOP_THRESHOLD:.2f} "
                f"— resuming new entries", "INFO")
        return self._capital_ok

    # ============ Persistent Begin Balance (across windows & restarts) ============
    def _get_config_snapshot(self) -> str:
        """Collect all bot-relevant env vars as sorted key=value string for change detection."""
        # Include V7_ vars + critical non-prefixed vars that affect trading behavior
        critical_keys = {
            "DRY_RUN", "POLYMARKET_FUNDER", "PRIVATE_KEY", "BOT_STOP_THRESHOLD",
            "POLYGON_RPC_URL", "BOT_ID",
        }
        items = sorted(
            (k, v) for k, v in os.environ.items()
            if k.startswith("V7_") or k in critical_keys
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
            self._persistent_balance_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            self._last_configs_path.write_text(self._get_config_snapshot(), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[PERSIST] Save failed: {e}")

    def _load_persistent_balance(self) -> bool:
        """Load persistent begin_balance if configs unchanged. Returns True if loaded."""
        if not self._persistent_balance_path.exists() or not self._last_configs_path.exists():
            return False
        try:
            data = json.loads(self._persistent_balance_path.read_text(encoding="utf-8"))
            saved_bal = data.get("begin_session_balance", 0)
            if saved_bal <= 0:
                return False
            saved_configs = self._last_configs_path.read_text(encoding="utf-8").strip()
            current_configs = self._get_config_snapshot().strip()
            if saved_configs != current_configs:
                self._log("CONFIG CHANGED — resetting begin_session_balance", "WARN")
                self._persistent_balance_path.unlink(missing_ok=True)
                self._last_configs_path.write_text(current_configs, encoding="utf-8")
                return False
            self._begin_session_balance = saved_bal
            self._need_begin_balance_capture = False
            self._log(f"PERSISTENT BEGIN_BALANCE LOADED: ${saved_bal:.2f} (configs unchanged)", "SUCCESS")
            return True
        except Exception as e:
            logger.warning(f"[PERSIST] Load failed: {e}")
            return False

    def _capture_begin_balance_if_needed(self):
        """Capture begin_session_balance after first successful balance read."""
        if not self._need_begin_balance_capture:
            return
        if self.current_balance <= 0:
            return
        positions_value = sum(p.current_value() for p in self.state.positions.values())
        self._begin_session_balance = self.current_balance + positions_value + self._unredeemed_value
        self._need_begin_balance_capture = False
        self._log(f"BEGIN_SESSION_BALANCE: ${self._begin_session_balance:.2f} "
                  f"(balance=${self.current_balance:.2f} + positions=${positions_value:.2f} "
                  f"+ unredeemed=${self._unredeemed_value:.2f})", "INFO")
        self._save_persistent_balance()

    def _get_price_to_beat(self, symbol: str) -> float:
        """Lookup priceToBeat from _known_markets by matching symbol prefix in slug."""
        sym_lower = symbol.lower()
        for cid, info in self._known_markets.items():
            slug = info.get("slug", "")
            if slug.startswith(f"{sym_lower}-") and info.get("price_to_beat", 0) > 0:
                return info["price_to_beat"]
        return 0.0

    def _is_choppy_paused(self) -> bool:
        """Check external watchdog. Prefers HTTP API when configured, else falls
        back to legacy file-exists check. Result cached for 10s so per-tick
        scans don't hammer the backend."""
        if _cfg.CHOPPY_API_URL and _cfg.CHOPPY_API_USER and _cfg.CHOPPY_API_PASS:
            return self._is_choppy_paused_api()
        try:
            p = Path(_cfg.CHOPPY_SIGNAL_FILE)
            if not p.exists():
                return False
            # Stale check: ignore if file older than 5 minutes
            age = time.time() - p.stat().st_mtime
            return age < 300
        except Exception:
            return False

    def _is_choppy_paused_api(self) -> bool:
        cache = getattr(self, "_choppy_api_cache", None)
        now = time.time()
        if cache and now - cache["ts"] < 10:
            return cache["paused"]
        try:
            import urllib.request
            import base64
            req = urllib.request.Request(_cfg.CHOPPY_API_URL)
            creds = f"{_cfg.CHOPPY_API_USER}:{_cfg.CHOPPY_API_PASS}".encode()
            req.add_header("Authorization", "Basic " + base64.b64encode(creds).decode())
            with urllib.request.urlopen(req, timeout=2) as resp:
                data = json.loads(resp.read().decode())
            if data.get("status") in ("stale", "no_data", "error", "invalid_pair"):
                paused = False
            else:
                paused = str(data.get("should_enter", "YES")).upper() != "YES"
            self._choppy_api_cache = {"ts": now, "paused": paused}
            return paused
        except Exception:
            # Keep last known value if fresh, else don't block entries
            if cache and now - cache["ts"] < 60:
                return cache["paused"]
            return False

    def _check_volatility_entry_gate(self, symbol: str) -> bool:
        """Block entries when price move is below threshold (flat market).
        Uses prediction WS price_change_pct (multi-exchange) with OKX fallback.
        Only active in 'gate' mode. Returns True if entry allowed, False if blocked."""
        if _cfg.VOLATILITY_MODE != "gate":
            return True  # not in gate mode — allow entry
        threshold = _cfg.VOLATILITY_HEDGE_THRESHOLD.get(
            symbol.upper(), _cfg.VOLATILITY_HEDGE_THRESHOLD_DEFAULT)
        # Primary: prediction WS price_change_pct (7-exchange combined)
        _pred_ws = getattr(self, '_ws_pred_client', None)
        if _pred_ws:
            pct = abs(_pred_ws.get_price_change_pct(symbol))
            if pct > 0.001:
                return pct > threshold
        # Fallback: OKX single-exchange move
        if self._okx_feed:
            pct = abs(self._okx_feed.get_move_pct(symbol))
            if pct > 0.001:
                return pct > threshold
        # No data — block entry
        return False

    def _get_effective_hedge_price(self, symbol: str) -> float:
        """Calculate effective hedge price based on Binance volatility data.
        Only active in 'hedge' mode. Otherwise returns VOLATILITY_HEDGE_PRICE."""
        if _cfg.VOLATILITY_MODE != "hedge" or not self._okx_feed:
            return _cfg.VOLATILITY_HEDGE_PRICE
        # Check WS task health — restart if crashed
        if self._okx_ws_task and self._okx_ws_task.done():
            logger.warning("[BINANCE-WS] Task died, restarting...")
            self._okx_ws_task = asyncio.create_task(self._okx_feed.run())
            return _cfg.VOLATILITY_HEDGE_PRICE
        # No data yet (startup) — fallback
        price = self._okx_feed.get_price(symbol)
        if price <= 0:
            return _cfg.VOLATILITY_HEDGE_PRICE
        move_pct = self._okx_feed.get_move_pct(symbol)
        threshold = _cfg.VOLATILITY_HEDGE_THRESHOLD.get(
            symbol.upper(), _cfg.VOLATILITY_HEDGE_THRESHOLD_DEFAULT)
        hedge_on = move_pct <= threshold
        # Only log on state changes (track per-symbol)
        cache_key = f"_vol_hedge_{symbol}"
        prev = getattr(self, cache_key, None)
        if prev != hedge_on:
            setattr(self, cache_key, hedge_on)
            if hedge_on:
                self._log(
                    f"[VOLATILITY] {symbol} move={move_pct:.2f}% <= "
                    f"{threshold}% -> HEDGE ON "
                    f"(${_cfg.VOLATILITY_HEDGE_PRICE})", "INFO")
            else:
                self._log(
                    f"[VOLATILITY] {symbol} move={move_pct:.2f}% > "
                    f"{threshold}% -> HEDGE OFF", "INFO")
        return _cfg.VOLATILITY_HEDGE_PRICE if hedge_on else 0.0

    def read_prediction(self, symbol: str = "BTC"):
        """Read prediction signal — polls HTTP/WS/Redis and caches result.

        Called from main loop (_scan_market) at CHECK_INTERVAL pace.
        WS handler reads cached result via read_prediction_cached().
        """
        _wts = self._get_window_ts()
        if _cfg.PREDICTION_SOURCE == "ws" and self._ws_pred_client:
            result = self._ws_pred_client.get_prediction(symbol)
            if result[0]:
                self._last_prediction[symbol] = result
                return result
            if _cfg.PREDICTION_API_URL:
                result = _v7_engine.read_prediction_signal_http(
                    _cfg.PREDICTION_API_URL, _cfg.PREDICTION_API_USER,
                    _cfg.PREDICTION_API_PASS, symbol=symbol,
                    expected_window_ts=_wts)
                if result[0]:
                    self._last_prediction[symbol] = result
                return result
            return result
        if _cfg.PREDICTION_SOURCE == "http":
            result = _v7_engine.read_prediction_signal_http(
                _cfg.PREDICTION_API_URL, _cfg.PREDICTION_API_USER,
                _cfg.PREDICTION_API_PASS, symbol=symbol,
                expected_window_ts=_wts)
            if result[0]:
                self._last_prediction[symbol] = result
            return result
        redis_key = _cfg.REDIS_PREDICTION_KEY.replace("{symbol}", symbol)
        result = _v7_engine.read_prediction_signal(self._redis, redis_key=redis_key)
        if result[0]:
            self._last_prediction[symbol] = result
        return result

    def read_prediction_cached(self, symbol: str = "BTC"):
        """Return last cached prediction without polling. Used by WS handler."""
        return self._last_prediction.get(symbol, ("", 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0))

    def read_hyper_prediction(self, symbol: str = "BTC"):
        """Fetch hyper prediction via HTTP — used for HYPER_BOOST gate relaxation."""
        if not getattr(_cfg, "HYPER_BOOST_ENABLED", False):
            return None
        try:
            _wts = self._get_window_ts()
            return _v7_engine.read_prediction_signal_http(
                _cfg.HYPER_PREDICTION_API_URL, _cfg.PREDICTION_API_USER,
                _cfg.PREDICTION_API_PASS, symbol=symbol,
                expected_window_ts=_wts)
        except Exception as e:
            logger.warning(f"[HYPER] read error: {e}")
            return None

    # ============ Regime Detection ============

    def _record_price_tick(self, market_symbol: str, yes_mid: float, spread_pct: float):
        """Record price + spread for local regime detection (ring buffer, ~5min)."""
        now = time.time()
        buf = self._price_ticks.setdefault(market_symbol, deque(maxlen=300))
        buf.append((now, yes_mid))
        sbuf = self._spread_ticks.setdefault(market_symbol, deque(maxlen=300))
        sbuf.append((now, spread_pct))

    def _compute_flip_rate(self, market_symbol: str, lookback_sec: float = 180.0,
                           interval_sec: float = 10.0) -> float:
        """Compute direction flips per minute from price history at 10s intervals.

        Samples yes_mid at 10s intervals over last 3min, counts sign changes.
        Returns flip_rate = flips / minutes.
        """
        buf = self._price_ticks.get(market_symbol)
        if not buf or len(buf) < 3:
            return 0.0
        now = time.time()
        cutoff = now - lookback_sec
        # Sample prices at interval_sec boundaries
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
        # Count direction changes
        flips = 0
        for i in range(2, len(samples)):
            prev_dir = 1 if samples[i - 1] > samples[i - 2] else -1
            curr_dir = 1 if samples[i] > samples[i - 1] else -1
            if prev_dir != curr_dir and prev_dir != 0 and curr_dir != 0:
                flips += 1
        minutes = lookback_sec / 60.0
        return flips / minutes if minutes > 0 else 0.0

    def _compute_spread_expansion(self, market_symbol: str) -> float:
        """Compute spread expansion ratio: current spread / rolling average spread.

        Returns ratio (e.g. 2.5 means spread is 2.5× the average).
        """
        sbuf = self._spread_ticks.get(market_symbol)
        if not sbuf or len(sbuf) < 5:
            return 1.0
        avg_spread = sum(s for _, s in sbuf) / len(sbuf)
        if avg_spread <= 0:
            return 1.0
        _, current_spread = sbuf[-1]
        return current_spread / avg_spread

    def _detect_regime(self, market_symbol: str, confidence: float,
                       momentum: float, small_candle_ratio: float) -> tuple:
        """Classify market as TREND or CHOP using 4 signals.

        Returns: (regime, chop_signals, details_dict)
        CHOP if 2+ of 4 chop signals fire.
        """
        # Signal 1: Confidence + Momentum → TREND indicator
        is_trend = (confidence >= _cfg.REGIME_TREND_CONFIDENCE
                    and abs(momentum) >= _cfg.REGIME_TREND_MOMENTUM)
        sig1_chop = 0 if is_trend else 1

        # Signal 2: Direction flip rate (local, 10s intervals over 3min)
        flip_rate = self._compute_flip_rate(market_symbol)
        sig2_chop = 1 if flip_rate > _cfg.REGIME_FLIP_RATE_THRESHOLD else 0

        # Signal 3: Noise ratio proxy (small_candle_ratio from API)
        sig3_chop = 1 if small_candle_ratio > _cfg.REGIME_NOISE_THRESHOLD else 0

        # Signal 4: Spread expansion (local, from orderbook)
        spread_ratio = self._compute_spread_expansion(market_symbol)
        sig4_chop = 1 if spread_ratio > _cfg.REGIME_SPREAD_MULTIPLIER else 0

        chop_signals = sig1_chop + sig2_chop + sig3_chop + sig4_chop
        regime = "CHOP" if chop_signals >= 2 else "TREND"

        details = {
            "regime": regime,
            "chop_signals": chop_signals,
            "sig1_trend": is_trend,
            "sig1_conf": confidence,
            "sig1_mom": momentum,
            "sig2_flip_rate": flip_rate,
            "sig3_noise": small_candle_ratio,
            "sig4_spread_ratio": spread_ratio,
        }
        return regime, chop_signals, details

    def _get_window_ts(self) -> int:
        ws = _cfg.WINDOW_SECONDS
        return (int(datetime.now(timezone.utc).timestamp()) // ws) * ws

    def _reset_window(self, window_ts: int):
        if self._last_window_ts == window_ts:
            return
        self._log(f"NEW WINDOW: {self._last_window_ts} -> {window_ts}", "INFO")
        # LOOSEN state persists across windows — only reverts via explicit
        # TIGHTEN command or bot restart.
        if self.state.positions:
            for pos in self.state.positions.values():
                # Skip TP_CLOSED — PnL already counted at sell time
                if pos.phase == "TP_CLOSED":
                    self._log(
                        f"  ARCHIVE {pos.market_slug} side={pos.entry_side} "
                        f"phase=TP_CLOSED (PnL already counted)", "INFO")
                    self.state.settled_count += 1
                    self.state.win_count += 1
                    continue
                # Skip CUT — main side loss already counted at cut time;
                # only count remaining hedge side value if any
                if pos.phase == "CUT":
                    hedge_tokens = pos.no_tokens if pos.entry_side == "YES" else pos.yes_tokens
                    hedge_price = pos.no_price if pos.entry_side == "YES" else pos.yes_price
                    hedge_entry = pos.no_entry_price if pos.entry_side == "YES" else pos.yes_entry_price
                    hedge_pnl = 0.0
                    if hedge_tokens >= 1 and hedge_entry > 0:
                        hedge_pnl = hedge_tokens * (hedge_price - hedge_entry)
                        self.state.total_pnl += hedge_pnl
                    self.state.settled_count += 1
                    self._log(
                        f"  ARCHIVE {pos.market_slug} side={pos.entry_side} "
                        f"phase=CUT (main PnL already counted, hedge=${hedge_pnl:.2f})", "INFO")
                    continue
                # Use resolution price: if mid-price PnL positive → resolved to $1 (use 0.99),
                # if negative → resolved to $0 (total loss)
                mid_pnl = pos.current_pnl()
                tokens = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
                if mid_pnl > 0:
                    pnl = tokens * 0.99 - pos.entry_cost
                    if self.dry_run:
                        self.current_balance += pos.entry_cost + pnl
                else:
                    pnl = -pos.entry_cost
                self.state.total_pnl += pnl
                self.state.settled_count += 1
                if pnl > 0:
                    self.state.win_count += 1
                self._log(
                    f"  ARCHIVE {pos.market_slug} side={pos.entry_side} "
                    f"phase={pos.phase} PnL=${pnl:.2f}", "INFO")
                if self.mongo:
                    _entry_p = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price
                    _pnl_pct = (pnl / pos.entry_cost * 100) if pos.entry_cost > 0 else 0
                    self.mongo.log_trade({
                        "type": "v7_WINDOW_CLOSE", "market_slug": pos.market_slug,
                        "phase": pos.phase, "entry_side": pos.entry_side,
                        "entry_type": getattr(pos, 'entry_type', ''),
                        "entry_price": _entry_p,
                        "pnl": pnl, "pnl_pct": round(_pnl_pct, 2),
                        "entry_cost": pos.entry_cost,
                        "hedge_cost": pos.hedge_cost,
                    })
                # Add archived trade to dashboard so it shows in trade history
                if hasattr(self, 'dashboard'):
                    _entry_p = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price
                    _exit_p = 0.99 if mid_pnl > 0 else 0.0
                    _pnl_pct = (pnl / pos.entry_cost * 100) if pos.entry_cost > 0 else 0
                    self.dashboard.add_trade({
                        "side": pos.entry_side, "entry_time": pos.entry_time,
                        "exit_time": time.time(), "entry_price": _entry_p,
                        "exit_price": round(_exit_p, 4),
                        "pnl_percent": round(_pnl_pct, 1),
                        "pnl_cash": round(pnl, 2),
                        "exit_reason": f"WINDOW_{pos.phase}",
                        "market_slug": pos.market_slug,
                        "window_ts": self._last_window_ts,
                    })
            self._log(
                f"CLEARED {len(self.state.positions)} positions | "
                f"Session PnL: ${self.state.total_pnl:.2f} "
                f"({self.state.win_count}/{self.state.settled_count} wins)", "INFO")
            self.state.positions.clear()
        self._seeded_this_window.clear()
        self._window_trends.clear()
        self._locked_prediction.clear()
        self._pred_confirm.clear()
        # Carry over pre-discovered next-window markets if available
        if self._next_window_ts == window_ts and self._next_window_markets:
            self._known_markets = self._next_window_markets
            self._market_cache = self._next_window_cache
            self._log(f"[PRE-DISC] Using pre-discovered markets for {window_ts}", "INFO")
        else:
            self._known_markets.clear()
            self._market_cache.clear()
        # Clear pace history but PRESERVE pre-collected ticks for new-window
        # tokens (subscribed ~5s before window boundary). This gives pace ~5s
        # of warmup data so it can fire immediately at window start.
        _keep_tokens = set()
        for _info in self._known_markets.values():
            _keep_tokens.add(_info.get("yes_token_id", ""))
            _keep_tokens.add(_info.get("no_token_id", ""))
        _v7_pace.clear_history_except(_keep_tokens)
        self._next_window_markets = {}
        self._next_window_cache = {}
        self._next_window_ts = 0
        # Clear stale WS subscriptions and re-subscribe with new tokens
        if self._pm_feed:
            self._pm_feed.clear_subscriptions()
        if self._ws_handler:
            self._ws_handler._token_to_cid.clear()
            # Immediately rebuild token map so WS callbacks can record prices
            if self._known_markets:
                self._ws_handler.rebuild_token_map()
        self._last_window_ts = window_ts

    def _sync_from_api(self):
        if not self._known_markets:
            return
        try:
            api_pos = _v7_sync.fetch_api_positions(
                _cfg.DATA_HOST, self.trader.trading_address)
            if api_pos:
                prev_count = len(self.state.positions)
                _v7_sync.sync_positions(self, api_pos, self._known_markets, create_new=True)
                self._unredeemed_value = _v7_sync.compute_unredeemed_value(api_pos)
                # Save state if new positions were adopted (manual buys)
                if len(self.state.positions) > prev_count:
                    self._save_state()
        except Exception as e:
            logger.warning(f"[SYNC] Error: {e}")

    def _is_ws_active(self) -> bool:
        """Check if PM WS feed is connected.

        Only checks connection status — PM WS may not send initial book
        snapshots, so requiring price data would cause permanent fallback.
        """
        return bool(self._pm_feed and self._pm_feed.is_connected)

    def _has_active_positions(self) -> bool:
        """True if any tracked position is not yet closed."""
        return any(p.phase != "TP_CLOSED" for p in self.state.positions.values())

    def _log(self, message: str, level: str = "INFO"):
        # Strip ANSI codes for structured logger (file/syslog)
        _clean = re.sub(r'\033\[[0-9;]*m', '', message)
        logger.info(f"[{level}] {_clean}")
        self.dashboard.log(_clean, level)
        # Write colored output via TeeStdout for journalctl --output=cat
        if self.headless:
            print(f"[{level}] {message}")

    def _publish_tui_to_redis(self):
        """Publish latest TUI snapshot to Redis for web dashboard consumption."""
        try:
            if not self._dashboard_redis:
                import redis
                self._dashboard_redis = redis.Redis(
                    host=_cfg.REDIS_HOST,
                    port=_cfg.REDIS_PORT,
                    decode_responses=True,
                    socket_timeout=2,
                    socket_connect_timeout=2,
                )
                self._dashboard_redis.ping()
                # Clean stale keys from previous mode (e.g. DRY-RUN -> LIVE)
                for key in self._dashboard_redis.keys("konis:tui:V7-*"):
                    if key != f"konis:tui:{self.dashboard.instance_id}":
                        self._dashboard_redis.delete(key)
                        logger.info(f"[REDIS] Deleted stale key: {key}")
            snapshot = getattr(self.dashboard, '_last_snapshot', None)
            if snapshot:
                # Inject current loosen state for dashboard button rendering
                snapshot = dict(snapshot)  # shallow copy to avoid mutating bot state
                snapshot["tp_loosened"] = (_cfg.TP_RATIO == 0 and _cfg.TP_RATIO_ORIGINAL > 0)
                snapshot["sl_loosened"] = (_cfg.SL_RATIO == 0 and _cfg.SL_RATIO_ORIGINAL > 0)
                snapshot["cfg_min_confidence"] = _cfg.MIN_CONFIDENCE
                snapshot["cfg_tp_ratio"] = _cfg.TP_RATIO
                snapshot["cfg_sl_ratio"] = _cfg.SL_RATIO
                snapshot["cfg_position_size_usd"] = _cfg.POSITION_SIZE_USD
                iid = self.dashboard.instance_id or "latest"
                self._dashboard_redis.set(f"konis:tui:{iid}", json.dumps(snapshot, default=str))
        except Exception as e:
            logger.warning(f"[REDIS] TUI publish failed: {type(e).__name__}: {e}")
            self._dashboard_redis = None  # Reset on failure, retry next cycle

    def _check_redis_commands(self):
        """Check Redis for commands from web dashboard (e.g. EXIT_ALL)."""
        try:
            if not self._dashboard_redis:
                return
            iid = getattr(self.dashboard, 'instance_id', None) or "latest"
            cmd = self._dashboard_redis.get(f"konis:cmd:{iid}")
            if cmd == "EXIT_ALL":
                self._dashboard_redis.delete(f"konis:cmd:{iid}")
                logger.info("[WEB CMD] EXIT_ALL received from dashboard")
                self._manual_exit_all = True
            elif cmd in ("LOOSEN_TP", "LOOSEN_SL"):
                self._dashboard_redis.delete(f"konis:cmd:{iid}")
                # Mutate cfg in-memory. Also clear per-position overrides so cfg=0
                # takes effect for already-open positions. Auto-reverts at next
                # window reset, or via explicit TIGHTEN_TP/TIGHTEN_SL command.
                if cmd == "LOOSEN_TP":
                    _cfg.TP_RATIO = 0.0
                    for _p in self.state.positions.values():
                        _p.tp_ratio = 0.0
                    logger.warning("[WEB CMD] LOOSEN_TP — TP_RATIO=0 (all positions)")
                    self._publish_exit_result("success", "TP LOOSENED (=0)")
                else:
                    _cfg.SL_RATIO = 0.0
                    for _p in self.state.positions.values():
                        _p.sl_ratio = 0.0
                    logger.warning("[WEB CMD] LOOSEN_SL — SL_RATIO=0 (all positions)")
                    self._publish_exit_result("success", "SL LOOSENED (=0)")
            elif cmd in ("TIGHTEN_TP", "TIGHTEN_SL"):
                self._dashboard_redis.delete(f"konis:cmd:{iid}")
                if cmd == "TIGHTEN_TP":
                    _cfg.TP_RATIO = _cfg.TP_RATIO_ORIGINAL
                    logger.warning(
                        f"[WEB CMD] TIGHTEN_TP — TP_RATIO restored to "
                        f"{_cfg.TP_RATIO_ORIGINAL:.0%}")
                    self._publish_exit_result(
                        "success", f"TP TIGHTENED ({_cfg.TP_RATIO_ORIGINAL:.0%})")
                else:
                    _cfg.SL_RATIO = _cfg.SL_RATIO_ORIGINAL
                    logger.warning(
                        f"[WEB CMD] TIGHTEN_SL — SL_RATIO restored to "
                        f"{_cfg.SL_RATIO_ORIGINAL:.0%}")
                    self._publish_exit_result(
                        "success", f"SL TIGHTENED ({_cfg.SL_RATIO_ORIGINAL:.0%})")
            elif cmd and cmd.startswith("SET_CONFIDENCE:"):
                self._dashboard_redis.delete(f"konis:cmd:{iid}")
                val = float(cmd.split(":")[1]) / 100.0
                _cfg.MIN_CONFIDENCE = val
                logger.warning(f"[WEB CMD] SET_CONFIDENCE — MIN_CONFIDENCE={val:.0%}")
                self._publish_exit_result("success", f"CONFIDENCE={val:.0%}")
            elif cmd and cmd.startswith("SET_TP:"):
                self._dashboard_redis.delete(f"konis:cmd:{iid}")
                val = float(cmd.split(":")[1]) / 100.0
                _cfg.TP_RATIO = val
                _cfg.TP_RATIO_ORIGINAL = val
                for _p in self.state.positions.values():
                    _p.tp_ratio = val
                logger.warning(f"[WEB CMD] SET_TP — TP_RATIO={val:.0%}")
                self._publish_exit_result("success", f"TP={val:.0%}")
            elif cmd and cmd.startswith("SET_SL:"):
                self._dashboard_redis.delete(f"konis:cmd:{iid}")
                val = float(cmd.split(":")[1]) / 100.0
                _cfg.SL_RATIO = val
                _cfg.SL_RATIO_ORIGINAL = val
                for _p in self.state.positions.values():
                    _p.sl_ratio = val
                logger.warning(f"[WEB CMD] SET_SL — SL_RATIO={val:.0%}")
                self._publish_exit_result("success", f"SL={val:.0%}")
            elif cmd and cmd.startswith("ENTER_"):
                self._dashboard_redis.delete(f"konis:cmd:{iid}")
                self._manual_entry_cmd = cmd
                logger.warning(f"[WEB CMD] Manual entry queued: {cmd}")
                self._publish_exit_result("success", f"ENTRY QUEUED: {cmd}")
        except Exception as e:
            logger.warning(f"[REDIS] Command check failed: {e}")

    def _publish_exit_result(self, status: str, detail: str):
        """Publish exit command result back to Redis for web dashboard polling."""
        try:
            if not self._dashboard_redis:
                return
            iid = getattr(self.dashboard, 'instance_id', None) or "latest"
            result = json.dumps({"status": status, "detail": detail,
                                 "ts": time.time()})
            self._dashboard_redis.set(f"konis:cmd_result:{iid}", result, ex=60)
        except Exception:
            pass

    async def _fetch_price_to_beat(self, slug: str, symbol: str = "BTC") -> float:
        """Fetch priceToBeat from Polymarket via headless browser (Playwright).
        The value is rendered client-side from Chainlink data, not in raw HTML."""
        if not slug:
            return 0.0
        try:
            from playwright.async_api import async_playwright
            url = f"https://polymarket.com/event/{slug}"
            # Use project tmp dir to avoid read-only /tmp in systemd sandboxed services
            pw_tmp = str(_cfg.SCRIPT_DIR / ".playwright-tmp")
            os.makedirs(pw_tmp, exist_ok=True)
            os.environ["TMPDIR"] = pw_tmp
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # Wait for Price to Beat element inside the chart container
                loc = page.locator(
                    'xpath=//*[@id="price-chart-container"]'
                    '/div/div/div[1]/div/div[1]/div[1]/span')
                text = await loc.text_content(timeout=30000)
                await browser.close()
                if text:
                    ptb = float(text.replace("$", "").replace(",", ""))
                    _d_lvl = logging.DEBUG if self._has_active_positions() else logging.INFO
                    logger.log(_d_lvl, f"[DISCOVER] {slug} priceToBeat=${ptb:,.2f} (browser)")
                    return ptb
        except Exception as e:
            logger.warning(f"[DISCOVER] Failed to fetch priceToBeat for {slug}: {e}")
        return 0.0

    async def _discover_markets(self):
        wts = self._get_window_ts()
        # Skip re-discovery if all markets already cached for this window
        if self._market_cache and all(
                mc["slug_pattern"] in self._market_cache for mc in self.markets):
            return
        self._market_cache.clear()
        for mc in self.markets:
            try:
                market = await _v7_engine.find_market(
                    _cfg.GAMMA_HOST, mc["slug_pattern"], wts)
                if not market:
                    logger.debug(f"[DISCOVER] Market not found: {mc['slug_pattern']}-{wts}")
                    continue
                cid = market.get("conditionId", "")
                yes_id, no_id = _v7_engine.parse_market_tokens(market)
                if yes_id and no_id:
                    slug = market.get("slug", "")
                    # Reuse cached ptb if already fetched for this CID
                    cached_ptb = self._known_markets.get(cid, {}).get("price_to_beat", 0.0)
                    if cached_ptb > 0:
                        ptb = cached_ptb
                    else:
                        # Try Gamma API eventMetadata first
                        ptb = 0.0
                        for evt in (market.get("events") or []):
                            em = evt.get("eventMetadata") or {}
                            if "priceToBeat" in em:
                                ptb = float(em["priceToBeat"])
                                break
                        # Fallback: fetch from Polymarket website (only if polymarket ptb source + hedge needs it)
                        if (ptb <= 0 and _cfg.VOLATILITY_HEDGE_ENABLED
                                and _cfg.PTB_SOURCE != "okx"):
                            sym = mc.get("slug_pattern", "").split("-")[0].upper()
                            ptb = await self._fetch_price_to_beat(slug, sym)
                    self._known_markets[cid] = {
                        "slug": slug,
                        "yes_token_id": yes_id, "no_token_id": no_id,
                        "price_to_beat": ptb,
                    }
                    if ptb > 0:
                        _d_lvl = logging.DEBUG if self._has_active_positions() else logging.INFO
                        logger.log(_d_lvl, f"[DISCOVER] {slug} priceToBeat=${ptb:,.2f}")
                    self._market_cache[mc["slug_pattern"]] = market
            except Exception as e:
                logger.warning(f"[DISCOVER] {mc.get('name', '?')}: {e}")

    async def _pre_discover_next_window(self):
        """Pre-discover next window's markets ~10s before window ends.
        Stores results in _next_window_* fields, carried over by _reset_window.
        """
        next_ts = self._last_window_ts + _cfg.WINDOW_SECONDS
        if self._next_window_ts == next_ts:
            return  # already pre-discovered
        self._next_window_markets = {}
        self._next_window_cache = {}
        for mc in self.markets:
            try:
                market = await _v7_engine.find_market(
                    _cfg.GAMMA_HOST, mc["slug_pattern"], next_ts)
                if not market:
                    continue
                cid = market.get("conditionId", "")
                yes_id, no_id = _v7_engine.parse_market_tokens(market)
                if yes_id and no_id:
                    slug = market.get("slug", "")
                    self._next_window_markets[cid] = {
                        "slug": slug,
                        "yes_token_id": yes_id, "no_token_id": no_id,
                        "price_to_beat": 0.0,
                    }
                    self._next_window_cache[mc["slug_pattern"]] = market
                    self._log(f"[PRE-DISC] Found next market: {slug}", "INFO")
            except Exception as e:
                logger.warning(f"[PRE-DISC] {mc.get('name', '?')}: {e}")
        if self._next_window_markets:
            self._next_window_ts = next_ts
            # Pre-subscribe WS to next window's tokens so prices flow immediately at :00
            if self._pm_feed:
                _tids = []
                for _km in self._next_window_markets.values():
                    _tids.extend([_km["yes_token_id"], _km["no_token_id"]])
                await self._pm_feed.subscribe(_tids)
                self._log(f"[PRE-DISC] Pre-subscribed {len(_tids)} token(s)", "INFO")

    async def _scan_market(self, mc: dict):
        try:
            market = self._market_cache.get(mc["slug_pattern"])
            if not market:
                return
            cid = market.get("conditionId", "")
            # Extract symbol from slug pattern (e.g. "btc-updown-15m" → "BTC")
            slug_parts = mc.get("slug_pattern", "").split("-")
            market_symbol = slug_parts[0].upper() if slug_parts and slug_parts[0] else "BTC"
            yes_id, no_id = _v7_engine.parse_market_tokens(market)
            if not yes_id or not no_id:
                return

            # Retry priceToBeat if missing — only when volatility hedge needs it
            info = self._known_markets.get(cid, {})
            if _cfg.VOLATILITY_HEDGE_ENABLED and info and info.get("price_to_beat", 0) <= 0:
                _last_ptb_try = getattr(self, '_last_ptb_retry', {})
                _now = time.time()
                if _now - _last_ptb_try.get(cid, 0) >= 10:
                    _last_ptb_try[cid] = _now
                    self._last_ptb_retry = _last_ptb_try
                    slug = info.get("slug", market.get("slug", ""))
                    sym = market_symbol
                    # Try Gamma API first (re-fetch market)
                    ptb = 0.0
                    try:
                        _mkt = await _v7_engine.find_market(
                            _cfg.GAMMA_HOST, mc["slug_pattern"], self._get_window_ts())
                        if _mkt:
                            for evt in (_mkt.get("events") or []):
                                em = evt.get("eventMetadata") or {}
                                if "priceToBeat" in em:
                                    ptb = float(em["priceToBeat"])
                                    break
                    except Exception:
                        pass
                    # Fallback: browser fetch
                    if ptb <= 0:
                        ptb = await self._fetch_price_to_beat(slug, sym)
                    if ptb > 0:
                        info["price_to_beat"] = ptb
                        logger.info(f"[DISCOVER] {slug} priceToBeat=${ptb:,.2f} (retry)")

            # Early exit: no position + outside trading hours → skip orderbook API calls
            if cid not in self.state.positions:
                if _cfg.TRADING_WINDOW_START >= 0 and _cfg.TRADING_WINDOW_END >= 0:
                    import datetime as _dt
                    _now = _dt.datetime.now(_dt.timezone.utc)
                    _now_mins = _now.hour * 60 + _now.minute
                    if _cfg.TRADING_WINDOW_START <= _cfg.TRADING_WINDOW_END:
                        outside = _now_mins < _cfg.TRADING_WINDOW_START or _now_mins >= _cfg.TRADING_WINDOW_END
                    else:
                        outside = _now_mins >= _cfg.TRADING_WINDOW_END and _now_mins < _cfg.TRADING_WINDOW_START
                    if outside:
                        self._log(
                            f"[SCAN] {market.get('slug', '?')} — skip: outside trading window "
                            f"({_cfg.TRADING_WINDOW_START // 60:02d}:{_cfg.TRADING_WINDOW_START % 60:02d}-"
                            f"{_cfg.TRADING_WINDOW_END // 60:02d}:{_cfg.TRADING_WINDOW_END % 60:02d} UTC)",
                            "INFO")
                        return

            yes_book = await _v7_engine.get_orderbook(_cfg.CLOB_HOST, yes_id)
            no_book = await _v7_engine.get_orderbook(_cfg.CLOB_HOST, no_id)
            yes_bid, yes_ask, yes_mid = _v7_engine.get_best_prices(yes_book)
            no_bid, no_ask, no_mid = _v7_engine.get_best_prices(no_book)

            # Guard: empty orderbook returns bid=0/ask=1.0/mid=1.0 — skip
            if yes_bid <= 0 and no_bid <= 0:
                slug = market.get("slug", mc.get("slug_pattern", "?"))
                self._log(
                    f"[SCAN] {slug} YES=${yes_mid:.3f} NO=${no_mid:.3f} "
                    f"— skip: empty orderbook (no bids)", "INFO")
                return

            _regime = "TREND"  # regime detection removed

            # --- Existing position: update prices + manage ---
            # Also update dual position prices (evaluated separately)
            _dual_pos = self.state.positions.get(cid + "_dual")
            if _dual_pos and _dual_pos.phase not in ("TP_CLOSED",):
                if 0 < yes_mid < 0.999:
                    _dual_pos.yes_price = yes_mid
                if 0 < no_mid < 0.999:
                    _dual_pos.no_price = no_mid

            if cid in self.state.positions:
                pos = self.state.positions[cid]
                if pos.phase == "TP_CLOSED":
                    return  # Already sold, wait for window reset
                # Prefer WS prices over REST when WS is active and fresh (<5s)
                # REST orderbook can lag behind WS, causing stale SL triggers
                import time as _time_mod
                _ws_fresh = (self._pm_feed and
                             (_time_mod.monotonic() - self._pm_feed.get_last_update_ts(yes_id)) < 5)
                if 0 < yes_mid < 0.999 and not _ws_fresh:
                    pos.yes_price = yes_mid
                elif 0 < yes_mid < 0.999 and _ws_fresh:
                    yes_mid = pos.yes_price  # use WS value for logging below
                if 0 < no_mid < 0.999 and not _ws_fresh:
                    pos.no_price = no_mid
                elif 0 < no_mid < 0.999 and _ws_fresh:
                    no_mid = pos.no_price  # use WS value for logging below
                # Log current prices for open position (consumed by positions UI)
                main_p = yes_mid if pos.entry_side == "YES" else no_mid
                entry_p = pos.yes_entry_price if pos.entry_side == "YES" else pos.no_entry_price
                pnl_pct = ((main_p - entry_p) / entry_p * 100) if entry_p > 0 else 0
                # SL/TP target prices
                _eff_tp = pos.tp_ratio if pos.tp_ratio > 0 else _cfg.TP_RATIO
                _eff_sl = pos.sl_ratio if pos.sl_ratio > 0 else _cfg.SL_RATIO
                _tp_str = f" tp=${entry_p * (1 + _eff_tp):.3f}" if _eff_tp > 0 else ""
                _sl_str = f" sl=${entry_p * (1 - _eff_sl):.3f}" if _eff_sl > 0 else ""
                # OKX market price move % (15-min candle)
                _okx_move = self._okx_feed.get_move_pct(market_symbol) if self._okx_feed else 0.0
                _okx_str = f" {market_symbol}={_okx_move:+.3f}%" if _okx_move != 0 else ""
                # ANSI color for PnL: green=profit, red=loss
                _pnl_raw = f"{pnl_pct:+.1f}%"
                if pnl_pct > 0:
                    _pnl_col = f"\033[32m{_pnl_raw}\033[0m"  # green
                elif pnl_pct < 0:
                    _pnl_col = f"\033[31m{_pnl_raw}\033[0m"  # red
                else:
                    _pnl_col = _pnl_raw
                # Log [POS] on every REST scan cycle with current PnL
                _tsl_str = (f" TSL_L{pos.tsl_level}(floor={pos.tsl_floor:+.0%})"
                            if pos.tsl_floor > 0 else "")
                self._log(
                    f"[POS] {pos.market_slug} {pos.entry_side} "
                    f"now=${main_p:.3f} entry=${entry_p:.3f} "
                    f"pnl={_pnl_col}{_okx_str}{_tp_str}{_sl_str}{_tsl_str} phase={pos.phase}", "INFO")
                # --- Centralized TP/TSL/SL (regime-aware) ---
                _sold = await _v7_actions.evaluate_tp_tsl(
                    self, pos, _regime, _cfg, _cfg.MAX_RETRIES, _cfg.CLOB_HOST)
                if _sold:
                    return

                # Prediction-flip exit — fresh prediction opposes locked entry with high confidence
                if (_cfg.EXIT_BY_PREDICTION
                        and pos.phase in ("ENTERED", "HEDGED")
                        and market_symbol in self._locked_prediction):
                    _locked_pred, _locked_conf, *_ = self._locked_prediction[market_symbol]
                    _fresh_pred, _fresh_conf, _, _fresh_q, *_ = self.read_prediction(market_symbol)
                    _fresh_side = "YES" if _fresh_pred == "UP" else (
                        "NO" if _fresh_pred == "DOWN" else None)
                    if (_fresh_side and _fresh_side != pos.entry_side
                            and _fresh_conf >= _cfg.MIN_EXIT_CONFIDENCE):
                        self._log(
                            f"[PRED-FLIP] {pos.market_slug} locked={_locked_pred} "
                            f"({_locked_conf:.0%}) → fresh={_fresh_pred} "
                            f"({_fresh_conf:.0%}) — exiting {pos.entry_side}",
                            "WARN")
                        sold = await _v7_actions.sell_all_positions(
                            self, pos, _cfg.MAX_RETRIES, reason="PRED_FLIP")
                        if sold:
                            return
                # Check hedge opportunity (only in "hedge" volatility mode)
                if pos.phase == "ENTERED" and _cfg.VOLATILITY_MODE == "hedge":
                    hedge_price = self._get_effective_hedge_price(market_symbol)
                    await _v7_actions.check_hedge_opportunity(
                        self, pos, _cfg.CLOB_HOST, hedge_price,
                        _cfg.HEDGE_ENTRY_PRICE_USD, _cfg.MAX_RETRIES)
                # Yolo with existing position: just hold for resolution
                return

            # --- No position yet: check entry conditions ---
            if _cfg.MANAGE_POSITIONS_ONLY:
                return  # manage-only mode: skip new entries
            slug = market.get("slug", "?")
            _prices = f"YES=${yes_mid:.3f} NO=${no_mid:.3f}"

            # Yolo mode: market buy whichever side ask <= VOLATILITY_HEDGE_PRICE
            if _cfg.VOLATILITY_MODE == "yolo":
                if not self._capital_ok:
                    self._log(
                        f"[SCAN] {slug} {_prices} — yolo skip: capital below threshold", "INFO")
                    return
                yolo_side = None
                yolo_ask = 0
                if 0 < yes_ask <= _cfg.VOLATILITY_HEDGE_PRICE:
                    yolo_side, yolo_ask = "YES", yes_ask
                elif 0 < no_ask <= _cfg.VOLATILITY_HEDGE_PRICE:
                    yolo_side, yolo_ask = "NO", no_ask
                if yolo_side:
                    await _v7_actions.execute_yolo_entry(
                        self, market, yes_id, no_id, yolo_side,
                        yolo_ask, _cfg.HEDGE_ENTRY_PRICE_USD,
                        _cfg.MAX_RETRIES,
                        volatility_hedge_price=_cfg.VOLATILITY_HEDGE_PRICE)
                else:
                    self._log(
                        f"[SCAN] {slug} {_prices} — yolo: "
                        f"no side ask <= ${_cfg.VOLATILITY_HEDGE_PRICE}", "INFO")
                return

            # Cheap mode: buy any side with mid <= CHEAP_ENTRY_PRICE (skip timing/prediction/band)
            if _cfg.CHEAP_MODE:
                if not self._capital_ok:
                    self._log(
                        f"[SCAN] {slug} {_prices} — cheap skip: capital below threshold", "INFO")
                    return
                # Prefer WS prices for lower latency
                _ym_c, _nm_c, _ya_c, _na_c = yes_mid, no_mid, yes_ask, no_ask
                if self._pm_feed:
                    _ws = self._pm_feed.get_mid(yes_id)
                    if _ws > 0:
                        _ym_c = _ws
                    _ws = self._pm_feed.get_mid(no_id)
                    if _ws > 0:
                        _nm_c = _ws
                    _ws = self._pm_feed.get_best_ask(yes_id)
                    if _ws > 0:
                        _ya_c = _ws
                    _ws = self._pm_feed.get_best_ask(no_id)
                    if _ws > 0:
                        _na_c = _ws

                # Counter-trend gate: only enter when prediction flips from confirmed trend
                if _cfg.CHEAP_COUNTER_TREND:
                    prediction, confidence, _, _q, *_ = self.read_prediction(market_symbol)
                    prev_trend = self._window_trends.get(market_symbol)
                    wts = self._get_window_ts()
                    elapsed_min = (time.time() - wts) / 60
                    if not prev_trend:
                        # Wait for ENTRY_MINUTE before confirming initial trend
                        if elapsed_min < _cfg.ENTRY_MINUTE:
                            self._log(
                                f"[SCAN] {slug} {_prices} — cheap-ct: waiting for "
                                f"min {elapsed_min:.1f}/{_cfg.ENTRY_MINUTE} before "
                                f"trend lock", "INFO")
                            return
                        # No trend confirmed yet — need confident prediction to establish
                        if prediction and confidence >= _cfg.MIN_CONFIDENCE:
                            self._window_trends[market_symbol] = prediction
                            self._log(
                                f"[CHEAP-CT] {slug} trend confirmed: {prediction} "
                                f"({confidence:.0%}) — watching for flip", "INFO")
                        else:
                            self._log(
                                f"[SCAN] {slug} {_prices} — cheap-ct: waiting for "
                                f"trend confirmation (pred={prediction or 'none'} "
                                f"{confidence:.0%} < {_cfg.MIN_CONFIDENCE:.0%})", "INFO")
                        return
                    # Trend confirmed — check if prediction flipped
                    if not prediction or prediction == prev_trend:
                        self._log(
                            f"[SCAN] {slug} {_prices} — cheap-ct: trend={prev_trend} "
                            f"pred={prediction or 'none'} ({confidence:.0%}) — no flip yet",
                            "INFO")
                        return
                    # Prediction flipped! Now check price threshold
                    self._log(
                        f"[CHEAP-CT] {slug} FLIP detected: {prev_trend} -> {prediction} "
                        f"({confidence:.0%})", "INFO")

                cheap_side, cheap_ask = None, 0
                # Pick cheapest side that falls under threshold
                if 0 < _ym_c <= _cfg.CHEAP_ENTRY_PRICE:
                    cheap_side, cheap_ask = "YES", _ya_c
                if 0 < _nm_c <= _cfg.CHEAP_ENTRY_PRICE and (
                        not cheap_side or _nm_c < _ym_c):
                    cheap_side, cheap_ask = "NO", _na_c
                if cheap_side:
                    cheap_mid = _ym_c if cheap_side == "YES" else _nm_c
                    ct_tag = "CHEAP-CT" if _cfg.CHEAP_COUNTER_TREND else "CHEAP"
                    self._log(
                        f"{ct_tag} ENTRY {slug}: {cheap_side} mid=${cheap_mid:.3f} "
                        f"<= ${_cfg.CHEAP_ENTRY_PRICE} — buying ${_cfg.POSITION_SIZE_USD}",
                        "ENTRY")
                    pos = await _v7_actions.execute_entry(
                        self, market, yes_id, no_id, cheap_side, cheap_ask,
                        _cfg.POSITION_SIZE_USD, _cfg.MAX_RETRIES,
                        max_fill_price=_cfg.CHEAP_ENTRY_PRICE)
                    if pos and _cfg.DUAL_MODE_ENABLED:
                        opp = "NO" if cheap_side == "YES" else "YES"
                        opp_ask = _na_c if opp == "NO" else _ya_c
                        if opp_ask > 0:
                            await _v7_actions.execute_dual_entry(
                                self, market, yes_id, no_id, opp, opp_ask,
                                _cfg.DUAL_POSITION_SIZE_USD, _cfg.DUAL_TP_RATIO,
                                _cfg.DUAL_SL_RATIO, _cfg.MAX_RETRIES)
                else:
                    ct_tag = "cheap-ct" if _cfg.CHEAP_COUNTER_TREND else "cheap"
                    self._log(
                        f"[SCAN] {slug} {_prices} — {ct_tag}: no side "
                        f"<= ${_cfg.CHEAP_ENTRY_PRICE}", "INFO")
                return

            # Prediction source health gate — skip new entries if service unavailable
            if not self._redis_healthy:
                self._log(
                    f"[SCAN] {slug} {_prices} — skip: "
                    f"{_cfg.PREDICTION_SOURCE.upper()} prediction service unavailable", "INFO")
                return

            # Capital protection gate — skip new entries if account value below threshold
            if not self._capital_ok:
                total = self._compute_total_account_value()
                self._log(
                    f"[SCAN] {slug} {_prices} — skip: capital below threshold "
                    f"(${total:.2f} < ${_cfg.BOT_STOP_THRESHOLD:.2f})", "INFO")
                return

            wts = self._get_window_ts()
            elapsed_sec = time.time() - wts
            elapsed_min = elapsed_sec / 60

            # --- Manual entry from dashboard (bypasses all gates) ---
            if self._manual_entry_cmd:
                _me_cmd = self._manual_entry_cmd
                self._manual_entry_cmd = None
                parts = _me_cmd.split(":")
                _me_type = parts[0]  # ENTER_FIXED or ENTER_PCT
                _me_value = float(parts[1])
                _me_side = parts[2] if len(parts) > 2 else "YES"
                if _me_type == "ENTER_PCT":
                    _me_amount = self.current_balance * _me_value / 100.0
                else:
                    _me_amount = _me_value
                _me_ask = yes_ask if _me_side == "YES" else no_ask
                _me_mid = yes_mid if _me_side == "YES" else no_mid
                if _me_ask > 0 and _me_amount > 0:
                    self._log(
                        f"MANUAL ENTRY {slug}: {_me_side} mid=${_me_mid:.3f} "
                        f"— buying ${_me_amount:.1f} (dashboard)", "ENTRY")
                    _me_meta = {
                        "entry_type": "MANUAL",
                        "manual_cmd": _me_cmd,
                        "elapsed_sec": round(elapsed_sec, 1),
                        "yes_mid": round(yes_mid, 4), "no_mid": round(no_mid, 4),
                    }
                    pos = await _v7_actions.execute_entry(
                        self, market, yes_id, no_id, _me_side, _me_ask,
                        _me_amount, _cfg.MAX_RETRIES,
                        max_fill_price=0.95, entry_meta=_me_meta)
                    if pos:
                        pos.entry_type = "MANUAL"
                        self._seeded_this_window.add(cid)
                        self._save_state()
                    return
                else:
                    self._log(
                        f"[MANUAL] {slug} — skip: no ask or zero amount "
                        f"(ask=${_me_ask:.3f} amount=${_me_amount:.1f})", "WARN")

            # Choppy market gate — block entries when external watchdog flags choppiness
            if self._is_choppy_paused():
                self._log(f"[SCAN] {slug} {_prices} — skip: choppy market (watchdog signal)", "INFO")
                return

            # Volatility entry gate — block entries when market is flat
            if not self._check_volatility_entry_gate(market_symbol):
                okx_price = self._okx_feed.get_price(market_symbol) if self._okx_feed else 0
                ptb = self._get_price_to_beat(market_symbol)
                threshold = _cfg.VOLATILITY_HEDGE_THRESHOLD.get(
                    market_symbol.upper(), _cfg.VOLATILITY_HEDGE_THRESHOLD_DEFAULT)
                if ptb > 0 and okx_price > 0:
                    dist = abs(okx_price - ptb) / ptb * 100
                    self._log(
                        f"[SCAN] {slug} {_prices} — skip: flat market "
                        f"({market_symbol.upper()} strike_dist={dist:.3f}% < {threshold}% "
                        f"ptb=${ptb:,.4g} okx=${okx_price:,.4g})",
                        "INFO")
                else:
                    self._log(
                        f"[SCAN] {slug} {_prices} — skip: no priceToBeat "
                        f"({market_symbol.upper()})",
                        "INFO")
                return

            # --- Liquidity gate ---
            if _cfg.MIN_LIQUIDITY > 0:
                depth = (_v7_engine.get_book_depth(yes_book)
                         + _v7_engine.get_book_depth(no_book))
                if depth < _cfg.MIN_LIQUIDITY:
                    self._log(
                        f"[SCAN] {slug} {_prices} — skip: low liquidity "
                        f"(${depth:.0f} < ${_cfg.MIN_LIQUIDITY:.0f})", "INFO")
                    return

            # --- Pace detection: enter when PM price surges on one side ---
            elapsed_sec = (time.time() - wts)
            _pace_triggered = False
            # Record REST prices for pace detection (supplements WS feed)
            if _cfg.PACE_DETECT:
                _info = self._known_markets.get(cid, {})
                _ytid = _info.get("yes_token_id", "")
                _ntid = _info.get("no_token_id", "")
                if _ytid and yes_mid > 0:
                    _v7_pace.record_price(_ytid, yes_mid)
                if _ntid and no_mid > 0:
                    _v7_pace.record_price(_ntid, no_mid)
            if (_cfg.PACE_DETECT
                    and cid not in self._seeded_this_window
                    and cid not in self.state.positions):
                _info = self._known_markets.get(cid, {})
                _pace_side, _fak_price, _pace_pct = _v7_pace.evaluate_pace(
                    _cfg, _info.get("yes_token_id", ""), _info.get("no_token_id", ""),
                    yes_mid, no_mid, yes_ask, no_ask,
                    elapsed_sec, slug, self._log)
                if _pace_side:
                    # Gate: require HYPER prediction agreement if enabled
                    if _cfg.HYPER_PREDICTION:
                        try:
                            _hp = _v7_engine.read_prediction_signal_http(
                                _cfg.HYPER_PREDICTION_API_URL, _cfg.PREDICTION_API_USER,
                                _cfg.PREDICTION_API_PASS, symbol=market_symbol,
                                expected_window_ts=wts)
                            _hp_dir = _hp[0] if _hp else ""
                            _hp_side = "YES" if _hp_dir == "UP" else ("NO" if _hp_dir == "DOWN" else "")
                            if _hp_side != _pace_side:
                                self._log(
                                    f"[PACE] {slug} — skip: pace={_pace_side} but hyper={_hp_dir} "
                                    f"(disagree, pace={_pace_pct:+.1f}%)", "INFO")
                                return
                        except Exception as _hpe:
                            self._log(f"[PACE] {slug} — hyper check error: {_hpe}", "WARN")
                    # Gate: require WHALE trend agreement if enabled
                    if _cfg.WHALE_TREND_SCALPING:
                        try:
                            import httpx
                            _wr = httpx.get(_cfg.WHALE_TREND_API_URL, timeout=3,
                                            auth=(_cfg.PREDICTION_API_USER, _cfg.PREDICTION_API_PASS))
                            _wt = _wr.json().get("trend", "") if _wr.status_code == 200 else ""
                            _wt_side = "YES" if _wt == "BULLISH" else ("NO" if _wt == "BEARISH" else "")
                            if _wt_side != _pace_side:
                                self._log(
                                    f"[PACE] {slug} — skip: pace={_pace_side} but whale={_wt} "
                                    f"(disagree, pace={_pace_pct:+.1f}%)", "INFO")
                                return
                        except Exception as _we:
                            self._log(f"[PACE] {slug} — whale check error: {_we}", "WARN")
                    _pace_triggered = True
                    # Build entry meta with all prediction state for mongo logging
                    _pred = self._last_prediction.get(market_symbol, ("", 0, 0, 0, 0, 0, 0, 0))
                    _entry_meta = {
                        "entry_type": "PACE",
                        "pace_pct": round(_pace_pct, 3),
                        "pace_window_sec": _cfg.PACE_DETECT_WINDOW_SEC,
                        "elapsed_sec": round(elapsed_sec, 1),
                        "yes_mid": round(yes_mid, 4), "no_mid": round(no_mid, 4),
                        "prediction": _pred[0], "confidence": round(_pred[1], 4),
                        "quality_score": round(_pred[3], 4) if _pred[3] else 0,
                        "cross_pair_agreement": _pred[4],
                        "momentum": round(_pred[5], 4) if _pred[5] else 0,
                        "weighted_signal": round(_pred[7], 4) if len(_pred) > 7 and _pred[7] else 0,
                    }
                    pos = await _v7_actions.execute_entry(
                        self, market, yes_id, no_id, _pace_side, _fak_price,
                        _cfg.POSITION_SIZE_USD, _cfg.MAX_RETRIES,
                        max_fill_price=_cfg.PACE_DETECT_PRICE_CAP,
                        entry_meta=_entry_meta)
                    if pos:
                        pos.entry_type = "PACE"
                        self._seeded_this_window.add(cid)
                        self._save_state()
                    return

            # --- Hyper prediction: enter based on Hyperliquid trade direction ---
            if (_cfg.HYPER_PREDICTION
                    and not _pace_triggered
                    and cid not in self._seeded_this_window
                    and cid not in self.state.positions
                    and _cfg.HYPER_PREDICTION_ENTRY_SEC <= elapsed_sec <= _cfg.HYPER_PREDICTION_MAX_SEC):
                try:
                    _hyper = _v7_engine.read_prediction_signal_http(
                        _cfg.HYPER_PREDICTION_API_URL, _cfg.PREDICTION_API_USER,
                        _cfg.PREDICTION_API_PASS, symbol=market_symbol,
                        expected_window_ts=wts)
                    _h_pred, _h_conf = _hyper[0], _hyper[1]
                    if _h_pred in ("UP", "DOWN"):
                        _h_side = "YES" if _h_pred == "UP" else "NO"
                        _h_mid = yes_mid if _h_side == "YES" else no_mid
                        _h_ask = yes_ask if _h_side == "YES" else no_ask
                        # Buy band gate
                        if _cfg.BUY_BAND_LOW <= _h_mid <= _cfg.BUY_BAND_HIGH and _h_ask > 0:
                            _h_fak = min(_h_ask, _h_mid + _cfg.BUY_MAX_ABOVE_MID) if _cfg.BUY_MAX_ABOVE_MID > 0 else _h_ask
                            self._log(
                                f"\033[96mHYPER ENTRY\033[0m {slug}: {_h_side} mid=${_h_mid:.3f} "
                                f"hyper={_h_pred} @{elapsed_sec:.0f}s "
                                f"— buying ${_cfg.POSITION_SIZE_USD}", "ENTRY")
                            _h_meta = {
                                "entry_type": "HYPER",
                                "hyper_prediction": _h_pred,
                                "hyper_confidence": round(_h_conf, 4),
                                "elapsed_sec": round(elapsed_sec, 1),
                                "yes_mid": round(yes_mid, 4), "no_mid": round(no_mid, 4),
                            }
                            pos = await _v7_actions.execute_entry(
                                self, market, yes_id, no_id, _h_side, _h_fak,
                                _cfg.POSITION_SIZE_USD, _cfg.MAX_RETRIES,
                                max_fill_price=_cfg.BUY_BAND_HIGH,
                                entry_meta=_h_meta)
                            if pos:
                                pos.entry_type = "HYPER"
                                self._seeded_this_window.add(cid)
                                self._save_state()
                            return
                        else:
                            self._log(
                                f"[HYPER] {slug} — skip: {_h_side} mid=${_h_mid:.3f} "
                                f"outside band [{_cfg.BUY_BAND_LOW}-{_cfg.BUY_BAND_HIGH}]", "INFO")
                except Exception as _he:
                    self._log(f"[HYPER] {slug} — API error: {_he}", "WARN")

            # --- Oracle gate: CEX-implied early-entry accelerator ---
            if (self._oracle_gate and not _pace_triggered
                    and cid not in self._seeded_this_window
                    and cid not in self.state.positions):
                # Feed oracle with latest Binance prices
                if self._okx_feed:
                    _o_price = self._okx_feed.get_price(market_symbol)
                    _o_open = self._okx_feed.get_window_open(market_symbol)
                    if _o_price > 0 and _o_open > 0:
                        self._oracle_gate.update(market_symbol, _o_price, _o_open)
                # Check oracle early-entry window (after ORACLE_EARLY_ENTRY_SEC, before ENTRY_MINUTE)
                _entry_min_sec = _cfg.ENTRY_MINUTE * 60 if not _cfg.CROSS_MARKET_TRADE else _cfg.CROSS_MARKET_ENTRY_MIN * 60
                if (elapsed_sec >= _cfg.ORACLE_EARLY_ENTRY_SEC
                        and elapsed_sec < _entry_min_sec):
                    _o_signal = self._oracle_gate.get_signal(market_symbol, yes_mid)
                    if _o_signal and _o_signal.divergence >= _cfg.ORACLE_MIN_DIFF:
                        # Oracle sees divergence — check if prediction agrees
                        _o_pred = self.read_prediction(market_symbol)
                        _o_pred_dir = _o_pred[0] if _o_pred[0] in ("UP", "DOWN") else None
                        _o_conf = float(_o_pred[1]) if _o_pred[1] else 0.0
                        if (_o_pred_dir and _o_signal.direction == _o_pred_dir
                                and _o_conf >= _cfg.MIN_CONFIDENCE):
                            _o_side = "YES" if _o_pred_dir == "UP" else "NO"
                            _o_ask = yes_ask if _o_side == "YES" else no_ask
                            self._log(
                                f"[ORACLE] {slug} {_prices} — early entry! "
                                f"oracle={_o_signal.oracle_yes:.3f} div={_o_signal.divergence:.3f} "
                                f"dir={_o_signal.direction} pred={_o_pred_dir}({_o_conf:.0%}) "
                                f"@{elapsed_sec:.0f}s → {_o_side}", "INFO")
                            pos = await _v7_actions.execute_entry(
                                self, market, yes_id, no_id, _o_side, _o_ask,
                                _cfg.POSITION_SIZE_USD, _cfg.MAX_RETRIES)
                            if pos:
                                pos.entry_type = "ORACLE"
                                self._seeded_this_window.add(cid)
                                self._save_state()
                            return
                        elif _o_pred_dir and _o_signal.direction != _o_pred_dir:
                            self._log(
                                f"[ORACLE] {slug} — oracle={_o_signal.direction} vs "
                                f"pred={_o_pred_dir} disagree @{elapsed_sec:.0f}s, waiting", "DEBUG")

            # --- Cross-market mode: entry timing override for 15m markets using 5m signals ---
            if _cfg.CROSS_MARKET_TRADE:
                if elapsed_min < _cfg.CROSS_MARKET_ENTRY_MIN:
                    self._log(
                        f"[CROSS] {slug} {_prices} — waiting: "
                        f"min {elapsed_min:.1f}/{_cfg.CROSS_MARKET_ENTRY_MIN}", "INFO")
                    return
                if elapsed_min > _cfg.CROSS_MARKET_ENTRY_MAX:
                    self._log(
                        f"[CROSS] {slug} {_prices} — skip: "
                        f"past entry window (min {elapsed_min:.1f} > {_cfg.CROSS_MARKET_ENTRY_MAX})", "INFO")
                    return
                # Log cross-market entry evaluation details
                _leading = "YES" if yes_mid > no_mid else "NO"
                _trailing = "NO" if _leading == "YES" else "YES"
                _lead_price = yes_mid if _leading == "YES" else no_mid
                _trail_price = no_mid if _leading == "YES" else yes_mid
                self._log(
                    f"[CROSS] {slug} {_prices} — evaluating: "
                    f"{_cfg.WINDOW_MINUTES}m leading={_leading}(${_lead_price:.3f}) "
                    f"trailing={_trailing}(${_trail_price:.3f}) "
                    f"min {elapsed_min:.1f} in [{_cfg.CROSS_MARKET_ENTRY_MIN}-{_cfg.CROSS_MARKET_ENTRY_MAX}]",
                    "INFO")

            # --- Pace/Hyper-only mode: skip normal prediction entry ---
            if _cfg.PACE_DETECT or _cfg.HYPER_PREDICTION:
                return

            # --- Unified entry path ---
            # ENTRY_MINUTE gate (skipped when cross-market handles timing above)
            if not _cfg.CROSS_MARKET_TRADE and elapsed_min < _cfg.ENTRY_MINUTE:
                self._log(
                    f"[SCAN] {slug} {_prices} — waiting: "
                    f"min {elapsed_min:.1f}/{_cfg.ENTRY_MINUTE}", "INFO")
                return
            # Max entry time — don't enter too late in the window
            if _cfg.ENTRY_MAX_MINUTE > 0 and elapsed_min > _cfg.ENTRY_MAX_MINUTE:
                self._log(
                    f"[SCAN] {slug} {_prices} — skip: too late "
                    f"(min {elapsed_min:.1f} > max {_cfg.ENTRY_MAX_MINUTE})", "INFO")
                return

            # Prefer WS prices for lower latency; fall back to REST
            _ym, _nm, _ya, _na = yes_mid, no_mid, yes_ask, no_ask
            if self._pm_feed:
                _ws_ym = self._pm_feed.get_mid(yes_id)
                _ws_nm = self._pm_feed.get_mid(no_id)
                _ws_ya = self._pm_feed.get_best_ask(yes_id)
                _ws_na = self._pm_feed.get_best_ask(no_id)
                if _ws_ym > 0:
                    _ym = _ws_ym
                if _ws_nm > 0:
                    _nm = _ws_nm
                if _ws_ya > 0:
                    _ya = _ws_ya
                if _ws_na > 0:
                    _na = _ws_na

            # --- Shared entry-side decision ---
            _lock_had_key = market_symbol in self._locked_prediction
            side, ask, prediction, confidence, quality_score, cross_pair_agreement, skip_reason = \
                _v7_entry.decide_entry_side(
                    _cfg, _ym, _nm, _ya, _na,
                    self.read_prediction, self._locked_prediction,
                    market_symbol, elapsed_min,
                    read_hyper_fn=self.read_hyper_prediction)
            # Log when decide_entry_side freshly wrote the prediction lock
            if (not _lock_had_key and market_symbol in self._locked_prediction):
                self._log(
                    f"[PRED-LOCK] {slug} locked prediction: {prediction} "
                    f"({confidence:.0%}) at min {elapsed_min:.1f}", "INFO")

            if not side:
                _scan_tag = "[CROSS]" if _cfg.CROSS_MARKET_TRADE else "[SCAN]"
                _cp = self._last_prediction.get(market_symbol)
                _ws_tag = ""
                if _cp and _cp[0]:
                    _ws_v = _cp[7] if len(_cp) > 7 else 0.0
                    _mom_v = _cp[5] if len(_cp) > 5 else 0.0
                    _qs_v = _cp[3] if len(_cp) > 3 else 0.0
                    _ws_tag = f" qs={_qs_v:+.3f} ws={_ws_v:+.3f} mom={_mom_v:+.3f}"
                self._log(f"{_scan_tag} {slug} {_prices} — {skip_reason}{_ws_tag}", "INFO")
                # Mark seeded if prediction-driven band-rejected (don't retry same prediction)
                if "outside band" in skip_reason and _cfg.PREDICTION_SOURCE != "none" and _cfg.ENTRY_PRICE <= 0:
                    self._seeded_this_window.add(cid)
                return

            # Prediction confirmation gate — check counter (advanced above per HTTP poll)
            _confirm_n = _cfg.PRED_CONFIRM_TICKS
            if _confirm_n > 0:
                _prev_side, _prev_count = self._pred_confirm.get(market_symbol, (None, 0))
                if _prev_side != side or _prev_count < _confirm_n:
                    _cp2 = self._last_prediction.get(market_symbol)
                    _ws_t2 = ""
                    if _cp2 and _cp2[0]:
                        _ws_t2 = f" qs={_cp2[3] if len(_cp2) > 3 else 0:+.3f} ws={_cp2[7] if len(_cp2) > 7 else 0:+.3f} mom={_cp2[5] if len(_cp2) > 5 else 0:+.3f}"
                    self._log(
                        f"[SCAN] {slug} {_prices} — pred confirm {_prev_count}/{_confirm_n} ({side}){_ws_t2}",
                        "INFO")
                    return

            mid_price = _ym if side == "YES" else _nm

            # Execute entry — check seeded before logging to avoid misleading logs
            if cid in self._seeded_this_window or cid in self.state.positions:
                return

            # Hybrid maker/taker: use maker when confidence is medium + spread is wide enough
            _use_maker = False
            _entry_conf = confidence if _cfg.PREDICTION_SOURCE != "none" else 0
            _entry_bid = yes_bid if side == "YES" else no_bid
            _entry_spread = ask - _entry_bid if _entry_bid > 0 else 0
            if (_cfg.MAKER_MODE
                    and _entry_conf >= _cfg.MIN_CONFIDENCE
                    and _entry_conf < _cfg.MAKER_CONFIDENCE
                    and _entry_spread >= _cfg.MAKER_MIN_SPREAD
                    and _entry_bid > 0):
                _use_maker = True

            # Build prediction tag for entry log
            _pred_tag = ""
            if _cfg.PREDICTION_SOURCE != "none":
                _p = prediction or "none"
                _c = confidence if confidence else 0
                _q_tag = f" q={quality_score:.2f}" if quality_score > 0 else ""
                _pred_tag = f" pred={_p}({_c:.0%}){_q_tag}"

            _entry_label = "\033[35mCROSS ENTRY\033[0m" if _cfg.CROSS_MARKET_TRADE else "ENTRY"
            if _use_maker:
                _maker_label = "\033[35mCROSS MAKER ENTRY\033[0m" if _cfg.CROSS_MARKET_TRADE else "MAKER ENTRY"
                self._log(
                    f"{_maker_label} {slug}: {side} mid=${mid_price:.3f} "
                    f"bid=${_entry_bid:.3f} spread=${_entry_spread:.3f} "
                    f"conf={_entry_conf:.0%}{_pred_tag} — maker ${_cfg.POSITION_SIZE_USD}",
                    "ENTRY")
                pos = await _v7_actions.execute_maker_entry(
                    self, market, yes_id, no_id, side, _entry_bid,
                    _cfg.POSITION_SIZE_USD, _cfg.MAX_RETRIES,
                    max_fill_price=_cfg.BUY_BAND_HIGH)
            else:
                # Cap FAK price at mid + buffer to avoid overpaying spread
                _fak_price = ask
                if _cfg.BUY_MAX_ABOVE_MID > 0:
                    _fak_price = min(ask, mid_price + _cfg.BUY_MAX_ABOVE_MID)
                self._log(
                    f"{_entry_label} {slug}: {side} mid=${mid_price:.3f}"
                    f"{_pred_tag} — buying ${_cfg.POSITION_SIZE_USD}", "ENTRY")
                _pred = self._last_prediction.get(market_symbol, ("", 0, 0, 0, 0, 0, 0, 0))
                _entry_meta = {
                    "entry_type": "NORMAL",
                    "elapsed_sec": round(elapsed_sec, 1),
                    "yes_mid": round(yes_mid, 4), "no_mid": round(no_mid, 4),
                    "prediction": _pred[0], "confidence": round(_pred[1], 4),
                    "quality_score": round(_pred[3], 4) if _pred[3] else 0,
                    "cross_pair_agreement": _pred[4],
                    "momentum": round(_pred[5], 4) if _pred[5] else 0,
                    "weighted_signal": round(_pred[7], 4) if len(_pred) > 7 and _pred[7] else 0,
                }
                pos = await _v7_actions.execute_entry(
                    self, market, yes_id, no_id, side, _fak_price,
                    _cfg.POSITION_SIZE_USD, _cfg.MAX_RETRIES,
                    max_fill_price=_cfg.BUY_BAND_HIGH,
                    entry_meta=_entry_meta)
            # Hedge limit order after entry
            if pos and _cfg.HEDGE_PRICE > 0 and _cfg.HEDGE_ENTRY_PRICE_USD > 0:
                await _v7_actions.place_entry_hedge_limit(
                    self, pos, _cfg.HEDGE_PRICE,
                    _cfg.HEDGE_ENTRY_PRICE_USD, _cfg.MAX_RETRIES)
            # Dual mode: buy opposite side with independent TP/SL
            if pos and _cfg.DUAL_MODE_ENABLED:
                opp_side = "NO" if side == "YES" else "YES"
                opp_ask = _na if opp_side == "NO" else _ya
                if opp_ask > 0:
                    await _v7_actions.execute_dual_entry(
                        self, market, yes_id, no_id, opp_side, opp_ask,
                        _cfg.DUAL_POSITION_SIZE_USD, _cfg.DUAL_TP_RATIO,
                        _cfg.DUAL_SL_RATIO, _cfg.MAX_RETRIES)

        except Exception as e:
            logger.error(f"Error scanning {mc.get('name', '?')}: {e}")

    def _startup_sync(self):
        """One-time sync at boot: load existing positions from API to prevent double-entry."""
        if self.dry_run or not self.trader or not self._known_markets:
            return
        try:
            api_pos = _v7_sync.fetch_api_positions(
                _cfg.DATA_HOST, self.trader.trading_address)
            if api_pos:
                _v7_sync.sync_positions(
                    self, api_pos, self._known_markets, create_new=True)
                if self.state.positions:
                    self._log(
                        f"[STARTUP] Loaded {len(self.state.positions)} "
                        f"existing positions from API", "INFO")
        except Exception as e:
            logger.warning(f"[STARTUP SYNC] Error: {e}")

    def _save_state(self):
        """Persist positions + window state to JSON file (atomic write).

        Called after every state change so a restart can resume without double-entry.
        """
        try:
            state_path = Path(_cfg.STATE_FILE)
            tmp_path = state_path.with_suffix(".json.tmp")
            payload = {
                "last_window_ts": self._last_window_ts,
                "seeded_this_window": list(self._seeded_this_window),
                "begin_session_balance": self._begin_session_balance,
                # Dry-run balance must persist across restarts — otherwise
                # SIMULATED_BALANCE resets to default at boot and entry_cost
                # debits from the pre-restart session get lost.
                "dry_run_current_balance": self.current_balance if self.dry_run else None,
                "positions": {
                    cid: asdict(pos)
                    for cid, pos in self.state.positions.items()
                },
            }
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            os.replace(tmp_path, state_path)
        except Exception as e:
            logger.warning(f"[STATE] Save failed: {e}")

    def _load_state(self):
        """Load persisted session state from JSON file.

        Only restores positions when saved window matches current window
        (stale state from a previous window is discarded).
        """
        state_path = Path(_cfg.STATE_FILE)
        if not state_path.exists():
            return  # first run — nothing to load
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            logger.warning(f"[STATE] Load failed (corrupt file?): {e} — starting fresh")
            return

        saved_ts = payload.get("last_window_ts", 0)
        current_ts = self._get_window_ts()
        if saved_ts != current_ts:
            logger.info(
                f"[STATE] Stale state (saved={saved_ts} current={current_ts}) — discarding")
            return

        # Restore seeded set
        self._seeded_this_window = set(payload.get("seeded_this_window", []))

        # Restore dry-run balance if present — authoritative source of truth
        # post-restart. When absent (old state files), the re-debit block below
        # approximates it by subtracting restored entry_costs.
        self._dry_balance_loaded_from_state = False
        if self.dry_run:
            saved_bal = payload.get("dry_run_current_balance")
            if saved_bal is not None and saved_bal >= 0:
                self.current_balance = float(saved_bal)
                self._dry_balance_loaded_from_state = True
                logger.info(
                    f"[STATE] Dry-run balance restored from state file: "
                    f"${self.current_balance:.2f}"
                )

        # Reconstruct HedgePosition objects
        HedgePosition = _v7_pos.HedgePosition
        restored = 0
        for cid, data in payload.get("positions", {}).items():
            try:
                self.state.positions[cid] = HedgePosition(**data)
                restored += 1
            except Exception as e:
                logger.warning(f"[STATE] Could not restore position {cid}: {e}")

        if restored:
            # Pre-populate market caches from restored positions to skip Gamma API discovery
            for cid, pos in self.state.positions.items():
                if cid.endswith("_dual"):
                    continue  # dual shares same CID as main
                real_cid = pos.condition_id
                # Match to a market config by slug prefix
                matched_pattern = None
                for mc in self.markets:
                    if pos.market_slug.startswith(mc["slug_pattern"]):
                        matched_pattern = mc["slug_pattern"]
                        break
                if not matched_pattern:
                    continue
                # Populate _known_markets
                self._known_markets[real_cid] = {
                    "slug": pos.market_slug,
                    "yes_token_id": pos.yes_token_id,
                    "no_token_id": pos.no_token_id,
                    "price_to_beat": 0.0,  # will be fetched on first discovery if needed
                }
                # Build minimal market dict for _market_cache (used by _scan_market)
                self._market_cache[matched_pattern] = {
                    "conditionId": real_cid,
                    "slug": pos.market_slug,
                    "clobTokenIds": [pos.yes_token_id, pos.no_token_id],
                    "outcomes": ["Up", "Down"],
                }
            logger.info(
                f"[STATE] Restored {restored} position(s) from {state_path.name} "
                f"(window={saved_ts})")

            # Dry-run only: re-debit balance for restored positions so the
            # accounting stays consistent across restarts. Live mode pulls
            # real chain balance on demand so this isn't needed there.
            # Without this, restart-while-open double-counts entry_cost at
            # resolution (balance credited +entry_cost at exit without ever
            # being decremented at entry), inflating current_balance.
            # Skip when balance was already loaded authoritatively from the
            # state file (new state-file format includes dry_run_current_balance).
            if self.dry_run and restored > 0 and not getattr(self, "_dry_balance_loaded_from_state", False):
                total_restored_cost = sum(
                    (getattr(p, "entry_cost", 0.0) or 0.0)
                    + (getattr(p, "hedge_cost", 0.0) or 0.0)
                    for p in self.state.positions.values()
                )
                if total_restored_cost > 0:
                    self.current_balance -= total_restored_cost
                    logger.info(
                        f"[STATE] Dry-run re-debited ${total_restored_cost:.2f} "
                        f"for {restored} restored position(s); "
                        f"balance=${self.current_balance:.2f}"
                    )

    async def _subscribe_ws_tokens(self):
        """Subscribe PM WS feed to all discovered token IDs + rebuild handler map."""
        if self._pm_feed and self._known_markets:
            _tids = []
            for _km in self._known_markets.values():
                _tids.extend([_km["yes_token_id"], _km["no_token_id"]])
            await self._pm_feed.subscribe(_tids)
        if self._ws_handler:
            self._ws_handler.rebuild_token_map()

    def _rotate_log_if_needed(self, log_file: Path) -> Path:
        """Check and rotate log file if window changed. Returns current log path."""
        new_log = _cfg.get_log_file()
        if new_log == log_file:
            return log_file
        try:
            root = logging.getLogger()
            for h in root.handlers[:]:
                h.close()
                root.removeHandler(h)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S")
            fh = logging.FileHandler(new_log, encoding="utf-8")
            fh.setFormatter(fmt)
            sh = logging.StreamHandler(sys.__stdout__)
            sh.setFormatter(fmt)
            root.addHandler(fh)
            root.addHandler(sh)
            _cfg.rotate_tee(new_log)
        except Exception:
            if not logging.getLogger().handlers:
                logging.getLogger().addHandler(logging.StreamHandler(sys.__stdout__))
        return new_log

    def _log_okx_volatility(self):
        """Log OKX price volatility per market with % distance from threshold."""
        if not self._okx_feed:
            return
        parts = []
        for mc in self.markets:
            sym = mc.get("slug_pattern", "").split("-")[0].upper()
            if not sym:
                continue
            move = self._okx_feed.get_move_pct(sym)
            price = self._okx_feed.get_price(sym)
            threshold = _cfg.VOLATILITY_HEDGE_THRESHOLD.get(
                sym, _cfg.VOLATILITY_HEDGE_THRESHOLD_DEFAULT)
            ptb = self._get_price_to_beat(sym)
            if ptb > 0:
                strike_dist = abs(price - ptb) / ptb * 100
                if _cfg.VOLATILITY_MODE == "hedge":
                    tag = "HEDGE" if strike_dist <= threshold else "SKIP"
                else:
                    tag = "GATE" if strike_dist <= threshold else "ENTER"
                dist = ((threshold - strike_dist) / threshold * 100) if threshold > 0 else 0
                parts.append(
                    f"{sym}=strike:{strike_dist:.3f}%/{threshold}%[{tag}]"
                    f"({dist:+.0f}%)@${price:,.4g}|ptb=${ptb:,.4g}")
            else:
                # No priceToBeat — show old format but tag as blocked
                if _cfg.VOLATILITY_MODE == "hedge":
                    tag = "HEDGE" if move <= threshold else "SKIP"
                else:
                    tag = "NO_PTB"
                parts.append(f"{sym}={move:.3f}%/{threshold}%[{tag}]@${price:.1f}")
        if parts:
            _okx_lvl = logging.DEBUG if self._has_active_positions() else logging.INFO
            logger.log(_okx_lvl, f"[OKX] {' | '.join(parts)}")

    async def _check_ws_stale_positions(self):
        """REST fallback for positions with stale WS prices (no update > threshold)."""
        if not self._pm_feed or not self._ws_handler or _cfg.WS_STALE_THRESHOLD <= 0:
            return
        import time as _time
        now = _time.monotonic()
        for cid, pos in list(self.state.positions.items()):
            if pos.phase in ("TP_CLOSED",) or cid.endswith("_dual"):
                continue
            info = self._known_markets.get(cid)
            if not info:
                continue
            yes_id = info["yes_token_id"]
            no_id = info["no_token_id"]
            # Check staleness of held side's token
            held_token = yes_id if pos.entry_side == "YES" else no_id
            last_ts = self._pm_feed.get_last_update_ts(held_token)
            if last_ts > 0 and (now - last_ts) < _cfg.WS_STALE_THRESHOLD:
                continue  # WS data is fresh
            # WS data stale — fetch REST orderbook and trigger evaluation
            try:
                yes_book = await _v7_engine.get_orderbook(_cfg.CLOB_HOST, yes_id)
                no_book = await _v7_engine.get_orderbook(_cfg.CLOB_HOST, no_id)
                yes_bid, yes_ask, yes_mid = _v7_engine.get_best_prices(yes_book)
                no_bid, no_ask, no_mid = _v7_engine.get_best_prices(no_book)
                if yes_mid > 0 or no_mid > 0:
                    stale_str = "never" if last_ts <= 0 else f"{now - last_ts:.1f}s"
                    logger.info(
                        f"[WS-STALE] {pos.market_slug}: REST fallback "
                        f"(stale {stale_str}) Y={yes_mid:.3f} N={no_mid:.3f}")
                    # Feed prices into WS handler for TP/SL evaluation
                    await self._ws_handler._evaluate_position(
                        cid, yes_mid, no_mid, yes_ask, no_ask)
            except Exception as e:
                logger.warning(f"[WS-STALE] REST fallback error {pos.market_slug}: {e}")

    async def _quick_price_scan(self):
        """Lightweight REST scan using /midpoint instead of full orderbook.

        Fetches mid prices cheaply, runs regime + entry decision.
        If entry triggered, delegates to WS handler's _evaluate_entry with REST prices.
        """
        for cid, info in self._known_markets.items():
            if not self.running:
                break
            if cid in self.state.positions or cid in self._seeded_this_window:
                continue
            yes_id = info["yes_token_id"]
            no_id = info["no_token_id"]
            slug = info.get("slug", "")
            try:
                yes_mid, no_mid = await asyncio.gather(
                    _v7_engine.get_midpoint(_cfg.CLOB_HOST, yes_id),
                    _v7_engine.get_midpoint(_cfg.CLOB_HOST, no_id))
                if yes_mid <= 0 and no_mid <= 0:
                    continue
                # Seed PM feed cache so WS handler can use these prices
                if self._pm_feed:
                    import time as _t
                    _mono = _t.monotonic()
                    if yes_mid > 0:
                        self._pm_feed._prices[yes_id] = {
                            "best_bid": yes_mid, "best_ask": yes_mid,
                            "mid": yes_mid, "_ts": _mono}
                    if no_mid > 0:
                        self._pm_feed._prices[no_id] = {
                            "best_bid": no_mid, "best_ask": no_mid,
                            "mid": no_mid, "_ts": _mono}
                    # Trigger WS handler evaluation with seeded prices
                    if self._ws_handler:
                        self._ws_handler.on_price_update(
                            yes_id, yes_mid, yes_mid, yes_mid)
            except Exception as e:
                logger.debug(f"[QUICK-SCAN] {slug}: {e}")

    async def _background_sync(self):
        """Background housekeeping: balance, API sync, capital, stale WS check.

        Market discovery + entry/TP/SL now handled by WsPriceHandler on every tick.
        This runs at BG_SYNC_INTERVAL as lightweight maintenance only.
        Discovery only runs as fallback when pre-discovery missed a window.
        """
        self._refresh_balance()
        self._capture_begin_balance_if_needed()
        # Fallback discovery if pre-discovery missed this window
        if not self._known_markets:
            await self._discover_markets()
            await self._subscribe_ws_tokens()
        self._check_prediction_health()
        if not self.dry_run and self.trader:
            self._sync_from_api()
        self._check_capital()
        await self._check_ws_stale_positions()
        # WS idle detection: only reconnect when positions need price monitoring
        _ws_idle = (self._ws_handler and
                    __import__('time').monotonic() - self._ws_handler._last_callback_mono
                    > _cfg.BG_SYNC_INTERVAL)
        if _ws_idle and self.running and self._known_markets and self.state.positions:
            # Force PM WS reconnect to get fresh book snapshots for exit management
            if self._pm_feed and self._pm_feed.is_connected:
                logger.info("[PM-WS] WS idle — forcing reconnect (positions active)")
                await self._pm_feed.force_reconnect()
        self._save_state()

    async def _full_rest_scan(self):
        """Full REST polling fallback: discover + orderbook + enforce/TP/SL."""
        self._refresh_balance()
        await self._discover_markets()
        await self._subscribe_ws_tokens()
        self._check_prediction_health()
        if not self.dry_run and self.trader:
            self._sync_from_api()
        self._check_capital()
        if self.running and self.markets:
            results = await asyncio.gather(
                *[self._scan_market(mc) for mc in self.markets],
                return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException):
                    logger.error(f"Market scan error: {r}")
            if _cfg.DUAL_MODE_ENABLED:
                await self._scan_dual_positions()
            self._save_state()

    async def _scan_dual_positions(self):
        """Background safety-net: evaluate TP/SL for dual-mode positions independently."""
        for pos_key in list(self.state.positions.keys()):
            if not pos_key.endswith("_dual"):
                continue
            pos = self.state.positions[pos_key]
            if pos.phase in ("TP_CLOSED", "CUT"):
                continue
            # Centralized TP/TSL/SL — dual positions use TREND (no regime override)
            await _v7_actions.evaluate_tp_tsl(
                self, pos, "TREND", _cfg, _cfg.MAX_RETRIES, _cfg.CLOB_HOST)

    # --- Manual exit via keystroke -------------------------------------------
    def _start_key_listener(self):
        """Background thread: reads stdin for 'K' to trigger manual exit-all.
        Sets terminal to raw mode so keypresses are immediate (no Enter needed).
        Only works when attached to a TTY (interactive terminal). No-op as systemd service."""
        import threading, sys as _sys
        if not _sys.stdin.isatty():
            return
        def _listen():
            import tty, termios
            fd = _sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setcbreak(fd)  # cbreak: immediate char reads, still allows Ctrl-C
                while self.running:
                    ch = _sys.stdin.read(1)
                    if ch.upper() == "K":
                        logger.info("[KEY] 'K' pressed — manual exit all positions")
                        self._manual_exit_all = True
            except Exception:
                pass
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        t = threading.Thread(target=_listen, daemon=True, name="key-listener")
        t.start()

    async def _execute_manual_exit_all(self):
        """Sell entry side of every open position (bypass grace period)."""
        positions = [p for p in self.state.positions.values()
                     if p.phase not in ("TP_CLOSED", "RESOLVED")]
        if not positions:
            self._log("[MANUAL EXIT] No open positions to exit", "INFO")
            self._publish_exit_result("no_positions", "No open positions to exit")
            return
        self._log(f"[MANUAL EXIT] Exiting {len(positions)} position(s)...", "INFO")
        sold_count, fail_count = 0, 0
        for pos in positions:
            try:
                sold = await _v7_actions.sell_all_positions(
                    self, pos, _cfg.MAX_RETRIES, reason="MANUAL_EXIT")
                if sold:
                    self._log(f"[MANUAL EXIT] {pos.market_slug} — sold", "INFO")
                    sold_count += 1
                else:
                    self._log(f"[MANUAL EXIT] {pos.market_slug} — sell returned False", "WARNING")
                    fail_count += 1
            except Exception as e:
                logger.error(f"[MANUAL EXIT] {pos.market_slug} error: {e}")
                fail_count += 1
        # Publish result to Redis for web dashboard
        if fail_count == 0:
            self._publish_exit_result("success", f"Exited {sold_count} position(s)")
        else:
            self._publish_exit_result("partial", f"Sold {sold_count}, failed {fail_count}")

    async def run(self):
        await self.initialize()
        _mode_tag = " [MANAGE ONLY]" if _cfg.MANAGE_POSITIONS_ONLY else ""
        self._log(f"v7 bot started{_mode_tag}", "INFO")
        self._last_window_ts = self._get_window_ts()
        # Startup: discover markets then sync existing positions from API
        await self._discover_markets()
        await self._subscribe_ws_tokens()
        self._startup_sync()
        # Load persisted state AFTER startup sync so API-synced positions aren't overwritten
        # by stale file state; but restore seeded_this_window to prevent re-entry
        self._load_state()
        # Load persistent begin_balance (survives across windows & restarts, resets on config change)
        self._load_persistent_balance()
        # Sync unredeemed value from API BEFORE capturing begin balance
        # (otherwise CEIL_099 tokens from previous session are missing from begin
        #  but counted in running portfolio → inflated session profit)
        if not self.dry_run and self.trader:
            self._sync_from_api()
        self._capture_begin_balance_if_needed()
        log_file = _cfg.get_log_file()

        # Grace period: let WS connections finish handshake before first loop
        await asyncio.sleep(3)

        # Start keystroke listener thread (TTY only — no-op when run as service)
        self._start_key_listener()

        while self.running:
            try:
                log_file = self._rotate_log_if_needed(log_file)

                # Manual exit: keystroke 'K' or Redis command from web dashboard
                if self._manual_exit_all:
                    self._manual_exit_all = False
                    await self._execute_manual_exit_all()
                self._check_redis_commands()

                prev_window = self._last_window_ts
                self._reset_window(self._get_window_ts())
                if self._last_window_ts != prev_window:
                    # Re-subscribe WS immediately with pre-discovered tokens
                    await self._subscribe_ws_tokens()
                    self._save_state()

                # Pre-discover next window's market ~5s before window ends
                _secs_left = (self._last_window_ts + _cfg.WINDOW_SECONDS
                              - time.time())
                if 0 < _secs_left <= 5 and self._next_window_ts == 0:
                    await self._pre_discover_next_window()

                ws_active = self._is_ws_active()

                # Poll HTTP prediction + advance pred confirm counter every loop tick
                # (WS handler reads cached prediction + counter, doesn't poll itself)
                for _mc in self.markets:
                    _sym = _mc.get("slug_pattern", "").split("-")[0].upper()
                    if _sym:
                        self.read_prediction(_sym)  # polls HTTP, caches result, advances counter below
                        _cp = self._last_prediction.get(_sym)
                        if _cp and _cp[0]:
                            _pd = "YES" if _cp[0] == "UP" else "NO"
                            _ps, _pc = self._pred_confirm.get(_sym, (None, 0))
                            _nc = _pc + 1 if _pd == _ps else 1
                            self._pred_confirm[_sym] = (_pd, _nc)
                        else:
                            self._pred_confirm.pop(_sym, None)

                # Always run REST scan for entry logic (WS handles exits only)
                await self._full_rest_scan()
                if ws_active and self.state.positions:
                    await self._check_ws_stale_positions()
                sleep_interval = _cfg.CHECK_INTERVAL

                self._log_okx_volatility()
                try:
                    _v7_dash.update_dashboard(self, _cfg.CLOB_HOST)
                except Exception as e:
                    logger.warning(f"Dashboard error: {e}")
                self._publish_tui_to_redis()

                # Check Redis commands again before sleep (minimize latency for web dashboard)
                self._check_redis_commands()
                if self._manual_exit_all:
                    self._manual_exit_all = False
                    await self._execute_manual_exit_all()

                await asyncio.sleep(sleep_interval)
            except KeyboardInterrupt:
                self.running = False
            except (SystemExit, GeneratorExit):
                self.running = False
            except BaseException as e:
                logger.error(f"Main loop error ({type(e).__name__}): {e}")
                await asyncio.sleep(5)


async def main():
    parser = argparse.ArgumentParser(description="Konis v7 Prediction Entry Bot")
    parser.add_argument("--env", "-e", type=str, help="Path to .env file")
    parser.add_argument("--markets", "-m", type=str, help="Path to markets JSON")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Dry-run mode")
    parser.add_argument("--headless", "-H", action="store_true", help="Headless mode")
    args = parser.parse_args()
    bot = ScalpingBotv7(dry_run=args.dry_run or _cfg.DRY_RUN, headless=args.headless)
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
