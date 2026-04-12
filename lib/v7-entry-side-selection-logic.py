"""
V7 Entry Side Selection — shared logic for REST polling and WS price paths.

Returns the entry side + ask price decision given current prices and prediction,
without any I/O side effects. Both ScalpingBotv7._scan_market() (REST) and
WsPriceHandler._evaluate_entry() (WS) call this and handle their own logging.

Result tuple:
    (side, ask, prediction, confidence, quality_score, cross_pair_agreement, skip_reason)
    - side: "YES" | "NO" | None  (None → do not enter)
    - skip_reason: human-readable string when side is None, "" when entering
"""

import importlib.util
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger("scalp_v7")

_dir = Path(__file__).resolve().parent
_project_root = _dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def _import_kebab(name: str, filepath: str):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_cfg = _import_kebab("v7_cfg", str(_dir / "v7-bot-config-and-logging.py"))

# --- ML model lazy singleton cache (keyed by symbol) ---
_ml_models: dict = {}
_ml_tick_buffers: dict = {}  # symbol -> deque of tick dicts for tick-level models


def _get_ml_model(symbol: str):
    """Lazy-load ML model for symbol. Returns PredictionModel or None."""
    if not _cfg.ML_MODEL_ENABLED:
        return None
    if symbol not in _ml_models:
        try:
            from ai_prediction.inference import PredictionModel
            model = PredictionModel(symbol, timing_mode=_cfg.ML_MODEL_MODE)
            _ml_models[symbol] = model if model.is_loaded else None
            if model.is_loaded:
                logger.info(f"[ML] Loaded {_cfg.ML_MODEL_MODE} model for {symbol}")
            else:
                logger.warning(f"[ML] No model available for {symbol} ({_cfg.ML_MODEL_MODE})")
        except Exception as e:
            logger.error(f"[ML] Failed to load model for {symbol}: {e}")
            _ml_models[symbol] = None
    return _ml_models.get(symbol)


def _append_ml_tick(symbol: str, tick_dict: dict):
    """Append tick to ML buffer for tick-level models."""
    from collections import deque
    if symbol not in _ml_tick_buffers:
        _ml_tick_buffers[symbol] = deque(maxlen=60)
    _ml_tick_buffers[symbol].append(tick_dict)


def _get_recent_ticks(symbol: str) -> list:
    """Get recent tick buffer for tick-level models."""
    return list(_ml_tick_buffers.get(symbol, []))


EntryDecision = Tuple[
    Optional[str],   # side: "YES" | "NO" | None
    float,           # ask
    Optional[str],   # prediction
    float,           # confidence
    float,           # quality_score
    float,           # cross_pair_agreement
    str,             # skip_reason ("" on success)
]


def decide_entry_side(
    cfg,
    yes_mid: float,
    no_mid: float,
    yes_ask: float,
    no_ask: float,
    read_prediction_fn,
    locked_prediction: Optional[dict],
    market_symbol: str,
    elapsed_min: float,
    read_hyper_fn=None,
) -> EntryDecision:
    """Determine entry side using price + prediction logic.

    Args:
        cfg: bot config module
        yes_mid / no_mid: current mid prices
        yes_ask / no_ask: current ask prices
        read_prediction_fn: callable(market_symbol) -> (prediction, confidence, _, quality_score, cross_pair_agreement)
        locked_prediction: dict keyed by market_symbol with (pred, conf, qs, cpa) or None
        market_symbol: e.g. "BTC"
        elapsed_min: minutes elapsed in current window

    Returns:
        EntryDecision tuple — side=None means skip
    """
    side: Optional[str] = None
    ask = 0.0
    prediction: Optional[str] = None
    confidence = 0.0
    quality_score = 0.0
    cross_pair_agreement = 0.0
    _pred_momentum = 0.0
    _pred_weighted_signal = 0.0

    # Price-driven: pick side from ENTRY_PRICE threshold (unless prediction drives)
    if cfg.ENTRY_PRICE > 0 and not cfg.FAVOR_PREDICTION:
        if yes_mid >= cfg.ENTRY_PRICE:
            side, ask = "YES", yes_ask
        if no_mid >= cfg.ENTRY_PRICE and (not side or no_mid > yes_mid):
            side, ask = "NO", no_ask

    # Prediction gate / prediction-driven side selection
    if cfg.PREDICTION_SOURCE != "none":
        locked = locked_prediction or {}

        # Use locked prediction if available
        if cfg.LOCK_PREDICTION_AT_ENTRY_MINUTE and market_symbol in locked:
            prediction, confidence, quality_score, cross_pair_agreement = locked[market_symbol]
            _from_lock = True
            _pred_momentum, _pred_weighted_signal = 0.0, 0.0
        else:
            _pred_raw = read_prediction_fn(market_symbol)
            prediction, confidence = _pred_raw[0], float(_pred_raw[1])
            quality_score = float(_pred_raw[3]) if len(_pred_raw) > 3 else 0.0
            cross_pair_agreement = float(_pred_raw[4]) if len(_pred_raw) > 4 else 0.0
            _pred_momentum = float(_pred_raw[5]) if len(_pred_raw) > 5 else 0.0
            _pred_weighted_signal = float(_pred_raw[7]) if len(_pred_raw) > 7 else 0.0
            _from_lock = False

        pred_side = "YES" if prediction == "UP" else (
            "NO" if prediction == "DOWN" else None)

        # --- Hyper boost: if hyper agrees + conf > threshold, relax confidence & extend band ---
        _eff_min_conf = cfg.MIN_CONFIDENCE
        _eff_band_low = cfg.BUY_BAND_LOW
        _eff_band_high = cfg.BUY_BAND_HIGH
        _eff_min_momentum = cfg.ENTRY_MIN_MOMENTUM
        if getattr(cfg, "HYPER_BOOST_ENABLED", False) and pred_side and read_hyper_fn:
            try:
                _hyper = read_hyper_fn(market_symbol)
                if _hyper and _hyper[0] in ("UP", "DOWN"):
                    _h_side = "YES" if _hyper[0] == "UP" else "NO"
                    _h_conf = float(_hyper[1])
                    if _h_side == pred_side and _h_conf > cfg.HYPER_BOOST_MIN_CONF:
                        _eff_min_conf = cfg.MIN_CONFIDENCE * (1 - cfg.HYPER_BOOST_CONF_RELAX)
                        _eff_band_low = cfg.BUY_BAND_LOW * (1 - cfg.HYPER_BOOST_BAND_EXTEND)
                        _eff_band_high = cfg.BUY_BAND_HIGH * (1 + cfg.HYPER_BOOST_BAND_EXTEND)
                        _eff_min_momentum = cfg.ENTRY_MIN_MOMENTUM * (1 - getattr(cfg, "HYPER_BOOST_MOMENTUM_RELAX", 0.70))
                        logger.info(
                            f"[HYPER BOOST] {market_symbol} hyper={_hyper[0]}@{_h_conf:.0%} — "
                            f"conf {cfg.MIN_CONFIDENCE:.0%}->{_eff_min_conf:.0%} "
                            f"band [{cfg.BUY_BAND_LOW:.2f}-{cfg.BUY_BAND_HIGH:.2f}]->"
                            f"[{_eff_band_low:.2f}-{_eff_band_high:.2f}] "
                            f"mom {cfg.ENTRY_MIN_MOMENTUM:.3f}->{_eff_min_momentum:.3f}")
            except Exception as e:
                logger.warning(f"[HYPER BOOST] fetch error: {e}")

        if cfg.FAVOR_PREDICTION:
            # Prediction drives side — ignore ENTRY_PRICE side selection
            if not prediction or confidence < _eff_min_conf:
                return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                        f"skip: low confidence ({prediction or 'none'} {confidence:.0%} < {_eff_min_conf:.0%})")
            if not pred_side:
                return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                        f"skip: unknown prediction '{prediction}'")
            side = pred_side
            ask = yes_ask if side == "YES" else no_ask

        elif side:
            # Price picked a side — prediction must agree AND meet confidence
            if not pred_side or pred_side != side or confidence < _eff_min_conf:
                _reason = ("disagrees" if (not pred_side or pred_side != side)
                           else f"low conf {confidence:.0%}<{_eff_min_conf:.0%}")
                _disp = yes_mid if side == "YES" else no_mid
                return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                        f"skip: {side} mid=${_disp:.3f} prediction={prediction or 'none'} ({confidence:.0%}) {_reason}")

        elif cfg.ENTRY_PRICE > 0:
            # ENTRY_PRICE set but not met — don't allow prediction-only entry
            _best = max(yes_mid, no_mid)
            return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                    f"waiting: mid=${_best:.3f} < entry_price=${cfg.ENTRY_PRICE}")

        else:
            # No price signal — use prediction (with confidence gate)
            if not prediction or confidence < _eff_min_conf:
                return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                        f"skip: low confidence ({prediction or 'none'} {confidence:.0%} < {_eff_min_conf:.0%})")
            if not pred_side:
                return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                        f"skip: unknown prediction '{prediction}'")
            side = pred_side
            ask = yes_ask if side == "YES" else no_ask

        # Persist lock if conditions met (only when not already locked)
        if (cfg.LOCK_PREDICTION_AT_ENTRY_MINUTE and not _from_lock
                and side and prediction and confidence >= _eff_min_conf
                and locked_prediction is not None):
            locked_prediction[market_symbol] = (prediction, confidence, quality_score, cross_pair_agreement)
            # Caller should log the lock event with elapsed_min context

        # Quality score gate
        if cfg.MIN_QUALITY_SCORE > 0 and quality_score < cfg.MIN_QUALITY_SCORE:
            return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                    f"skip: {side} quality={quality_score:.2f}<{cfg.MIN_QUALITY_SCORE:.2f}")

        # Cross-pair agreement gate
        if cfg.MIN_CROSS_PAIRS_AGREEMENT > 0 and cross_pair_agreement < cfg.MIN_CROSS_PAIRS_AGREEMENT:
            return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                    f"skip: {side} cross_pair={cross_pair_agreement}/{cfg.MIN_CROSS_PAIRS_AGREEMENT}")

        # Momentum magnitude gate — require minimum abs(momentum) regardless of direction
        # Uses effective threshold (relaxed by hyper boost if applicable)
        _eff_mom = locals().get('_eff_min_momentum', cfg.ENTRY_MIN_MOMENTUM)
        if _eff_mom > 0 and abs(_pred_momentum) < _eff_mom:
            return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                    f"skip: {side} momentum={_pred_momentum:+.3f} "
                    f"(need abs>={_eff_mom:.3f})")

        # Weighted signal gate — require minimum abs(weighted_signal) to filter noisy predictions
        if cfg.MIN_WEIGHTED_SIGNAL > 0 and abs(_pred_weighted_signal) < cfg.MIN_WEIGHTED_SIGNAL:
            return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                    f"skip: {side} weighted_signal={_pred_weighted_signal:+.3f} "
                    f"(need abs>={cfg.MIN_WEIGHTED_SIGNAL})")

    if not side:
        return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                f"waiting: enforce-price (>= ${cfg.ENTRY_PRICE})")

    # Buy band filter (uses effective band from hyper boost if applicable)
    _eff_bl = locals().get('_eff_band_low', cfg.BUY_BAND_LOW)
    _eff_bh = locals().get('_eff_band_high', cfg.BUY_BAND_HIGH)
    mid_price = yes_mid if side == "YES" else no_mid
    if mid_price < _eff_bl or mid_price > _eff_bh:
        return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                f"skip: {side} mid=${mid_price:.3f} outside band [${_eff_bl:.2f}-${_eff_bh:.2f}]")

    # ML model gate — XGBoost classifier as additional entry filter
    if cfg.ML_MODEL_ENABLED:
        ml_model = _get_ml_model(market_symbol)
        if ml_model:
            import time as _time
            _wts = int(_time.time()) // (cfg.WINDOW_SECONDS) * cfg.WINDOW_SECONDS
            _elapsed = int(_time.time()) - _wts
            tick_dict = {
                "confidence": confidence,
                "quality_score": quality_score,
                "cross_pair_agreement": cross_pair_agreement,
                "weighted_signal": _pred_weighted_signal,
                "price_change_pct": 0.0,
                "momentum_accumulated": _pred_momentum,
                "momentum_signal": 0.0,
                "orderbook_imbalance_score": 0.0,
                "orderbook_sweep_score": 0.0,
                "candle_size_factor": 0.0,
                "cex_leader_score": 0.0,
                "exchange_consensus_score": 0.0,
                "persistence_score": 0.0,
                "whale_flow_accumulated": 0.0,
                "polymarket_yes_price": yes_mid,
                "polymarket_no_price": no_mid,
                "window_start_ts": _wts,
                "elapsed_sec": _elapsed,
                "prediction": prediction,
            }
            _append_ml_tick(market_symbol, tick_dict)
            recent_ticks = _get_recent_ticks(market_symbol)
            ml_proba = ml_model.predict_proba(tick_dict, recent_ticks=recent_ticks)
            if ml_proba < cfg.ML_MIN_PROBA:
                return (None, 0.0, prediction, confidence, quality_score, cross_pair_agreement,
                        f"skip: ML model {cfg.ML_MODEL_MODE} proba={ml_proba:.2f}<{cfg.ML_MIN_PROBA:.2f}")

    return (side, ask, prediction, confidence, quality_score, cross_pair_agreement, "")
