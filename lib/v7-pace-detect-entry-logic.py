"""
V7 Pace Detection — enter when Polymarket price shows sustained momentum.

Tracks price history per token from WS + REST feeds.
Uses accumulated average (all prices since window start) as baseline.
Triggers when avg(2s) is X% above accumulated baseline AND directionally
confirmed (avg_2 > avg_3 > avg_5).

Used by both REST (_scan_market) and WS (on_price_update) entry paths.
"""
import logging
import time
from collections import deque

logger = logging.getLogger("scalp_v7")

# Rolling price history per token_id: deque of (monotonic_ts, mid)
# Kept for full window (no trimming) so accumulated avg is accurate
_price_history: dict[str, deque] = {}


def record_price(token_id: str, mid: float):
    """Append a WS price tick to the rolling history for this token."""
    now = time.monotonic()
    if token_id not in _price_history:
        _price_history[token_id] = deque(maxlen=2000)
    _price_history[token_id].append((now, mid))


def clear_history():
    """Clear all price history — call on window reset."""
    _price_history.clear()


def clear_history_except(keep_tokens: set):
    """Clear all history except for tokens in keep_tokens.

    Used at window reset to preserve pre-collected ticks for the next window's
    tokens (which were pre-subscribed ~5s before window boundary).
    """
    keep = {tid: dq for tid, dq in _price_history.items() if tid in keep_tokens}
    _price_history.clear()
    _price_history.update(keep)


def _avg_in_window(dq: deque, now: float, lookback_sec: int) -> float:
    """Average mid price over the last `lookback_sec` seconds."""
    cutoff = now - lookback_sec
    prices = [mid for ts, mid in dq if ts >= cutoff]
    if not prices:
        return 0.0
    return sum(prices) / len(prices)


def get_pace_pct(token_id: str, window_sec: int) -> float:
    """Compute momentum % as avg(2s) vs accumulated baseline.

    Accumulated baseline = average of ALL prices since window start.
    Returns positive % if avg(2s) is above baseline AND directionally
    confirmed (avg_2 > avg_3 > avg_5). Returns 0 otherwise.
    """
    dq = _price_history.get(token_id)
    if not dq or len(dq) < 4:
        return 0.0
    # Require at least 5 seconds of data span
    data_span = dq[-1][0] - dq[0][0]
    if data_span < 5.0:
        return 0.0

    now = dq[-1][0]

    # Accumulated baseline = average of ALL prices since window start
    all_prices = [mid for _, mid in dq]
    baseline = sum(all_prices) / len(all_prices)

    # Rolling averages for directional confirmation (shorter windows for 5s minimum)
    avg_2 = _avg_in_window(dq, now, 2)
    avg_3 = _avg_in_window(dq, now, 3)
    avg_5 = _avg_in_window(dq, now, 5)

    if baseline <= 0 or avg_2 <= 0 or avg_3 <= 0 or avg_5 <= 0:
        return 0.0

    # Directional confirmation: avg_2 > avg_3 > avg_5
    if not (avg_2 > avg_3 > avg_5):
        return 0.0

    # Momentum = how far avg_2 is above accumulated baseline
    return ((avg_2 - baseline) / baseline) * 100


def evaluate_pace(cfg, yes_token_id: str, no_token_id: str,
                  yes_mid: float, no_mid: float,
                  yes_ask: float, no_ask: float,
                  elapsed_sec: float, slug: str, log_fn):
    """Evaluate pace detection entry conditions.

    Triggers when avg(10s) is >= threshold% above accumulated baseline
    AND directionally confirmed (avg_10 > avg_20 > avg_30).

    Returns:
        (side, fak_price, pace_pct) if pace triggers, or (None, 0, 0) if not.
    """
    if not cfg.PACE_DETECT:
        return None, 0, 0.0
    # Compute true wall-clock elapsed from window start (ignores bot restart time)
    from datetime import datetime, timezone as _tz
    _ws = cfg.WINDOW_SECONDS
    _now_ts = int(datetime.now(_tz.utc).timestamp())
    _wts = (_now_ts // _ws) * _ws
    wall_elapsed = _now_ts - _wts
    # Require at least 5s into window
    if wall_elapsed < 5:
        return None, 0, 0.0
    # Hard gate: no entry after PACE_DETECT_MAX_SEC of window (wall clock)
    if wall_elapsed > cfg.PACE_DETECT_MAX_SEC:
        return None, 0, 0.0
    # Override elapsed_sec for logging
    elapsed_sec = wall_elapsed

    threshold = cfg.PACE_DETECT_PCT

    yes_pace = get_pace_pct(yes_token_id, 0)
    no_pace = get_pace_pct(no_token_id, 0)

    # Always log pace data for monitoring
    _y_pts = len(_price_history.get(yes_token_id, []))
    _n_pts = len(_price_history.get(no_token_id, []))
    logger.info(f"[PACE] {slug} YES={yes_pace:+.2f}% NO={no_pace:+.2f}% "
                f"@{elapsed_sec:.0f}s "
                f"(YES=${yes_mid:.3f} NO=${no_mid:.3f}) "
                f"pts={_y_pts}/{_n_pts} thr={threshold}%")

    # Check which side has momentum above threshold
    pace_side = None
    pace_pct = 0.0
    if yes_pace >= threshold and no_pace >= threshold:
        if yes_pace >= no_pace:
            pace_side, pace_pct = "YES", yes_pace
        else:
            pace_side, pace_pct = "NO", no_pace
    elif yes_pace >= threshold:
        pace_side, pace_pct = "YES", yes_pace
    elif no_pace >= threshold:
        pace_side, pace_pct = "NO", no_pace
    else:
        return None, 0, 0.0

    # Price gates
    entry_mid = yes_mid if pace_side == "YES" else no_mid
    entry_ask = yes_ask if pace_side == "YES" else no_ask

    # Only enter above $0.50 baseline
    if entry_mid < 0.50:
        return None, 0, 0.0

    if entry_mid < cfg.BUY_BAND_LOW:
        log_fn(f"[PACE] {slug} — skip: {pace_side} mid=${entry_mid:.3f} "
               f"< band_low ${cfg.BUY_BAND_LOW} (pace={pace_pct:+.2f}%)", "INFO")
        return None, 0, pace_pct

    if entry_mid > cfg.PACE_DETECT_PRICE_CAP:
        log_fn(f"[PACE] {slug} — skip: {pace_side} mid=${entry_mid:.3f} "
               f"> cap ${cfg.PACE_DETECT_PRICE_CAP} (pace={pace_pct:+.2f}%)", "INFO")
        return None, 0, pace_pct

    if entry_ask <= 0:
        log_fn(f"[PACE] {slug} — skip: no ask for {pace_side}", "INFO")
        return None, 0, pace_pct

    # Compute FAK price
    fak_price = entry_ask
    if cfg.BUY_MAX_ABOVE_MID > 0:
        fak_price = min(entry_ask, entry_mid + cfg.BUY_MAX_ABOVE_MID)

    log_fn(f"\033[36mPACE ENTRY\033[0m {slug}: {pace_side} mid=${entry_mid:.3f} "
           f"pace={pace_pct:+.2f}% (avg10 vs baseline) @{elapsed_sec:.0f}s "
           f"— buying ${cfg.POSITION_SIZE_USD}", "ENTRY")

    return pace_side, fak_price, pace_pct
