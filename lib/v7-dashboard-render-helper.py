"""
V7 Dashboard Render Helper — Builds position data and calls ScalpingDashboard.render().

Separated from strategy actions to keep modules under 200 lines.
"""

import os
import sys
import time

import importlib.util
from pathlib import Path

def _imp(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_cfg = _imp("v7_cfg", str(Path(__file__).resolve().parent / "v7-bot-config-and-logging.py"))


def update_dashboard(bot, clob_host: str):
    """Render terminal UI with current bot state."""
    positions_list = []
    total_cost = 0.0
    total_value = 0.0
    combined_pnl = 0.0

    for pos in bot.state.positions.values():
        if pos.phase in ("TP_CLOSED", "CUT"):
            continue
        for side in ("YES", "NO"):
            tokens = pos.yes_tokens if side == "YES" else pos.no_tokens
            if tokens <= 0:
                continue
            entry = pos.yes_entry_price if side == "YES" else pos.no_entry_price
            price = pos.yes_price if side == "YES" else pos.no_price
            cost = tokens * entry
            val = tokens * price
            pnl_pct = ((price - entry) / entry) if entry > 0 else 0
            is_main = (side == pos.entry_side)
            if getattr(pos, 'is_dual', False):
                label = "DUAL"
            elif is_main:
                label = "MAIN"
            else:
                label = "HEDGE"
            positions_list.append({
                "side": side, "entry_price": entry,
                "current_price": price, "size": tokens,
                "pnl_pct": pnl_pct, "duration": time.time() - pos.entry_time,
                "market": pos.market_slug,
                "label": label,
            })
            total_cost += cost
            total_value += val
        combined_pnl += pos.current_pnl()

    total_tokens = sum(p.get("size", 0) for p in positions_list)
    pct = (combined_pnl / total_cost) if total_cost > 0 else 0

    # Use bot.read_prediction() to support both HTTP and Redis sources
    try:
        pred, conf, _, _q = bot.read_prediction()
    except Exception:
        pred, conf = None, 0.0

    active = [p for p in bot.state.positions.values()
              if p.phase not in ("TP_CLOSED", "CUT")]
    closed = len(bot.state.positions) - len(active)
    phases = [p.phase for p in active]
    entries = [p.entry_side for p in active]
    phase_str = ",".join(phases) if phases else "IDLE"
    entry_str = ",".join(entries) if entries else ""
    status = f"V7 5m PRED: {len(active)} active"
    if closed:
        status += f" +{closed} closed"
    status += f" [{phase_str}]"
    if entry_str:
        status += f" ({entry_str})"
    if pred:
        status += f" | Pred: {pred} {conf:.0%}"

    window_ts = bot._get_window_ts()
    el_min = (time.time() - window_ts) / 60
    first = positions_list[0] if positions_list else {}
    second = positions_list[1] if len(positions_list) > 1 else {}

    # Get YES/NO prices from active position; fall back to WS live prices
    any_pos = next((p for p in bot.state.positions.values()
                    if p.phase not in ("TP_CLOSED", "CUT")), None)
    if not any_pos:
        any_pos = next(iter(bot.state.positions.values()), None)
    dash_yes = any_pos.yes_price if any_pos else 0
    dash_no = any_pos.no_price if any_pos else 0
    # No position prices — use WS orderbook mid prices
    if dash_yes <= 0 and dash_no <= 0 and getattr(bot, '_pm_feed', None):
        for _km in getattr(bot, '_known_markets', {}).values():
            _ym = bot._pm_feed.get_mid(_km.get("yes_token_id", ""))
            _nm = bot._pm_feed.get_mid(_km.get("no_token_id", ""))
            if _ym > 0 or _nm > 0:
                dash_yes, dash_no = _ym, _nm
                break

    # Compute session profit relative to persistent begin_session_balance
    begin_bal = getattr(bot, '_begin_session_balance', 0.0)
    portfolio = bot.current_balance + total_value + getattr(bot, '_unredeemed_value', 0.0)
    session_profit = portfolio - begin_bal if begin_bal > 0 else 0.0
    session_profit_pct = session_profit / begin_bal if begin_bal > 0 else 0.0

    bot.dashboard.render(
        status=status, window_ts=window_ts, elapsed_minutes=el_min,
        prediction=pred or None, confidence=conf,
        current_balance=bot.current_balance,
        yes_price=dash_yes,
        no_price=dash_no,
        entry_range=(0, 1.0),
        position_side=first.get("side"),
        position_entry_price=first.get("entry_price", 0),
        position_pnl_pct=first.get("pnl_pct", 0),
        position_size=first.get("size", 0),
        position_current_price=first.get("current_price", 0),
        tsl_level=0, tsl_floor=0,
        position_duration=first.get("duration", 0),
        positions=positions_list,
        combined_pnl_usd=combined_pnl, combined_pnl_pct=pct,
        session_total_cost=total_cost, session_total_value=total_value,
        session_total_profit=total_value - total_cost,
        session_avg_entry=(total_cost / total_tokens) if total_tokens > 0 else 0.0,
        session_total_tokens=total_tokens,
        begin_session_balance=begin_bal,
        portfolio_value=portfolio,
        session_profit=session_profit,
        session_profit_pct=session_profit_pct,
        width=min(os.get_terminal_size().columns, 160) if sys.stdout.isatty() else 120,
        logs_only=False,
        window_minutes=float(_cfg.WINDOW_MINUTES),
    )
