"""
V7 Position Sync — Syncs on-chain positions from Polymarket Data API (5m markets).

Every tick, fetches positions and updates V7 internal state.
Handles: manual buys, partial fills, crash recovery.
Uses conditionId matching (not slug) since Data API doesn't return market slug.
"""

import importlib.util
import logging
import time
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger("scalp_v7")

_dir = Path(__file__).resolve().parent


def _imp(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_v7_pos = _imp("v7_pos", str(_dir / "v7-hedge-position.py"))
_cfg = _imp("v7_cfg", str(_dir / "v7-bot-config-and-logging.py"))
HedgePosition = _v7_pos.HedgePosition


def fetch_api_positions(data_host: str, address: str) -> List[dict]:
    """Fetch positions from Polymarket Data API (sync HTTP)."""
    try:
        import httpx
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                f"{data_host}/positions",
                params={"user": address, "limit": 100})
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning(f"[SYNC] API fetch failed: {e}")
        return []


def sync_positions(bot, api_positions: List[dict],
                   known_markets: Dict[str, dict],
                   create_new: bool = False):
    """Sync API positions into bot.state.positions."""
    # Suppress verbose sync logs when active positions exist
    _has_pos = any(p.phase != "TP_CLOSED" for p in bot.state.positions.values())
    _sync_lvl = logging.DEBUG if _has_pos else logging.INFO
    api_by_cid: Dict[str, Dict[str, dict]] = {}
    for p in api_positions:
        size = float(p.get("size", 0))
        cid = p.get("conditionId", "")
        outcome = (p.get("outcome", "") or "").lower()
        cur_price = float(p.get("curPrice", 0))
        if cid in known_markets:
            logger.log(_sync_lvl,
                f"[SYNC RAW] cid={cid[:8]}.. outcome={outcome} "
                f"size={size} curPrice={cur_price:.3f} "
                f"avgPrice={p.get('avgPrice', '?')}")
        if size <= 0:
            continue
        if not cid or cid not in known_markets:
            continue
        if cur_price <= 0.001 or cur_price >= 0.999:
            continue
        if cid not in api_by_cid:
            api_by_cid[cid] = {}
        if outcome in ("yes", "up"):
            api_by_cid[cid]["YES"] = p
        elif outcome in ("no", "down"):
            api_by_cid[cid]["NO"] = p

    # Auto-close resolved positions (curPrice >= 0.999 means market settled)
    for p in api_positions:
        cid = p.get("conditionId", "")
        cur_price = float(p.get("curPrice", 0))
        if cur_price < 0.999:
            continue
        for pos_key in (cid, cid + "_dual"):
            pos = bot.state.positions.get(pos_key)
            if pos and pos.phase not in ("TP_CLOSED",):
                outcome = (p.get("outcome", "") or "").upper()
                logger.info(
                    f"[SYNC] {pos.market_slug} {'DUAL ' if pos_key.endswith('_dual') else ''}"
                    f"RESOLVED — {outcome} won (curPrice={cur_price:.3f})")
                pos.phase = "TP_CLOSED"

    synced = 0
    for cid, sides in api_by_cid.items():
        yes_data = sides.get("YES")
        no_data = sides.get("NO")
        mkt = known_markets[cid]

        # If TP_CLOSED but API shows new tokens, re-adopt only in manage-only mode
        # Grace period: don't re-adopt within 60s of close (API may not have synced the sell yet)
        if cid in bot.state.positions and bot.state.positions[cid].phase == "TP_CLOSED":
            closed_ts = getattr(bot.state.positions[cid], '_closed_ts', 0)
            if closed_ts > 0 and (time.time() - closed_ts) < 60:
                continue  # too soon — API hasn't reflected the sell yet
            has_tokens = ((yes_data and float(yes_data.get("size", 0)) > 0) or
                          (no_data and float(no_data.get("size", 0)) > 0))
            if has_tokens and create_new and _cfg.MANAGE_POSITIONS_ONLY:
                old_slug = bot.state.positions[cid].market_slug
                del bot.state.positions[cid]
                bot.state.positions.pop(cid + "_dual", None)
                logger.info(f"[SYNC] {old_slug} TP_CLOSED but new tokens found — re-adopting")
            else:
                continue

        if cid in bot.state.positions:
            pos = bot.state.positions[cid]
            # After CUT: only update prices + hedge-side tokens
            if pos.phase == "CUT" and hasattr(pos, '_cut_ts'):
                if yes_data:
                    pos.yes_price = float(yes_data.get("curPrice", pos.yes_price))
                if no_data:
                    pos.no_price = float(no_data.get("curPrice", pos.no_price))
                hedge_side = "NO" if pos.entry_side == "YES" else "YES"
                if hedge_side == "YES" and yes_data:
                    pos.yes_tokens = float(yes_data["size"])
                    api_avg = float(yes_data.get("avgPrice", 0))
                    if api_avg > 0:
                        pos.yes_entry_price = api_avg
                elif hedge_side == "NO" and no_data:
                    pos.no_tokens = float(no_data["size"])
                    api_avg = float(no_data.get("avgPrice", 0))
                    if api_avg > 0:
                        pos.no_entry_price = api_avg
                logger.info(
                    f"[SYNC SKIP] {pos.market_slug} phase=CUT — "
                    f"prices + hedge only (Y={pos.yes_tokens:.1f} N={pos.no_tokens:.1f})")
                synced += 1
                continue
            # When dual position exists, split API tokens between main and dual
            dual_pos = bot.state.positions.get(cid + "_dual")
            if dual_pos and dual_pos.phase != "TP_CLOSED":
                # Main owns its entry_side; dual owns the opposite
                for p, p_label in ((pos, "MAIN"), (dual_pos, "DUAL")):
                    own_side = p.entry_side  # "YES" or "NO"
                    own_data = yes_data if own_side == "YES" else no_data
                    opp_data = no_data if own_side == "YES" else yes_data
                    if own_data:
                        if own_side == "YES":
                            p.yes_tokens = float(own_data["size"])
                            p.yes_price = float(own_data.get("curPrice", p.yes_price))
                            avg = float(own_data.get("avgPrice", 0))
                            if avg > 0:
                                p.yes_entry_price = avg
                            p.no_tokens = 0
                        else:
                            p.no_tokens = float(own_data["size"])
                            p.no_price = float(own_data.get("curPrice", p.no_price))
                            avg = float(own_data.get("avgPrice", 0))
                            if avg > 0:
                                p.no_entry_price = avg
                            p.yes_tokens = 0
                    else:
                        if own_side == "YES":
                            p.yes_tokens = 0
                        else:
                            p.no_tokens = 0
                    # Update opposite side price only (no tokens)
                    if opp_data:
                        opp_cur = float(opp_data.get("curPrice", 0))
                        if own_side == "YES" and opp_cur > 0:
                            p.no_price = opp_cur
                        elif opp_cur > 0:
                            p.yes_price = opp_cur
                    cost = (p.yes_tokens * p.yes_entry_price
                            if p.entry_side == "YES"
                            else p.no_tokens * p.no_entry_price)
                    if cost > 0:
                        p.entry_cost = cost
                    logger.log(_sync_lvl,
                        f"[SYNC UPD] {p.market_slug} {p_label} "
                        f"entry={p.entry_side} "
                        f"Y={p.yes_tokens:.1f} N={p.no_tokens:.1f}")
                synced += 2
            else:
                # No dual — update main with all API tokens
                if yes_data:
                    pos.yes_tokens = float(yes_data["size"])
                    pos.yes_price = float(yes_data.get("curPrice", pos.yes_price))
                    api_yes_avg = float(yes_data.get("avgPrice", 0))
                    if api_yes_avg > 0:
                        pos.yes_entry_price = api_yes_avg
                else:
                    pos.yes_tokens = 0
                if no_data:
                    pos.no_tokens = float(no_data["size"])
                    pos.no_price = float(no_data.get("curPrice", pos.no_price))
                    api_no_avg = float(no_data.get("avgPrice", 0))
                    if api_no_avg > 0:
                        pos.no_entry_price = api_no_avg
                else:
                    pos.no_tokens = 0
                main_cost = (pos.yes_tokens * pos.yes_entry_price
                             if pos.entry_side == "YES"
                             else pos.no_tokens * pos.no_entry_price)
                hedge_cost = (pos.no_tokens * pos.no_entry_price
                              if pos.entry_side == "YES"
                              else pos.yes_tokens * pos.yes_entry_price)
                if main_cost > 0:
                    pos.entry_cost = main_cost
                if hedge_cost > 0:
                    pos.hedge_cost = hedge_cost
                logger.log(_sync_lvl,
                    f"[SYNC UPD] {pos.market_slug} phase={pos.phase} "
                    f"entry={pos.entry_side} Y={pos.yes_tokens:.1f} N={pos.no_tokens:.1f}")
                # Auto-close position when all tokens gone (e.g. manual sell on Polymarket)
                entry_tokens = pos.yes_tokens if pos.entry_side == "YES" else pos.no_tokens
                if entry_tokens < 1 and pos.phase not in ("TP_CLOSED", "RESOLVED"):
                    logger.info(
                        f"[SYNC] {pos.market_slug} entry tokens=0 — marking TP_CLOSED "
                        f"(likely sold externally)")
                    pos.phase = "TP_CLOSED"
                    pos._closed_ts = time.time()
            synced += 1
        else:
            slug = mkt.get("slug", "")
            yes_tokens = float(yes_data["size"]) if yes_data else 0
            no_tokens = float(no_data["size"]) if no_data else 0

            if not create_new:
                logger.info(
                    f"[SYNC SKIP] {slug}: Y={yes_tokens:.0f} N={no_tokens:.0f} "
                    f"(not tracked locally)")
                continue

            yes_avg = float(yes_data.get("avgPrice", 0)) if yes_data else 0
            no_avg = float(no_data.get("avgPrice", 0)) if no_data else 0

            # Skip if any active side has avgPrice=0 (API not ready — prevents fake TP/SL)
            if (yes_data and float(yes_data.get("size", 0)) > 0 and yes_avg <= 0) or \
               (no_data and float(no_data.get("size", 0)) > 0 and no_avg <= 0):
                logger.warning(
                    f"[SYNC SKIP] {slug}: avgPrice=0 — waiting for API to populate entry price")
                continue

            yes_cur = float(yes_data.get("curPrice", yes_avg)) if yes_data else 0
            no_cur = float(no_data.get("curPrice", no_avg)) if no_data else 0

            # Dual mode: split into main (higher price) + dual (lower price)
            if yes_tokens > 0 and no_tokens > 0 and _cfg.DUAL_MODE_ENABLED:
                # Higher current price = main (enforce-price triggered side)
                if yes_cur >= no_cur:
                    main_side, dual_side = "YES", "NO"
                else:
                    main_side, dual_side = "NO", "YES"

                # Main position
                m_tokens = yes_tokens if main_side == "YES" else no_tokens
                m_avg = yes_avg if main_side == "YES" else no_avg
                m_cur = yes_cur if main_side == "YES" else no_cur
                pos_main = HedgePosition(
                    market_slug=slug, condition_id=cid,
                    yes_token_id=mkt.get("yes_token_id", ""),
                    no_token_id=mkt.get("no_token_id", ""),
                    yes_tokens=m_tokens if main_side == "YES" else 0.0,
                    yes_entry_price=m_avg if main_side == "YES" else 0.0,
                    no_tokens=m_tokens if main_side == "NO" else 0.0,
                    no_entry_price=m_avg if main_side == "NO" else 0.0,
                    entry_side=main_side, entry_cost=m_tokens * m_avg,
                    phase="ENTERED",
                    entry_time=float(bot._get_window_ts()),
                    yes_price=yes_cur, no_price=no_cur,
                )
                bot.state.positions[cid] = pos_main
                bot._seeded_this_window.add(cid)

                # Dual position
                d_tokens = yes_tokens if dual_side == "YES" else no_tokens
                d_avg = yes_avg if dual_side == "YES" else no_avg
                dual_cid = cid + "_dual"
                pos_dual = HedgePosition(
                    market_slug=slug, condition_id=cid,
                    yes_token_id=mkt.get("yes_token_id", ""),
                    no_token_id=mkt.get("no_token_id", ""),
                    yes_tokens=d_tokens if dual_side == "YES" else 0.0,
                    yes_entry_price=d_avg if dual_side == "YES" else 0.0,
                    no_tokens=d_tokens if dual_side == "NO" else 0.0,
                    no_entry_price=d_avg if dual_side == "NO" else 0.0,
                    entry_side=dual_side, entry_cost=d_tokens * d_avg,
                    phase="ENTERED",
                    entry_time=float(bot._get_window_ts()),
                    yes_price=yes_cur, no_price=no_cur,
                    tp_ratio=_cfg.DUAL_TP_RATIO,
                    sl_ratio=_cfg.DUAL_SL_RATIO,
                    is_dual=True,
                )
                bot.state.positions[dual_cid] = pos_dual
                bot._seeded_this_window.add(dual_cid)

                logger.info(
                    f"[STARTUP] Loaded {slug}: MAIN={main_side} "
                    f"({m_tokens:.0f}@${m_avg:.3f}) DUAL={dual_side} "
                    f"({d_tokens:.0f}@${d_avg:.3f})")
                synced += 2
            else:
                if yes_tokens > 0 and no_tokens > 0:
                    entry_side = "YES" if (yes_tokens * yes_avg) >= (no_tokens * no_avg) else "NO"
                    phase = "HEDGED"
                elif yes_tokens > 0:
                    entry_side = "YES"
                    phase = "ENTERED"
                else:
                    entry_side = "NO"
                    phase = "ENTERED"

                entry_cost = (yes_tokens * yes_avg) if entry_side == "YES" else (no_tokens * no_avg)
                hedge_cost = (no_tokens * no_avg) if entry_side == "YES" else (yes_tokens * yes_avg)

                pos = HedgePosition(
                    market_slug=slug, condition_id=cid,
                    yes_token_id=mkt.get("yes_token_id", ""),
                    no_token_id=mkt.get("no_token_id", ""),
                    yes_tokens=yes_tokens, yes_entry_price=yes_avg,
                    no_tokens=no_tokens, no_entry_price=no_avg,
                    entry_side=entry_side, entry_cost=entry_cost,
                    hedge_cost=hedge_cost if phase == "HEDGED" else 0.0,
                    phase=phase,
                    entry_time=float(bot._get_window_ts()),
                    yes_price=yes_cur, no_price=no_cur,
                )
                bot.state.positions[cid] = pos
                bot._seeded_this_window.add(cid)
                logger.info(
                    f"[STARTUP] Loaded {slug}: entry={entry_side} "
                    f"Y={yes_tokens:.0f} N={no_tokens:.0f} phase={phase}")
                synced += 1

    # Detect manually sold positions: tracked locally but no tokens in API
    for cid, pos in list(bot.state.positions.items()):
        if cid.endswith("_dual"):
            continue
        real_cid = pos.condition_id
        if real_cid not in known_markets:
            continue
        if pos.phase == "TP_CLOSED":
            continue
        if real_cid not in api_by_cid:
            # Grace period: API is eventually consistent, new fills may take
            # 30-60s to appear. Skip "tokens gone" for recently entered positions.
            age = time.time() - getattr(pos, "entry_time", 0)
            if age < _cfg.SYNC_GRACE_SECONDS:
                logger.info(
                    f"[SYNC] {pos.market_slug} not in API yet — "
                    f"skip close (entered {age:.0f}s ago, grace {_cfg.SYNC_GRACE_SECONDS}s)")
                continue
            # API says no tokens — verify on-chain before closing (API can be flaky)
            _token_id = pos.yes_token_id if pos.yes_tokens > 0 else pos.no_token_id
            if _token_id and hasattr(bot, 'trader') and bot.trader:
                from utils.subgraph_positions import verify_position_balance
                _onchain = verify_position_balance(
                    bot.trader.trading_address, _token_id)
                if _onchain is not None and _onchain > 0:
                    logger.info(
                        f"[SYNC] {pos.market_slug} not in API but on-chain={_onchain:.1f}"
                        f" — keeping position (API flaky)")
                    continue
            logger.info(
                f"[SYNC] {pos.market_slug} tokens gone (API + on-chain) — closing "
                f"(Y={pos.yes_tokens:.0f} N={pos.no_tokens:.0f})")
            pos.yes_tokens = 0
            pos.no_tokens = 0
            pos.phase = "TP_CLOSED"
            # Also close dual if exists
            dual = bot.state.positions.get(cid + "_dual")
            if dual and dual.phase != "TP_CLOSED":
                dual.yes_tokens = 0
                dual.no_tokens = 0
                dual.phase = "TP_CLOSED"

    if synced > 0:
        logger.debug(f"[SYNC] Updated {synced} positions from API")


def compute_unredeemed_value(api_positions: List[dict]) -> float:
    """Sum value of resolved-but-unredeemed winning positions (curPrice >= 0.999)."""
    total = 0.0
    for p in api_positions:
        size = float(p.get("size", 0))
        cur_price = float(p.get("curPrice", 0))
        if size > 0 and cur_price >= 0.999:
            total += size
    return total
