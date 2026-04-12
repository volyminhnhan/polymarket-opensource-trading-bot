#!/usr/bin/env python3
"""
Polymarket Position Fetcher via Chainstack RPC

Direct on-chain ERC1155 balance queries via Chainstack Polygon RPC.
No subgraph dependency - uses your paid Chainstack node (250 req/s).

CTF Contract: 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045 (Polygon)
"""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv

# Load .env from polymarket_konis root, not utils/ subfolder
_env_path = Path(__file__).parent.parent / ".env"
load_dotenv(_env_path)

logger = logging.getLogger(__name__)

# Cache
_positions_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
CACHE_TTL_SECS = 30

# Polymarket CTF ERC1155 Contract
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# ERC1155 function selectors
BALANCE_OF = "0x00fdd58e"  # balanceOf(address,uint256)
BALANCE_OF_BATCH = "0x4e1273f4"  # balanceOfBatch(address[],uint256[])


def get_rpc_url() -> str:
    """Get Chainstack/QuikNode RPC URL from env."""
    url = os.getenv("POLYGON_RPC_URL")
    if not url:
        raise ValueError("POLYGON_RPC_URL not set in .env")
    return url


# ERC20 USDC on Polygon PoS
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
ERC20_BALANCE_OF = "0x70a08231"  # balanceOf(address)


def fetch_usdc_balance(address: str) -> Optional[float]:
    """Fetch USDC balance via RPC. Returns USD float (6 decimals), or None on error."""
    addr = address.lower().replace("0x", "").zfill(64)
    data = f"{ERC20_BALANCE_OF}{addr}"
    try:
        result = _eth_call(USDC_CONTRACT, data)
        if result and result != "0x":
            return int(result, 16) / 1e6
        return 0.0
    except Exception as e:
        logger.warning(f"USDC balance fetch failed: {e}")
        return None


def _eth_call(to: str, data: str, rpc_url: str = None) -> str:
    """Execute eth_call via Chainstack RPC."""
    url = rpc_url or get_rpc_url()

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": to, "data": data}, "latest"]
    }

    with httpx.Client(timeout=15) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
        result = r.json()

        if "error" in result:
            raise Exception(f"RPC error: {result['error']}")

        return result.get("result", "0x")


def fetch_single_balance(address: str, token_id: str, rpc_url: str = None) -> Optional[int]:
    """Fetch single ERC1155 token balance. Returns None on error."""
    addr = address.lower().replace("0x", "").zfill(64)
    token_hex = hex(int(token_id))[2:].zfill(64)
    data = f"{BALANCE_OF}{addr}{token_hex}"

    try:
        result = _eth_call(CTF_CONTRACT, data, rpc_url)
        if result and result != "0x":
            return int(result, 16)
        return 0  # Valid response: balance is 0
    except Exception as e:
        logger.warning(f"Balance fetch failed: {e}")
        return None  # Error: return None to indicate failure


def fetch_batch_balances(address: str, token_ids: List[str], rpc_url: str = None) -> Dict[str, int]:
    """Fetch multiple token balances in single RPC call."""
    if not token_ids:
        return {}

    addr = address.lower().replace("0x", "").zfill(64)
    n = len(token_ids)

    # ABI encode balanceOfBatch(address[], uint256[])
    offset1 = "0000000000000000000000000000000000000000000000000000000000000040"
    offset2 = hex(0x40 + 0x20 + n * 0x20)[2:].zfill(64)

    addr_len = hex(n)[2:].zfill(64)
    addresses = addr_len + (addr * n)

    token_len = hex(n)[2:].zfill(64)
    tokens = token_len + "".join(hex(int(t))[2:].zfill(64) for t in token_ids)

    data = f"{BALANCE_OF_BATCH}{offset1}{offset2}{addresses}{tokens}"

    try:
        result = _eth_call(CTF_CONTRACT, data, rpc_url)

        if not result or result == "0x":
            return {}

        # Decode uint256[]
        hex_data = result[2:]
        offset = int(hex_data[0:64], 16)
        length_start = offset * 2
        length = int(hex_data[length_start:length_start + 64], 16)

        balances = []
        elem_start = length_start + 64
        for i in range(length):
            elem = hex_data[elem_start + i * 64:elem_start + (i + 1) * 64]
            balances.append(int(elem, 16) if elem else 0)

        return {token_ids[i]: balances[i] for i in range(min(len(token_ids), len(balances)))}

    except Exception as e:
        logger.warning(f"Batch balance fetch failed: {e}")
        return {}


def verify_position_balance(address: str, token_id: str) -> Optional[float]:
    """Verify position balance via RPC. Returns shares (balance / 1e6), or None on error."""
    raw = fetch_single_balance(address, token_id)
    if raw is None:
        return None  # RPC failed
    return raw / 1_000_000


def fetch_positions_by_token_ids(
    address: str,
    token_ids: List[str],
    token_metadata: Dict[str, Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Fetch positions directly via Chainstack RPC using known token IDs.
    NO Data API needed - uses on-chain balance queries.

    Args:
        address: User wallet address
        token_ids: List of token IDs to check (YES and NO tokens)
        token_metadata: Optional dict mapping token_id -> {outcome, condition_id, title, etc}

    Returns:
        List of positions with balance > 0
    """
    if not token_ids:
        return []

    token_metadata = token_metadata or {}

    # Fetch all balances in single RPC call
    balances = fetch_batch_balances(address, token_ids)

    positions = []
    for token_id, raw_balance in balances.items():
        if raw_balance <= 0:
            continue

        # Convert to shares (USDC has 6 decimals)
        size = raw_balance / 1_000_000

        # Get metadata if provided
        meta = token_metadata.get(token_id, {})
        outcome = meta.get("outcome", "Yes" if int(token_id) % 2 == 1 else "No")
        condition_id = meta.get("condition_id", "")
        title = meta.get("title", "")

        positions.append({
            "asset": token_id,
            "conditionId": condition_id,
            "outcome": outcome,
            "size": size,
            "title": title,
            "curPrice": 0.5,  # Need CLOB for real price
            "avgPrice": 0.0,
            "cashPnl": 0.0,
            "percentPnl": 0.0,
            "currentValue": size * 0.5,
            "_source": "chainstack_rpc"
        })

    logger.debug(f"Chainstack RPC: {len(positions)} positions with balance > 0")
    return positions


def fetch_market_positions_rpc(
    address: str,
    yes_token_id: str,
    no_token_id: str,
    condition_id: str = "",
    title: str = ""
) -> List[Dict[str, Any]]:
    """
    Fetch positions for a specific market using Chainstack RPC.
    Pass YES and NO token IDs from MarketRef.

    Args:
        address: User wallet address
        yes_token_id: YES token ID from market
        no_token_id: NO token ID from market
        condition_id: Market condition ID
        title: Market title

    Returns:
        List of positions (0-2 items: YES and/or NO if balance > 0)
    """
    token_ids = [yes_token_id, no_token_id]
    token_metadata = {
        yes_token_id: {"outcome": "Yes", "condition_id": condition_id, "title": title},
        no_token_id: {"outcome": "No", "condition_id": condition_id, "title": title},
    }
    return fetch_positions_by_token_ids(address, token_ids, token_metadata)


def fetch_positions_with_fallback(
    address: str,
    data_host: str = None,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Fetch positions from Data API (only source for position discovery).
    Chainstack RPC verifies balances but can't discover positions.

    Includes 30-second cache.
    """
    cache_key = address.lower()
    if cache_key in _positions_cache:
        cached_time, cached_positions = _positions_cache[cache_key]
        if time.time() - cached_time < CACHE_TTL_SECS:
            logger.debug(f"Using cached positions ({len(cached_positions)})")
            return cached_positions

    # Data API is the only way to discover positions
    if data_host is None:
        data_host = os.getenv("DATA_HOST", "https://data-api.polymarket.com")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            with httpx.Client(timeout=30) as client:
                r = client.get(f"{data_host}/positions", params={"user": address, "limit": limit})
                r.raise_for_status()
                positions = r.json()
                _positions_cache[cache_key] = (time.time(), positions)
                logger.debug(f"Data API returned {len(positions)} positions")
                return positions
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (502, 503, 504) and attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                logger.warning(f"Data API {e.response.status_code}, retry {attempt + 1}/{max_retries} in {wait}s...")
                time.sleep(wait)
                continue
            logger.warning(f"Data API failed: {e.response.status_code}")
            return []
        except httpx.TimeoutException:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                logger.warning(f"Data API timeout, retry {attempt + 1}/{max_retries} in {wait}s...")
                time.sleep(wait)
                continue
            logger.warning("Data API timeout after retries")
            return []

    return []


# Alias for backward compatibility
def fetch_positions_subgraph(address: str, limit: int = 20, subgraph_url: str = None) -> List[Dict[str, Any]]:
    """Deprecated: Use fetch_positions_with_fallback instead."""
    return fetch_positions_with_fallback(address, limit=limit)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG)

    # Use BOT_TRADER_ADDRESS from .env, or command line arg
    default_addr = os.getenv("BOT_TRADER_ADDRESS", "")
    address = sys.argv[1] if len(sys.argv) > 1 else default_addr

    if not address:
        print("Error: No address. Set BOT_TRADER_ADDRESS in .env or pass as argument")
        sys.exit(1)

    print(f"\n=== Chainstack RPC Test ===")
    print(f"RPC: {get_rpc_url()[:50]}...")

    # Test RPC connectivity
    try:
        with httpx.Client(timeout=10) as client:
            r = client.post(get_rpc_url(), json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []})
            block = int(r.json()["result"], 16)
            print(f"Current block: {block}")
            print("Chainstack RPC OK")
    except Exception as e:
        print(f"RPC Error: {e}")
        sys.exit(1)

    # Fetch positions from Data API
    print(f"\n=== Fetching positions for {address[:20]}... ===")
    positions = fetch_positions_with_fallback(address)

    if positions:
        print(f"Found {len(positions)} positions")

        # Verify via Chainstack RPC
        print("\n=== Verifying via Chainstack RPC ===")
        for p in positions[:3]:
            token_id = p.get("asset", "")
            if token_id:
                rpc_balance = verify_position_balance(address, token_id)
                api_size = float(p.get("size", 0))
                print(f"  {p.get('outcome', '?')}: API={api_size:.4f} RPC={rpc_balance:.4f}")
    else:
        print("No positions found")
