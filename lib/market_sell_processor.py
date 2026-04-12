#!/usr/bin/env python3
"""
Market Sell Processor - Parallel sell execution across markets.

Problem: sell_position(), clob.get_order_book(), verify_position_balance() are
synchronous blocking calls that prevent asyncio.gather() from truly parallelizing
market operations. When Market A's sell blocks the event loop (~1-2s per attempt),
Market B's coroutine must wait, causing sequential XRP > SOL > XRP execution.

Solution: Wrap blocking I/O calls in asyncio.run_in_executor() via a ThreadPoolExecutor.
Each market's sell operations run in separate threads, allowing true parallelism.
State updates remain in the asyncio thread (after await) for thread safety.
"""

import asyncio
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

logger = logging.getLogger(__name__)


class MarketSellProcessor:
    """Thread pool dispatcher for blocking sell I/O operations.

    Usage in async sell functions:
        # Before (blocks event loop):
        result = self.trader.sell_position(token_id=..., size=...)

        # After (runs in thread, event loop stays responsive):
        result = await self.sell_processor.run_io(
            self.trader.sell_position, token_id=..., size=...)
    """

    def __init__(self, max_workers: int = 4):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="sell-io",
        )
        logger.info(f"[SELL-PROC] Initialized with {max_workers} workers")

    async def run_io(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking I/O function in the thread pool.

        The calling coroutine suspends until the function completes,
        allowing other coroutines (other markets) to run on the event loop.

        Args:
            fn: Synchronous blocking function to execute
            *args, **kwargs: Arguments passed to fn

        Returns:
            Whatever fn returns
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            functools.partial(fn, *args, **kwargs),
        )

    def shutdown(self):
        """Shutdown the thread pool gracefully."""
        self._executor.shutdown(wait=False)
        logger.info("[SELL-PROC] Shutdown complete")
