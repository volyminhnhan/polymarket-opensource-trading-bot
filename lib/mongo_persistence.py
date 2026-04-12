"""
MongoDB persistence for KoNiS Polymarket Trading transactions.

Stores trade details similar to what `list_positions.py --top 20` outputs.
Uses async motor for non-blocking database operations.

Position Lifecycle:
1. ENTRY: Save position immediately with tx_hash as unique ID
2. UPDATE: Track PnL changes during position lifecycle
3. EXIT: Final update when position is closed

Schema uses tx_hash (transaction hash) as unique identifier, e.g.:
  0x636142e4f33033f5d6d213459252e5cd239b9cdf7a2ad1d543e1423eaed1cb50
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

import httpx
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo import ASCENDING, DESCENDING, ReplaceOne

logger = logging.getLogger(__name__)


class PolymarketMongoPersistence:
    """
    MongoDB persistence for KoNiS Polymarket Trading transactions.

    Schema matches the data structure from Data API positions/activity.
    Uses tx_hash (transaction hash) as unique identifier for positions.
    """

    COLLECTION_NAME = "polymarket_transactions"
    CANDLE_STATUS_COLLECTION = "polymarket_candle_status"  # Track each candle's decision
    SCALPING_COLLECTION = "polymarket_scalping"  # Scalping bot trade stats
    CHUNKS_COLLECTION = "scalping_chunks"  # V3: Individual trade chunk tracking
    # NOTE: positions_collection removed - using transactions as single source of truth
    BATCH_INTERVAL_SEC = 10
    RETENTION_DAYS = 90  # Keep trade history for 90 days

    def __init__(self, mongodb_url: str, database_name: str = "konis_polymarket"):
        self.mongodb_url = mongodb_url
        self.database_name = database_name
        self.data_host = os.getenv("DATA_HOST", "https://data-api.polymarket.com")

        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None
        self.collection: Optional[AsyncIOMotorCollection] = None
        self.candle_status_collection: Optional[AsyncIOMotorCollection] = None
        self.scalping_collection: Optional[AsyncIOMotorCollection] = None
        self.chunks_collection: Optional[AsyncIOMotorCollection] = None

        self.pending_writes: List[Dict] = []
        self.writer_task: Optional[asyncio.Task] = None
        self.running = False

        # Pending exit_reasons - applied when transaction appears in DB
        # Key: token_id, Value: {"exit_reason": str, "timestamp": int}
        self.pending_exit_reasons: Dict[str, Dict[str, Any]] = {}

    async def connect(self):
        """Connect to MongoDB and initialize collections.
        Pre-checks TCP reachability to avoid Motor spawning noisy background threads on unreachable hosts.
        """
        import socket
        from urllib.parse import urlparse
        try:
            # Pre-check: TCP connect to mongo host before creating Motor client
            parsed = urlparse(self.mongodb_url.replace("mongodb://", "http://").replace("mongodb+srv://", "https://"))
            host = parsed.hostname or "localhost"
            port = parsed.port or 27017
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            try:
                sock.connect((host, port))
                sock.close()
            except (socket.timeout, OSError) as e:
                sock.close()
                raise ConnectionError(f"MongoDB host {host}:{port} unreachable: {e}")

            self._client = AsyncIOMotorClient(
                self.mongodb_url,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )

            # Test connection
            await self._client.admin.command('ping')

            self._db = self._client[self.database_name]
            self.collection = self._db[self.COLLECTION_NAME]
            self.candle_status_collection = self._db[self.CANDLE_STATUS_COLLECTION]
            self.scalping_collection = self._db[self.SCALPING_COLLECTION]
            self.chunks_collection = self._db[self.CHUNKS_COLLECTION]

            # Create indexes
            await self._create_indexes()

            # Start batch writer
            self.running = True
            self.writer_task = asyncio.create_task(self._batch_writer())

            logger.info(f"MongoDB persistence connected to {self.database_name}")
            logger.info(f"  Collections: {self.COLLECTION_NAME}, {self.CANDLE_STATUS_COLLECTION}, {self.SCALPING_COLLECTION}, {self.CHUNKS_COLLECTION}")

        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            # Close Motor client to stop background reconnect attempts
            if self._client:
                self._client.close()
                self._client = None
            raise

    async def _create_indexes(self):
        """Create indexes for efficient querying"""
        try:
            # === TRANSACTIONS COLLECTION INDEXES ===
            # Primary lookup by trade_id
            await self.collection.create_index(
                [("trade_id", ASCENDING)],
                name="trade_id_idx",
                unique=True
            )

            # Query by bot instance and session
            await self.collection.create_index(
                [("bot_instance_id", ASCENDING), ("session_id", ASCENDING)],
                name="instance_session_idx"
            )

            # Query by window timestamp
            await self.collection.create_index(
                [("window_ts", DESCENDING)],
                name="window_ts_idx"
            )

            # Query by status (open positions)
            await self.collection.create_index(
                [("status", ASCENDING), ("created_at", DESCENDING)],
                name="status_created_idx"
            )

            # Time-based queries
            await self.collection.create_index(
                [("created_at", DESCENDING)],
                name="created_at_idx"
            )

            # === CANDLE STATUS COLLECTION INDEXES ===
            # Primary lookup by window_ts and bot_instance
            await self.candle_status_collection.create_index(
                [("window_ts", ASCENDING), ("bot_instance_id", ASCENDING)],
                name="window_instance_idx",
                unique=True
            )

            # Query by status and time
            await self.candle_status_collection.create_index(
                [("status", ASCENDING), ("created_at", DESCENDING)],
                name="status_created_idx"
            )

            # Query by session
            await self.candle_status_collection.create_index(
                [("session_id", ASCENDING), ("window_ts", DESCENDING)],
                name="session_window_idx"
            )

            # Time-based queries for recent candles
            await self.candle_status_collection.create_index(
                [("created_at", DESCENDING)],
                name="candle_created_at_idx"
            )

            # === SCALPING COLLECTION INDEXES ===
            # Primary lookup by trade_id
            await self.scalping_collection.create_index(
                [("trade_id", ASCENDING)],
                name="scalping_trade_id_idx",
                unique=True
            )

            # Query by session
            await self.scalping_collection.create_index(
                [("session_id", ASCENDING), ("window_ts", DESCENDING)],
                name="scalping_session_window_idx"
            )

            # Query by status (open positions)
            await self.scalping_collection.create_index(
                [("status", ASCENDING), ("entry_time", DESCENDING)],
                name="scalping_status_entry_idx"
            )

            # Query by exit_reason for analytics
            await self.scalping_collection.create_index(
                [("exit_reason", ASCENDING), ("exit_time", DESCENDING)],
                name="scalping_exit_reason_idx"
            )

            # Time-based queries
            await self.scalping_collection.create_index(
                [("entry_time", DESCENDING)],
                name="scalping_entry_time_idx"
            )

            # === CHUNKS COLLECTION INDEXES ===
            # Primary lookup by chunk_id
            await self.chunks_collection.create_index(
                [("chunk_id", ASCENDING)],
                name="chunk_id_idx",
                unique=True
            )

            # Query by session
            await self.chunks_collection.create_index(
                [("session_id", ASCENDING), ("status", ASCENDING)],
                name="chunks_session_status_idx"
            )

            # Query open chunks by condition_id
            await self.chunks_collection.create_index(
                [("condition_id", ASCENDING), ("status", ASCENDING)],
                name="chunks_condition_status_idx"
            )

            # Time-based queries
            await self.chunks_collection.create_index(
                [("entry_time", DESCENDING)],
                name="chunks_entry_time_idx"
            )

            logger.debug("MongoDB indexes created for all collections")

        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")

    async def close(self):
        """Close MongoDB connection"""
        self.running = False

        # Flush remaining writes
        if self.pending_writes:
            await self._flush_pending_writes()

        if self.writer_task:
            self.writer_task.cancel()
            try:
                await self.writer_task
            except asyncio.CancelledError:
                pass

        if self._client:
            self._client.close()

        logger.info("MongoDB persistence closed")

    async def _batch_writer(self):
        """Background task to batch write pending documents"""
        while self.running:
            try:
                await asyncio.sleep(self.BATCH_INTERVAL_SEC)
                if self.pending_writes:
                    await self._flush_pending_writes()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch writer error: {e}")

    async def _flush_pending_writes(self):
        """Flush pending writes to MongoDB"""
        if not self.pending_writes:
            return

        # Flush scalping writes first
        await self._flush_scalping_writes()

        # Flush chunk writes
        await self._flush_chunk_writes()

        if not self.pending_writes or self.collection is None:
            return

        writes = self.pending_writes.copy()
        self.pending_writes.clear()

        try:
            operations = []
            token_id_updates = []

            for doc in writes:
                # Check if this is a token_id-based update (exit update for existing entry)
                if doc.get("_update_by_token_id"):
                    token_id = doc.pop("_update_by_token_id")
                    token_id_updates.append((token_id, doc))
                else:
                    operations.append(
                        ReplaceOne(
                            filter={"trade_id": doc["trade_id"]},
                            replacement=doc,
                            upsert=True
                        )
                    )

            # Process regular writes
            if operations:
                result = await self.collection.bulk_write(operations, ordered=False)
                logger.debug(f"Flushed {len(operations)} writes to MongoDB (upserted={result.upserted_count}, modified={result.modified_count})")

            # Process token_id-based exit updates (find OPEN trade by token_id and update)
            for token_id, doc in token_id_updates:
                # Try to update existing OPEN entry by token_id
                result = await self.collection.update_one(
                    {"token_id": token_id, "status": "OPEN"},
                    {"$set": doc}
                )
                if result.modified_count > 0:
                    logger.info(f"[MONGO] Updated exit by token_id: {token_id[:20]}... | reason={doc.get('exit_reason')}")
                else:
                    # Fallback: try by trade_id
                    result = await self.collection.update_one(
                        {"trade_id": doc.get("trade_id")},
                        {"$set": doc}
                    )
                    if result.modified_count > 0:
                        logger.info(f"[MONGO] Updated exit by trade_id: {doc.get('trade_id')} | reason={doc.get('exit_reason')}")
                    else:
                        logger.warning(f"[MONGO] Could not find trade to update: token={token_id[:20]}..., trade_id={doc.get('trade_id')}")

        except Exception as e:
            logger.error(f"Failed to flush writes to MongoDB: {e}")
            # Re-queue failed writes
            self.pending_writes.extend(writes)

    def log_entry(
        self,
        trade_id: str,
        bot_instance_id: str,
        session_id: str,
        window_ts: int,
        market_slug: str,
        condition_id: str,
        side: str,
        direction: str,
        confidence: float,
        entry_price: float,
        size: float,
        amount_usdc: float,
        token_id: str = "",
    ):
        """
        Log trade entry to MongoDB.

        Args:
            trade_id: Order ID or unique trade identifier
            bot_instance_id: Bot instance ID
            session_id: Trading session ID
            window_ts: 15-minute window timestamp
            market_slug: Market slug (e.g., btc-updown-15m-1234567890)
            condition_id: Market condition ID
            side: Trade side (YES/NO)
            direction: Prediction direction (UP/DOWN)
            confidence: Entry confidence score
            entry_price: Entry price
            size: Token size purchased
            amount_usdc: USDC amount spent
            token_id: Token ID for the position
        """
        now = datetime.now(timezone.utc)

        doc = {
            "trade_id": trade_id,
            "bot_instance_id": bot_instance_id,
            "session_id": session_id,

            # Market info
            "window_ts": window_ts,
            "market_slug": market_slug,
            "condition_id": condition_id,
            "token_id": token_id,

            # Trade details
            "side": side,
            "direction": direction,
            "confidence": confidence,

            # Prices & amounts
            "entry_price": entry_price,
            "exit_price": None,
            "size": size,
            "amount_usdc": amount_usdc,

            # PnL
            "pnl_percent": None,
            "pnl_cash": None,

            # Status
            "status": "OPEN",
            "exit_reason": None,

            # Timestamps
            "entry_time": now,
            "exit_time": None,
            "created_at": now,
            "updated_at": now,
        }

        self.pending_writes.append(doc)
        logger.info(f"[MONGO] Logged entry: {trade_id} | {side} @ {entry_price:.4f}")

    def log_trade(self, data: dict):
        """Log a generic V4 trade event (entry, DCA buy, DCA sell, exit).

        Accepts a flat dict and appends it to pending_writes with timestamp.
        """
        now = datetime.now(timezone.utc)
        data.setdefault("created_at", now)
        data.setdefault("updated_at", now)
        self.pending_writes.append(data)
        trade_type = data.get("type", "UNKNOWN")
        side = data.get("side", "?")
        price = data.get("price", 0)
        logger.info(f"[MONGO] Logged trade: {trade_type} | {side} @ ${price:.4f}")

    def log_exit(
        self,
        trade_id: str,
        exit_price: float,
        pnl_percent: float,
        pnl_cash: float,
        exit_reason: str,
        token_id: str = None,
    ):
        """
        Update trade record with exit information.

        Args:
            trade_id: Order ID or unique trade identifier
            exit_price: Exit price
            pnl_percent: PnL percentage
            pnl_cash: PnL in cash (USDC)
            exit_reason: Reason for exit
            token_id: Optional token_id for fallback matching
        """
        now = datetime.now(timezone.utc)

        # Find and update pending write if exists (by trade_id or token_id)
        for doc in self.pending_writes:
            if doc.get("trade_id") == trade_id or (token_id and doc.get("token_id") == token_id and doc.get("status") == "OPEN"):
                doc["exit_price"] = exit_price
                doc["pnl_percent"] = pnl_percent
                doc["pnl_cash"] = pnl_cash
                doc["status"] = "CLOSED"
                doc["exit_reason"] = exit_reason
                doc["exit_time"] = now
                doc["updated_at"] = now
                logger.info(f"[MONGO] Updated exit (pending): {trade_id} | PnL: {pnl_percent:+.1%} (${pnl_cash:+.2f}) | reason={exit_reason}")
                return

        # If not in pending, queue direct update with token_id for matching
        doc = {
            "_update_by_token_id": token_id,  # Flag for batch writer to use token_id matching
            "trade_id": trade_id,
            "exit_price": exit_price,
            "pnl_percent": pnl_percent,
            "pnl_cash": pnl_cash,
            "status": "CLOSED",
            "exit_reason": exit_reason,
            "exit_time": now,
            "updated_at": now,
        }

        self.pending_writes.append(doc)
        logger.info(f"[MONGO] Queued exit: {trade_id} | token={token_id[:20] if token_id else 'N/A'}... | PnL: {pnl_percent:+.1%} (${pnl_cash:+.2f}) | reason={exit_reason}")

    async def log_exit_async(
        self,
        trade_id: str,
        exit_price: float,
        pnl_percent: float,
        pnl_cash: float,
        exit_reason: str,
        token_id: str = None,
    ):
        """
        Directly update trade record in MongoDB (immediate write).
        Tries to find by trade_id first, then falls back to token_id if provided.
        """
        if self.collection is None:
            return

        now = datetime.now(timezone.utc)
        update_data = {
            "$set": {
                "exit_price": exit_price,
                "pnl_percent": pnl_percent,
                "pnl_cash": pnl_cash,
                "status": "CLOSED",
                "exit_reason": exit_reason,
                "exit_time": now,
                "updated_at": now,
            }
        }

        try:
            # Try by trade_id first
            result = await self.collection.update_one(
                {"trade_id": trade_id},
                update_data
            )

            if result.modified_count > 0:
                logger.info(f"[MONGO] Updated exit: {trade_id} | PnL: {pnl_percent:+.1%} (${pnl_cash:+.2f})")
                return

            # If not found by trade_id, try by token_id (most recent OPEN)
            if token_id:
                result = await self.collection.update_one(
                    {"token_id": token_id, "status": "OPEN"},
                    update_data
                )
                if result.modified_count > 0:
                    logger.info(f"[MONGO] Updated exit by token_id: {token_id[:20]}... | PnL: {pnl_percent:+.1%} (${pnl_cash:+.2f}) | reason={exit_reason}")
                    return

            logger.warning(f"[MONGO] Trade not found for exit update: trade_id={trade_id}, token_id={token_id[:20] if token_id else 'N/A'}...")

        except Exception as e:
            logger.error(f"Failed to update exit in MongoDB: {e}")

    async def update_transaction_exit_reason(
        self,
        token_id: str,
        exit_reason: str,
        exit_time: Optional[datetime] = None,
    ):
        """
        Update exit_reason directly in konis_polymarket.transactions collection.
        This is the SINGLE SOURCE OF TRUTH for trade data.

        Finds the most recent SELL transaction for this token_id and adds exit_reason.
        If no transaction found (not synced yet), stores in pending_exit_reasons for later.

        Args:
            token_id: The asset/token_id to match
            exit_reason: Exit reason (TP%, SL%, etc.)
            exit_time: Optional exit time to help narrow down the transaction
        """
        if self._client is None:
            logger.warning("[TRANSACTIONS] MongoDB client not connected")
            # Store for later even if not connected
            self.pending_exit_reasons[token_id] = {
                "exit_reason": exit_reason,
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
            }
            return False

        try:
            tx_collection = self._client["konis_polymarket"]["transactions"]

            # Find the most recent SELL transaction for this token_id
            query = {
                "asset": token_id,
                "side": "SELL",
            }

            # If exit_time provided, look for transactions within 5 minutes
            if exit_time:
                exit_ts = int(exit_time.timestamp()) if hasattr(exit_time, 'timestamp') else int(exit_time)
                query["timestamp"] = {"$gte": exit_ts - 300, "$lte": exit_ts + 300}

            # Find and update the most recent matching transaction
            result = await tx_collection.find_one_and_update(
                query,
                {"$set": {"exit_reason": exit_reason, "exit_reason_updated_at": datetime.now(timezone.utc)}},
                sort=[("timestamp", DESCENDING)],
                return_document=True
            )

            if result:
                logger.info(f"[TRANSACTIONS] Updated exit_reason: {token_id[:20]}... -> {exit_reason}")
                # Remove from pending if was stored
                self.pending_exit_reasons.pop(token_id, None)
                return True
            else:
                # Transaction not synced yet - store for later
                logger.info(f"[TRANSACTIONS] SELL not found yet, queuing exit_reason: {token_id[:20]}... -> {exit_reason}")
                self.pending_exit_reasons[token_id] = {
                    "exit_reason": exit_reason,
                    "timestamp": int(datetime.now(timezone.utc).timestamp()),
                }
                return False

        except Exception as e:
            logger.error(f"[TRANSACTIONS] Failed to update exit_reason: {e}")
            # Store for later on error too
            self.pending_exit_reasons[token_id] = {
                "exit_reason": exit_reason,
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
            }
            return False

    async def apply_pending_exit_reasons(self):
        """
        Apply pending exit_reasons to transactions that have now appeared.
        Called after transaction scanner syncs new data.
        """
        if not self.pending_exit_reasons or self._client is None:
            return

        tx_collection = self._client["konis_polymarket"]["transactions"]
        applied = []
        now_ts = int(datetime.now(timezone.utc).timestamp())

        for token_id, data in list(self.pending_exit_reasons.items()):
            exit_reason = data["exit_reason"]
            created_ts = data["timestamp"]

            # Skip if too old (> 1 hour) - transaction probably never happened
            if now_ts - created_ts > 3600:
                logger.warning(f"[TRANSACTIONS] Pending exit_reason expired: {token_id[:20]}...")
                applied.append(token_id)
                continue

            try:
                result = await tx_collection.find_one_and_update(
                    {"asset": token_id, "side": "SELL"},
                    {"$set": {"exit_reason": exit_reason, "exit_reason_updated_at": datetime.now(timezone.utc)}},
                    sort=[("timestamp", DESCENDING)],
                    return_document=True
                )

                if result:
                    logger.info(f"[TRANSACTIONS] Applied pending exit_reason: {token_id[:20]}... -> {exit_reason}")
                    applied.append(token_id)
            except Exception as e:
                logger.error(f"[TRANSACTIONS] Failed to apply pending exit_reason: {e}")

        # Remove applied items
        for token_id in applied:
            self.pending_exit_reasons.pop(token_id, None)

        if applied:
            logger.info(f"[TRANSACTIONS] Applied {len(applied)} pending exit_reasons")

    async def update_transaction_entry_data(
        self,
        token_id: str,
        bot_instance_id: str,
        session_id: str,
        window_ts: int,
        market_slug: str,
        direction: str,
        confidence: float,
        entry_time: Optional[datetime] = None,
    ):
        """
        Enrich BUY transaction in konis_polymarket.transactions with entry metadata.
        This adds bot-specific context to blockchain transaction data.

        Args:
            token_id: The asset/token_id to match
            bot_instance_id: Bot instance ID
            session_id: Trading session ID
            window_ts: 15-minute window timestamp
            market_slug: Market slug
            direction: Prediction direction (UP/DOWN)
            confidence: Entry confidence score
            entry_time: Optional entry time to help narrow down the transaction
        """
        if self._client is None:
            logger.warning("[TRANSACTIONS] MongoDB client not connected")
            return False

        try:
            tx_collection = self._client["konis_polymarket"]["transactions"]

            # Find the most recent BUY transaction for this token_id
            query = {
                "asset": token_id,
                "side": "BUY",
            }

            # If entry_time provided, look for transactions within 5 minutes
            if entry_time:
                entry_ts = int(entry_time.timestamp()) if hasattr(entry_time, 'timestamp') else int(entry_time)
                query["timestamp"] = {"$gte": entry_ts - 300, "$lte": entry_ts + 300}

            # Enrich with bot metadata
            update_data = {
                "bot_instance_id": bot_instance_id,
                "session_id": session_id,
                "window_ts": window_ts,
                "market_slug": market_slug,
                "direction": direction,
                "confidence": confidence,
                "entry_data_updated_at": datetime.now(timezone.utc),
            }

            # Find and update the most recent matching transaction
            result = await tx_collection.find_one_and_update(
                query,
                {"$set": update_data},
                sort=[("timestamp", DESCENDING)],
                return_document=True
            )

            if result:
                logger.info(f"[TRANSACTIONS] Enriched entry: {token_id[:20]}... | {direction} @ {confidence:.0%}")
                return True
            else:
                logger.debug(f"[TRANSACTIONS] No BUY transaction found for token_id: {token_id[:20]}...")
                return False

        except Exception as e:
            logger.error(f"[TRANSACTIONS] Failed to enrich entry data: {e}")
            return False

    async def get_recent_trades(
        self,
        bot_instance_id: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Get recent trades from MongoDB.

        Args:
            bot_instance_id: Filter by bot instance
            session_id: Filter by session
            limit: Max number of trades to return

        Returns:
            List of trade documents
        """
        if self.collection is None:
            return []

        try:
            query = {}
            if bot_instance_id:
                query["bot_instance_id"] = bot_instance_id
            if session_id:
                query["session_id"] = session_id

            cursor = self.collection.find(query).sort("created_at", DESCENDING).limit(limit)
            trades = await cursor.to_list(length=limit)

            return trades

        except Exception as e:
            logger.error(f"Failed to get recent trades: {e}")
            return []

    async def get_open_trades(self) -> List[Dict[str, Any]]:
        """Get all open (unclosed) trades"""
        if self.collection is None:
            return []

        try:
            cursor = self.collection.find({"status": "OPEN"}).sort("created_at", DESCENDING)
            trades = await cursor.to_list(length=100)
            return trades
        except Exception as e:
            logger.error(f"Failed to get open trades: {e}")
            return []

    async def get_session_summary(
        self,
        session_id: str
    ) -> Dict[str, Any]:
        """
        Get summary statistics for a trading session.

        Returns dict with total_trades, winning_trades, total_pnl, etc.
        """
        if self.collection is None:
            return {}

        try:
            pipeline = [
                {"$match": {"session_id": session_id}},
                {
                    "$group": {
                        "_id": "$session_id",
                        "total_trades": {"$sum": 1},
                        "winning_trades": {
                            "$sum": {"$cond": [{"$gt": ["$pnl_cash", 0]}, 1, 0]}
                        },
                        "total_pnl": {"$sum": {"$ifNull": ["$pnl_cash", 0]}},
                        "total_volume": {"$sum": "$amount_usdc"},
                        "first_trade": {"$min": "$created_at"},
                        "last_trade": {"$max": "$created_at"},
                    }
                }
            ]

            result = await self.collection.aggregate(pipeline).to_list(length=1)

            if result:
                summary = result[0]
                summary["win_rate"] = (
                    summary["winning_trades"] / summary["total_trades"]
                    if summary["total_trades"] > 0
                    else 0
                )
                return summary

            return {}

        except Exception as e:
            logger.error(f"Failed to get session summary: {e}")
            return {}

    # ========================================================================
    # POSITION TRACKING METHODS (with tx_hash as unique ID)
    # ========================================================================

    async def fetch_tx_hash_from_activity(
        self,
        trading_address: str,
        condition_id: str,
        side: str = "BUY",
        limit: int = 10
    ) -> Optional[str]:
        """
        Fetch the transaction hash from Polymarket Data API activity endpoint.

        Args:
            trading_address: The wallet address to query
            condition_id: The market condition ID
            side: Trade side (BUY/SELL)
            limit: Number of recent activities to check

        Returns:
            Transaction hash (e.g., 0x636142e4f33033f5d6d213459252e5cd239b9cdf7a2ad1d543e1423eaed1cb50)
            or None if not found
        """
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    f"{self.data_host}/activity",
                    params={"user": trading_address, "limit": limit}
                )
                r.raise_for_status()
                activities = r.json()

                # Find the most recent activity matching condition_id and side
                for activity in activities:
                    act_condition = activity.get("conditionId", "")
                    act_side = activity.get("side", "")
                    tx_hash = activity.get("transactionHash", "")

                    if act_condition == condition_id and act_side == side and tx_hash:
                        return tx_hash

                return None

        except Exception as e:
            logger.warning(f"Failed to fetch tx_hash from activity: {e}")
            return None

    # ========================================================================
    # CANDLE STATUS TRACKING METHODS
    # ========================================================================

    async def save_candle_status(
        self,
        bot_instance_id: str,
        session_id: str,
        candle_status: Dict[str, Any],
    ) -> bool:
        """
        Save candle/window status to track decisions for each 15m window.

        Args:
            bot_instance_id: Bot instance ID
            session_id: Trading session ID
            candle_status: Dict containing candle status fields from CandleStatus.to_dict()

        Returns:
            True if saved successfully, False otherwise
        """
        if self.candle_status_collection is None:
            logger.warning("Candle status collection not initialized")
            return False

        try:
            # Add bot/session info
            doc = {
                "bot_instance_id": bot_instance_id,
                "session_id": session_id,
                **candle_status
            }

            # Use upsert to update if exists (same window_ts + bot_instance)
            result = await self.candle_status_collection.update_one(
                {
                    "window_ts": candle_status.get("window_ts"),
                    "bot_instance_id": bot_instance_id
                },
                {"$set": doc},
                upsert=True
            )

            if result.upserted_id or result.modified_count > 0:
                logger.debug(
                    f"[MONGO_CANDLE] Saved candle status: {candle_status.get('window_time_utc')} | "
                    f"Status: {candle_status.get('status')}"
                )
                return True

            return True

        except Exception as e:
            logger.error(f"Failed to save candle status: {e}")
            return False

    async def get_recent_candle_statuses(
        self,
        bot_instance_id: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get recent candle statuses for analysis.

        Args:
            bot_instance_id: Filter by bot instance
            session_id: Filter by session
            limit: Max number of candles to return

        Returns:
            List of candle status documents
        """
        if self.candle_status_collection is None:
            return []

        try:
            query = {}
            if bot_instance_id:
                query["bot_instance_id"] = bot_instance_id
            if session_id:
                query["session_id"] = session_id

            cursor = self.candle_status_collection.find(query).sort("window_ts", DESCENDING).limit(limit)
            statuses = await cursor.to_list(length=limit)

            return statuses

        except Exception as e:
            logger.error(f"Failed to get candle statuses: {e}")
            return []

    async def get_candle_status_summary(
        self,
        session_id: Optional[str] = None,
        hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Get aggregate summary of candle statuses.

        Returns:
            Dict with counts by status, skip reasons, etc.
        """
        if self.candle_status_collection is None:
            return {}

        try:
            # Calculate time cutoff
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

            match_stage = {"created_at": {"$gte": cutoff}}
            if session_id:
                match_stage["session_id"] = session_id

            pipeline = [
                {"$match": match_stage},
                {
                    "$group": {
                        "_id": "$status",
                        "count": {"$sum": 1},
                        "avg_confidence": {"$avg": "$max_confidence"},
                    }
                }
            ]

            result = await self.candle_status_collection.aggregate(pipeline).to_list(length=20)

            summary = {
                "total_candles": 0,
                "by_status": {},
                "hours_analyzed": hours,
            }

            for item in result:
                status = item["_id"]
                count = item["count"]
                summary["by_status"][status] = {
                    "count": count,
                    "avg_confidence": item.get("avg_confidence", 0),
                }
                summary["total_candles"] += count

            return summary

        except Exception as e:
            logger.error(f"Failed to get candle status summary: {e}")
            return {}

    # ========================================================================
    # TRANSACTIONS COLLECTION METHODS (from transactions_scan_to_mongo.py)
    # ========================================================================

    async def get_trade_history_from_transactions(
        self,
        limit: int = 50,
        from_hours: int = 24,
        trading_address: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get trade history from 'transactions' collection (Data API format from scanner).
        Matches BUY/SELL pairs by token_id to calculate PnL.

        Args:
            limit: Max number of trades to return
            from_hours: Only include transactions from last N hours (default 24)
            trading_address: Filter by wallet address (BOT_TRADER_ADDRESS)

        Returns:
            List of trades with PnL data
        """
        if self._client is None:
            logger.warning("MongoDB client not connected")
            return []

        try:
            # Use 'transactions' collection from konis_polymarket database
            # (populated by transactions_scan_to_mongo.py)
            tx_db = self._client["konis_polymarket"]
            tx_collection = tx_db["transactions"]

            # Build query - filter by recent time and address
            cutoff = datetime.now(timezone.utc) - timedelta(hours=from_hours)
            cutoff_ts = int(cutoff.timestamp())

            query = {"timestamp": {"$gte": cutoff_ts}}

            # Filter by trading address if provided
            if trading_address:
                query["address"] = trading_address.lower()

            # Get transactions sorted by time descending
            cursor = tx_collection.find(query).sort("timestamp", DESCENDING).limit(limit * 3)
            transactions = await cursor.to_list(length=limit * 3)

            logger.debug(f"[TRADE_HISTORY] Found {len(transactions)} transactions in last {from_hours}h")

            if not transactions:
                return []

            # Group by asset (token_id) to match BUY/SELL pairs
            token_trades: Dict[str, List[dict]] = {}
            for tx in transactions:
                # Field is 'asset' in the DB, not 'token_id'
                token_id = tx.get("asset", "")
                if token_id:
                    if token_id not in token_trades:
                        token_trades[token_id] = []
                    token_trades[token_id].append(tx)

            # Helper to convert datetime to timestamp
            def to_timestamp(val):
                if val is None:
                    return 0
                if hasattr(val, 'timestamp'):
                    # MongoDB stores datetime as UTC but may return naive datetime
                    # If naive, assume it's UTC before converting to timestamp
                    if hasattr(val, 'tzinfo') and val.tzinfo is None:
                        val = val.replace(tzinfo=timezone.utc)
                    return int(val.timestamp())
                if isinstance(val, (int, float)):
                    return int(val)
                return 0

            # Match BUY/SELL/REDEEM pairs
            # NOTE: exit_reason is now stored directly in transactions collection (single source of truth)
            all_trades = []
            for token_id, txs in token_trades.items():
                # Use "timestamp" field (raw int from MongoDB), not "tx_time"
                buys = sorted([t for t in txs if t.get("side") == "BUY"], key=lambda x: x.get("timestamp", 0))
                sells = sorted([t for t in txs if t.get("side") == "SELL"], key=lambda x: x.get("timestamp", 0))
                # Redemptions: type == "REDEEM" or no side with usdc > 0
                redeems = sorted([t for t in txs if t.get("type") == "REDEEM" or (not t.get("side") and t.get("usdc_size", 0) > 0)], key=lambda x: x.get("timestamp", 0))

                matched_buy_ids = set()

                # Match each SELL to FIRST unmatched BUY before it (FIFO)
                for sell in sells:
                    sell_time = sell.get("timestamp", 0)
                    sell_price = sell.get("price", 0)
                    sell_usdc = sell.get("usdc_size", 0)

                    # Find FIRST unmatched BUY before this sell (FIFO order)
                    matching_buy = None
                    for buy in buys:
                        if id(buy) in matched_buy_ids:
                            continue
                        buy_time = buy.get("timestamp", 0)
                        if buy_time and buy_time < sell_time:
                            matching_buy = buy
                            break  # FIFO: Take first unmatched buy, not last

                    if matching_buy:
                        buy_time = matching_buy.get("timestamp", 0)
                        buy_price = matching_buy.get("price", 0)
                        buy_usdc = matching_buy.get("usdc_size", 0)

                        # Calculate PnL using PRICE-BASED formula (consistent with displayed prices)
                        # pnl_percent = (exit_price - entry_price) / entry_price
                        # pnl_cash = buy_usdc * pnl_percent (uses entry investment as base)
                        # This ensures displayed PnL% matches what user calculates from prices
                        if buy_price > 0:
                            pnl_percent = (sell_price - buy_price) / buy_price
                            pnl_cash = buy_usdc * pnl_percent
                        else:
                            pnl_percent = 0
                            pnl_cash = 0
                        duration_secs = (sell_time - buy_time) if buy_time and sell_time else 0

                        # Get exit_reason directly from transactions collection (single source of truth)
                        exit_reason = sell.get("exit_reason") or "SOLD"

                        all_trades.append({
                            "trade_id": sell.get("tx_hash", ""),
                            "side": sell.get("outcome", "?"),
                            "direction": sell.get("outcome", "?"),
                            "entry_price": buy_price,
                            "exit_price": sell_price,
                            "pnl_percent": pnl_percent,
                            "pnl_cash": pnl_cash,
                            "entry_time": buy_time,
                            "exit_time": sell_time,
                            "duration_secs": duration_secs,
                            "exit_reason": exit_reason,
                            "status": "CLOSED",
                            "buy_usdc": buy_usdc,
                            "sell_usdc": sell_usdc,
                            "amount_usdc": buy_usdc,  # Entry cost for volume tracking
                        })
                        matched_buy_ids.add(id(matching_buy))

                # Match each REDEEM to FIRST unmatched BUY before it (FIFO)
                for redeem in redeems:
                    redeem_time = redeem.get("timestamp", 0)
                    redeem_usdc = redeem.get("usdc_size", 0)

                    # Find FIRST unmatched BUY before this redeem (FIFO order)
                    matching_buy = None
                    for buy in buys:
                        if id(buy) in matched_buy_ids:
                            continue
                        buy_time = buy.get("timestamp", 0)
                        if buy_time and buy_time < redeem_time:
                            matching_buy = buy
                            break  # FIFO: Take first unmatched buy, not last

                    if matching_buy:
                        buy_time = matching_buy.get("timestamp", 0)
                        buy_price = matching_buy.get("price", 0)
                        buy_usdc = matching_buy.get("usdc_size", 0)

                        # Calculate PnL for redemption
                        pnl_cash = redeem_usdc - buy_usdc
                        pnl_percent = (pnl_cash / buy_usdc) if buy_usdc > 0 else 0
                        duration_secs = (redeem_time - buy_time) if buy_time and redeem_time else 0

                        # Exit reason for redemptions
                        outcome = redeem.get("outcome", matching_buy.get("outcome", "?"))
                        exit_reason = f"REDEEMED ({outcome})"

                        all_trades.append({
                            "trade_id": redeem.get("tx_hash", ""),
                            "side": outcome,
                            "direction": outcome,
                            "entry_price": buy_price,
                            "exit_price": 1.0 if pnl_cash > 0 else 0.0,  # Redemption price is 1.0 for winner, 0.0 for loser
                            "pnl_percent": pnl_percent,
                            "pnl_cash": pnl_cash,
                            "entry_time": buy_time,
                            "exit_time": redeem_time,
                            "duration_secs": duration_secs,
                            "exit_reason": exit_reason,
                            "status": "REDEEMED",
                            "buy_usdc": buy_usdc,
                            "sell_usdc": redeem_usdc,
                            "amount_usdc": buy_usdc,  # Entry cost for volume tracking
                        })
                        matched_buy_ids.add(id(matching_buy))

                # Add unmatched BUYs as OPEN
                for buy in buys:
                    if id(buy) not in matched_buy_ids:
                        all_trades.append({
                            "trade_id": buy.get("tx_hash", ""),
                            "side": buy.get("outcome", "?"),
                            "direction": buy.get("outcome", "?"),
                            "entry_price": buy.get("price", 0),
                            "exit_price": 0,
                            "pnl_percent": 0,
                            "pnl_cash": 0,
                            "entry_time": buy.get("timestamp", 0),
                            "exit_time": 0,
                            "duration_secs": 0,
                            "exit_reason": "OPEN",
                            "status": "OPEN",
                            "amount_usdc": buy.get("usdc_size", 0),
                        })

            # Sort by time (exit_time for closed, entry_time for open)
            all_trades.sort(key=lambda x: x.get("exit_time", 0) or x.get("entry_time", 0), reverse=True)

            logger.debug(f"[TRADE_HISTORY] Returning {len(all_trades[:limit])} trades")
            return all_trades[:limit]

        except Exception as e:
            logger.error(f"Failed to get trade history from transactions: {e}")
            return []

    async def get_trade_history_stats(
        self,
        from_hours: int = 24,
        trading_address: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get aggregated trade statistics from 'transactions' collection.

        Args:
            from_hours: Only include transactions from last N hours
            trading_address: Filter by wallet address (BOT_TRADER_ADDRESS)

        Returns:
            Dict with total_trades, buy_count, sell_count, net_pnl, etc.
        """
        if self._client is None:
            return {}

        try:
            # Use 'transactions' collection from konis_polymarket database
            tx_db = self._client["konis_polymarket"]
            tx_collection = tx_db["transactions"]
            cutoff = datetime.now(timezone.utc) - timedelta(hours=from_hours)
            cutoff_ts = int(cutoff.timestamp())

            # Build match filter
            match_filter = {"timestamp": {"$gte": cutoff_ts}}
            if trading_address:
                match_filter["address"] = trading_address.lower()

            # Aggregate by side (BUY/SELL)
            pipeline = [
                {"$match": match_filter},
                {
                    "$group": {
                        "_id": "$side",
                        "count": {"$sum": 1},
                        "total_usdc": {"$sum": {"$ifNull": ["$usdc_size", 0]}},
                    }
                }
            ]

            result = await tx_collection.aggregate(pipeline).to_list(length=10)

            stats = {
                "total_trades": 0,
                "buy_count": 0,
                "sell_count": 0,
                "total_buy": 0.0,
                "total_sell": 0.0,
                "net_pnl": 0.0,
            }

            for item in result:
                side = item["_id"]
                count = item["count"]
                usdc = item.get("total_usdc", 0) or 0

                stats["total_trades"] += count

                if side == "BUY":
                    stats["buy_count"] = count
                    stats["total_buy"] = usdc
                elif side == "SELL":
                    stats["sell_count"] = count
                    stats["total_sell"] = usdc

            stats["net_pnl"] = stats["total_sell"] - stats["total_buy"]

            return stats

        except Exception as e:
            logger.error(f"Failed to get trade history stats: {e}")
            return {}

    # ========================================================================
    # SCALPING BOT METHODS
    # ========================================================================

    def log_scalping_entry(
        self,
        trade_id: str,
        session_id: str,
        window_ts: int,
        market_slug: str,
        condition_id: str,
        side: str,
        entry_price: float,
        entry_size: float,
        amount_usdc: float,
        token_id: str,
        entry_reason: str = "",
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Log scalping trade entry to MongoDB.

        Args:
            trade_id: Order ID or unique trade identifier
            session_id: Trading session ID
            window_ts: 15-minute window timestamp
            market_slug: Market slug
            condition_id: Market condition ID
            side: Trade side (YES/NO)
            entry_price: Entry price
            entry_size: Token size purchased
            amount_usdc: USDC amount spent
            token_id: Token ID
            entry_reason: Entry reason (in-range, rising)
            config: Bot configuration snapshot
        """
        now = datetime.now(timezone.utc)

        doc = {
            "trade_id": trade_id,
            "session_id": session_id,

            # Market info
            "window_ts": window_ts,
            "market_slug": market_slug,
            "condition_id": condition_id,
            "token_id": token_id,

            # Trade details
            "side": side,
            "entry_reason": entry_reason,

            # Prices & amounts
            "entry_price": entry_price,
            "exit_price": None,
            "entry_size": entry_size,
            "amount_usdc": amount_usdc,

            # PnL
            "pnl_percent": None,
            "pnl_cash": None,

            # Status
            "status": "OPEN",
            "exit_reason": None,

            # Config snapshot
            "config": config or {},

            # Timestamps
            "entry_time": now,
            "exit_time": None,
            "duration_secs": None,
        }

        self.pending_writes.append({"_scalping": True, **doc})
        logger.info(f"[SCALPING] Logged entry: {trade_id[:20]}... | {side} @ {entry_price:.4f} | reason={entry_reason}")

    def log_scalping_exit(
        self,
        trade_id: str,
        exit_price: float,
        pnl_percent: float,
        pnl_cash: float,
        exit_reason: str,
        token_id: str = None,
    ):
        """
        Update scalping trade record with exit information.

        Args:
            trade_id: Order ID or unique trade identifier
            exit_price: Exit price
            pnl_percent: PnL percentage
            pnl_cash: PnL in cash (USDC)
            exit_reason: Exit reason (TP, TP%, SL, manual, etc.)
            token_id: Token ID for fallback matching
        """
        now = datetime.now(timezone.utc)

        # Find and update pending write if exists
        for doc in self.pending_writes:
            if doc.get("_scalping") and (doc.get("trade_id") == trade_id or (token_id and doc.get("token_id") == token_id and doc.get("status") == "OPEN")):
                entry_time = doc.get("entry_time")
                duration = (now - entry_time).total_seconds() if entry_time else 0

                doc["exit_price"] = exit_price
                doc["pnl_percent"] = pnl_percent
                doc["pnl_cash"] = pnl_cash
                doc["status"] = "CLOSED"
                doc["exit_reason"] = exit_reason
                doc["exit_time"] = now
                doc["duration_secs"] = duration
                logger.info(f"[SCALPING] Updated exit (pending): {trade_id[:20]}... | {exit_reason} | PnL: {pnl_percent:+.1%} (${pnl_cash:+.2f})")
                return

        # If not in pending, queue for direct update
        doc = {
            "_scalping_exit": True,
            "_token_id": token_id,
            "trade_id": trade_id,
            "exit_price": exit_price,
            "pnl_percent": pnl_percent,
            "pnl_cash": pnl_cash,
            "status": "CLOSED",
            "exit_reason": exit_reason,
            "exit_time": now,
        }

        self.pending_writes.append(doc)
        logger.info(f"[SCALPING] Queued exit: {trade_id[:20]}... | {exit_reason} | PnL: {pnl_percent:+.1%} (${pnl_cash:+.2f})")

    async def _flush_scalping_writes(self):
        """Flush pending scalping writes to MongoDB"""
        if not self.pending_writes or self.scalping_collection is None:
            return

        scalping_writes = [w for w in self.pending_writes if w.get("_scalping") or w.get("_scalping_exit")]
        if not scalping_writes:
            return

        # Remove from pending
        self.pending_writes = [w for w in self.pending_writes if not (w.get("_scalping") or w.get("_scalping_exit"))]

        try:
            for doc in scalping_writes:
                if doc.get("_scalping_exit"):
                    # Exit update
                    token_id = doc.pop("_token_id", None)
                    doc.pop("_scalping_exit", None)
                    trade_id = doc.get("trade_id")

                    # Try update by trade_id first
                    result = await self.scalping_collection.update_one(
                        {"trade_id": trade_id},
                        {"$set": doc}
                    )

                    if result.modified_count == 0 and token_id:
                        # Fallback: update by token_id
                        result = await self.scalping_collection.update_one(
                            {"token_id": token_id, "status": "OPEN"},
                            {"$set": doc}
                        )
                        if result.modified_count > 0:
                            logger.debug(f"[SCALPING] Updated exit by token_id: {token_id[:20]}...")

                else:
                    # Entry insert/upsert
                    doc.pop("_scalping", None)
                    await self.scalping_collection.replace_one(
                        {"trade_id": doc["trade_id"]},
                        doc,
                        upsert=True
                    )

            logger.debug(f"[SCALPING] Flushed {len(scalping_writes)} writes")

        except Exception as e:
            logger.error(f"[SCALPING] Failed to flush writes: {e}")
            # Re-queue failed writes
            self.pending_writes.extend(scalping_writes)

    async def get_scalping_session_stats(
        self,
        session_id: Optional[str] = None,
        hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Get aggregated scalping session statistics.

        Args:
            session_id: Filter by session ID
            hours: Time window in hours

        Returns:
            Dict with total_trades, winning_trades, total_pnl, win_rate,
            avg_pnl, by_exit_reason breakdown, etc.
        """
        if self.scalping_collection is None:
            return {}

        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

            match_filter: Dict[str, Any] = {"entry_time": {"$gte": cutoff}}
            if session_id:
                match_filter["session_id"] = session_id

            # Main stats aggregation
            pipeline = [
                {"$match": match_filter},
                {
                    "$group": {
                        "_id": None,
                        "total_trades": {"$sum": 1},
                        "winning_trades": {
                            "$sum": {"$cond": [{"$gt": ["$pnl_cash", 0]}, 1, 0]}
                        },
                        "losing_trades": {
                            "$sum": {"$cond": [{"$lt": ["$pnl_cash", 0]}, 1, 0]}
                        },
                        "total_pnl": {"$sum": {"$ifNull": ["$pnl_cash", 0]}},
                        "total_volume": {"$sum": "$amount_usdc"},
                        "avg_pnl_percent": {"$avg": {"$ifNull": ["$pnl_percent", 0]}},
                        "avg_duration_secs": {"$avg": {"$ifNull": ["$duration_secs", 0]}},
                        "max_win": {"$max": {"$ifNull": ["$pnl_cash", 0]}},
                        "max_loss": {"$min": {"$ifNull": ["$pnl_cash", 0]}},
                    }
                }
            ]

            result = await self.scalping_collection.aggregate(pipeline).to_list(length=1)

            stats: Dict[str, Any] = {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "total_pnl": 0.0,
                "total_volume": 0.0,
                "win_rate": 0.0,
                "avg_pnl_percent": 0.0,
                "avg_duration_secs": 0.0,
                "max_win": 0.0,
                "max_loss": 0.0,
                "hours_analyzed": hours,
                "by_exit_reason": {},
                "by_side": {},
            }

            if result:
                r = result[0]
                stats.update({
                    "total_trades": r.get("total_trades", 0),
                    "winning_trades": r.get("winning_trades", 0),
                    "losing_trades": r.get("losing_trades", 0),
                    "total_pnl": r.get("total_pnl", 0.0),
                    "total_volume": r.get("total_volume", 0.0),
                    "avg_pnl_percent": r.get("avg_pnl_percent", 0.0),
                    "avg_duration_secs": r.get("avg_duration_secs", 0.0),
                    "max_win": r.get("max_win", 0.0),
                    "max_loss": r.get("max_loss", 0.0),
                })
                if stats["total_trades"] > 0:
                    stats["win_rate"] = stats["winning_trades"] / stats["total_trades"]

            # Breakdown by exit_reason
            reason_pipeline = [
                {"$match": {**match_filter, "status": "CLOSED"}},
                {
                    "$group": {
                        "_id": "$exit_reason",
                        "count": {"$sum": 1},
                        "total_pnl": {"$sum": {"$ifNull": ["$pnl_cash", 0]}},
                        "avg_pnl": {"$avg": {"$ifNull": ["$pnl_cash", 0]}},
                    }
                }
            ]

            reason_result = await self.scalping_collection.aggregate(reason_pipeline).to_list(length=20)
            for item in reason_result:
                reason = item["_id"] or "Unknown"
                stats["by_exit_reason"][reason] = {
                    "count": item["count"],
                    "total_pnl": item["total_pnl"],
                    "avg_pnl": item["avg_pnl"],
                }

            # Breakdown by side (YES/NO)
            side_pipeline = [
                {"$match": match_filter},
                {
                    "$group": {
                        "_id": "$side",
                        "count": {"$sum": 1},
                        "wins": {"$sum": {"$cond": [{"$gt": ["$pnl_cash", 0]}, 1, 0]}},
                        "total_pnl": {"$sum": {"$ifNull": ["$pnl_cash", 0]}},
                    }
                }
            ]

            side_result = await self.scalping_collection.aggregate(side_pipeline).to_list(length=5)
            for item in side_result:
                side = item["_id"] or "Unknown"
                count = item["count"]
                stats["by_side"][side] = {
                    "count": count,
                    "wins": item["wins"],
                    "win_rate": item["wins"] / count if count > 0 else 0,
                    "total_pnl": item["total_pnl"],
                }

            return stats

        except Exception as e:
            logger.error(f"[SCALPING] Failed to get session stats: {e}")
            return {}

    async def get_recent_scalping_trades(
        self,
        session_id: Optional[str] = None,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Get recent scalping trades.

        Args:
            session_id: Filter by session ID
            limit: Max number of trades

        Returns:
            List of trade documents
        """
        if self.scalping_collection is None:
            return []

        try:
            query: Dict[str, Any] = {}
            if session_id:
                query["session_id"] = session_id

            cursor = self.scalping_collection.find(query).sort("entry_time", DESCENDING).limit(limit)
            trades = await cursor.to_list(length=limit)

            return trades

        except Exception as e:
            logger.error(f"[SCALPING] Failed to get recent trades: {e}")
            return []

    # ========================================================================
    # V3: TRADE CHUNK TRACKING
    # ========================================================================

    def log_chunk_entry(
        self,
        chunk_id: str,
        session_id: str,
        market_slug: str,
        condition_id: str,
        side: str,
        token_id: str,
        entry_price: float,
        entry_tokens: float,
        entry_usd: float,
    ):
        """
        Log a trade chunk entry.

        Each buy creates a chunk. Chunks are tracked individually for P&L.
        Combined P&L across chunks determines exit timing.
        """
        now = datetime.now(timezone.utc)

        doc = {
            "_chunk": True,  # Flag for batch writer
            "chunk_id": chunk_id,
            "session_id": session_id,

            # Market info
            "market_slug": market_slug,
            "condition_id": condition_id,
            "token_id": token_id,
            "side": side,

            # Entry
            "entry_price": entry_price,
            "entry_tokens": entry_tokens,
            "entry_usd": entry_usd,
            "entry_time": now,

            # Exit (filled on close)
            "exit_price": None,
            "exit_tokens": None,
            "exit_usd": None,
            "exit_time": None,

            # PnL
            "pnl_pct": None,
            "pnl_usd": None,

            # Status
            "status": "open",  # open, closed, resolved

            "created_at": now,
            "updated_at": now,
        }

        self.pending_writes.append(doc)
        logger.info(f"[CHUNK] Entry: {chunk_id[:16]}... | {side} {entry_tokens:.2f} @ ${entry_price:.4f}")

    def log_chunk_exit(
        self,
        chunk_id: str,
        exit_price: float,
        exit_tokens: float,
        pnl_pct: float,
        pnl_usd: float,
        exit_reason: str = "",
    ):
        """Update chunk with exit info."""
        now = datetime.now(timezone.utc)

        # Try to update in pending writes first
        for doc in self.pending_writes:
            if doc.get("_chunk") and doc.get("chunk_id") == chunk_id:
                doc["exit_price"] = exit_price
                doc["exit_tokens"] = exit_tokens
                doc["exit_usd"] = exit_tokens * exit_price
                doc["exit_time"] = now
                doc["pnl_pct"] = pnl_pct
                doc["pnl_usd"] = pnl_usd
                doc["status"] = "closed"
                doc["exit_reason"] = exit_reason
                doc["updated_at"] = now
                logger.info(f"[CHUNK] Exit (pending): {chunk_id[:16]}... | {pnl_pct*100:+.1f}% (${pnl_usd:+.2f})")
                return

        # Queue for direct update
        doc = {
            "_chunk_exit": True,
            "chunk_id": chunk_id,
            "exit_price": exit_price,
            "exit_tokens": exit_tokens,
            "exit_usd": exit_tokens * exit_price,
            "exit_time": now,
            "pnl_pct": pnl_pct,
            "pnl_usd": pnl_usd,
            "status": "closed",
            "exit_reason": exit_reason,
            "updated_at": now,
        }
        self.pending_writes.append(doc)
        logger.info(f"[CHUNK] Exit (queued): {chunk_id[:16]}... | {pnl_pct*100:+.1f}%")

    async def get_session_chunks(
        self,
        session_id: str,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get all chunks for a session, optionally filtered by status."""
        if self.chunks_collection is None:
            return []

        try:
            query: Dict[str, Any] = {"session_id": session_id}
            if status:
                query["status"] = status

            cursor = self.chunks_collection.find(query).sort("entry_time", ASCENDING)
            chunks = await cursor.to_list(length=500)
            return chunks
        except Exception as e:
            logger.error(f"[CHUNK] Failed to get session chunks: {e}")
            return []

    async def get_condition_chunks(
        self,
        condition_id: str,
        status: str = "open",
    ) -> List[Dict[str, Any]]:
        """Get open chunks for a specific market condition."""
        if self.chunks_collection is None:
            return []

        try:
            cursor = self.chunks_collection.find({
                "condition_id": condition_id,
                "status": status,
            }).sort("entry_time", ASCENDING)
            chunks = await cursor.to_list(length=100)
            return chunks
        except Exception as e:
            logger.error(f"[CHUNK] Failed to get condition chunks: {e}")
            return []

    async def _flush_chunk_writes(self):
        """Flush pending chunk writes to MongoDB."""
        if not self.pending_writes or self.chunks_collection is None:
            return

        chunk_writes = [w for w in self.pending_writes if w.get("_chunk") or w.get("_chunk_exit")]
        if not chunk_writes:
            return

        # Remove from pending
        self.pending_writes = [w for w in self.pending_writes if not (w.get("_chunk") or w.get("_chunk_exit"))]

        try:
            for doc in chunk_writes:
                if doc.get("_chunk_exit"):
                    # Exit update
                    chunk_id = doc.pop("chunk_id")
                    doc.pop("_chunk_exit", None)

                    await self.chunks_collection.update_one(
                        {"chunk_id": chunk_id},
                        {"$set": doc}
                    )
                else:
                    # Entry insert/upsert
                    doc.pop("_chunk", None)
                    await self.chunks_collection.replace_one(
                        {"chunk_id": doc["chunk_id"]},
                        doc,
                        upsert=True
                    )

            logger.debug(f"[CHUNK] Flushed {len(chunk_writes)} writes")

        except Exception as e:
            logger.error(f"[CHUNK] Failed to flush writes: {e}")
            self.pending_writes.extend(chunk_writes)

