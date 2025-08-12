"""
DigitalOcean-compatible Order Book Manager for real-time DOM capture
"""
import websockets
import json
import requests
import asyncio
from collections import defaultdict
from typing import Dict, List, Optional
import time
import psycopg2
import psycopg2.errors
from psycopg2.extras import Json
import os
import logging

# Configure logging for DigitalOcean
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderBookManagerDO:
    def __init__(self, symbol: str = None):
        # Use environment variable or default
        self.symbol = symbol or os.getenv('SYMBOL', 'XMRUSDT')
        
        # price -> quantity
        self.bids = {}
        self.asks = {}
        self.last_update_id = None
        self.previous_final_update_id = None
        
        # WebSocket and API URLs
        self.ws_url = f"wss://fstream.binance.com/stream?streams={self.symbol.lower()}@depth"
        self.snapshot_url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self.symbol}&limit=1000"
        
        # Database connection
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        self.db_conn = None
        self.db_cur = None
        
        logger.info(f"OrderBookManager initialized for {self.symbol}")

    async def initialize(self):
        """Initialize the order book with a snapshot and start processing updates."""
        try:
            # Ensure DB is ready before streaming
            self.connect_db()
            self.ensure_table()
            await self.get_snapshot()
            await self.process_stream()
        except Exception as e:
            logger.error(f"Failed to initialize OrderBookManager: {e}")
            raise

    async def get_snapshot(self):
        """Get the initial order book snapshot."""
        logger.info("Getting order book snapshot...")
        try:
            response = requests.get(self.snapshot_url, timeout=10)
            response.raise_for_status()
            snapshot = response.json()

            self.last_update_id = snapshot['lastUpdateId']
            self.bids = {float(price): float(qty) for price, qty in snapshot['bids']}
            self.asks = {float(price): float(qty) for price, qty in snapshot['asks']}
            logger.info(f"Snapshot received for {self.symbol}. Last update ID: {self.last_update_id}")
        except Exception as e:
            logger.error(f"Failed to get snapshot: {e}")
            raise

    def connect_db(self):
        """Establish a PostgreSQL connection using DATABASE_URL."""
        try:
            if self.db_conn is None or self.db_conn.closed:
                self.db_conn = psycopg2.connect(self.database_url)
                self.db_conn.autocommit = False
                self.db_cur = self.db_conn.cursor()
                logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def ensure_table(self):
        """Create the dom_snapshots table if it does not exist."""
        try:
            assert self.db_cur is not None
            
            # Create the table without unique constraint to avoid conflicts
            self.db_cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.dom_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    symbol TEXT NOT NULL,
                    update_id BIGINT NOT NULL,
                    bids JSONB NOT NULL,
                    asks JSONB NOT NULL,
                    best_bid_price NUMERIC NOT NULL,
                    best_bid_qty NUMERIC NOT NULL,
                    best_ask_price NUMERIC NOT NULL,
                    best_ask_qty NUMERIC NOT NULL
                );
                
                CREATE INDEX IF NOT EXISTS idx_dom_snapshots_symbol_ts 
                ON public.dom_snapshots(symbol, ts);
                
                CREATE INDEX IF NOT EXISTS idx_dom_snapshots_update_id 
                ON public.dom_snapshots(update_id);
                
                CREATE INDEX IF NOT EXISTS idx_dom_snapshots_symbol_update_id 
                ON public.dom_snapshots(symbol, update_id);
                """
            )
            
            self.db_conn.commit()
            logger.info("DOM snapshots table ready")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            raise

    def save_top50_snapshot(self, update_id: int):
        """Persist the top 50 bid/ask levels to the database for the current book."""
        if not self.bids or not self.asks:
            return  # Need both sides to compute snapshot

        try:
            # Compute top 50 levels for each side
            top_bids = sorted(self.bids.items(), reverse=True)[:50]
            top_asks = sorted(self.asks.items())[:50]

            if not top_bids or not top_asks:
                return

            best_bid_price, best_bid_qty = top_bids[0]
            best_ask_price, best_ask_qty = top_asks[0]

            # Ensure DB connection is alive
            self.connect_db()
            assert self.db_cur is not None and self.db_conn is not None

            # Simple INSERT without ON CONFLICT to avoid constraint issues
            self.db_cur.execute(
                """
                INSERT INTO public.dom_snapshots
                (symbol, update_id, bids, asks, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty)
                VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s)
                """,
                (
                    self.symbol,
                    int(update_id),
                    json.dumps([[p, q] for p, q in top_bids]),
                    json.dumps([[p, q] for p, q in top_asks]),
                    best_bid_price,
                    best_bid_qty,
                    best_ask_price,
                    best_ask_qty,
                ),
            )
            self.db_conn.commit()
            
            # Log periodically (every 100 saves)
            if update_id % 100 == 0:
                logger.info(f"DOM snapshot saved for {self.symbol}, update_id: {update_id}")
                
        except psycopg2.errors.UniqueViolation:
            # Duplicate update_id, just skip and rollback
            if self.db_conn:
                self.db_conn.rollback()
            # Don't log this as an error since duplicates are expected
            pass
        except Exception as e:
            # Rollback on failure and log other errors
            if self.db_conn:
                self.db_conn.rollback()
            logger.error(f"DB insert error for update_id {update_id}: {e}")

    def update_order_book(self, bids: List[List[str]], asks: List[List[str]]):
        """Update the order book with new bids and asks."""
        # Update bids
        for bid in bids:
            price, quantity = float(bid[0]), float(bid[1])
            if quantity == 0:
                self.bids.pop(price, None)  # Remove the price level if it exists
            else:
                self.bids[price] = quantity

        # Update asks
        for ask in asks:
            price, quantity = float(ask[0]), float(ask[1])
            if quantity == 0:
                self.asks.pop(price, None)  # Remove the price level if it exists
            else:
                self.asks[price] = quantity

    async def process_stream(self):
        """Process the websocket stream for order book updates."""
        reconnect_count = 0
        max_reconnects = 10
        
        while reconnect_count < max_reconnects:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,  # Send ping every 20 seconds
                    ping_timeout=10,   # Wait 10 seconds for pong response
                    close_timeout=10   # Wait 10 seconds for close response
                ) as websocket:
                    logger.info(f"Connected to websocket stream for {self.symbol}")
                    reconnect_count = 0  # Reset counter on successful connection
                    
                    while True:
                        msg = await websocket.recv()
                        data = json.loads(msg)
                        event = data['data']

                        # Check if we need to drop this event
                        if event['u'] <= self.last_update_id:
                            continue

                        # For first processed event after snapshot
                        if self.previous_final_update_id is None:
                            if not (event['U'] <= self.last_update_id + 1 <= event['u']):
                                logger.warning("Out of sync, reinitializing...")
                                await self.get_snapshot()
                                continue
                        else:
                            # Check if event is properly sequenced
                            if event['pu'] != self.previous_final_update_id:
                                logger.warning("Out of sync, reinitializing...")
                                await self.get_snapshot()
                                continue

                        # Update the order book
                        self.update_order_book(event['b'], event['a'])
                        self.previous_final_update_id = event['u']
                        
                        # Save top 50 snapshot to DB
                        self.save_top50_snapshot(event['u'])
                        
                        # Small delay to prevent overwhelming the database
                        await asyncio.sleep(0.1)

            except websockets.exceptions.ConnectionClosed as e:
                reconnect_count += 1
                logger.warning(f"Connection closed ({e}). Reconnecting... (attempt {reconnect_count})")
                self.previous_final_update_id = None  # Reset the update ID
                await asyncio.sleep(min(reconnect_count * 2, 30))  # Exponential backoff
                
            except Exception as e:
                reconnect_count += 1
                logger.error(f"Error in stream processing: {e}. Reconnecting... (attempt {reconnect_count})")
                self.previous_final_update_id = None  # Reset the update ID
                await asyncio.sleep(min(reconnect_count * 2, 30))  # Exponential backoff
        
        logger.error(f"Max reconnection attempts ({max_reconnects}) reached. Stopping DOM capture.")

async def run_orderbook_manager():
    """Main function to run the orderbook manager"""
    try:
        symbol = os.getenv('SYMBOL', 'XMRUSDT')
        manager = OrderBookManagerDO(symbol)
        logger.info(f"Starting DOM capture for {symbol}")
        await manager.initialize()
    except Exception as e:
        logger.error(f"OrderBook manager failed: {e}")
        # Don't raise - let it restart automatically

if __name__ == "__main__":
    asyncio.run(run_orderbook_manager())
