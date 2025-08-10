#!/usr/bin/env python3
"""
Database client module for FHCharts GUI - DigitalOcean PostgreSQL
Provides connection pooling and query functions for live data access
"""
import psycopg2
import psycopg2.extras
import psycopg2.pool
import sys
import json
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import configuration
try:
    from config import DatabaseConfig
    config = DatabaseConfig()
    DATABASE_URL = config.get_connection_string()
except ImportError:
    # Fallback with DigitalOcean credentials
    DATABASE_URL = "postgresql://doadmin:AVNS_wHb-IBfuVa4-6cZdP5J@db-postgresql-sgp1-89042-do-user-24496237-0.g.db.ondigitalocean.com:25060/defaultdb?sslmode=require"

class FHChartsDatabase:
    """Database client for FHCharts with connection pooling and query methods"""
    
    def __init__(self, database_url: str = DATABASE_URL, pool_size: int = 5):
        self.database_url = database_url
        self.pool = None
        self._create_connection_pool(pool_size)
    
    def _create_connection_pool(self, pool_size: int):
        """Create a connection pool for efficient database access"""
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=pool_size,
                dsn=self.database_url
            )
            logger.info("✅ Database connection pool created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create connection pool: {e}")
            self.pool = None
    
    def get_connection(self):
        """Get a connection from the pool with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if self.pool:
                    conn = self.pool.getconn()
                    if conn:
                        return conn
                
                # Fallback to direct connection
                conn = psycopg2.connect(self.database_url)
                conn.autocommit = True
                return conn
                
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait before retry
                else:
                    logger.error(f"❌ All connection attempts failed: {e}")
                    return None
    
    def return_connection(self, conn):
        """Return connection to pool or close if not using pool"""
        try:
            if self.pool and conn:
                self.pool.putconn(conn)
            elif conn:
                conn.close()
        except Exception as e:
            logger.warning(f"Error returning connection: {e}")
    
    def test_connection(self):
        """Test database connectivity"""
        conn = self.get_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    self.return_connection(conn)
                    logger.info("✅ Database connection test successful")
                    return True
            except Exception as e:
                logger.error(f"❌ Database test failed: {e}")
                self.return_connection(conn)
                return False
        return False
    
    def get_timeframe_data(self, symbol: str, timeframe: str, limit: int = 100) -> List[Dict]:
        """
        Fetch recent timeframe data for GUI display
        
        Args:
            symbol: Trading pair (e.g., 'XMRUSDT')
            timeframe: Time bucket (e.g., '1m', '5m', '1h')
            limit: Number of recent buckets to fetch
            
        Returns:
            List of bucket data dictionaries
        """
        conn = self.get_connection()
        if not conn:
            return []
        
        # Normalize symbol to lowercase to match database format
        symbol_lower = symbol.lower()
        
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                query = """
                SELECT * FROM timeframe_data 
                WHERE symbol = %s AND timeframe = %s 
                ORDER BY bucket DESC 
                LIMIT %s
                """
                cursor.execute(query, (symbol_lower, timeframe, limit))
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries and parse JSON fields
                data = []
                for row in reversed(rows):  # Reverse to get chronological order
                    bucket_data = dict(row)
                    
                    # Parse JSON fields - JSONB fields are already parsed by psycopg2
                    try:
                        # Check if already parsed (dict/list) or needs parsing (string)
                        if isinstance(bucket_data['price_levels'], str):
                            bucket_data['price_levels'] = json.loads(bucket_data['price_levels'])
                        elif bucket_data['price_levels'] is None:
                            bucket_data['price_levels'] = {}
                            
                        if isinstance(bucket_data['imbalances'], str):
                            bucket_data['imbalances'] = json.loads(bucket_data['imbalances'])
                        elif bucket_data['imbalances'] is None:
                            bucket_data['imbalances'] = {}
                            
                        if isinstance(bucket_data['hvns'], str):
                            bucket_data['hvns'] = json.loads(bucket_data['hvns'])
                        elif bucket_data['hvns'] is None:
                            bucket_data['hvns'] = []
                            
                        if isinstance(bucket_data['lvns'], str):
                            bucket_data['lvns'] = json.loads(bucket_data['lvns'])
                        elif bucket_data['lvns'] is None:
                            bucket_data['lvns'] = []
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"JSON parsing error for bucket {bucket_data.get('bucket')}: {e}")
                        # Set defaults on error
                        bucket_data['price_levels'] = {}
                        bucket_data['imbalances'] = {}
                        bucket_data['hvns'] = []
                        bucket_data['lvns'] = []
                    
                    # Convert timestamp to datetime - bucket is already a timestamp
                    bucket_data['bucket_datetime'] = datetime.fromtimestamp(bucket_data['bucket'])
                    
                    # Map database column names to expected names
                    bucket_data['open'] = bucket_data.get('open_price', 0)
                    bucket_data['high'] = bucket_data.get('high_price', 0)
                    bucket_data['low'] = bucket_data.get('low_price', 0)
                    bucket_data['close'] = bucket_data.get('close_price', 0)
                    bucket_data['volume'] = bucket_data.get('total_volume', 0)
                    bucket_data['bucket_timestamp'] = bucket_data['bucket']
                    
                    data.append(bucket_data)
                
                self.return_connection(conn)
                logger.info(f"✅ Fetched {len(data)} buckets for {symbol} {timeframe}")
                return data
                
        except Exception as e:
            logger.error(f"❌ Error fetching timeframe data: {e}")
            self.return_connection(conn)
            return []
    
    def get_latest_buckets(self, symbol: str, timeframe: str, count: int = 50) -> List[Dict]:
        """
        Get the most recent completed buckets
        
        Args:
            symbol: Trading pair
            timeframe: Time bucket  
            count: Number of buckets to fetch
            
        Returns:
            List of recent bucket data
        """
        return self.get_timeframe_data(symbol, timeframe, count)
    
    def get_bucket_range(self, symbol: str, timeframe: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """
        Fetch buckets within a specific time range
        
        Args:
            symbol: Trading pair
            timeframe: Time bucket
            start_time: Start datetime
            end_time: End datetime
            
        Returns:
            List of bucket data within range
        """
        conn = self.get_connection()
        if not conn:
            return []
        
        # Normalize symbol to lowercase
        symbol_lower = symbol.lower()
        
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                start_ts = int(start_time.timestamp())
                end_ts = int(end_time.timestamp())
                
                query = """
                SELECT * FROM timeframe_data 
                WHERE symbol = %s AND timeframe = %s 
                AND bucket BETWEEN %s AND %s
                ORDER BY bucket ASC
                """
                cursor.execute(query, (symbol_lower, timeframe, start_ts, end_ts))
                rows = cursor.fetchall()
                
                # Convert and parse data
                data = []
                for row in rows:
                    bucket_data = dict(row)
                    
                    # Parse JSON fields
                    # Parse JSON fields - JSONB fields are already parsed by psycopg2
                    try:
                        # Check if already parsed (dict/list) or needs parsing (string)
                        if isinstance(bucket_data['price_levels'], str):
                            bucket_data['price_levels'] = json.loads(bucket_data['price_levels'])
                        elif bucket_data['price_levels'] is None:
                            bucket_data['price_levels'] = {}
                            
                        if isinstance(bucket_data['imbalances'], str):
                            bucket_data['imbalances'] = json.loads(bucket_data['imbalances'])
                        elif bucket_data['imbalances'] is None:
                            bucket_data['imbalances'] = {}
                            
                        if isinstance(bucket_data['hvns'], str):
                            bucket_data['hvns'] = json.loads(bucket_data['hvns'])
                        elif bucket_data['hvns'] is None:
                            bucket_data['hvns'] = []
                            
                        if isinstance(bucket_data['lvns'], str):
                            bucket_data['lvns'] = json.loads(bucket_data['lvns'])
                        elif bucket_data['lvns'] is None:
                            bucket_data['lvns'] = []
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"JSON parsing error for bucket {bucket_data.get('bucket')}: {e}")
                        # Set defaults on error
                        bucket_data['price_levels'] = {}
                        bucket_data['imbalances'] = {}
                        bucket_data['hvns'] = []
                        bucket_data['lvns'] = []
                    
                    bucket_data['bucket_datetime'] = datetime.fromtimestamp(bucket_data['bucket'])
                    bucket_data['bucket_timestamp'] = bucket_data['bucket']
                    
                    # Map database column names to GUI expected names
                    bucket_data['open'] = bucket_data.get('open_price', 0)
                    bucket_data['high'] = bucket_data.get('high_price', 0)
                    bucket_data['low'] = bucket_data.get('low_price', 0)
                    bucket_data['close'] = bucket_data.get('close_price', 0)
                    bucket_data['volume'] = bucket_data.get('total_volume', 0)
                    
                    data.append(bucket_data)
                
                self.return_connection(conn)
                logger.info(f"✅ Fetched {len(data)} buckets in time range")
                return data
                
        except Exception as e:
            logger.error(f"❌ Error fetching bucket range: {e}")
            self.return_connection(conn)
            return []
    
    def get_latest_bucket_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        """
        Get the timestamp of the most recent bucket
        Used for incremental updates
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        # Normalize symbol to lowercase
        symbol_lower = symbol.lower()
        
        try:
            with conn.cursor() as cursor:
                query = """
                SELECT bucket FROM timeframe_data 
                WHERE symbol = %s AND timeframe = %s 
                ORDER BY bucket DESC 
                LIMIT 1
                """
                cursor.execute(query, (symbol_lower, timeframe))
                result = cursor.fetchone()
                self.return_connection(conn)
                
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"❌ Error getting latest timestamp: {e}")
            self.return_connection(conn)
            return None
    
    def get_latest_update_time(self, symbol: str, timeframe: str) -> Optional[str]:
        """
        Get the updated_at timestamp of the most recently modified bucket
        This detects when existing buckets are updated with new tick data
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        # Normalize symbol to lowercase
        symbol_lower = symbol.lower()
        
        try:
            with conn.cursor() as cursor:
                query = """
                SELECT updated_at FROM timeframe_data 
                WHERE symbol = %s AND timeframe = %s 
                ORDER BY updated_at DESC 
                LIMIT 1
                """
                cursor.execute(query, (symbol_lower, timeframe))
                result = cursor.fetchone()
                self.return_connection(conn)
                
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"❌ Error getting latest update time: {e}")
            self.return_connection(conn)
            return None

    def save_dom_snapshot(self, symbol: str, timestamp: float, bids_data: str, asks_data: str) -> bool:
        """Save DOM snapshot to database"""
        conn = self.get_connection()
        if not conn:
            return False
            
        try:
            # Calculate metadata
            best_bid = None
            best_ask = None
            spread = None
            levels_count = 0
            
            if bids_data:
                bid_levels = bids_data.split(',')
                levels_count += len(bid_levels)
                if bid_levels and ':' in bid_levels[0]:
                    best_bid = float(bid_levels[0].split(':')[0])
            
            if asks_data:
                ask_levels = asks_data.split(',')
                levels_count += len(ask_levels)
                if ask_levels and ':' in ask_levels[0]:
                    best_ask = float(ask_levels[0].split(':')[0])
            
            if best_bid and best_ask:
                spread = best_ask - best_bid
            
            # Insert to database
            insert_sql = """
            INSERT INTO dom_snapshots 
            (symbol, timestamp, bids_data, asks_data, levels_count, best_bid, best_ask, spread)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                bids_data = EXCLUDED.bids_data,
                asks_data = EXCLUDED.asks_data,
                levels_count = EXCLUDED.levels_count,
                best_bid = EXCLUDED.best_bid,
                best_ask = EXCLUDED.best_ask,
                spread = EXCLUDED.spread
            """
            
            with conn.cursor() as cursor:
                cursor.execute(insert_sql, (
                    symbol.lower(), timestamp, bids_data, asks_data, 
                    levels_count, best_bid, best_ask, spread
                ))
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving DOM snapshot: {e}")
            return False
        finally:
            self.return_connection(conn)

    def get_dom_snapshots(self, symbol: str, start_timestamp: float, end_timestamp: float, limit: int = 1000):
        """Get DOM snapshots for a time range"""
        conn = self.get_connection()
        if not conn:
            return []
            
        try:
            query = """
            SELECT timestamp, bids_data, asks_data, best_bid, best_ask, spread, created_at
            FROM dom_snapshots 
            WHERE symbol = %s AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp ASC
            LIMIT %s
            """
            
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, (symbol.lower(), start_timestamp, end_timestamp, limit))
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"❌ Error fetching DOM snapshots: {e}")
            return []
        finally:
            self.return_connection(conn)

    def cleanup_old_dom_data(self, symbol: str, days_to_keep: int = 7):
        """Remove old DOM snapshots (high frequency data, shorter retention)"""
        conn = self.get_connection()
        if not conn:
            return 0
            
        try:
            query = """
            DELETE FROM dom_snapshots 
            WHERE symbol = %s AND created_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
            """
            
            with conn.cursor() as cursor:
                cursor.execute(query, (symbol.lower(), days_to_keep))
                deleted_count = cursor.rowcount
                logger.info(f"✅ Cleaned up {deleted_count} old DOM snapshots for {symbol}")
                return deleted_count
                
        except Exception as e:
            logger.error(f"❌ Error cleaning up old DOM data: {e}")
            return 0
        finally:
            self.return_connection(conn)
    
    def close(self):
        """Close all connections and cleanup"""
        try:
            if self.pool:
                self.pool.closeall()
                logger.info("🔒 Database connection pool closed")
        except Exception as e:
            logger.warning(f"Error closing connection pool: {e}")


# Global database instance
db_client = None

def get_database_client() -> FHChartsDatabase:
    """Get or create the global database client instance"""
    global db_client
    if db_client is None:
        db_client = FHChartsDatabase()
    return db_client

def connect_db():
    """Legacy function for backward compatibility"""
    client = get_database_client()
    return client.get_connection()

def run_query(query: str):
    """Execute a SQL query and return results"""
    client = get_database_client()
    conn = client.get_connection()
    if not conn:
        print("❌ Could not connect to database")
        return
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query)
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                print(f"✅ Query returned {len(results)} rows")
                for row in results:
                    print(dict(row))
            else:
                print("✅ Query executed successfully")
    except Exception as e:
        print(f"❌ Query failed: {e}")
    finally:
        client.return_connection(conn)

def interactive_mode():
    """Interactive query mode"""
    client = get_database_client()
    
    # Test connection first
    if not client.test_connection():
        print("❌ Cannot connect to database. Check your network and IP whitelist.")
        return
    
    print("\n🔍 Interactive Query Mode")
    print("Type SQL queries or commands:")
    print("  - 'test' : Test connection")
    print("  - 'tables' : List tables") 
    print("  - 'schema' : Show timeframe_data schema")
    print("  - 'latest' : Show latest 5 buckets")
    print("  - 'count' : Count total records")
    print("  - 'quit' : Exit")
    print("-" * 50)
    
    while True:
        try:
            cmd = input("\nSQL> ").strip()
            
            if cmd.lower() in ['quit', 'exit', 'q']:
                break
            elif cmd.lower() == 'test':
                if client.test_connection():
                    print("✅ Connection successful")
                else:
                    print("❌ Connection failed")
            elif cmd.lower() == 'tables':
                run_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            elif cmd.lower() == 'schema':
                run_query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'timeframe_data'")
            elif cmd.lower() == 'latest':
                run_query("SELECT symbol, timeframe, bucket, open_price, close_price, total_volume FROM timeframe_data ORDER BY bucket DESC LIMIT 5")
            elif cmd.lower() == 'count':
                run_query("SELECT COUNT(*) as total_buckets FROM timeframe_data")
            elif cmd:
                run_query(cmd)
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
    
    client.close()

if __name__ == "__main__":
    print("🐘 FHCharts Database Client")
    print("========================================")
    
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # Test mode
        client = get_database_client()
        if client.test_connection():
            print("✅ Database connection successful!")
            
            # Test data fetching
            print("\n📊 Testing data fetch...")
            data = client.get_timeframe_data('XMRUSDT', '1m', 5)
            print(f"Fetched {len(data)} buckets")
            
            if data:
                latest = data[-1]
                print(f"Latest bucket: {latest['bucket']} - OHLC: {latest['open']:.2f}/{latest['high']:.2f}/{latest['low']:.2f}/{latest['close']:.2f}")
                
                # Show structure of first bucket for debugging
                first_bucket = data[0]
                print(f"\n🔍 First bucket structure:")
                print(f"  Available keys: {list(first_bucket.keys())}")
                print(f"  Symbol: {first_bucket['symbol']}")
                print(f"  Timeframe: {first_bucket['timeframe']}")
                print(f"  Bucket: {first_bucket['bucket']}")
                print(f"  OHLC: {first_bucket.get('open', 'N/A')}, {first_bucket.get('high', 'N/A')}, {first_bucket.get('low', 'N/A')}, {first_bucket.get('close', 'N/A')}")
                print(f"  Volume: {first_bucket.get('volume', 'N/A')}")
                print(f"  Total Volume (DB): {first_bucket.get('total_volume', 'N/A')}")
                print(f"  Buy/Sell Volume: {first_bucket.get('buy_volume', 'N/A')}/{first_bucket.get('sell_volume', 'N/A')}")
                print(f"  Delta: {first_bucket.get('delta', 'N/A')}")
                print(f"  CVD: {first_bucket.get('cvd', 'N/A')}")
                print(f"  POC: {first_bucket.get('poc', 'N/A')}")
                print(f"  Price levels type: {type(first_bucket.get('price_levels', 'N/A'))}")
                print(f"  Imbalances type: {type(first_bucket.get('imbalances', 'N/A'))}")
                print(f"  HVNs type: {type(first_bucket.get('hvns', 'N/A'))}")
                print(f"  LVNs type: {type(first_bucket.get('lvns', 'N/A'))}")
                
                # Show a sample of price levels if available
                price_levels = first_bucket.get('price_levels')
                if price_levels and isinstance(price_levels, dict):
                    sample_prices = list(price_levels.items())[:2]
                    print(f"  Sample price levels: {sample_prices}")
                    
            # Clean up connection pool
            client.close()
        else:
            print("❌ Database connection failed!")
    else:
        # Interactive mode
        interactive_mode()
