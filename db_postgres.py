"""
PostgreSQL Database adapter for FHCharts - DigitalOcean App Platform
"""
import psycopg2
import psycopg2.extras
import json
import os
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgreSQLDatabase:
    """Handles PostgreSQL database operations for timeframe data"""
    
    def __init__(self):
        # DigitalOcean App Platform provides database URL as environment variable
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        self.connection = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(self.database_url)
            self.connection.autocommit = True
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def create_tables(self):
        """Create tables if they don't exist"""
        # Create timeframe_data table (existing HTF aggregation)
        timeframe_table_sql = """
        CREATE TABLE IF NOT EXISTS timeframe_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timeframe VARCHAR(10) NOT NULL,
            bucket BIGINT NOT NULL,
            total_volume DECIMAL(20,8),
            buy_volume DECIMAL(20,8),
            sell_volume DECIMAL(20,8),
            buy_contracts INTEGER,
            sell_contracts INTEGER,
            open_price DECIMAL(20,8),
            high_price DECIMAL(20,8),
            low_price DECIMAL(20,8),
            close_price DECIMAL(20,8),
            delta DECIMAL(20,8),
            max_delta DECIMAL(20,8),
            min_delta DECIMAL(20,8),
            cvd DECIMAL(20,8),
            large_order_count INTEGER,
            market_limit_ratio DECIMAL(10,4),
            buy_sell_ratio DECIMAL(10,4),
            poc DECIMAL(20,8),
            price_levels JSONB,
            hvns JSONB,
            lvns JSONB,
            imbalances JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timeframe, bucket)
        );
        
        CREATE INDEX IF NOT EXISTS idx_symbol_timeframe_bucket 
        ON timeframe_data(symbol, timeframe, bucket);
        
        CREATE INDEX IF NOT EXISTS idx_created_at 
        ON timeframe_data(created_at);
        """
        
        # Create dom_snapshots table (new DOM capture)
        dom_snapshots_table_sql = """
        CREATE TABLE IF NOT EXISTS dom_snapshots (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            timestamp DECIMAL(20,10) NOT NULL,
            bids_data TEXT NOT NULL,
            asks_data TEXT NOT NULL,
            levels_count INTEGER DEFAULT 100,
            best_bid DECIMAL(20,8),
            best_ask DECIMAL(20,8),
            spread DECIMAL(20,8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_dom_symbol_timestamp 
        ON dom_snapshots(symbol, timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_dom_created_at 
        ON dom_snapshots(created_at);
        
        CREATE INDEX IF NOT EXISTS idx_dom_best_prices 
        ON dom_snapshots(symbol, best_bid, best_ask);
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(timeframe_table_sql)
            cursor.execute(dom_snapshots_table_sql)
            logger.info("Database tables created/verified (timeframe_data + dom_snapshots)")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def load_timeframe_data(self, symbol: str, timeframe: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Load existing data for a specific symbol and timeframe"""
        query = """
        SELECT symbol, timeframe, bucket, total_volume, buy_volume, sell_volume,
               buy_contracts, sell_contracts, open_price as "open", high_price as "high", 
               low_price as "low", close_price as "close", delta, max_delta, min_delta,
               cvd, large_order_count, market_limit_ratio, buy_sell_ratio, poc,
               price_levels, hvns, lvns, imbalances
        FROM timeframe_data 
        WHERE symbol = %s AND timeframe = %s 
        ORDER BY bucket ASC 
        LIMIT %s
        """
        
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query, (symbol, timeframe, limit))
            rows = cursor.fetchall()
            
            # Convert to list of dicts and handle JSON fields
            result = []
            for row in rows:
                row_dict = dict(row)
                # Convert bucket to string (to match CSV format)
                row_dict['bucket'] = str(row_dict['bucket'])
                # Convert JSON fields to strings (to match CSV format)
                for json_field in ['price_levels', 'hvns', 'lvns', 'imbalances']:
                    if row_dict[json_field]:
                        row_dict[json_field] = json.dumps(row_dict[json_field])
                result.append(row_dict)
            
            logger.info(f"Loaded {len(result)} existing rows for {symbol} {timeframe}")
            return result
            
        except Exception as e:
            logger.error(f"Error loading data for {symbol} {timeframe}: {e}")
            return []
    
    def save_bucket_data(self, symbol: str, timeframe: str, bucket_data: Dict[str, Any]) -> bool:
        """Save or update a single bucket's data"""
        upsert_sql = """
        INSERT INTO timeframe_data (
            symbol, timeframe, bucket, total_volume, buy_volume, sell_volume,
            buy_contracts, sell_contracts, open_price, high_price, low_price, close_price,
            delta, max_delta, min_delta, cvd, large_order_count, market_limit_ratio,
            buy_sell_ratio, poc, price_levels, hvns, lvns, imbalances, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (symbol, timeframe, bucket) 
        DO UPDATE SET
            total_volume = EXCLUDED.total_volume,
            buy_volume = EXCLUDED.buy_volume,
            sell_volume = EXCLUDED.sell_volume,
            buy_contracts = EXCLUDED.buy_contracts,
            sell_contracts = EXCLUDED.sell_contracts,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            delta = EXCLUDED.delta,
            max_delta = EXCLUDED.max_delta,
            min_delta = EXCLUDED.min_delta,
            cvd = EXCLUDED.cvd,
            large_order_count = EXCLUDED.large_order_count,
            market_limit_ratio = EXCLUDED.market_limit_ratio,
            buy_sell_ratio = EXCLUDED.buy_sell_ratio,
            poc = EXCLUDED.poc,
            price_levels = EXCLUDED.price_levels,
            hvns = EXCLUDED.hvns,
            lvns = EXCLUDED.lvns,
            imbalances = EXCLUDED.imbalances,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            # Parse JSON fields
            price_levels = json.loads(bucket_data.get('price_levels', '{}'))
            hvns = json.loads(bucket_data.get('hvns', '[]'))
            lvns = json.loads(bucket_data.get('lvns', '[]'))
            imbalances = json.loads(bucket_data.get('imbalances', '{}'))
            
            cursor = self.connection.cursor()
            cursor.execute(upsert_sql, (
                symbol, timeframe, int(bucket_data['bucket']),
                float(bucket_data['total_volume']), float(bucket_data['buy_volume']), 
                float(bucket_data['sell_volume']), int(bucket_data['buy_contracts']),
                int(bucket_data['sell_contracts']), 
                float(bucket_data['open']) if bucket_data['open'] else None,
                float(bucket_data['high']) if bucket_data['high'] else None,
                float(bucket_data['low']) if bucket_data['low'] else None,
                float(bucket_data['close']) if bucket_data['close'] else None,
                float(bucket_data['delta']), float(bucket_data['max_delta']),
                float(bucket_data['min_delta']), float(bucket_data['cvd']),
                int(bucket_data['large_order_count']),
                float(bucket_data['market_limit_ratio']) if bucket_data['market_limit_ratio'] else None,
                float(bucket_data['buy_sell_ratio']), 
                float(bucket_data['poc']) if bucket_data['poc'] else None,
                json.dumps(price_levels), json.dumps(hvns), 
                json.dumps(lvns), json.dumps(imbalances)
            ))
            return True
            
        except Exception as e:
            logger.error(f"Error saving bucket data for {symbol} {timeframe}: {e}")
            return False
    
    def get_latest_bucket(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Get the latest bucket for a symbol/timeframe"""
        query = """
        SELECT symbol, timeframe, bucket, total_volume, buy_volume, sell_volume,
               buy_contracts, sell_contracts, open_price as "open", high_price as "high", 
               low_price as "low", close_price as "close", delta, max_delta, min_delta,
               cvd, large_order_count, market_limit_ratio, buy_sell_ratio, poc,
               price_levels, hvns, lvns, imbalances
        FROM timeframe_data 
        WHERE symbol = %s AND timeframe = %s 
        ORDER BY bucket DESC 
        LIMIT 1
        """
        
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query, (symbol, timeframe))
            row = cursor.fetchone()
            
            if row:
                row_dict = dict(row)
                row_dict['bucket'] = str(row_dict['bucket'])
                # Convert JSON fields to strings
                for json_field in ['price_levels', 'hvns', 'lvns', 'imbalances']:
                    if row_dict[json_field]:
                        row_dict[json_field] = json.dumps(row_dict[json_field])
                return row_dict
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest bucket for {symbol} {timeframe}: {e}")
            return None
    
    def cleanup_old_data(self, symbol: str, timeframe: str, days_to_keep: int = 30):
        """Remove data older than specified days"""
        query = """
        DELETE FROM timeframe_data 
        WHERE symbol = %s AND timeframe = %s 
        AND created_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (symbol, timeframe, days_to_keep))
            deleted_count = cursor.rowcount
            logger.info(f"Cleaned up {deleted_count} old rows for {symbol} {timeframe}")
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return 0

    def save_dom_snapshot(self, symbol: str, timestamp: float, bids_data: str, asks_data: str) -> bool:
        """Save DOM snapshot to database"""
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
            
            cursor = self.connection.cursor()
            cursor.execute(insert_sql, (
                symbol.lower(), timestamp, bids_data, asks_data, 
                levels_count, best_bid, best_ask, spread
            ))
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving DOM snapshot: {e}")
            return False

    def get_dom_snapshots(self, symbol: str, start_timestamp: float, end_timestamp: float, limit: int = 1000):
        """Get DOM snapshots for a time range"""
        query = """
        SELECT timestamp, bids_data, asks_data, best_bid, best_ask, spread, created_at
        FROM dom_snapshots 
        WHERE symbol = %s AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
        LIMIT %s
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (symbol.lower(), start_timestamp, end_timestamp, limit))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching DOM snapshots: {e}")
            return []

    def cleanup_old_dom_data(self, symbol: str, days_to_keep: int = 7):
        """Remove old DOM snapshots (high frequency data, shorter retention)"""
        query = """
        DELETE FROM dom_snapshots 
        WHERE symbol = %s AND created_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (symbol, days_to_keep))
            deleted_count = cursor.rowcount
            logger.info(f"Cleaned up {deleted_count} old DOM snapshots for {symbol}")
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old DOM data: {e}")
            return 0
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


# Global database instance
db = None

def get_db():
    """Get database instance (singleton)"""
    global db
    if db is None:
        db = PostgreSQLDatabase()
    return db
