"""
DOM Database Worker - Handles saving DOM snapshots to database
"""
import logging
import time
from typing import Optional
from db_client_new import get_database_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DOMDatabaseWorker:
    """Worker class to handle DOM snapshot database operations"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.db_client = get_database_client()
        self.enabled = True
        self.last_save_time = 0
        self.save_count = 0
        self.error_count = 0
        
        # Test database connection
        if not self.db_client.test_connection():
            logger.warning("❌ Database connection failed - DOM snapshots will be disabled")
            self.enabled = False
    
    def save_snapshot(self, timestamp: float, agg_bids: dict, agg_asks: dict) -> bool:
        """Save DOM snapshot to database with top 50 levels"""
        if not self.enabled:
            return False
            
        try:
            # Get top 50 levels for each side
            top_bids = dict(sorted(agg_bids.items(), reverse=True)[:50])  # Top 50 bids (highest prices)
            top_asks = dict(sorted(agg_asks.items())[:50])               # Top 50 asks (lowest prices)
            
            # Create compact depth strings for top 50 only
            bid_str = ",".join([f"{price}:{qty}" for price, qty in sorted(top_bids.items(), reverse=True)])
            ask_str = ",".join([f"{price}:{qty}" for price, qty in sorted(top_asks.items())])
            
            # Save to database
            success = self.db_client.save_dom_snapshot(
                symbol=self.symbol,
                timestamp=timestamp,
                bids_data=bid_str,
                asks_data=ask_str
            )
            
            if success:
                self.save_count += 1
                self.last_save_time = timestamp
                
                # Log progress every 100 saves
                if self.save_count % 100 == 0:
                    logger.info(f"✅ DOM snapshots saved: {self.save_count} for {self.symbol}")
            else:
                self.error_count += 1
                if self.error_count % 10 == 0:
                    logger.warning(f"❌ DOM save errors: {self.error_count} for {self.symbol}")
            
            return success
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"❌ Error saving DOM snapshot for {self.symbol}: {e}")
            return False
    
    def get_stats(self) -> dict:
        """Get statistics about DOM saving"""
        return {
            'symbol': self.symbol,
            'enabled': self.enabled,
            'save_count': self.save_count,
            'error_count': self.error_count,
            'last_save_time': self.last_save_time,
            'success_rate': (self.save_count / max(1, self.save_count + self.error_count)) * 100
        }
    
    def cleanup_old_data(self, days_to_keep: int = 7) -> int:
        """Clean up old DOM snapshots"""
        if not self.enabled:
            return 0
            
        try:
            deleted_count = self.db_client.cleanup_old_dom_data(self.symbol, days_to_keep)
            logger.info(f"🧹 Cleaned up {deleted_count} old DOM snapshots for {self.symbol}")
            return deleted_count
        except Exception as e:
            logger.error(f"❌ Error cleaning up DOM data for {self.symbol}: {e}")
            return 0
    
    def test_connection(self) -> bool:
        """Test database connection"""
        return self.db_client.test_connection()
    
    def disable(self):
        """Disable DOM saving"""
        self.enabled = False
        logger.info(f"📴 DOM database saving disabled for {self.symbol}")
    
    def enable(self):
        """Enable DOM saving"""
        if self.test_connection():
            self.enabled = True
            logger.info(f"✅ DOM database saving enabled for {self.symbol}")
        else:
            logger.warning(f"❌ Cannot enable DOM saving - database connection failed for {self.symbol}")

# Test function
if __name__ == "__main__":
    import time
    
    # Test DOM worker
    worker = DOMDatabaseWorker("XMRUSDT")
    
    if worker.enabled:
        # Test data
        test_bids = {275.88: 10.5, 275.87: 5.2, 275.86: 15.3}
        test_asks = {275.89: 8.1, 275.90: 12.4, 275.91: 6.7}
        
        print("Testing DOM database saving...")
        success = worker.save_snapshot(time.time(), test_bids, test_asks)
        print(f"Save result: {success}")
        print(f"Stats: {worker.get_stats()}")
    else:
        print("DOM worker disabled - check database connection")
