"""
Configuration module for FHCharts
Handles database connections and environment settings
"""
import os
from typing import Optional

# Database Configuration
class DatabaseConfig:
    """Database connection configuration"""
    
    def __init__(self):
        # Try to get from environment first (for production)
        self.database_url = os.getenv('DATABASE_URL')
        
        # Fallback to environment variables or error (for development)
        if not self.database_url:
            # Try individual environment variables
            db_host = os.getenv('DB_HOST')
            db_user = os.getenv('DB_USER') 
            db_password = os.getenv('DB_PASSWORD')
            db_name = os.getenv('DB_NAME', 'defaultdb')
            db_port = os.getenv('DB_PORT', '25060')
            
            if all([db_host, db_user, db_password]):
                self.database_url = (
                    f"postgresql://{db_user}:{db_password}@{db_host}:"
                    f"{db_port}/{db_name}?sslmode=require"
                )
            else:
                raise ValueError(
                    "DATABASE_URL not found. Set DATABASE_URL environment variable "
                    "or DB_HOST, DB_USER, DB_PASSWORD environment variables"
                )
    
    def get_connection_string(self) -> str:
        """Get the database connection string"""
        return self.database_url
    
    def update_credentials(self, host: str, user: str, password: str, 
                          database: str = "defaultdb", port: int = 25060):
        """Update database credentials"""
        self.database_url = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
        )

# GUI Configuration  
class GUIConfig:
    """GUI display configuration"""
    
    def __init__(self):
        self.default_symbol = "XMRUSDT"
        self.default_timeframe = "1m"
        self.default_limit = 100
        self.refresh_interval = 1000  # milliseconds
        
        # Timeframe-specific bucket loads
        self.timeframe_limits = {
            "1m": 30,
            "3m": 25,
            "5m": 20,
            "15m": 15,
            "30m": 12,
            "1h": 10,
            "4h": 10
        }
        
        # Connection status display
        self.show_connection_status = True
        self.show_last_update = True
        
        # Data fetching
        self.max_retries = 3
        self.retry_delay = 1.0  # seconds
        
        # Chart display
        self.bin_size_map = {
            "1m": 0.05,
            "3m": 0.1,
            "5m": 0.1,
            "15m": 0.2,
            "30m": 0.3,
            "1h": 0.5,
            "4h": 1.0
        }
    
    def get_timeframe_limit(self, timeframe: str) -> int:
        """Get the bucket load limit for a specific timeframe"""
        return self.timeframe_limits.get(timeframe, self.default_limit)

# Global configuration instances
db_config = DatabaseConfig()
gui_config = GUIConfig()

def get_database_url() -> str:
    """Get the current database connection URL"""
    return db_config.get_connection_string()

def update_database_credentials(host: str, user: str, password: str):
    """Update database connection credentials"""
    db_config.update_credentials(host, user, password)
    print(f"✅ Database credentials updated for {user}@{host}")

def test_configuration():
    """Test current configuration"""
    print("🔧 FHCharts Configuration")
    print("=" * 40)
    print(f"Database URL: {db_config.get_connection_string()[:50]}...")
    print(f"Default Symbol: {gui_config.default_symbol}")
    print(f"Default Timeframe: {gui_config.default_timeframe}")
    print(f"Refresh Interval: {gui_config.refresh_interval}ms")
    print(f"Connection Status Display: {gui_config.show_connection_status}")
    print("\nTimeframe Bucket Limits:")
    for tf, limit in gui_config.timeframe_limits.items():
        print(f"  {tf}: {limit} buckets")

if __name__ == "__main__":
    test_configuration()
