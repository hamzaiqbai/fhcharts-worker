"""
Database module for FHCharts - handles CSV data persistence and loading
"""
import csv
import os
from typing import List, Dict, Any


class CSVDatabase:
    """Handles CSV file operations for timeframe data"""
    
    def __init__(self, base_path: str = "."):
        self.base_path = base_path
        self.csv_fields = [
            'bucket','total_volume','buy_volume','sell_volume','buy_contracts','sell_contracts',
            'open','high','low','close','delta','max_delta','min_delta','CVD', 'large_order_count', 
            'market_limit_ratio','buy_sell_ratio','poc','price_levels','hvns','lvns','imbalances'
        ]
    
    def load_timeframe_data(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        """Load existing CSV data for a specific symbol and timeframe"""
        filename = f"{symbol}_{timeframe}.csv"
        filepath = os.path.join(self.base_path, filename)
        
        if not os.path.exists(filepath):
            print(f"No existing data found for {symbol} {timeframe}")
            return []
        
        try:
            with open(filepath, 'r', newline='') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                print(f"Loaded {len(rows)} existing rows for {symbol} {timeframe}")
                return rows
        except Exception as e:
            print(f"Error loading data for {symbol} {timeframe}: {e}")
            return []
    
    def save_timeframe_data(self, symbol: str, timeframe: str, data: List[Dict[str, Any]]) -> bool:
        """Save data to CSV file for a specific symbol and timeframe"""
        filename = f"{symbol}_{timeframe}.csv"
        filepath = os.path.join(self.base_path, filename)
        temp_filepath = filepath + ".tmp"
        
        try:
            # Write to temporary file first
            with open(temp_filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_fields)
                writer.writeheader()
                writer.writerows(data)
            
            # Atomic replace
            for _ in range(5):
                try:
                    os.replace(temp_filepath, filepath)
                    return True
                except OSError:
                    # File might be locked, retry
                    import time
                    time.sleep(0.1)
            
            # Fallback to direct write
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_fields)
                writer.writeheader()
                writer.writerows(data)
            return True
            
        except Exception as e:
            print(f"Error saving data for {symbol} {timeframe}: {e}")
            return False
    
    def append_data(self, symbol: str, timeframe: str, new_rows: List[Dict[str, Any]]) -> bool:
        """Append new rows to existing CSV file"""
        existing_data = self.load_timeframe_data(symbol, timeframe)
        combined_data = existing_data + new_rows
        return self.save_timeframe_data(symbol, timeframe, combined_data)
    
    def get_latest_timestamp(self, symbol: str, timeframe: str) -> int:
        """Get the latest timestamp from existing data"""
        data = self.load_timeframe_data(symbol, timeframe)
        if not data:
            return 0
        
        try:
            # Assuming bucket field contains timestamp
            latest_bucket = max(int(float(row['bucket'])) for row in data if row['bucket'])
            return latest_bucket
        except (ValueError, KeyError):
            return 0
    
    def clean_old_data(self, symbol: str, timeframe: str, days_to_keep: int = 30):
        """Remove data older than specified days"""
        import time
        cutoff_timestamp = int(time.time() - (days_to_keep * 24 * 60 * 60)) * 1000
        
        data = self.load_timeframe_data(symbol, timeframe)
        if not data:
            return
        
        filtered_data = []
        for row in data:
            try:
                bucket_ts = int(float(row['bucket']))
                if bucket_ts >= cutoff_timestamp:
                    filtered_data.append(row)
            except (ValueError, KeyError):
                # Keep rows with invalid timestamps
                filtered_data.append(row)
        
        if len(filtered_data) < len(data):
            print(f"Cleaned {len(data) - len(filtered_data)} old rows from {symbol} {timeframe}")
            self.save_timeframe_data(symbol, timeframe, filtered_data)


# Example usage
if __name__ == "__main__":
    db = CSVDatabase()
    
    # Load existing data
    data = db.load_timeframe_data("xmrusdt", "1m")
    print(f"Found {len(data)} rows")
    
    # Get latest timestamp
    latest = db.get_latest_timestamp("xmrusdt", "1m")
    print(f"Latest timestamp: {latest}")
