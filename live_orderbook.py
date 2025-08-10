import sys
import json
import asyncio
import requests
import websockets
import math
import time
import csv
import os
from datetime import datetime


from typing import Dict, List, Optional, Callable

from PyQt5.QtCore import Qt, QThread, pyqtSignal, pyqtSlot, QSize
from PyQt5.QtGui import QColor, QPainter, QFont
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QScrollArea,
    QVBoxLayout,
    QHBoxLayout,
    QComboBox,
)

# Import DOM database worker
try:
    from dom_database_worker import DOMDatabaseWorker
    DOM_DATABASE_AVAILABLE = True
except ImportError:
    print("⚠️ DOM database worker not available - will save to CSV only")
    DOM_DATABASE_AVAILABLE = False


# ------------------------------------------------------------
# 1)  Async order‑book logic (same sequencing rules as before)
# ------------------------------------------------------------

class OrderBookManager:
    """Maintains a live order book and calls the callback every time it changes."""

    def __init__(self, symbol: str, on_update: Callable[[dict], None], tick_size: float = 0.01, enable_database: bool = True):
        self.symbol = symbol.upper()
        self.on_update = on_update  # push book to GUI
        self.tick_size = tick_size

        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: Optional[int] = None
        self.previous_final_update_id: Optional[int] = None

        self.ws_url = f"wss://fstream.binance.com/stream?streams={symbol.lower()}@depth"
        self.snapshot_url = (
            f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
        )
        
        # Initialize DOM database worker
        self.enable_database = enable_database and DOM_DATABASE_AVAILABLE
        self.dom_worker = None
        
        if self.enable_database:
            try:
                self.dom_worker = DOMDatabaseWorker(symbol)
                if self.dom_worker.enabled:
                    print(f"✅ DOM database saving enabled for {symbol}")
                else:
                    print(f"❌ DOM database saving disabled for {symbol}")
                    self.enable_database = False
            except Exception as e:
                print(f"❌ Failed to initialize DOM database worker: {e}")
                self.enable_database = False
        else:
            print(f"📄 DOM will save to CSV only for {symbol}")

    # ---------- public API ----------

    async def start(self):
        await self._get_snapshot()
        await self._stream_loop()

    # ---------- private ----------

    async def _get_snapshot(self):
        resp = requests.get(self.snapshot_url, timeout=5)
        snap = resp.json()
        self.last_update_id = snap["lastUpdateId"]
        self.bids = {float(p): float(q) for p, q in snap["bids"]}
        self.asks = {float(p): float(q) for p, q in snap["asks"]}
        self._emit()

    def _update_book(self, bids: List[List[str]], asks: List[List[str]]):
        for p, q in bids:
            price, qty = float(p), float(q)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for p, q in asks:
            price, qty = float(p), float(q)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

    def _aggregate(self, book: Dict[float,float]) -> Dict[float,float]:
        """Bin raw price→qty into self.tick_size buckets."""
        if not self.tick_size:
            return book
        t = self.tick_size
        out: Dict[float,float] = {}
        for price, qty in book.items():
            # bids round down, asks round up
            b = math.floor(price/t)*t if book is self.bids else math.ceil(price/t)*t
            out[b] = out.get(b, 0) + qty
        return out

    def _emit(self):
        # first aggregate into buckets if requested
        agg_bids = self._aggregate(self.bids)
        agg_asks = self._aggregate(self.asks)
        
        # Save snapshot to database (top 50 levels) if enabled
        if self.enable_database and self.dom_worker:
            timestamp = time.time()
            self.dom_worker.save_snapshot(timestamp, agg_bids, agg_asks)
        
        # Save snapshot to CSV (optional, can be disabled)
        # self._save_dom_snapshot()
        
        payload = {
            "tick_size": self.tick_size,                # include for GUI if you like
            "bids": sorted(agg_bids.items(), reverse=True),
            "asks": sorted(agg_asks.items()),
            "tick_size": self.tick_size,
        }
        self.on_update(payload)

    def _save_dom_snapshot(self):
        """Save a complete DOM snapshot at 0.01 tick size to CSV file"""
        timestamp = time.time()
        dt = datetime.fromtimestamp(timestamp)
        
        # Force 0.01 tick size for snapshot regardless of current display tick size
        snapshot_tick_size = 0.01
        temp_tick_size = self.tick_size
        self.tick_size = snapshot_tick_size
        
        # Get aggregated data at 0.01 tick size
        agg_bids = self._aggregate(self.bids)
        agg_asks = self._aggregate(self.asks)
        
        # Restore original tick size
        self.tick_size = temp_tick_size
        
        # Create snapshot directory if it doesn't exist
        snapshot_dir = "dom_snapshots"
        os.makedirs(snapshot_dir, exist_ok=True)
        
        # Choose format: 'csv' or 'json'
        format_type = 'json'  # Change this to switch formats
        
        if format_type == 'csv':
            self._save_csv_format(timestamp, dt, agg_bids, agg_asks, snapshot_dir)
        else:
            self._save_json_format(timestamp, dt, agg_bids, agg_asks, snapshot_dir)

    def _save_csv_format(self, timestamp, dt, agg_bids, agg_asks, snapshot_dir):
        """Save in CSV format (original method) - TOP 50 LEVELS ONLY"""
        filename = f"{snapshot_dir}/{self.symbol}_dom_{dt.strftime('%Y%m%d')}.csv"
        
        # Get top 50 levels for each side
        top_bids = dict(sorted(agg_bids.items(), reverse=True)[:50])  # Top 50 bids (highest prices)
        top_asks = dict(sorted(agg_asks.items())[:50])               # Top 50 asks (lowest prices)
        
        file_exists = os.path.exists(filename)
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            if not file_exists:
                writer.writerow(['timestamp', 'datetime', 'side', 'price', 'quantity'])
            
            # Write only top 50 bid levels
            for price, qty in sorted(top_bids.items(), reverse=True):
                writer.writerow([timestamp, dt.isoformat(), 'bid', price, qty])
            
            # Write only top 50 ask levels  
            for price, qty in sorted(top_asks.items()):
                writer.writerow([timestamp, dt.isoformat(), 'ask', price, qty])

    def _save_json_format(self, timestamp, dt, agg_bids, agg_asks, snapshot_dir):
        """Save in CSV format with timestamp and snapshot columns - TOP 50 LEVELS ONLY"""
        filename = f"{snapshot_dir}/{self.symbol}_dom_{dt.strftime('%Y%m%d')}.csv"
        
        # Get top 50 levels for each side
        top_bids = dict(sorted(agg_bids.items(), reverse=True)[:50])  # Top 50 bids (highest prices)
        top_asks = dict(sorted(agg_asks.items())[:50])               # Top 50 asks (lowest prices)
        
        # Create compact depth strings for top 50 only
        bid_str = ",".join([f"{price}:{qty}" for price, qty in sorted(top_bids.items(), reverse=True)])
        ask_str = ",".join([f"{price}:{qty}" for price, qty in sorted(top_asks.items())])
        
        # Create snapshot as JSON string
        snapshot_json = json.dumps({
            "bids": bid_str,
            "asks": ask_str
        })
        
        # Write to CSV with 2 columns: timestamp, snapshot
        file_exists = os.path.exists(filename)
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header if file is new
            if not file_exists:
                writer.writerow(['timestamp', 'snapshot'])
            
            # Write timestamp and snapshot JSON as string
            writer.writerow([timestamp, snapshot_json])

    async def _stream_loop(self):
        while True:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    max_queue=None,
                ) as ws:
                    async for raw in ws:
                        data = json.loads(raw)["data"]
                        if data["u"] <= self.last_update_id:
                            continue
                        if self.previous_final_update_id is None:
                            if not (data["U"] <= self.last_update_id + 1 <= data["u"]):
                                await self._get_snapshot();
                                continue
                        elif data["pu"] != self.previous_final_update_id:
                            await self._get_snapshot();
                            continue
                        self._update_book(data["b"], data["a"])
                        self.previous_final_update_id = data["u"]
                        self._emit()
            except Exception as e:
                # Any error – short back‑off and resubscribe
                await asyncio.sleep(1)
                self.previous_final_update_id = None

    def get_dom_stats(self) -> dict:
        """Get DOM database statistics"""
        if self.enable_database and self.dom_worker:
            return self.dom_worker.get_stats()
        return {"enabled": False, "reason": "Database not available"}

    def cleanup_old_dom_data(self, days_to_keep: int = 7) -> int:
        """Clean up old DOM data"""
        if self.enable_database and self.dom_worker:
            return self.dom_worker.cleanup_old_data(days_to_keep)
        return 0

# ------------------------------------------------------------
# 2)  Qt worker thread that runs the asyncio loop
# ------------------------------------------------------------

class OrderBookWorker(QThread):
    book_updated = pyqtSignal(dict)

    def __init__(self, symbol: str):
        super().__init__()
        self.symbol    = symbol
        self._tick_size = 0.01
        self.mgr       = None

    @pyqtSlot(float)
    def set_tick_size(self, t: float):
        """GUI calls this to tell us which bucket size to use."""
        self._tick_size = t
        # if we've already spun up the manager, update it on the fly:
        if self.mgr is not None:
            self.mgr.tick_size = t
            # force a new payload
            self.mgr._emit()
    # Qt calls this in the new thread context
    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.mgr = OrderBookManager(self.symbol, self.book_updated.emit, tick_size=self._tick_size, enable_database=True)

        loop.run_until_complete(self.mgr.start())

# ------------------------------------------------------------
# 3)  GUI: custom widget that paints bars & prices
# ------------------------------------------------------------

class PricePanel(QWidget):
    ROW_H = 18
    FONT = QFont("Inter", 8)

    def __init__(self):
        super().__init__()
        self.bids = []  # list[(price, qty)]
        self.asks = []
        self.bid_map = {}       # price ➜ qty for bids
        self.ask_map = {}       # price ➜ qty for asks
        self.prices  = []       # keeps our fixed ladder order
        self.tick  = 0.01          # current “price‑level” size
        self.raw_bids = []         # un‑aggregated lists are kept here
        self.raw_asks = []
        self.setMinimumWidth(280)
        self.setFont(self.FONT)
        self.setStyleSheet("background-color: #111; color: black;")
        

        # ---------- API called from MainWindow ----------
    def update_book(self, payload):
        self.tick = payload.get("tick_size", self.tick)  # optional
        # raw lists are already aggregated
        self.bid_map = {p:q for p,q in payload["bids"]}
        self.ask_map = {p:q for p,q in payload["asks"]}
        self.prices  = sorted(self.bid_map | self.ask_map, reverse=True)
        self.setMinimumHeight(len(self.prices)*self.ROW_H)
        self.update()


    # ---------- painting ----------
    def set_tick_size(self, tick: float):
        if tick != self.tick:
            self.tick = tick
            self._rebuild_ladder()     # redraw with new bucket width

    def _rebuild_ladder(self):
        # rebuild our internal ladder layout using the current tick:
        # simply re-emit the last book payload so update_book will re-aggregate and repaint
        payload = {
            "tick_size": self.tick,
            "bids": list(self.bid_map.items()),
            "asks": list(self.ask_map.items()),
        }
        self.update_book(payload)



    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setPen(Qt.black)
        w = self.width()
        row_h = self.ROW_H          # height of each ladder row


        # --- geometry constants (left‑aligned ladder) ---
        price_x      = 10            # left margin for price labels
        price_width  = 60            # horizontal space reserved for “337.40”
        bar_start_x  = price_x + price_width + 4   # 4‑px gap after the price text
        max_bar_px   = w - bar_start_x - 10        # keep 10‑px right padding

        # one global scale for both colours
        max_val = max(
            max(self.bid_map.values(), default=1),
            max(self.ask_map.values(), default=1),
        )



        for i, price in enumerate(self.prices):
            y = i * row_h

            bid_qty = self.bid_map.get(price, 0)
            ask_qty = self.ask_map.get(price, 0)

            # ---- bids (now right) ----
            if bid_qty:
                bar_w = int(bid_qty / max_val * max_bar_px)
                painter.fillRect(bar_start_x, y + 2, bar_w, row_h - 4, QColor(0, 160, 0))
                painter.drawText(bar_start_x + bar_w + 4, y + row_h - 4, f"{bid_qty:.0f}")




            # ---- asks (right) ----
            if ask_qty:
                bar_w = int(ask_qty / max_val * max_bar_px)
                painter.fillRect(bar_start_x, y + 2, bar_w, row_h - 4, QColor(180, 0, 0))
                painter.drawText(bar_start_x + bar_w + 4, y + row_h - 4, f"{ask_qty:.0f}")



            # price text (always shown)
            painter.drawText(price_x, y + row_h - 4, f"{price:.2f}")


        painter.end()

# ------------------------------------------------------------
# 4)  Main window wiring everything together
# ------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self, symbol: str):
        super().__init__()
        self.setWindowTitle(f"{symbol} Order Book")
        self.resize(360, 600)

        # 1)  price panel (the ladder widget)
        self.panel = PricePanel()
        scroll = QScrollArea()
        scroll.setWidget(self.panel)
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        # 2)  tick‑size selector (drop‑down)  <<-- create first
        top_bar = QHBoxLayout()
        self.tick_combo = QComboBox()
        for txt in ["0.01", "0.05", "0.10", "0.20", "0.25", "0.50", "1.00", "2.50", "5.00", "10.00"]:
            self.tick_combo.addItem(txt)
        self.tick_combo.setCurrentIndex(0)
        self.tick_combo.currentTextChanged.connect(
            lambda v: self.panel.set_tick_size(float(v))
        )
        top_bar.addWidget(self.tick_combo)

        # 3)  assemble the main vertical layout  <<-- now we can reference top_bar
        container = QWidget()
        lay = QVBoxLayout(container)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.addLayout(top_bar)      # safe: top_bar already exists
        lay.addWidget(scroll)
        self.setCentralWidget(container)

        # ① construct with only the symbol
        self.worker = OrderBookWorker(symbol)
        # ② immediately tell it your initial tick‐size
        self.worker.set_tick_size(0.01)
        # ③ hook up its updates to the price panel
        self.worker.book_updated.connect(self.panel.update_book)
        # ④ whenever the combo changes, update tick‐size in the worker
        self.tick_combo.currentTextChanged.connect(
            lambda v: self.worker.set_tick_size(float(v))
        )
        # ⑤ finally, start the worker thread
        self.worker.start()


    def closeEvent(self, event):
        # ensure worker thread stops when window closes
        self.worker.quit()
        self.worker.wait()
        event.accept()

# ------------------------------------------------------------

if __name__ == "__main__":
    app = QApplication(sys.argv)
    mw = MainWindow("XMRUSDT")
    mw.show()
    sys.exit(app.exec_())
