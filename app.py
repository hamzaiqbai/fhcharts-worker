"""
Main application for DigitalOcean App Platform with DOM capture
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
import time
import asyncio
import logging
from db_postgres import get_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            try:
                # Check database connectivity
                db = get_db()
                # Simple query to test connection
                cursor = db.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
                # Check DOM snapshots count
                cursor.execute("SELECT COUNT(*) FROM dom_snapshots")
                dom_count = cursor.fetchone()[0]
                
                response = {
                    "status": "healthy",
                    "timestamp": int(time.time()),
                    "database": "connected",
                    "dom_snapshots": dom_count
                }
                self.send_response(200)
            except Exception as e:
                response = {
                    "status": "unhealthy",
                    "timestamp": int(time.time()),
                    "database": "disconnected",
                    "error": str(e)
                }
                self.send_response(500)
            
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

def start_dom_capture():
    """Start DOM capture service"""
    try:
        logger.info("🚀 Starting DOM capture service...")
        
        # Import DOM worker
        from dom_database_worker import DOMDatabaseWorker
        import time
        
        # Test DOM worker
        worker = DOMDatabaseWorker("XMRUSDT")
        if worker.enabled:
            logger.info("✅ DOM worker enabled")
            
            # Save a test snapshot to verify it works
            test_bids = {275.50: 10.0}
            test_asks = {275.51: 8.0}
            success = worker.save_snapshot(time.time(), test_bids, test_asks)
            logger.info(f"📊 Test DOM snapshot: {success}")
            
            # Try to start headless capture
            try:
                from headless_dom_capture import HeadlessDOMCapture
                capture = HeadlessDOMCapture("XMRUSDT")
                asyncio.run(capture.start())
            except ImportError:
                logger.warning("⚠️ Headless capture not available, using worker only")
                
        else:
            logger.error("❌ DOM worker not enabled")
            
    except Exception as e:
        logger.error(f"❌ DOM capture error: {e}")

def start_health_server():
    """Start health check server in background thread"""
    server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
    server.serve_forever()

if __name__ == '__main__':
    logger.info("🎯 Starting DigitalOcean App with DOM capture")
    
    # Start health check server in background
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    logger.info("✅ Health check server started on port 8080")
    
    # Start DOM capture
    try:
        start_dom_capture()
    except Exception as e:
        logger.error(f"❌ Failed to start DOM capture: {e}")
        
        # Fallback to original worker
        logger.info("🔄 Falling back to original worker...")
        try:
            from worker_do import main
            asyncio.run(main())
        except Exception as worker_error:
            logger.error(f"❌ Worker also failed: {worker_error}")
            
            # Keep health server running
            logger.info("🏥 Keeping health server running...")
            while True:
                time.sleep(60)
