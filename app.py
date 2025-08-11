"""
Main application for DigitalOcean App Platform with Main Worker + DOM Capture
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

def start_health_server():
    """Start health check server in background thread"""
    server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
    server.serve_forever()

async def run_main_worker():
    """Run the main worker in async context"""
    try:
        # Import inside the function to avoid import-time issues
        from worker_do import main
        logger.info("Starting main worker...")
        await main()
    except ImportError as e:
        logger.error(f"Main worker import failed (DATABASE_URL required): {e}")
        raise
    except Exception as e:
        logger.error(f"Main worker failed: {e}")
        raise

async def run_dom_capture():
    """Run DOM capture in async context"""
    try:
        # Import inside the function to avoid import-time issues  
        from orderbook_do import run_orderbook_manager
        logger.info("Starting DOM capture...")
        await run_orderbook_manager()
    except ImportError as e:
        logger.error(f"DOM capture import failed: {e}")
        raise
    except Exception as e:
        logger.error(f"DOM capture failed: {e}")
        raise

async def run_all_services():
    """Run both main worker and DOM capture concurrently"""
    tasks = [
        asyncio.create_task(run_main_worker()),
        asyncio.create_task(run_dom_capture())
    ]
    
    # Run both services concurrently
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Services failed: {e}")

if __name__ == '__main__':
    logger.info("Starting DigitalOcean App with Main Worker + DOM Capture")
    
    # Start health check server in background
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    logger.info("Health check server started on port 8080")
    
    # Start both services
    try:
        asyncio.run(run_all_services())
    except KeyboardInterrupt:
        logger.info("Shutting down services...")
    except Exception as e:
        logger.error(f"Failed to start services: {e}")
        
        # Keep health server running for debugging
        logger.info("Keeping health server running...")
        while True:
            time.sleep(60)
