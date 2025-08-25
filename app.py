from dotenv import load_dotenv
load_dotenv()
"""
Main application for DigitalOcean App Platform with Main Worker + DOM Capture
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
from db_postgres import get_db
import logging
import time
import json
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            try:
                db = get_db()
                cursor = db.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                response = {
                    "status": "healthy",
                    "timestamp": int(time.time()),
                    "database": "connected"
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
    logger.info("Health check server started on port 8080")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Health check server stopped.")

async def run_main_worker():
    # Placeholder for main worker logic
    while True:
        await asyncio.sleep(60)

async def run_all_services():
    """Run only the main worker (DOM capture removed)"""
    tasks = [
        asyncio.create_task(run_main_worker())
    ]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Services failed: {e}")

if __name__ == '__main__':
    # Start the health check server in a separate thread
    import threading
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    # Start the main worker service
    try:
        asyncio.run(run_all_services())
    except KeyboardInterrupt:
        logger.info("Shutting down application.")
