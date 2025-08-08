"""
Health check endpoint for DigitalOcean App Platform
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
import time
from db_postgres import get_db

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
    server.serve_forever()

if __name__ == '__main__':
    # Start health check server in background
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    # Import and run main worker
    from worker_do import main
    import asyncio
    asyncio.run(main())
