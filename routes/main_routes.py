# routes/main_routes.py
from flask import jsonify
from config.database import CONN_STR
import pyodbc
from datetime import datetime

def register_main_routes(app):
    """Register main routes directly with the app"""
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint"""
        try:
            conn = pyodbc.connect(CONN_STR)
            conn.close()
            return jsonify({
                "status": "healthy",
                "database": "connected",
                "server": "running",
                "timestamp": datetime.now().isoformat()
            }), 200
        except Exception as ex:
            return jsonify({
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(ex)
            }), 500

    @app.route('/', methods=['GET'])
    def index():
        """Root endpoint with API information"""
        return jsonify({
            "api": "SmartGold ERP API",
            "version": "2.0",
            "timestamp": datetime.now().isoformat(),
            "endpoints": {
                "auth": {"/GetMenu": "POST - Get user menu"},
                "health": "/health - Health check"
            }
        }), 200

    @app.after_request
    def after_request(response):
        """Add CORS headers to every response"""
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        return response