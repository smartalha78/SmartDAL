# routes/main_routes.py
from flask import jsonify, request
from config.database import CONN_STR
from utils.jwt_helper import token_required
import pyodbc
from datetime import datetime

def register_main_routes(app):
    """Register main routes directly with the app"""
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint - PUBLIC (no token required)"""
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
        """Root endpoint with API information - PUBLIC (no token required)"""
        return jsonify({
            "api": "SmartGold ERP API",
            "version": "2.0",
            "timestamp": datetime.now().isoformat(),
            "authentication": {
                "type": "JWT Bearer Token",
                "login_endpoint": "/GetMenu",
                "token_refresh": "/refresh-token",
                "instructions": "Include token in Authorization header: Bearer <your_token>"
            },
            "endpoints": {
                "auth": {
                    "/GetMenu": "POST - Login and get JWT token",
                    "/refresh-token": "POST - Refresh JWT token (requires token)"
                },
                "employee": {
                    "/insert-EmployeeHeadDet": "POST - Insert employee (requires token)"
                },
                "generic_crud": {
                    "/table/insert": "POST - Generic insert (requires token)",
                    "/table/update": "POST - Generic update (requires token)",
                    "/table/upsert": "POST - Generic upsert (requires token)",
                    "/table/delete": "POST - Generic delete (requires token)",
                    "/table/bulk-insert": "POST - Bulk insert (requires token)",
                    "/insert-SThead-det": "POST - Insert shift (requires token)",
                    "/update-SThead-det": "POST - Update shift (requires token)"
                },
                "health": "/health - Health check (public)"
            }
        }), 200

    @app.route('/verify-token', methods=['GET'])
    @token_required
    def verify_token():
        """Verify if current token is valid - PROTECTED (requires token)"""
        user = request.current_user
        return jsonify({
            "success": True,
            "message": "Token is valid",
            "user": {
                "username": user.get('username'),
                "user_id": user.get('user_id'),
                "expires_at": datetime.fromtimestamp(user.get('exp')).isoformat() if user.get('exp') else None
            }
        }), 200

    @app.route('/debug/routes', methods=['GET'])
    @token_required
    def debug_routes():
        """Debug endpoint to list all routes - PROTECTED (requires token)"""
        routes = []
        for rule in app.url_map.iter_rules():
            if rule.endpoint != 'static':
                routes.append({
                    "endpoint": rule.endpoint,
                    "methods": [m for m in rule.methods if m not in ['OPTIONS', 'HEAD']],
                    "url": str(rule)
                })
        return jsonify({
            "total_routes": len(routes),
            "routes": routes
        }), 200

    # Remove the @app.after_request handler since CORS is already configured in app.py
    # If you need to keep it for any reason, uncomment and modify below:
    # @app.after_request
    # def after_request(response):
    #     """Add CORS headers to every response"""
    #     response.headers.add('Access-Control-Allow-Origin', '*')
    #     response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    #     response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    #     return response