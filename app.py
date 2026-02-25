# Main entry point

from flask import Flask, jsonify, request
from flask_cors import CORS
import logging

# Import configuration
from config.database import CONN_STR, init_app as init_db

# Import route registration
from routes import register_routes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    """Application factory pattern"""
    app = Flask(__name__)
    
    # Enable CORS
    CORS(app)
    
    # Initialize database
    init_db(app)
    
    # Add after_request handler for CORS
    @app.after_request
    def after_request(response):
        """Add CORS headers to every response"""
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        return response
    
    # Register all routes
    register_routes(app)
    
    return app

# Create the app instance
app = create_app()

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Starting SmartGold ERP API Server v2.0")
    print("=" * 60)
    print("📍 Server will run on: http://0.0.0.0:8000")
    print("\n📋 AVAILABLE ENDPOINTS:")
    print("  ────────────────────────────────────────────")
    print("  🔧 AUTHENTICATION ENDPOINTS:")
    print("    POST /GetMenu       - Get user menu")
    print("\n  💰 VOUCHER ENDPOINTS:")
    print("    GET  /GetVno        - Get voucher number")
    print("    POST /FillTable     - Fill table data")
    print("    POST /gl_voucher_generation_status - Update GL voucher status")
    print("    POST /gl_Posting    - GL Posting")
    print("    POST /stk_Posting   - Stock Posting")
    print("\n  📊 TABLE STRUCTURE ENDPOINTS:")
    print("    POST /get-table-headers - Get column headers")
    print("    POST /get-table-structure - Get table structure")
    print("    POST /get-table-relationships - Get relationships")
    print("    POST /get-table-data - Get paginated data")
    print("    GET  /debug/table-structure/<table_name> - Debug table structure")
    print("    GET  /check-table/<table_name> - Check table")
    print("\n  👔 EMPLOYEE MANAGEMENT:")
    print("    📋 POST /insert-EmployeeHeadDet - Insert employee with all details")
    print("\n  ✨ GENERIC CRUD ENDPOINTS:")
    print("    📥 POST /table/insert  - Generic INSERT for any table")
    print("    📝 POST /table/update  - Generic UPDATE for any table")
    print("    🔄 POST /table/upsert  - Generic UPSERT")
    print("    🗑️  POST /table/delete  - Generic DELETE")
    print("    📦 POST /table/bulk-insert - Bulk insert multiple records")
    print("\n  🖥️  SCREEN CONFIGURATION:")
    print("    POST /screen/get-config")
    print("    POST /screen/document-statuses")
    print("    POST /screen/menu-permissions")
    print("    POST /screen/update-employment-status")
    print("    POST /screen/refresh-table-data")
    print("\n  ❤️  HEALTH ENDPOINT:")
    print("    GET  /health        - Health check")
    print("    GET  /              - API Information")
    print("=" * 60)
    
    app.run(host="0.0.0.0", port=8000, debug=True)