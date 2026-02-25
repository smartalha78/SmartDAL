# Route registration
# This file makes the routes directory a Python package

from flask import Blueprint, jsonify
from flask_cors import CORS

# Create blueprints for each route module
auth_bp = Blueprint('auth', __name__)
generic_crud_bp = Blueprint('generic_crud', __name__)
employee_bp = Blueprint('employee', __name__)
screen_bp = Blueprint('screen', __name__)
table_bp = Blueprint('table', __name__)
voucher_bp = Blueprint('voucher', __name__)

# Import routes after blueprint creation to avoid circular imports
# These imports will register the routes with the blueprints
from . import auth_routes
from . import generic_crud_routes
from . import employee_routes
from . import screen_routes
from . import table_routes
from . import voucher_routes

def register_routes(app):
    """Register all blueprints with the app"""
    app.register_blueprint(auth_bp)
    app.register_blueprint(generic_crud_bp)
    app.register_blueprint(employee_bp)
    app.register_blueprint(screen_bp)
    app.register_blueprint(table_bp)
    app.register_blueprint(voucher_bp)
    
    # Also register the main index and health routes
    @app.route('/', methods=['GET'])
    def index():
        return jsonify({
            "api": "SmartGold ERP API",
            "version": "2.0",
            "endpoints": {
                "/GetMenu": "POST - Get user menu with credentials",
                "/GetVno": "GET - Get voucher number",
                "/FillTable": "POST - Fill table data",
                "/gl_voucher_generation_status": "POST - Update GL voucher status",
                "/get-table-headers": "POST - Get table column headers with sample data",
                "/get-table-structure": "POST - Get detailed table structure",
                "/get-table-relationships": "POST - Get foreign key relationships",
                "/get-table-data": "POST - Get paginated table data with filtering",
                "/insert-EmployeeHeadDet": "POST - Insert employee with all related details",
                "/table/insert": "POST - Generic INSERT for any table",
                "/table/update": "POST - Generic UPDATE for any table",
                "/table/upsert": "POST - Generic INSERT or UPDATE",
                "/table/delete": "POST - Generic DELETE with WHERE conditions",
                "/table/bulk-insert": "POST - Bulk insert multiple records",
                "/health": "GET - Health check"
            },
            "status": "running"
        }), 200
    
    @app.route('/health', methods=['GET'])
    def health_check():
        try:
            from config.database import CONN_STR
            import pyodbc
            conn = pyodbc.connect(CONN_STR)
            conn.close()
            return jsonify({
                "status": "healthy",
                "database": "connected",
                "server": "running"
            }), 200
        except Exception as ex:
            return jsonify({
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(ex)
            }), 500