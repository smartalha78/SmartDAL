# Route registration
# This file makes the routes directory a Python package

from flask import Blueprint, jsonify
from flask_cors import CORS

print("🔍 Loading routes/__init__.py")

# Create blueprints for each route module
auth_bp = Blueprint('auth', __name__)
generic_crud_bp = Blueprint('generic_crud', __name__)
employee_bp = Blueprint('employee', __name__)
screen_bp = Blueprint('screen', __name__)
table_bp = Blueprint('table', __name__)
voucher_bp = Blueprint('voucher', __name__)
user_rights_bp = Blueprint('user_rights', __name__)  # ADD THIS LINE
variable_allowance_bp = Blueprint('variable_allowance', __name__)  # ADD THIS LINE
attendance_bp = Blueprint('attendance', __name__)  # ADD THIS LINE
print(f"✅ Created blueprints: auth, generic_crud, employee, screen, table, voucher, user_rights")

# Import routes after blueprint creation to avoid circular imports
print("🔍 Importing route modules...")
from . import auth_routes
print("  ✅ auth_routes imported")
from . import generic_crud_routes
print("  ✅ generic_crud_routes imported")
from . import employee_routes
print("  ✅ employee_routes imported")
from . import screen_routes
print("  ✅ screen_routes imported")
from . import table_routes
print("  ✅ table_routes imported")
from . import voucher_routes
print("  ✅ voucher_routes imported")
from . import user_rights_routes  # ADD THIS LINE
print("  ✅ user_rights_routes imported")
from . import variable_allowance_routes  # ADD THIS LINE
print("  ✅ variable_allowance_routes imported")
from . import attendance_routes  # ADD THIS LINE
print("  ✅ variable_allowance_routes imported")

def register_routes(app):
    """Register all blueprints with the app"""
    print("🔍 Registering blueprints with app...")
    
    app.register_blueprint(auth_bp)
    print("  ✅ auth_bp registered")
    app.register_blueprint(generic_crud_bp)
    print("  ✅ generic_crud_bp registered")
    app.register_blueprint(employee_bp)
    print("  ✅ employee_bp registered")
    app.register_blueprint(screen_bp)
    print("  ✅ screen_bp registered")
    app.register_blueprint(table_bp)
    print("  ✅ table_bp registered")
    app.register_blueprint(voucher_bp)
    print("  ✅ voucher_bp registered")
    app.register_blueprint(user_rights_bp)  # ADD THIS LINE
    print("  ✅ user_rights_bp registered")
    app.register_blueprint(variable_allowance_bp)  # ADD THIS LINE
    print("  ✅ variable_allowance_bp registered")
    app.register_blueprint(attendance_bp)  # ADD THIS LINE
    print("  ✅ variable_allowance_bp registered")
    
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
    
    print("✅ All routes registered successfully")