# routes/__init__.py
from flask import Blueprint, jsonify, request
from flask_cors import CORS

print("🔍 Loading routes/__init__.py")

# Create blueprints for each route module
auth_bp = Blueprint('auth', __name__)
generic_crud_bp = Blueprint('generic_crud', __name__)
employee_bp = Blueprint('employee', __name__)
screen_bp = Blueprint('screen', __name__)
table_bp = Blueprint('table', __name__)
voucher_bp = Blueprint('voucher', __name__)
user_rights_bp = Blueprint('user_rights', __name__)
variable_allowance_bp = Blueprint('variable_allowance', __name__)
attendance_bp = Blueprint('attendance', __name__)

print(f"✅ Created blueprints: auth, generic_crud, employee, screen, table, voucher, user_rights, variable_allowance, attendance")

# Import routes after blueprint creation
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
from . import user_rights_routes
print("  ✅ user_rights_routes imported")
from . import variable_allowance_routes
print("  ✅ variable_allowance_routes imported")
from . import attendance_routes
print("  ✅ attendance_routes imported")

def register_routes(app):
    """Register all blueprints with the app"""
    print("🔍 Registering blueprints with app...")
    
    # Register attendance with /attendance prefix
    app.register_blueprint(attendance_bp, url_prefix='/attendance')
    print("  ✅ attendance_bp registered with url_prefix='/attendance'")
    
    # Register other blueprints
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
    app.register_blueprint(user_rights_bp)
    print("  ✅ user_rights_bp registered")
    app.register_blueprint(variable_allowance_bp)
    print("  ✅ variable_allowance_bp registered")
    
    # Register main routes directly on app
    @app.route('/', methods=['GET', 'OPTIONS'])
    def index():
        if request.method == 'OPTIONS':
            return '', 200
        return jsonify({
            "api": "SmartGold ERP API",
            "version": "2.0",
            "status": "running",
            "endpoints": {
                "login": "/GetMenu (POST)",
                "health": "/health (GET)",
                "test_cors": "/test-cors (GET)",
                "attendance": {
                    "years": "/attendance/years (GET)",
                    "months": "/attendance/months (GET)",
                    "employees": "/attendance/employees (GET)",
                    "search": "/attendance/search (POST)",
                    "update": "/attendance/update (POST)"
                }
            }
        }), 200
    
    @app.route('/health', methods=['GET', 'OPTIONS'])
    def health_check():
        if request.method == 'OPTIONS':
            return '', 200
        return jsonify({"status": "healthy"}), 200
    
    @app.route('/test-cors', methods=['GET', 'OPTIONS'])
    def test_cors():
        """Test CORS endpoint"""
        if request.method == 'OPTIONS':
            response = jsonify({'status': 'ok'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
            response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            return response, 200
        return jsonify({
            "success": True,
            "message": "CORS is working!",
            "origin": request.headers.get('Origin', 'Unknown')
        }), 200
    
    print("✅ All routes registered successfully")