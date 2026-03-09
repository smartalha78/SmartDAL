from flask import Flask, jsonify
from flask_cors import CORS
import logging

# Import configuration
from config.database import init_app as init_db

# Import route registration
from routes import register_routes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    """Application factory pattern"""
    app = Flask(__name__)
    
    # Enable CORS - USE ONLY THIS, remove the after_request handler
    CORS(app, origins=["http://localhost:3000"], supports_credentials=True)
    
    # Initialize database
    init_db(app)
    
    # Register all routes (this will register ALL blueprints)
    register_routes(app)
    
    return app

# Create the app instance
app = create_app()

# Debug route to check all registered endpoints
@app.route('/debug/routes', methods=['GET'])
def debug_routes():
    """List all registered routes"""
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
    })

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
    print("\n  👤 USER RIGHTS MANAGEMENT:")
    print("    GET  /user-rights/test         - Test endpoint (no DB)")
    print("    GET  /user-rights/test-db      - Test endpoint (with DB)")
    print("    POST /user-rights/get           - Get user rights for screen")
    print("    POST /user-rights/save          - Save user rights")
    print("    GET  /user-rights/users         - Get all users")
    print("    GET  /user-rights/menus         - Get all menus")
    print("    POST /user-rights/bulk-get      - Get all rights for user")
    print("    POST /user-rights/bulk-save     - Bulk save user rights")
    print("\n  🔍 DEBUG ENDPOINT:")
    print("    GET  /debug/routes       - List all registered routes")
    print("\n  ❤️  HEALTH ENDPOINT:")
    print("    GET  /health        - Health check")
    print("    GET  /              - API Information")
    print("=" * 60)
    
    app.run(host="0.0.0.0", port=8000, debug=True)

# from flask import Flask, jsonify, request
# from flask_cors import CORS
# import logging
# import os
# from dotenv import load_dotenv
# load_dotenv()  # This loads the .env file

# # Import configuration
# from config.database import CONN_STR, init_app as init_db

# # Import route registration
# from routes import register_routes

# # ===== AI INTEGRATION =====
# try:
#     import google.generativeai as genai
#     GEMINI_AVAILABLE = True
# except ImportError:
#     GEMINI_AVAILABLE = False
#     print("⚠️ google-generativeai not installed. Run: pip install google-generativeai")

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def create_app():
#     """Application factory pattern"""
#     app = Flask(__name__)
    
#     # Enable CORS
#     CORS(app)
    
#     # Initialize database
#     init_db(app)
    
#     # Add after_request handler for CORS
#     @app.after_request
#     def after_request(response):
#         """Add CORS headers to every response"""
#         response.headers.add('Access-Control-Allow-Origin', '*')
#         response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
#         response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
#         return response
    
#     # ===== OPTIMIZED AI HELPER FUNCTIONS =====
    
#     # Cache for AI model (load once, reuse forever)
#     _cached_model = None
#     _cached_model_name = None
    
#     def get_gemini_model():
#         """Get configured Gemini model with caching (FAST)"""
#         global _cached_model, _cached_model_name
        
#         api_key = os.environ.get('GEMINI_API_KEY')
#         if not GEMINI_AVAILABLE or not api_key:
#             return None, "AI not configured"
        
#         # Return cached model if available
#         if _cached_model is not None:
#             return _cached_model, None
        
#         try:
#             genai.configure(api_key=api_key)
            
#             # Your actual working models from the list
#             available_models = [
#                 'models/gemini-2.5-flash',
#                 'models/gemini-2.5-pro',
#                 'models/gemini-2.0-flash',
#                 'models/gemini-flash-latest',
#                 'models/gemini-pro-latest'
#             ]
            
#             # Try each available model
#             for model_name in available_models:
#                 try:
#                     print(f"🔄 Trying model: {model_name}")
#                     model = genai.GenerativeModel(model_name)
#                     # Test the model with a simple prompt
#                     test_response = model.generate_content("OK", generation_config={"max_output_tokens": 2})
#                     if test_response and test_response.text:
#                         print(f"✅ Caching model: {model_name}")
#                         _cached_model = model
#                         _cached_model_name = model_name
#                         return model, None
#                 except Exception as e:
#                     print(f"   Model {model_name} failed: {str(e)[:50]}...")
#                     continue
            
#             return None, "No working model found from available list"
#         except Exception as e:
#             return None, str(e)
    
#     # Cache for department names (prevents repeated database queries)
#     department_cache = {}
    
#     def get_department_name(dept_code):
#         """Get department name from code with caching (FAST)"""
#         if not dept_code:
#             return "Unknown"
        
#         # Return cached name if available
#         if dept_code in department_cache:
#             return department_cache[dept_code]
        
#         try:
#             from utils.db_helpers import execute_query
#             query = f"SELECT Name FROM HRMSDepartment WHERE Code = '{dept_code}'"
#             result = execute_query(query)
#             if result and len(result) > 0:
#                 dept_name = result[0].get('Name', f'Dept-{dept_code}')
#                 department_cache[dept_code] = dept_name
#                 return dept_name
#         except Exception as e:
#             logger.error(f"Error fetching department name: {e}")
        
#         # Fallback
#         department_cache[dept_code] = f"Dept-{dept_code}"
#         return f"Dept-{dept_code}"
    
#     # ===== FAST AI ENDPOINTS (OPTIMIZED) =====
    
#     @app.route('/ai/quick-count', methods=['GET'])
#     def ai_quick_count():
#         """Quick employee count - SUPER FAST (no AI)"""
#         try:
#             from utils.db_helpers import execute_query
            
#             total = execute_query("SELECT COUNT(*) as total FROM HRMSEmployee")[0]['total']
#             active = execute_query("SELECT COUNT(*) as total FROM HRMSEmployee WHERE IsActive = 1")[0]['total']
            
#             return jsonify({
#                 "success": True,
#                 "total": total,
#                 "active": active,
#                 "inactive": total - active
#             })
#         except Exception as e:
#             return jsonify({"success": False, "error": str(e)}), 500

#     @app.route('/ai/department-summary', methods=['GET'])
#     def ai_department_summary():
#         """Quick department summary - SUPER FAST (no AI)"""
#         try:
#             from utils.db_helpers import execute_query
            
#             # Get all employees grouped by department
#             query = "SELECT DepartmentCode, COUNT(*) as count FROM HRMSEmployee GROUP BY DepartmentCode"
#             result = execute_query(query) or []
            
#             # Enhance with department names
#             summary = []
#             for row in result:
#                 dept_code = row.get('DepartmentCode', 'Unknown')
#                 if dept_code:  # Skip null departments
#                     dept_name = get_department_name(dept_code)
#                     summary.append({
#                         "code": dept_code,
#                         "name": dept_name,
#                         "count": row.get('count', 0)
#                     })
            
#             # Sort by count descending
#             summary.sort(key=lambda x: x['count'], reverse=True)
            
#             return jsonify({
#                 "success": True,
#                 "total_departments": len(summary),
#                 "total_employees": sum(d['count'] for d in summary),
#                 "departments": summary
#             })
#         except Exception as e:
#             return jsonify({"success": False, "error": str(e)}), 500

#     @app.route('/ai/query-employees-fast', methods=['POST'])
#     def ai_query_employees_fast():
#         """FAST natural language query for employee data (optimized)"""
#         try:
#             from utils.db_helpers import execute_query
            
#             data = request.json
#             query = data.get('query', '').lower().strip()
            
#             if not query:
#                 return jsonify({"success": False, "error": "Query required"}), 400
            
#             # ===== PATTERN 1: COUNT QUERIES (No AI needed) =====
#             if any(word in query for word in ['count', 'how many', 'total']):
#                 # Determine if asking about active/inactive
#                 if 'active' in query:
#                     sql = "SELECT COUNT(*) as total FROM HRMSEmployee WHERE IsActive = 1"
#                     result = execute_query(sql)
#                     count = result[0]['total'] if result else 0
#                     return jsonify({
#                         "success": True,
#                         "type": "count",
#                         "subtype": "active",
#                         "data": {"count": count},
#                         "message": f"There are {count} active employees."
#                     })
#                 elif 'inactive' in query:
#                     sql = "SELECT COUNT(*) as total FROM HRMSEmployee WHERE IsActive = 0 OR IsActive IS NULL"
#                     result = execute_query(sql)
#                     count = result[0]['total'] if result else 0
#                     return jsonify({
#                         "success": True,
#                         "type": "count",
#                         "subtype": "inactive",
#                         "data": {"count": count},
#                         "message": f"There are {count} inactive employees."
#                     })
#                 else:
#                     sql = "SELECT COUNT(*) as total FROM HRMSEmployee"
#                     result = execute_query(sql)
#                     count = result[0]['total'] if result else 0
#                     return jsonify({
#                         "success": True,
#                         "type": "count",
#                         "subtype": "total",
#                         "data": {"count": count},
#                         "message": f"There are {count} employees in total."
#                     })
            
#             # ===== PATTERN 2: DEPARTMENT QUERIES (Fast with caching) =====
#             if any(word in query for word in ['department', 'dept', 'in']):
#                 # Get all employees
#                 emp_query = "SELECT Code, FName, LName, DepartmentCode, IsActive FROM HRMSEmployee"
#                 employees = execute_query(emp_query) or []
                
#                 # Group by department with names
#                 dept_map = {}
#                 for emp in employees:
#                     dept_code = emp.get('DepartmentCode')
#                     if not dept_code:
#                         continue
                    
#                     dept_name = get_department_name(dept_code)
                    
#                     if dept_name not in dept_map:
#                         dept_map[dept_name] = {
#                             "code": dept_code,
#                             "name": dept_name,
#                             "count": 0,
#                             "active": 0,
#                             "employees": []
#                         }
                    
#                     dept_map[dept_name]["count"] += 1
#                     if emp.get('IsActive') == 1:
#                         dept_map[dept_name]["active"] += 1
                    
#                     # Store first 3 employee names as examples
#                     if len(dept_map[dept_name]["employees"]) < 3:
#                         full_name = f"{emp.get('FName','')} {emp.get('LName','')}".strip()
#                         if full_name:
#                             dept_map[dept_name]["employees"].append(full_name)
                
#                 # Check if asking about specific department
#                 for dept_name, dept_info in dept_map.items():
#                     if dept_name.lower() in query or query in dept_name.lower():
#                         return jsonify({
#                             "success": True,
#                             "type": "department_specific",
#                             "department": dept_info,
#                             "message": f"Department '{dept_name}' has {dept_info['count']} employees ({dept_info['active']} active)."
#                         })
                
#                 # Return summary of all departments
#                 dept_list = list(dept_map.values())
#                 dept_list.sort(key=lambda x: x['count'], reverse=True)
                
#                 return jsonify({
#                     "success": True,
#                     "type": "department_summary",
#                     "total_departments": len(dept_list),
#                     "departments": dept_list[:10],  # Top 10 departments
#                     "message": f"Found {len(dept_list)} departments with employees."
#                 })
            
#             # ===== PATTERN 3: ACTIVE/INACTIVE QUERIES =====
#             if 'active' in query or 'inactive' in query:
#                 is_active = 1 if 'active' in query else 0
#                 status = "active" if is_active == 1 else "inactive"
                
#                 sql = f"SELECT COUNT(*) as total FROM HRMSEmployee WHERE IsActive = {is_active}"
#                 result = execute_query(sql)
#                 count = result[0]['total'] if result else 0
                
#                 # Get sample of these employees
#                 sample_sql = f"SELECT TOP 5 Code, FName, LName FROM HRMSEmployee WHERE IsActive = {is_active}"
#                 samples = execute_query(sample_sql) or []
                
#                 sample_names = [f"{s.get('FName','')} {s.get('LName','')}".strip() for s in samples if s.get('FName')]
                
#                 return jsonify({
#                     "success": True,
#                     "type": "status",
#                     "status": status,
#                     "count": count,
#                     "samples": sample_names,
#                     "message": f"There are {count} {status} employees."
#                 })
            
#             # ===== PATTERN 4: SEARCH BY NAME =====
#             search_terms = query.replace('find', '').replace('show', '').replace('get', '').replace('employee', '').strip()
#             if len(search_terms) > 2:  # Only search if we have meaningful terms
#                 search_sql = f"""
#                     SELECT TOP 10 Code, FName, LName, DepartmentCode, IsActive 
#                     FROM HRMSEmployee 
#                     WHERE FName LIKE '%{search_terms}%' OR LName LIKE '%{search_terms}%'
#                 """
#                 results = execute_query(search_sql) or []
                
#                 if results:
#                     enhanced_results = []
#                     for r in results:
#                         r_copy = dict(r)
#                         r_copy['DepartmentName'] = get_department_name(r.get('DepartmentCode'))
#                         enhanced_results.append(r_copy)
                    
#                     return jsonify({
#                         "success": True,
#                         "type": "search",
#                         "query": search_terms,
#                         "count": len(results),
#                         "results": enhanced_results,
#                         "message": f"Found {len(results)} employees matching '{search_terms}'."
#                     })
            
#             # ===== PATTERN 5: COMPLEX QUERIES (Use AI as fallback) =====
#             model, error = get_gemini_model()
#             if not model:
#                 return jsonify({
#                     "success": True,
#                     "type": "fallback",
#                     "message": "I couldn't understand your query. Try asking about counts, departments, or specific employees.",
#                     "suggestions": [
#                         "How many employees?",
#                         "Show active employees",
#                         "Employees in IT department",
#                         "Find employee named John"
#                     ]
#                 })
            
#             # Get sample data with department names
#             sample_query = "SELECT TOP 20 Code, FName, LName, DepartmentCode, IsActive FROM HRMSEmployee"
#             results = execute_query(sample_query) or []
            
#             # Enhance with department names
#             enhanced_results = []
#             for r in results:
#                 r_copy = dict(r)
#                 r_copy['DepartmentName'] = get_department_name(r.get('DepartmentCode'))
#                 enhanced_results.append(r_copy)
            
#             # Use AI for complex understanding
#             prompt = f"""
#             User query: "{query}"
            
#             Available employee data (first 20 records):
#             {enhanced_results}
            
#             Respond with:
#             1. What you think the user wants
#             2. The answer based on available data
#             3. Keep it concise and helpful
#             """
            
#             ai_response = model.generate_content(prompt)
            
#             return jsonify({
#                 "success": True,
#                 "type": "ai_assisted",
#                 "query": query,
#                 "data_sample": enhanced_results[:5],
#                 "response": ai_response.text
#             })
            
#         except Exception as e:
#             logger.error(f"Fast query error: {e}")
#             return jsonify({"success": False, "error": str(e)}), 500

#     # ===== ORIGINAL AI ENDPOINTS (Keep for compatibility) =====
    
#     @app.route('/ai/status', methods=['GET'])
#     def ai_status():
#         """Check AI integration status"""
#         api_key = os.environ.get('GEMINI_API_KEY')
#         model, error = get_gemini_model() if GEMINI_AVAILABLE and api_key else (None, None)
        
#         return jsonify({
#             "success": True,
#             "gemini_available": GEMINI_AVAILABLE,
#             "api_key_configured": api_key is not None,
#             "ai_enabled": GEMINI_AVAILABLE and api_key is not None and model is not None,
#             "model_working": model is not None,
#             "model_error": error,
#             "cached_model": _cached_model_name
#         })
    
#     @app.route('/ai/list-models', methods=['GET'])
#     def ai_list_models():
#         """List all available Gemini models"""
#         api_key = os.environ.get('GEMINI_API_KEY')
#         if not GEMINI_AVAILABLE or not api_key:
#             return jsonify({"success": False, "error": "AI not configured"}), 503
        
#         try:
#             genai.configure(api_key=api_key)
#             models = genai.list_models()
            
#             model_list = []
#             for m in models:
#                 if 'generateContent' in m.supported_generation_methods:
#                     model_list.append({
#                         "name": m.name,
#                         "display_name": m.display_name,
#                         "description": m.description
#                     })
            
#             return jsonify({
#                 "success": True,
#                 "models": model_list
#             })
#         except Exception as e:
#             return jsonify({"success": False, "error": str(e)}), 500
    
#     @app.route('/ai/ask', methods=['POST'])
#     def ai_ask():
#         """Simple AI question-answering endpoint"""
#         model, error = get_gemini_model()
#         if not model:
#             return jsonify({"success": False, "error": f"AI not available: {error}"}), 503
        
#         try:
#             data = request.json
#             question = data.get('question', '')
            
#             if not question:
#                 return jsonify({"success": False, "error": "Question required"}), 400
            
#             prompt = f"""
#             You are an AI assistant for an ERP system.
#             Answer this question helpfully and concisely: {question}
#             """
            
#             response = model.generate_content(prompt)
            
#             return jsonify({
#                 "success": True,
#                 "question": question,
#                 "answer": response.text
#             }), 200
            
#         except Exception as e:
#             logger.error(f"AI error: {e}")
#             return jsonify({"success": False, "error": str(e)}), 500

#     @app.route('/ai/help', methods=['POST'])
#     def ai_help():
#         """Contextual help for ERP features"""
#         model, error = get_gemini_model()
#         if not model:
#             return jsonify({"success": False, "error": f"AI not available: {error}"}), 503
        
#         try:
#             data = request.json
#             topic = data.get('topic', '')
            
#             if not topic:
#                 return jsonify({"success": False, "error": "Topic required"}), 400
            
#             prompt = f"""
#             You are an ERP system help assistant for SmartGold ERP.
            
#             User needs help with: "{topic}"
            
#             Provide clear, step-by-step help on how to use this feature.
#             Include:
#             1. What this feature does
#             2. How to access it
#             3. Step-by-step instructions
#             4. Tips and best practices
#             """
            
#             response = model.generate_content(prompt)
            
#             return jsonify({
#                 "success": True,
#                 "topic": topic,
#                 "help": response.text
#             }), 200
            
#         except Exception as e:
#             logger.error(f"AI help error: {e}")
#             return jsonify({"success": False, "error": str(e)}), 500
    
#     # Register all existing routes
#     register_routes(app)
    
#     return app

# # Create the app instance
# app = create_app()

# if __name__ == "__main__":
#     print("=" * 60)
#     print("🚀 Starting SmartGold ERP API Server v2.0")
#     print("=" * 60)
#     print("📍 Server will run on: http://0.0.0.0:8000")
#     print("\n📋 AVAILABLE ENDPOINTS:")
#     print("  ────────────────────────────────────────────")
#     print("  🔧 AUTHENTICATION ENDPOINTS:")
#     print("    POST /GetMenu       - Get user menu")
#     print("\n  💰 VOUCHER ENDPOINTS:")
#     print("    GET  /GetVno        - Get voucher number")
#     print("    POST /FillTable     - Fill table data")
#     print("    POST /gl_voucher_generation_status - Update GL voucher status")
#     print("    POST /gl_Posting    - GL Posting")
#     print("    POST /stk_Posting   - Stock Posting")
#     print("\n  📊 TABLE STRUCTURE ENDPOINTS:")
#     print("    POST /get-table-headers - Get column headers")
#     print("    POST /get-table-structure - Get table structure")
#     print("    POST /get-table-relationships - Get relationships")
#     print("    POST /get-table-data - Get paginated data")
#     print("    GET  /debug/table-structure/<table_name> - Debug table structure")
#     print("    GET  /check-table/<table_name> - Check table")
#     print("\n  👔 EMPLOYEE MANAGEMENT:")
#     print("    📋 POST /insert-EmployeeHeadDet - Insert employee with all details")
#     print("\n  ✨ GENERIC CRUD ENDPOINTS:")
#     print("    📥 POST /table/insert  - Generic INSERT for any table")
#     print("    📝 POST /table/update  - Generic UPDATE for any table")
#     print("    🔄 POST /table/upsert  - Generic UPSERT")
#     print("    🗑️  POST /table/delete  - Generic DELETE")
#     print("    📦 POST /table/bulk-insert - Bulk insert multiple records")
#     print("\n  🖥️  SCREEN CONFIGURATION:")
#     print("    POST /screen/get-config")
#     print("    POST /screen/document-statuses")
#     print("    POST /screen/menu-permissions")
#     print("    POST /screen/update-employment-status")
#     print("    POST /screen/refresh-table-data")
#     print("\n  ⚡ FAST AI ENDPOINTS (OPTIMIZED):")
#     print("    GET  /ai/quick-count              - Get employee counts instantly")
#     print("    GET  /ai/department-summary        - Get department summary instantly")
#     print("    POST /ai/query-employees-fast      - Fast natural language queries")
#     print("\n  🤖 STANDARD AI ENDPOINTS:")
#     print("    GET  /ai/status                    - Check AI configuration")
#     print("    GET  /ai/list-models                - List available AI models")
#     print("    POST /ai/ask                        - Ask any question to AI")
#     print("    POST /ai/help                       - Contextual help for ERP features")
#     print("\n  ❤️  HEALTH ENDPOINT:")
#     print("    GET  /health        - Health check")
#     print("    GET  /              - API Information")
#     print("=" * 60)
    
#     # Check AI configuration
#     api_key = os.environ.get('GEMINI_API_KEY')
#     if not GEMINI_AVAILABLE:
#         print("⚠️  AI features disabled: google-generativeai package not installed")
#         print("   Run: pip install google-generativeai")
#     elif not api_key:
#         print("⚠️  AI features disabled: GEMINI_API_KEY not set in environment")
#         print("   Create a .env file with: GEMINI_API_KEY=your-key-here")
#     else:
#         print("✅ AI configured - models will load on first request")
    
#     # Run the app
#     app.run(host="0.0.0.0", port=8000, debug=False)