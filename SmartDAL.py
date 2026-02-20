
from flask import Flask, request, jsonify
from flask_cors import CORS
import pyodbc
import math
import json
from datetime import datetime
import logging
import requests
app = Flask(__name__)
CORS(app)

# ===== DATABASE CONFIG =====
CONN_STR = (
    "DRIVER={SQL Server};"
    "SERVER=192.168.100.113;"
    "DATABASE=AwaisFancy;"
    "UID=sa;"
    "PWD=786"
)

# Cache for table structures
TABLE_STRUCTURE_CACHE = {}
COLUMN_LENGTHS_CACHE = {}
# Known computed columns that should never be inserted
COMPUTED_COLUMNS = ['Name', 'FullName', 'DisplayName']  # Add more as needed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.after_request
def after_request(response):
    """Add CORS headers to every response"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

def guess_type(value):
    """Utility function to guess the type of a value"""
    if value == "" or value is None:
        return "string"
    try:
        int(value)
        return "integer"
    except (ValueError, TypeError):
        try:
            float(value)
            return "float"
        except (ValueError, TypeError):
            return "string"

def execute_soap_query(query):
    """Utility function to execute SQL queries and return results in a consistent format"""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            row_dict = {}
            for idx, col in enumerate(columns):
                value = row[idx]
                if value is None:
                    value = ""
                row_dict[col] = [str(value)] if value != "" else [""]
            result.append(row_dict)
        
        return result
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_non_query(query, params=None):
    """Execute INSERT, UPDATE, DELETE queries"""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_many_non_query(queries_and_params):
    """Execute multiple queries in a transaction"""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        for query, params in queries_and_params:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
        
        conn.commit()
        return True
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_table_primary_keys(table_name):
    """Get primary key columns for a table"""
    query = f"""
        SELECT 
            COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE 
            TABLE_NAME = '{table_name}' 
            AND CONSTRAINT_NAME LIKE 'PK%'
        ORDER BY ORDINAL_POSITION
    """
    
    try:
        result = execute_soap_query(query)
        pk_columns = [row.get("COLUMN_NAME", [""])[0] for row in result if row.get("COLUMN_NAME", [""])[0]]
        return pk_columns
    except:
        return []

def get_table_identity_column(table_name):
    """Get identity/auto-increment column for a table"""
    query = f"""
        SELECT 
            COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_NAME = '{table_name}'
            AND COLUMNPROPERTY(OBJECT_ID('{table_name}'), COLUMN_NAME, 'IsIdentity') = 1
    """
    
    try:
        result = execute_soap_query(query)
        if result:
            return result[0].get("COLUMN_NAME", [""])[0]
        return None
    except:
        return None

def validate_columns(table_name, data):
    """Validate that all columns in data exist in the table structure"""
    structure_result = get_table_structure_data(table_name)
    if not structure_result.get("success"):
        return False, "Could not validate table structure"
    
    valid_columns = [col["name"] for col in structure_result["structure"]]
    invalid_columns = [col for col in data.keys() if col not in valid_columns]
    
    if invalid_columns:
        return False, f"Invalid columns: {', '.join(invalid_columns)}"
    
    return True, "Valid"

def get_table_structure_data(table_name):
    """Get table structure for validation"""
    try:
        query = f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        
        columns = execute_soap_query(query)
        structure = []
        
        for col in columns:
            structure.append({
                "name": col.get("COLUMN_NAME", [""])[0],
                "type": col.get("DATA_TYPE", [""])[0],
                "maxLength": col.get("CHARACTER_MAXIMUM_LENGTH", [None])[0],
                "nullable": col.get("IS_NULLABLE", ["NO"])[0] == "YES"
            })
        
        return {"success": True, "structure": structure}
    except Exception as err:
        return {"success": False, "error": str(err)}

def build_insert_query(table_name, rows):
    """Build INSERT query for multiple rows"""
    if not rows or len(rows) == 0:
        return None
    
    # Get columns from first row
    columns = list(rows[0].keys())
    columns_str = ", ".join(columns)
    
    # Build placeholders for each row
    all_placeholders = []
    for row in rows:
        placeholders = ", ".join(["?" for _ in row])
        all_placeholders.append(f"({placeholders})")
    
    placeholders_str = ", ".join(all_placeholders)
    
    return f"INSERT INTO {table_name} ({columns_str}) VALUES {placeholders_str}"

def format_date_for_sql():
    """Get current date in SQL format"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_user_uid(username):
    """Get UID from comUsers table"""
    try:
        query = f"SELECT TOP 1 Uid FROM comUsers WHERE Userlogin = '{username.replace("'", "''")}'"
        rows = execute_soap_query(query)
        if rows and rows[0].get("Uid") and rows[0]["Uid"][0]:
            return rows[0]["Uid"][0]
    except Exception as err:
        print(f"Could not fetch Uid from comUsers: {err}")
    return "07"  # Default UID

def get_column_lengths(table_name):
    """Get maximum lengths for string columns"""
    if table_name in COLUMN_LENGTHS_CACHE:
        return COLUMN_LENGTHS_CACHE[table_name]
    
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        """
        
        result = execute_query(query)
        lengths = {}
        for row in result:
            col_name = row['COLUMN_NAME']
            max_length = row['CHARACTER_MAXIMUM_LENGTH']
            if max_length and max_length > 0:  # Only for varchar/nvarchar columns
                lengths[col_name] = max_length
        
        COLUMN_LENGTHS_CACHE[table_name] = lengths
        return lengths
    except Exception as e:
        print(f"Error getting column lengths for {table_name}: {e}")
        return {}

def truncate_string(value, max_length):
    """Truncate string to max_length if needed"""
    if value and isinstance(value, str) and max_length:
        if len(value) > max_length:
            print(f"⚠️ Truncating string from {len(value)} to {max_length} characters")
            return value[:max_length]
    return value

def get_table_columns(table_name):
    """Get column names for a table (with caching)"""
    if table_name in TABLE_STRUCTURE_CACHE:
        return TABLE_STRUCTURE_CACHE[table_name]
    
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        """
        
        result = execute_query(query)
        columns = [row['COLUMN_NAME'] for row in result]
        TABLE_STRUCTURE_CACHE[table_name] = columns
        return columns
    except Exception as e:
        print(f"Error getting table columns for {table_name}: {e}")
        return []

def filter_valid_columns(table_name, input_data, exclude_computed=True):
    """Filter input data to only include columns that exist in the table"""
    valid_columns = get_table_columns(table_name)
    column_lengths = get_column_lengths(table_name)
    
    if not valid_columns:
        return input_data
    
    filtered_data = {}
    for key, value in input_data.items():
        # Skip if column doesn't exist in table
        if key not in valid_columns:
            print(f"⚠️ Column '{key}' not found in {table_name}, skipping")
            continue
        
        # Skip computed columns if exclude_computed is True
        if exclude_computed and key in COMPUTED_COLUMNS:
            print(f"⚠️ Column '{key}' is a computed column, skipping INSERT")
            continue
        
        # Truncate string values if they exceed column length
        if key in column_lengths and isinstance(value, str):
            value = truncate_string(value, column_lengths[key])
        
        filtered_data[key] = value
    
    return filtered_data

def guess_type(value):
    """Guess the type of a value (for frontend display)"""
    if value is None or value == "":
        return "string"
    
    str_val = str(value).lower()
    
    if str_val in ["true", "false"]:
        return "boolean"
    if str_val.replace('-', '').isdigit():
        return "int"
    if str_val.replace('.', '').replace('-', '').isdigit() and '.' in str_val:
        return "decimal"
    if str_val[:10].count('-') == 2 and len(str_val) >= 10:  # Simple date check
        return "date"
    
    return "string"

def format_date_for_sql(date=None):
    """Format date for SQL Server"""
    if date is None:
        date = datetime.now()
    elif isinstance(date, str):
        try:
            date = datetime.strptime(date, "%Y-%m-%d")
        except:
            try:
                date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            except:
                date = datetime.now()
    
    return date.strftime("%Y-%m-%d %H:%M:%S")

def build_where_clause(where_obj):
    """Build WHERE clause from object"""
    if not where_obj:
        return ""
    
    clauses = []
    for key, val in where_obj.items():
        if val is None:
            clauses.append(f"{key} IS NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")
            clauses.append(f"{key} = '{escaped_val}'")
        elif isinstance(val, bool):
            clauses.append(f"{key} = {1 if val else 0}")
        else:
            clauses.append(f"{key} = {val}")
    
    return " AND ".join(clauses)

def build_insert_query(table_name, rows, exclude_columns=None):
    """Build INSERT query with column validation"""
    if not rows or len(rows) == 0:
        return None
    
    if exclude_columns is None:
        exclude_columns = []
    
    # Add computed columns to exclude list
    exclude_columns.extend(COMPUTED_COLUMNS)
    
    # Get valid columns for this table
    valid_columns = get_table_columns(table_name)
    
    if not valid_columns:
        print(f"⚠️ Could not get columns for table: {table_name}")
        return None
    
    print(f"🔍 Building INSERT for: {table_name}")
    print(f"🔍 Valid columns in table: {valid_columns}")
    print(f"🔍 Excluding columns: {exclude_columns}")
    
    # Process each row and filter columns
    processed_rows = []
    for row in rows:
        # Filter to only valid columns and exclude specified ones
        filtered_row = {}
        for key, val in row.items():
            if (key in valid_columns and 
                key not in exclude_columns and 
                key not in COMPUTED_COLUMNS):
                filtered_row[key] = val
            else:
                print(f"  Skipping column '{key}' - not in table, excluded, or computed")
        
        processed_rows.append(filtered_row)
    
    if not processed_rows or not processed_rows[0]:
        print(f"⚠️ No valid columns to insert for table: {table_name}")
        return None
    
    # Get columns from first row
    columns = list(processed_rows[0].keys())
    print(f"🔍 Columns to insert: {columns}")
    
    # Build values for each row
    values_list = []
    for row_idx, row in enumerate(processed_rows):
        row_values = []
        for col in columns:
            val = row.get(col)
            
            # Handle different value types
            if val is None:
                row_values.append("NULL")
            elif val == "":
                row_values.append("''")
            elif isinstance(val, bool):
                row_values.append("1" if val else "0")
            elif isinstance(val, (int, float)):
                row_values.append(str(val))
            elif isinstance(val, str) and val.upper() == "NULL":
                row_values.append("NULL")
            elif isinstance(val, str):
                # Check if it's a date that needs formatting
                if 'Date' in col or col in ['DOB', 'JoiningDate', 'AppointmentDate', 'ProbitionDate', 
                                           'ContractStartDate', 'ContractEndDate', 'LeftDate', 
                                           'IDExpiryDate', 'PassportExpiryDate']:
                    try:
                        # Try to format the date properly
                        if ' ' in val:
                            date_obj = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                        else:
                            date_obj = datetime.strptime(val, "%Y-%m-%d")
                        formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                        row_values.append(f"'{formatted_date}'")
                    except:
                        escaped_val = val.replace("'", "''")
                        row_values.append(f"'{escaped_val}'")
                else:
                    escaped_val = val.replace("'", "''")
                    row_values.append(f"'{escaped_val}'")
            else:
                str_val = str(val)
                escaped_val = str_val.replace("'", "''")
                row_values.append(f"'{escaped_val}'")
        
        values_list.append(f"({', '.join(row_values)})")
    
    values_str = ", ".join(values_list)
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES {values_str}"
    
    print(f"🔍 Generated Query: {query[:200]}...")
    return query

def execute_query(query, params=None):
    """Execute SQL query and return results"""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Check if this is a SELECT query
        if query.strip().upper().startswith("SELECT"):
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                row_dict = {}
                for idx, col in enumerate(columns):
                    value = row[idx]
                    if value is None:
                        value = ""
                    elif isinstance(value, datetime):
                        value = value.strftime("%Y-%m-%d %H:%M:%S")
                    row_dict[col] = value
                result.append(row_dict)
            return result
        else:
            # For INSERT/UPDATE/DELETE, commit and return rowcount
            conn.commit()
            return cursor.rowcount
            
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_non_query(query, params=None):
    """Execute INSERT, UPDATE, DELETE queries"""
    return execute_query(query, params)

def get_user_uid(username):
    """Get UID from comUsers table"""
    try:
        query = f"SELECT TOP 1 Uid FROM comUsers WHERE Userlogin = '{username.replace(chr(39), chr(39)+chr(39))}'"
        rows = execute_query(query)
        if rows and len(rows) > 0:
            return rows[0].get('Uid', '07')
    except Exception as err:
        print(f"Could not fetch Uid from comUsers: {err}")
    return "07"  # Default UID



def get_company_data():
    try:
        # Call your login API to get full menu
        response = requests.post(
            "http://192.168.100.113:8000/GetMenu",
            json={
                "username": "administrator",
                "userpassword": "admin",
                "Menuid": "01",
                "nooftables": "3"
            }
        )
        data = response.json()
        if not data or not data.get("data") or not data["data"].get("tbl3"):
            return None

        menu = data["data"]["tbl3"]

        return {
            "company": data["data"]["tbl1"][0] if data["data"].get("tbl1") else {},
            "branches": data["data"]["tbl2"] if data["data"].get("tbl2") else [],
            "menu": menu
        }

    except Exception as e:
        print(f"Error getting company data: {e}")
        return None
# ===== SCREEN CONFIG ENDPOINTS =====

@app.route('/screen/get-config', methods=['POST'])
def get_screen_config():
    """
    Get screen configuration dynamically from menu data
    No defaults - everything comes from the menu based on screenName
    """
    data = request.json
    screen_name = data.get('screenName') if data else None

    try:
        if not screen_name:
            return jsonify({
                "success": False,
                "error": "screenName is required"
            }), 400

        logger.info(f"🔍 Getting screen config for: \"{screen_name}\"")

        # Get menu data from login API
        company_data = get_company_data()
        
        if not company_data or not company_data.get('menu') or not isinstance(company_data['menu'], list):
            return jsonify({
                "success": False,
                "error": "Could not fetch menu data from API"
            }), 500

        logger.info(f"📋 Found {len(company_data['menu'])} menu items from API")
        
        # Search for the screen in the menu data (exact match first)
        found_screen = None
        for item in company_data['menu']:
            if item.get('MenuTitle') == screen_name:
                found_screen = item
                break

        # If no exact match, try case-insensitive
        if not found_screen:
            for item in company_data['menu']:
                if item.get('MenuTitle', '').lower() == screen_name.lower():
                    found_screen = item
                    break

        # If still not found, try partial match
        if not found_screen:
            lower_screen_name = screen_name.lower()
            for item in company_data['menu']:
                if lower_screen_name in item.get('MenuTitle', '').lower():
                    found_screen = item
                    break

        if found_screen:
            logger.info(f"✅ Found screen: {found_screen.get('MenuTitle')} (ID: {found_screen.get('Menuid')})")
            
            return jsonify({
                "success": True,
                "screen": {
                    "id": found_screen.get('Menuid'),
                    "title": found_screen.get('MenuTitle'),
                    "url": found_screen.get('MenuURL'),
                    "parentId": found_screen.get('ParentId'),
                    "isAdd": found_screen.get('isAdd', False),
                    "isEdit": found_screen.get('isEdit', False),
                    "isDelete": found_screen.get('isDelete', False),
                    "isPost": found_screen.get('isPost', False),
                    "isPrint": found_screen.get('isPrint', False),
                    "isSearch": found_screen.get('isSearch', False),
                    "isUpload": found_screen.get('isUpload', False),
                    "isCopy": found_screen.get('isCopy', False),
                    "isBackDate": found_screen.get('IsBackDate', False),
                    "menuType": found_screen.get('MenuType'),
                    "menuSystem": found_screen.get('MenuSystem'),
                    "toolbarOrder": found_screen.get('ToolbarOrder')
                },
                "source": "api_get_full_menu"
            }), 200

        # If not found, return all similar screens as suggestions
        lower_screen_name = screen_name.lower()
        similar_screens = [
            item for item in company_data['menu'] 
            if lower_screen_name in item.get('MenuTitle', '').lower()
        ]

        if similar_screens:
            return jsonify({
                "success": False,
                "error": f'Screen "{screen_name}" not found exactly. Did you mean one of these?',
                "suggestions": [s.get('MenuTitle') for s in similar_screens],
                "similarScreens": [
                    {
                        "id": s.get('Menuid'),
                        "title": s.get('MenuTitle'),
                        "url": s.get('MenuURL'),
                        "parentId": s.get('ParentId')
                    } for s in similar_screens
                ]
            }), 404

        return jsonify({
            "success": False,
            "error": f'Screen "{screen_name}" not found in menu',
            "totalMenuItems": len(company_data['menu']),
            "suggestions": [m.get('MenuTitle') for m in company_data['menu'][:10]]
        }), 404

    except Exception as err:
        logger.error(f"❌ getScreenConfig error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to fetch screen configuration",
            "details": str(err)
        }), 500


@app.route('/screen/document-statuses', methods=['POST'])
def get_document_statuses():
    """
    Get filtered document statuses based on nFilterSort
    menuId and cname come from frontend
    """
    data = request.json or {}
    menu_id = data.get('menuId')
    c_name = data.get('cname')

    try:
        # Validate input
        if not menu_id or not c_name:
            return jsonify({
                "success": False,
                "error": "menuId and cname are required"
            }), 400

        # Clean input
        menu_id = str(menu_id).strip()
        c_name = str(c_name).strip()

        logger.info(f"📊 menuId: {menu_id}, cname received: '{c_name}'")

        # Step 1: Get nFilterSort safely (case-insensitive + trimmed)
        filter_query = """
            SELECT nFilterSort
            FROM tblDocumentStatus
            WHERE Menuid = ?
              AND LTRIM(RTRIM(LOWER(cname))) = LTRIM(RTRIM(LOWER(?)))
        """

        filter_result = execute_query(filter_query, (menu_id, c_name))

        if not filter_result:
            logger.warning("⚠ No row found for given cname")
            return jsonify({
                "success": False,
                "message": "Status not found"
            }), 404

        n_filter_sort = filter_result[0].get('nFilterSort')

        if not n_filter_sort:
            logger.warning("⚠ nFilterSort is NULL or empty")
            return jsonify({
                "success": False,
                "message": "No filter configuration found"
            }), 404

        logger.info(f"📌 nFilterSort value: {n_filter_sort}")

        # Step 2: Convert '2,3,4' → [2,3,4]
        filter_values = []
        for val in str(n_filter_sort).split(','):
            val = val.strip()
            if val.isdigit():
                filter_values.append(int(val))

        if not filter_values:
            logger.warning("⚠ No valid numeric values found in nFilterSort")
            return jsonify({
                "success": False,
                "message": "Invalid filter configuration"
            }), 400

        # Step 3: Create dynamic placeholders
        placeholders = ','.join(['?'] * len(filter_values))

        status_query = f"""
            SELECT 
                ccode,
                cname,
                nFilterSort,
                isactive,
                StatusProcessCode,
                Menuid,
                isChange
            FROM tblDocumentStatus
            WHERE Menuid = ?
              AND ccode IN ({placeholders})
            ORDER BY nFilterSort
        """

        params = [menu_id] + filter_values

        result = execute_query(status_query, tuple(params))

        logger.info(f"✅ Returning {len(result) if result else 0} statuses")

        return jsonify({
            "success": True,
            "statuses": result if result else [],
            "count": len(result) if result else 0
        }), 200

    except Exception as err:
        logger.exception("❌ getDocumentStatuses error")
        return jsonify({
            "success": False,
            "error": "Failed to fetch document statuses",
            "details": str(err)
        }), 500
        
@app.route('/screen/menu-permissions', methods=['POST'])
def get_menu_permissions():
    """
    Get menu permissions for a specific menuId
    """
    data = request.json
    menu_id = data.get('menuId') if data else None

    try:
        if not menu_id:
            return jsonify({
                "success": False,
                "error": "menuId is required"
            }), 400

        # Get menu data from login API
        company_data = get_company_data()
        
        if not company_data or not company_data.get('menu'):
            return jsonify({
                "success": False,
                "error": "Could not fetch menu data"
            }), 500

        menu_item = None
        for item in company_data['menu']:
            if item.get('Menuid') == menu_id:
                menu_item = item
                break

        if menu_item:
            return jsonify({
                "success": True,
                "permissions": {
                    "isAdd": menu_item.get('isAdd', False),
                    "isEdit": menu_item.get('isEdit', False),
                    "isDelete": menu_item.get('isDelete', False),
                    "isPost": menu_item.get('isPost', False),
                    "isPrint": menu_item.get('isPrint', False),
                    "isSearch": menu_item.get('isSearch', False),
                    "isUpload": menu_item.get('isUpload', False),
                    "isCopy": menu_item.get('isCopy', False),
                    "isBackDate": menu_item.get('IsBackDate', False)
                }
            }), 200

        return jsonify({
            "success": False,
            "error": f"Menu item with ID {menu_id} not found"
        }), 404

    except Exception as err:
        logger.error(f"❌ getMenuPermissions error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to fetch menu permissions"
        }), 500


@app.route('/screen/update-employment-status', methods=['POST'])
def update_employment_status():
    """
    Post/Unpost records based on EmploymentStatus
    """
    data = request.json
    table_name = data.get('tableName') if data else None
    code = data.get('code') if data else None
    employment_status = data.get('employmentStatus') if data else None
    menu_id = data.get('menuId') if data else None

    try:
        if not table_name or not code or employment_status is None:
            return jsonify({
                "success": False,
                "error": "tableName, code, and employmentStatus are required"
            }), 400

        logger.info(f"📝 Updating EmploymentStatus for {table_name} code {code} to {employment_status}")

        # Get username from request context (you'll need to implement authentication)
        editby = "system"  # Replace with actual user from session/auth

        query = f"""
            UPDATE {table_name}
            SET EmploymentStatus = ?,
                editby = ?,
                editdate = GETDATE()
            WHERE Code = ?
        """

        execute_query(query, (employment_status, editby, code))

        # If menuId is provided, also update in tblDocumentStatus tracking if needed
        if menu_id:
            # Optional: Log the status change
            logger.info(f"Status updated for menuId: {menu_id}")

        return jsonify({
            "success": True,
            "message": f"Employment status updated to {employment_status}",
            "code": code,
            "employmentStatus": employment_status
        }), 200

    except Exception as err:
        logger.error(f"❌ updateEmploymentStatus error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to update employment status",
            "details": str(err)
        }), 500


@app.route('/screen/refresh-table-data', methods=['POST'])
def refresh_table_data():
    """
    Refresh table data with optional filters
    """
    data = request.json
    table_name = data.get('tableName') if data else None
    where = data.get('where') if data else None
    order_by = data.get('orderBy') if data else None
    use_pagination = data.get('usePagination') if data else None
    page = data.get('page') if data else None
    limit = data.get('limit') if data else None

    try:
        if not table_name:
            return jsonify({
                "success": False,
                "error": "tableName is required"
            }), 400

        logger.info(f"🔄 Refreshing data for table: {table_name}")

        query = f"SELECT * FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
        
        if order_by:
            query += f" ORDER BY {order_by}"

        # Add pagination if requested
        if use_pagination and page and limit:
            offset = (int(page) - 1) * int(limit)
            query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

        result = execute_query(query)

        return jsonify({
            "success": True,
            "rows": result,
            "count": len(result),
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as err:
        logger.error(f"❌ refreshTableData error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to refresh table data",
            "details": str(err)
        }), 500

# ===== DEBUG ENDPOINTS =====

@app.route('/debug/table-structure/<table_name>', methods=['GET'])
def debug_table_structure(table_name):
    """Get table structure to see actual column names and lengths"""
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        result = execute_query(query)
        return jsonify({
            "success": True,
            "table": table_name,
            "columns": result
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/check-table/<table_name>', methods=['GET'])
def check_table(table_name):
    """Check table structure"""
    try:
        columns = get_table_columns(table_name)
        lengths = get_column_lengths(table_name)
        return jsonify({
            "success": True,
            "table": table_name,
            "columns": columns,
            "column_lengths": lengths
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ===== EXISTING ROUTES =====

@app.route('/GetMenu', methods=['GET', 'POST', 'OPTIONS'])
def GetMenu():
    if request.method == 'OPTIONS':
        return '', 200
    
    if request.method == 'GET':
        username = request.args.get("username")
        userpassword = request.args.get("userpassword")
        menuid = request.args.get("Menuid")
        nooftables = int(request.args.get("nooftables", 0))
    else:
        data = request.json
        if not data:
            return jsonify({
                "status": "error",
                "message": "No JSON data provided"
            }), 400
        
        username = data.get("username")
        userpassword = data.get("userpassword")
        menuid = data.get("Menuid")
        nooftables = int(data.get("nooftables", 0))
   
    if not username or not userpassword:
        return jsonify({
            "status": "error",
            "message": "Username and password are required"
        }), 400
    
    if not menuid:
        menuid = "01"
    
    if not nooftables or nooftables < 1:
        nooftables = 3
    
    print(f"Processing request - Username: {username}, Menuid: {menuid}, Tables: {nooftables}")
    
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT uid
            FROM View_ComUsers
            WHERE uid <= 2000
              AND Userlogin = ?              
        """, (username,))

        row = cursor.fetchone()
        if not row:
            print(f"User not found: {username}")
            return jsonify({"status": "fail", "message": "Invalid user"}), 401

        user_id = row[0]
        print(f"User authenticated - User ID: {user_id}")

        response_data = {}

        for index in range(1, nooftables + 1):
            print(f"Executing stored procedure for table {index}")
            cursor.execute(
                "{CALL sp_Quick_Method_Index (?, ?, ?)}",
                (menuid, index, username)
            )
            
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
            print(f"Table {index}: {len(rows)} rows retrieved")

            table_data = []
            for r in rows:
                table_data.append(dict(zip(columns, r)))

            response_data[f"tbl{index}"] = table_data
        
        print(f"Successfully processed all tables")
        return jsonify({
            "status": "success",
            "data": response_data
        })

    except pyodbc.Error as db_ex:
        print(f"Database error: {str(db_ex)}")
        return jsonify({
            "status": "error",
            "message": f"Database error: {str(db_ex)}"
        }), 500
    except Exception as ex:
        print(f"General error: {str(ex)}")
        return jsonify({
            "status": "error",
            "message": str(ex)
        }), 500

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route('/gl_voucher_generation_status', methods=["POST", "OPTIONS"])
def gl_voucher_generation_status():
    if request.method == 'OPTIONS':
        return '', 200
    
    data = request.json
    if not data:
        return jsonify({"success": False, "message": "No data provided"}), 400
    
    sp_name = data.get("sp_name")
    vockey = data.get("vockey")
    offcode = data.get("offcode")
    bcode = data.get("bcode")
    vtype = data.get("vtype")
    ostatus = int(data.get("ostatus", 0))
    posted_by = data.get("posted_by")
    
    if not all([sp_name, vockey, offcode, bcode, vtype, posted_by]):
        return jsonify({"success": False, "message": "Missing required parameters"}), 400
  
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        sql = f"EXEC {sp_name} ?, ?, ?, ?, ?, ?"
        params = (vockey, offcode, bcode, vtype, ostatus, posted_by)
        
        cursor.execute(sql, params)
        conn.commit()

        return jsonify({
            "success": True
        })
      
    except Exception as ex:
        print("GL Voucher Error:", ex)
        return jsonify({
            "success": False,
            "message": str(ex)
        }), 500

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route('/GetVno', methods=['GET', 'OPTIONS'])
def GetVno():
    if request.method == 'OPTIONS':
        return '', 200
    
    Tablename = request.args.get("Tablename")
    Vdate = request.args.get("Vdate")
    Vtype = request.args.get("Vtype")
    Offcode = request.args.get("Offcode")
    Bcode = request.args.get("Bcode")

    conn = None
    cursor = None

    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        sql = f"EXEC dbo.spGetVno ?, ?, ?, ?, ?"
        params = (Tablename, Vdate, Vtype, Offcode, Bcode)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            return jsonify({"status": "fail", "message": "Invalid Voucher No"}), 401
              
        return jsonify({
            "status": "success",
            "vno": row[0]
        }), 200

    except Exception as ex:
        return jsonify({
            "status": "error",
            "message": str(ex)
        }), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== GL VOUCHER Generation (Final) ==================
@app.route('/gl_Posting', methods=["POST", "OPTIONS"])
def gl_Posting():
    if request.method == 'OPTIONS':
        # Handle preflight request
        return '', 200
    data = request.json or {}

    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        sql = f"EXEC Posting ?, ?, ?, ?, ?, ?"
        params = (
            data.get("vockey"),
            data.get("offcode"),
            data.get("bcode"),
            data.get("vtype"),
            int(data.get("ostatus", 0)),
            data.get("posted_by")
        )

        cursor.execute(sql, params)
        conn.commit()

        return jsonify({"success": True}), 200

    except Exception as ex:
        return jsonify({"success": False, "error": str(ex)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ================== ItemLedger and BIS Update (Final) ==================
@app.route('/stk_Posting', methods=["POST", "OPTIONS"])
def stk_Posting():
    if request.method == 'OPTIONS':
        # Handle preflight request
        return '', 200
    data = request.json or {}

    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        sql = f"EXEC Post_SP_ItemLedgerUpdate ?, ?, ?, ?, ?, ?"
        params = (
            data.get("vockey"),
            data.get("offcode"),
            data.get("bcode"),
            data.get("vtype"),
            int(data.get("ostatus", 0)),
            data.get("posted_by")
        )

        cursor.execute(sql, params)
        conn.commit()

        return jsonify({"success": True}), 200

    except Exception as ex:
        return jsonify({"success": False, "error": str(ex)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/FillTable', methods=['GET', 'POST', 'OPTIONS'])
def FillTable():
    if request.method == 'OPTIONS':
        return '', 200
    
    if request.method == 'GET':
        Tablename = request.args.get("Tablename")
        Offcode = request.args.get("Offcode")
        Bcode = request.args.get("Bcode")
    else:
        data = request.json
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        
        Tablename = data.get("Tablename")
        Offcode = data.get("Offcode")
        Bcode = data.get("Bcode")
    
    if not Tablename:
        return jsonify({"status": "fail", "message": "Tablename is required"}), 400
           
    conn = None
    cursor = None

    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        cursor.execute(
            "{CALL sp_FillTable_Method_Index (?, ?, ?)}",
            (Tablename, Offcode, Bcode)
        )
        
        row = cursor.fetchone()
        if not row:
            return jsonify({"status": "fail", "message": "No data found"}), 404

        TableQuery = row[0]

        return jsonify({
            "status": "success",
            "TableQuery": TableQuery
        }), 200 
    
    except Exception as ex:
        return jsonify({
            "status": "error",
            "message": str(ex)
        }), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ===== TABLE STRUCTURE & HEADERS APIs =====

@app.route('/get-table-headers', methods=['POST', 'OPTIONS'])
def get_table_headers():
    """Get column headers and sample data from a table"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        query = f"SELECT TOP(1) * FROM {table_name}"
        
        rows = execute_soap_query(query)
        first_row = rows[0] if rows else {}
        fields = {}
        
        for key in first_row.keys():
            val = first_row[key][0] if first_row.get(key) and len(first_row[key]) > 0 else ""
            fields[key] = {
                "value": val,
                "type": guess_type(val)
            }
        
        return jsonify({"success": True, "fields": fields})
    
    except Exception as err:
        print("Error in /get-table-headers:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@app.route('/get-table-structure', methods=['POST', 'OPTIONS'])
def get_table_structure():
    """Get detailed column structure from INFORMATION_SCHEMA"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        result = get_table_structure_data(table_name)
        return jsonify(result)
    
    except Exception as err:
        print("Error in /get-table-structure:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@app.route('/get-table-relationships', methods=['POST', 'OPTIONS'])
def get_table_relationships():
    """Get foreign key relationships for a table"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        query = f"""
            SELECT 
                fk.name AS 'ConstraintName',
                OBJECT_NAME(fk.parent_object_id) AS 'TableName',
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS 'ColumnName',
                OBJECT_NAME(fk.referenced_object_id) AS 'ReferencedTable',
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS 'ReferencedColumn'
            FROM 
                sys.foreign_keys AS fk
            INNER JOIN 
                sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
            WHERE 
                OBJECT_NAME(fk.parent_object_id) = '{table_name}'
        """
        
        relationships = execute_soap_query(query)
        
        formatted_relationships = []
        for rel in relationships:
            formatted_relationships.append({
                "ConstraintName": rel.get("ConstraintName", [""])[0],
                "TableName": rel.get("TableName", [""])[0],
                "ColumnName": rel.get("ColumnName", [""])[0],
                "ReferencedTable": rel.get("ReferencedTable", [""])[0],
                "ReferencedColumn": rel.get("ReferencedColumn", [""])[0]
            })
        
        return jsonify({"success": True, "relationships": formatted_relationships})
    
    except Exception as err:
        print("Error in /get-table-relationships:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@app.route('/get-table-data', methods=['POST', 'OPTIONS'])
def get_table_data():
    """Get paginated table data with optional WHERE clause and company filtering"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        frontend_where = data.get("where", "")
        page = int(data.get("page", 1))
        limit = int(data.get("limit", 10))
        use_pagination = data.get("usePagination", False)
        company_offcode = data.get("companyData", {}).get("company", {}).get("offcode") if data.get("companyData") else None
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        where_clauses = []
        
        if company_offcode:
            where_clauses.append(f"offcode = '{company_offcode}'")
        
        if frontend_where:
            where_clauses.append(f"({frontend_where})")
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        rows = []
        total_count = 0
        
        if use_pagination:
            start_row = (page - 1) * limit + 1
            end_row = page * limit
            
            data_query = f"""
                WITH Paginated AS (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
                    FROM {table_name}
                    {where_sql}
                )
                SELECT * FROM Paginated
                WHERE rn BETWEEN {start_row} AND {end_row}
            """
            count_query = f"SELECT COUNT(*) AS total FROM {table_name} {where_sql}"
            
            print("📤 DATA QUERY:", data_query)
            print("📤 COUNT QUERY:", count_query)
            
            result_data = execute_soap_query(data_query)
            result_count = execute_soap_query(count_query)
            
            rows = result_data
            total_count = int(result_count[0].get("total", [0])[0]) if result_count else 0
        else:
            data_query = f"SELECT * FROM {table_name} {where_sql}"
            print("📤 DATA QUERY:", data_query)
            rows = execute_soap_query(data_query)
            total_count = len(rows)
        
        json_rows = []
        for row in rows:
            obj = {}
            for key in row.keys():
                obj[key] = row[key][0] if row.get(key) and len(row[key]) > 0 else ""
            json_rows.append(obj)
        
        return jsonify({
            "success": True,
            "rows": json_rows,
            "totalCount": total_count,
            "page": page if use_pagination else 1,
            "limit": limit if use_pagination else total_count,
            "totalPages": math.ceil(total_count / limit) if use_pagination and limit > 0 else 1,
            "offcodeApplied": bool(company_offcode),
            "usePagination": use_pagination
        })
    
    except Exception as err:
        print("❌ getTableData fatal error:", err)
        return jsonify({"success": False, "error": str(err)}), 500


# ===== GENERIC CRUD APIs =====

@app.route('/table/insert', methods=['POST', 'OPTIONS'])
def generic_insert():
    """
    Generic INSERT API for any table
    """
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        row_data = data.get("data", {})
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        if not row_data:
            return jsonify({"success": False, "error": "data object is required"}), 400
        
        # Get identity column to exclude from INSERT
        identity_column = get_table_identity_column(table_name)
        
        # Get primary keys for reference
        primary_keys = get_table_primary_keys(table_name)
        
        # Validate columns
        is_valid, message = validate_columns(table_name, row_data)
        if not is_valid:
            return jsonify({"success": False, "error": message}), 400
        
        # Filter out identity column
        columns_to_insert = []
        values_to_insert = []
        params = []
        
        for column, value in row_data.items():
            if identity_column and column.lower() == identity_column.lower():
                continue
            columns_to_insert.append(column)
            values_to_insert.append("?")
            params.append(value if value != "" else None)
        
        if not columns_to_insert:
            return jsonify({"success": False, "error": "No valid columns to insert"}), 400
        
        # Build INSERT query
        columns_str = ", ".join(columns_to_insert)
        placeholders = ", ".join(values_to_insert)
        
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        print("📝 INSERT QUERY:", insert_query)
        print("📝 PARAMS:", params)
        
        # Execute INSERT
        rows_affected = execute_non_query(insert_query, params)
        
        # Get the newly inserted record
        new_record = None
        if primary_keys and len(primary_keys) > 0:
            where_conditions = []
            for pk in primary_keys:
                if pk in row_data:
                    where_conditions.append(f"{pk} = '{row_data[pk]}'")
            
            if where_conditions:
                select_query = f"SELECT * FROM {table_name} WHERE {' AND '.join(where_conditions)}"
                result = execute_soap_query(select_query)
                if result:
                    new_record = {}
                    for key in result[0].keys():
                        new_record[key] = result[0][key][0] if result[0].get(key) and len(result[0][key]) > 0 else ""
        
        return jsonify({
            "success": True,
            "message": f"Record inserted successfully into {table_name}",
            "rowsAffected": rows_affected,
            "insertedData": new_record or row_data,
            "primaryKeys": primary_keys,
            "identityColumn": identity_column
        }), 200
    
    except Exception as err:
        print("❌ Generic Insert Error:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@app.route('/table/update', methods=['POST', 'OPTIONS'])
def generic_update():
    """
    Generic UPDATE API for any table
    """
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        row_data = data.get("data", {})
        where_conditions = data.get("where", {})
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        if not row_data:
            return jsonify({"success": False, "error": "data object is required"}), 400
        
        # Get primary keys for default WHERE clause if not provided
        primary_keys = get_table_primary_keys(table_name)
        
        # If where conditions not provided, try to use primary keys
        if not where_conditions and primary_keys:
            for pk in primary_keys:
                if pk in row_data:
                    where_conditions[pk] = row_data[pk]
        
        if not where_conditions:
            return jsonify({
                "success": False, 
                "error": "WHERE conditions required. Provide 'where' object or ensure primary keys are in data"
            }), 400
        
        # Validate columns
        all_columns = {**row_data, **where_conditions}
        is_valid, message = validate_columns(table_name, all_columns)
        if not is_valid:
            return jsonify({"success": False, "error": message}), 400
        
        # Build SET clause
        set_clauses = []
        params = []
        
        for column, value in row_data.items():
            if column in where_conditions:
                continue
            set_clauses.append(f"{column} = ?")
            params.append(value if value != "" else None)
        
        if not set_clauses:
            return jsonify({"success": False, "error": "No columns to update"}), 400
        
        # Build WHERE clause
        where_clauses = []
        for column, value in where_conditions.items():
            where_clauses.append(f"{column} = ?")
            params.append(value)
        
        where_str = " AND ".join(where_clauses)
        
        # Build UPDATE query
        set_str = ", ".join(set_clauses)
        update_query = f"UPDATE {table_name} SET {set_str} WHERE {where_str}"
        
        print("📝 UPDATE QUERY:", update_query)
        print("📝 PARAMS:", params)
        
        # Execute UPDATE
        rows_affected = execute_non_query(update_query, params)
        
        # Get the updated record
        updated_record = None
        select_where = " AND ".join([f"{col} = '{val}'" for col, val in where_conditions.items()])
        select_query = f"SELECT * FROM {table_name} WHERE {select_where}"
        result = execute_soap_query(select_query)
        
        if result:
            updated_record = {}
            for key in result[0].keys():
                updated_record[key] = result[0][key][0] if result[0].get(key) and len(result[0][key]) > 0 else ""
        
        return jsonify({
            "success": True,
            "message": f"Record updated successfully in {table_name}",
            "rowsAffected": rows_affected,
            "updatedData": updated_record,
            "whereConditions": where_conditions
        }), 200
    
    except Exception as err:
        print("❌ Generic Update Error:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@app.route('/table/upsert', methods=['POST', 'OPTIONS'])
def generic_upsert():
    """
    Generic UPSERT (INSERT or UPDATE) API
    """
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        row_data = data.get("data", {})
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        if not row_data:
            return jsonify({"success": False, "error": "data object is required"}), 400
        
        # Get primary keys
        primary_keys = get_table_primary_keys(table_name)
        
        if not primary_keys:
            # If no primary keys, default to insert
            return generic_insert_logic(table_name, row_data)
        
        # Check if record exists
        where_conditions = {}
        for pk in primary_keys:
            if pk in row_data:
                where_conditions[pk] = row_data[pk]
        
        if not where_conditions:
            # No primary key values provided, assume insert
            return generic_insert_logic(table_name, row_data)
        
        # Build check query
        where_clauses = [f"{col} = '{val}'" for col, val in where_conditions.items()]
        check_query = f"SELECT COUNT(*) as count FROM {table_name} WHERE {' AND '.join(where_clauses)}"
        
        result = execute_soap_query(check_query)
        record_exists = int(result[0].get("count", [0])[0]) > 0
        
        if record_exists:
            # Perform UPDATE
            update_data = {k: v for k, v in row_data.items() if k not in where_conditions}
            return generic_update_logic(table_name, update_data, where_conditions)
        else:
            # Perform INSERT
            return generic_insert_logic(table_name, row_data)
    
    except Exception as err:
        print("❌ Generic Upsert Error:", err)
        return jsonify({"success": False, "error": str(err)}), 500

def generic_insert_logic(table_name, row_data):
    """Helper function for INSERT logic"""
    identity_column = get_table_identity_column(table_name)
    
    columns_to_insert = []
    values_to_insert = []
    params = []
    
    for column, value in row_data.items():
        if identity_column and column.lower() == identity_column.lower():
            continue
        columns_to_insert.append(column)
        values_to_insert.append("?")
        params.append(value if value != "" else None)
    
    columns_str = ", ".join(columns_to_insert)
    placeholders = ", ".join(values_to_insert)
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    rows_affected = execute_non_query(insert_query, params)
    
    return jsonify({
        "success": True,
        "operation": "insert",
        "message": f"Record inserted successfully into {table_name}",
        "rowsAffected": rows_affected,
        "insertedData": row_data
    }), 200

def generic_update_logic(table_name, update_data, where_conditions):
    """Helper function for UPDATE logic"""
    set_clauses = []
    params = []
    
    for column, value in update_data.items():
        set_clauses.append(f"{column} = ?")
        params.append(value if value != "" else None)
    
    where_clauses = []
    for column, value in where_conditions.items():
        where_clauses.append(f"{column} = ?")
        params.append(value)
    
    set_str = ", ".join(set_clauses)
    where_str = " AND ".join(where_clauses)
    update_query = f"UPDATE {table_name} SET {set_str} WHERE {where_str}"
    
    rows_affected = execute_non_query(update_query, params)
    
    return jsonify({
        "success": True,
        "operation": "update",
        "message": f"Record updated successfully in {table_name}",
        "rowsAffected": rows_affected,
        "updatedData": {**where_conditions, **update_data},
        "whereConditions": where_conditions
    }), 200

@app.route('/table/delete', methods=['POST', 'OPTIONS'])
def generic_delete():
    """
    Generic DELETE API for any table
    """
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        where_conditions = data.get("where", {})
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        if not where_conditions:
            return jsonify({
                "success": False, 
                "error": "WHERE conditions required for DELETE operation"
            }), 400
        
        # Validate columns
        is_valid, message = validate_columns(table_name, where_conditions)
        if not is_valid:
            return jsonify({"success": False, "error": message}), 400
        
        # Build WHERE clause
        where_clauses = []
        params = []
        
        for column, value in where_conditions.items():
            where_clauses.append(f"{column} = ?")
            params.append(value)
        
        where_str = " AND ".join(where_clauses)
        
        # Build DELETE query
        delete_query = f"DELETE FROM {table_name} WHERE {where_str}"
        
        print("📝 DELETE QUERY:", delete_query)
        print("📝 PARAMS:", params)
        
        # Execute DELETE
        rows_affected = execute_non_query(delete_query, params)
        
        return jsonify({
            "success": True,
            "message": f"Record(s) deleted successfully from {table_name}",
            "rowsAffected": rows_affected,
            "whereConditions": where_conditions
        }), 200
    
    except Exception as err:
        print("❌ Generic Delete Error:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@app.route('/table/bulk-insert', methods=['POST', 'OPTIONS'])
def generic_bulk_insert():
    """
    Generic BULK INSERT API for inserting multiple records at once
    """
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        rows_data = data.get("data", [])
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        if not rows_data or not isinstance(rows_data, list):
            return jsonify({"success": False, "error": "data array is required"}), 400
        
        identity_column = get_table_identity_column(table_name)
        results = []
        success_count = 0
        error_count = 0
        
        for idx, row_data in enumerate(rows_data):
            try:
                # Validate columns
                is_valid, message = validate_columns(table_name, row_data)
                if not is_valid:
                    results.append({
                        "row": idx,
                        "success": False,
                        "error": message
                    })
                    error_count += 1
                    continue
                
                # Filter out identity column
                columns_to_insert = []
                values_to_insert = []
                params = []
                
                for column, value in row_data.items():
                    if identity_column and column.lower() == identity_column.lower():
                        continue
                    columns_to_insert.append(column)
                    values_to_insert.append("?")
                    params.append(value if value != "" else None)
                
                if columns_to_insert:
                    columns_str = ", ".join(columns_to_insert)
                    placeholders = ", ".join(values_to_insert)
                    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    rows_affected = execute_non_query(insert_query, params)
                    
                    results.append({
                        "row": idx,
                        "success": True,
                        "rowsAffected": rows_affected,
                        "data": row_data
                    })
                    success_count += 1
                else:
                    results.append({
                        "row": idx,
                        "success": False,
                        "error": "No valid columns to insert"
                    })
                    error_count += 1
                    
            except Exception as row_err:
                results.append({
                    "row": idx,
                    "success": False,
                    "error": str(row_err)
                })
                error_count += 1
        
        return jsonify({
            "success": True,
            "message": f"Bulk insert completed. {success_count} successful, {error_count} failed",
            "totalProcessed": len(rows_data),
            "successCount": success_count,
            "errorCount": error_count,
            "results": results
        }), 200
    
    except Exception as err:
        print("❌ Generic Bulk Insert Error:", err)
        return jsonify({"success": False, "error": str(err)}), 500

# ===== MAIN EMPLOYEE INSERT ENDPOINT =====

@app.route('/insert-EmployeeHeadDet', methods=['POST', 'OPTIONS'])
def insert_employee_head_det():
    """Insert employee with all details"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        print("=" * 60)
        print("📥 INSERT EMPLOYEE HEAD DET REQUEST RECEIVED")
        print("=" * 60)
        
        # Extract data
        head = data.get("head")
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch")
        
        if not head:
            return jsonify({"success": False, "error": "Head object is required"}), 400
        
        # Get company data
        company_data = get_company_data()
        company_offcode = company_data.get("company", {}).get("offcode", "0101")
        
        # Get branch code
        branch_code = None
        if company_data.get("branches") and len(company_data["branches"]) > 0:
            if selected_branch:
                selected = next((b for b in company_data["branches"] if b.get("branch") == selected_branch), None)
                branch_code = selected.get("code") if selected else company_data["branches"][0].get("code")
            else:
                branch_code = company_data["branches"][0].get("code")
        
        # Get head table and data
        head_table = head.get('tableName', 'HRMSEmployee')
        head_data = head.get('data', {})
        
        emp_code = head_data.get("Code")
        if not emp_code:
            return jsonify({"success": False, "error": "Employee Code is required"}), 400
        
        createdby = head_data.get("createdby")
        if not createdby:
            createdby = "admin"
        
        # Get user ID
        uid = get_user_uid(createdby)
        print(f"👤 User: {createdby}, UID: {uid}")
        
        # Current date for createdate/editdate
        current_date = format_date_for_sql()
        
        # Prepare head row
        head_row = {}
        
        # Map fields according to database schema
        field_mapping = {
            # Personal Information
            "FName": "FName",
            "MName": "MName",
            "LName": "LName",
            "arName": "arName",
            "FatherName": "FatherName",
            "DOB": "DOB",
            "Gender": "Gender",
            "MarriadStatus": "MarriadStatus",
            "Nationality": "Nationality",
            "Religin": "Religin",
            "IDNo": "IDNo",
            "IDExpiryDate": "IDExpiryDate",
            "PassportNo": "PassportNo",
            "PassportExpiryDate": "PassportExpiryDate",
            
            # Contact Information
            "Mobile": "P_Mobile",
            "Email": "P_Email",
            
            # Permanent Address
            "P_Address": "P_Address",
            "P_City": "P_City",
            "P_Provience": "P_Provience",
            "P_Country": "P_Country",
            "P_PostalCode": "P_PostalCode",
            "P_ZipCode": "P_ZipCode",
            "P_Phone": "P_Phone",
            "P_Mobile": "P_Mobile",
            "P_Email": "P_Email",
            "P_ContactPerson": "P_ContactPerson",
            
            # Home Address
            "H_Address": "H_Address",
            "H_City": "H_City",
            "H_Provience": "H_Provience",
            "H_Country": "H_Country",
            "H_PostalCode": "H_PostalCode",
            "H_ZipCode": "H_ZipCode",
            "H_Phone": "H_Phone",
            "H_Mobile": "H_Mobile",
            "H_Email": "H_Email",
            "H_ContactPerson": "H_ContactPerson",
            
            # Employment Details
            "DepartmentCode": "DepartmentCode",
            "DesignationCode": "DesignationCode",
            "GradeCode": "GradeCode",
            "ShiftCode": "ShiftCode",
            "EmployeeLocationCode": "EmployeeLocationCode",
            "JobTitle": "JobTitle",
            "ManagerCode": "ManagerCode",
            "DepartmentHead": "DepartmentHead",
            "Subtitute": "Subtitute",
            "EmployeeReplacementCode": "EmployeeReplacementCode",
            
            # Dates
            "JoiningDate": "JoiningDate",
            "AppointmentDate": "AppointmentDate",
            "ProbitionDate": "ProbitionDate",
            "ContractStartDate": "ContractStartDate",
            "ContractEndDate": "ContractEndDate",
            "LeftDate": "LeftDate",
            
            # Contract & Salary
            "ContractType": "ContractType",
            "EmploymentStatus": "EmploymentStatus",
            "SalaryMode": "SalaryMode",
            "BasicPay": "BasicPay",
            "GrossPay": "GrossPay",
            "PerDayAvgCap": "PerDayAvgCap",
            
            # Banking
            "BankCode": "BankCode",
            "AccountNo": "AccountNo",
            "CardNo": "CardNo",
            "MachineRegistrationNo": "MachineRegistrationNo",
            
            # Benefits
            "EOBINo": "EOBINo",
            "IsEOBI": "IsEOBI",
            "SocialSecurityNo": "SocialSecurityNo",
            "IsSocialSecuirty": "IsSocialSecuirty",
            "ProvidentFundNo": "ProvidentFundNo",
            
            # Attendance Settings
            "offdayBonusAllow": "offdayBonusAllow",
            "AutoAttendanceAllow": "AutoAttendanceAllow",
            "OverTimeAllow": "OverTimeAllow",
            "LateTimeAllow": "LateTimeAllow",
            "EarlyLateAllow": "EarlyLateAllow",
            "HolyDayBonusAllow": "HolyDayBonusAllow",
            "PunctuailityAllown": "PunctuailityAllown",
            "EarlyLateNoofDeductionExempt": "EarlyLateNoofDeductionExempt",
            "OTAllowedPerDay": "OTAllowedPerDay",
            "NoOfDependant": "NoOfDependant",
            
            # Commission
            "EmployeeCommisionBonusActive": "EmployeeCommisionBonusActive",
            "EmployeeCommisionBonusPer": "EmployeeCommisionBonusPer",
            "EmployeeEarlyLateDeductionOnTimeActive": "EmployeeEarlyLateDeductionOnTimeActive",
            "EmployeePerDayType": "EmployeePerDayType",
            
            # Status Flags
            "IsActive": "IsActive",
            "isManagerFilter": "isManagerFilter",
            "isUserFilter": "isUserFilter",
            
            # Job Description
            "MainJobDuty": "MainJobDuty",
            "SecondryJobDuty": "SecondryJobDuty",
            "Remarks": "Remarks",
            
            # Reference
            "RefNo": "RefNo",
            "HRDocNo": "HRDocNo",
            "BarCode": "BarCode",
            "BenifitCode": "BenifitCode",
            "CheckInState": "CheckInState",
            "CheckOutState": "CheckOutState",
            
            # User Account
            "MUID": "MUID",
            "MUserlogin": "MUserlogin",
            "MUserpassword": "MUserpassword",
            
            # Images
            "pictureimg": "pictureimg",
            "pictureURL": "pictureURL"
        }
        
        # Apply mapping and handle values
        for json_field, db_field in field_mapping.items():
            if json_field in head_data:
                val = head_data[json_field]
                
                # Handle different value types
                if val is None:
                    head_row[db_field] = None
                elif val == "":
                    head_row[db_field] = ""
                elif isinstance(val, bool):
                    head_row[db_field] = 1 if val else 0
                elif isinstance(val, (int, float)):
                    head_row[db_field] = val
                elif isinstance(val, str):
                    if val.upper() in ["TRUE", "FALSE"]:
                        head_row[db_field] = 1 if val.upper() == "TRUE" else 0
                    else:
                        head_row[db_field] = val
                else:
                    head_row[db_field] = str(val)
        
        # Add system fields
        head_row["Code"] = emp_code
        head_row["offcode"] = company_offcode
        head_row["bcode"] = branch_code or "010101"
        head_row["compcode"] = "01"
        head_row["createdby"] = createdby
        head_row["createdate"] = current_date
        head_row["uid"] = uid
        
        # Ensure IsActive is properly set
        if "IsActive" in head_row:
            if isinstance(head_row["IsActive"], str):
                head_row["IsActive"] = 1 if head_row["IsActive"].upper() in ["TRUE", "1", "YES", "ON"] else 0
        else:
            head_row["IsActive"] = 1
        
        # Filter to only valid columns for the table (excluding computed columns)
        head_row = filter_valid_columns(head_table, head_row, exclude_computed=True)
        
        print(f"📋 Head row prepared with {len(head_row)} columns")
        print(f"📋 Columns: {list(head_row.keys())}")
        
        # Check for potential truncation issues
        column_lengths = get_column_lengths(head_table)
        for col, val in head_row.items():
            if col in column_lengths and isinstance(val, str):
                if len(val) > column_lengths[col]:
                    print(f"⚠️ WARNING: Column '{col}' length {len(val)} exceeds max {column_lengths[col]}")
        
        # Build and execute head insert query
        head_query = build_insert_query(head_table, [head_row])
        
        if not head_query:
            return jsonify({"success": False, "error": "Failed to build insert query"}), 400
        
        print("📝 HEAD QUERY:", head_query)
        
        # Execute head insert
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            cursor.execute(head_query)
            conn.commit()
            print("✅ Head inserted successfully!")
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"❌ Head insert failed: {e}")
            return jsonify({"success": False, "error": str(e)}), 500
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        
        # Process details
        detail_results = []
        
        for det_idx, det in enumerate(details):
            table_name = det.get("tableName")
            rows = det.get("rows", [])
            
            if not table_name or not rows:
                print(f"⚠️ Skipping detail {det_idx}: missing tableName or rows")
                continue
            
            print(f"📋 Processing detail: {table_name} with {len(rows)} rows")
            
            processed_rows = []
            for row in rows:
                # Flatten attendanceSpec if present
                if "attendanceSpec" in row and row["attendanceSpec"]:
                    att_spec = row["attendanceSpec"]
                    row["OverTimeAllow"] = att_spec.get("OverTimeAllow", False)
                    row["LateTimeAllow"] = att_spec.get("LateTimeAllow", False)
                    row["EarlyLateAllow"] = att_spec.get("EarlyLateAllow", False)
                    row["AutoAttendanceAllow"] = att_spec.get("AutoAttendanceAllow", False)
                    row["offdayBonusAllow"] = att_spec.get("offdayBonusAllow", False)
                    row["HolyDayBonusAllow"] = att_spec.get("HolyDayBonusAllow", False)
                    row["NoOfExempt"] = att_spec.get("NoOfExempt", 0)
                    if "attendanceSpec" in row:
                        del row["attendanceSpec"]
                
                # Add required fields
                row["Code"] = emp_code
                row["offcode"] = company_offcode
                
                # Handle boolean fields
                for key in ["OverTimeAllow", "LateTimeAllow", "EarlyLateAllow", 
                           "AutoAttendanceAllow", "offdayBonusAllow", "HolyDayBonusAllow", "IsActive"]:
                    if key in row:
                        if isinstance(row[key], str):
                            row[key] = 1 if row[key].upper() in ["TRUE", "1", "YES", "ON"] else 0
                        elif isinstance(row[key], bool):
                            row[key] = 1 if row[key] else 0
                
                # Filter to valid columns for this detail table (excluding computed columns)
                filtered_row = filter_valid_columns(table_name, row, exclude_computed=True)
                processed_rows.append(filtered_row)
            
            if processed_rows:
                det_query = build_insert_query(table_name, processed_rows)
                if det_query:
                    detail_results.append({
                        "table": table_name,
                        "row_count": len(processed_rows),
                        "query": det_query
                    })
                    print(f"✅ Prepared query for {table_name}")
        
        # Execute detail queries in a transaction
        if detail_results:
            conn = None
            cursor = None
            try:
                conn = pyodbc.connect(CONN_STR)
                cursor = conn.cursor()
                
                for detail in detail_results:
                    print(f"📝 Executing detail query for {detail['table']}")
                    cursor.execute(detail['query'])
                
                conn.commit()
                print(f"✅ Executed {len(detail_results)} detail queries")
            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"❌ Detail queries failed: {e}")
                return jsonify({"success": False, "error": str(e)}), 500
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        
        # Get inserted record for response
        select_query = f"SELECT * FROM {head_table} WHERE Code = '{emp_code}'"
        try:
            result = execute_query(select_query)
            inserted_record = result[0] if result else None
        except Exception as e:
            print(f"⚠️ Could not fetch inserted record: {e}")
            inserted_record = None
        
        return jsonify({
            "success": True,
            "message": "Employee and related details inserted successfully",
            "empCode": emp_code,
            "offcode": company_offcode,
            "uid": uid,
            "insertedRecord": inserted_record,
            "details_processed": len(detail_results)
        }), 200
        
    except Exception as err:
        print("❌ ERROR in insert_employee_head_det:")
        print(str(err))
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(err)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
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

@app.route('/', methods=['GET'])
def index():
    """Root endpoint with API information"""
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

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Starting SmartGold ERP API Server v2.0")
    print("=" * 60)
    print("📍 Server will run on: http://0.0.0.0:8000")
    print("\n📋 AVAILABLE ENDPOINTS:")
    print("  ────────────────────────────────────────────")
    print("  🔧 EXISTING ENDPOINTS:")
    print("    GET  /              - API Information")
    print("    POST /GetMenu       - Get user menu")
    print("    GET  /GetVno        - Get voucher number")
    print("    POST /FillTable     - Fill table data")
    print("    POST /gl_voucher_generation_status - Update GL voucher status")
    print("\n  📊 TABLE STRUCTURE ENDPOINTS:")
    print("    POST /get-table-headers - Get column headers")
    print("    POST /get-table-structure - Get table structure")
    print("    POST /get-table-relationships - Get relationships")
    print("    POST /get-table-data - Get paginated data")
    print("\n  👔 EMPLOYEE MANAGEMENT:")
    print("    📋 POST /insert-EmployeeHeadDet - Insert employee with all details")
    print("\n  ✨ GENERIC CRUD ENDPOINTS:")
    print("    📥 POST /table/insert  - Generic INSERT for any table")
    print("    📝 POST /table/update  - Generic UPDATE for any table")
    print("    🔄 POST /table/upsert  - Generic UPSERT")
    print("    🗑️  POST /table/delete  - Generic DELETE")
    print("   POST   /screen/get-config")
    print("   POST   /screen/document-statuses")
    print("   POST   /screen/menu-permissions")
    print("   POST   /screen/update-employment-status")
    print("   POST   /screen/refresh-table-data")
    print("    📦 POST /table/bulk-insert - Bulk insert multiple records")
    print("\n  ❤️  HEALTH ENDPOINT:")
    print("    GET  /health        - Health check")
    print("=" * 60)
    app.run(host="0.0.0.0", port=8000, debug=True)