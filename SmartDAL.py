from flask import Flask, request, jsonify
from flask_cors import CORS
import pyodbc
from datetime import datetime
import math

app = Flask(__name__)

# ================== CORS CONFIGURATION ==================
CORS(
    app,
    resources={r"/*": {"origins": "*"}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "OPTIONS"]
)

@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    return response

# ================== DATABASE CONFIG ==================
CONN_STR = (
    "DRIVER={SQL Server};"
    "SERVER=192.168.100.103;"
    "DATABASE=SmartGold;"
    "UID=sa;"
    "PWD=786"
)
# ================== GET SOAP Query (Final)==================
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
                # Format each value as a list with single element to match SOAP response format
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

# ================== GET MENU (Final)==================

@app.route('/GetMenu', methods=['GET', 'POST', 'OPTIONS'])
def GetMenu():
    if request.method == 'OPTIONS':
        # Handle preflight request
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
   
    # Validate required parameters
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

        # ===== USER VALIDATION =====
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

        # ===== LOOP STORED PROCEDURE =====
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

# ================== FILL TABLE (Final)==================
@app.route('/FillTable', methods=['GET', 'POST', 'OPTIONS'])
def FillTable():
    if request.method == 'OPTIONS':
        # Handle preflight request
        return '', 200
    if request.method == 'GET':
        Tablename = request.args.get("Tablename")
        Select = request.args.get("Select")
        Where = request.args.get("Where")
    else:
        body = request.json or {}
        Tablename = body.get("Tablename")
        Select = body.get("Select")
        Where = body.get("Where")

    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        cursor.execute(
            "{CALL sp_FillTable_Method_Index (?, ?, ?)}",
            (Tablename, Select, Where)
        )

        rows = cursor.fetchall()
        if not rows:
            return jsonify({"status": "fail", "message": "No data found"}), 404

        columns = [col[0] for col in cursor.description]

        result = [dict(zip(columns, row)) for row in rows]

        return jsonify({
            "status": "success",
            "data": result
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

# ================== GET VOUCHER NO (Final)==================
@app.route('/GetVno', methods=['GET', 'POST', 'OPTIONS'])
def GetVno():
    if request.method == 'OPTIONS':
        # Handle preflight request
        return '', 200  
    if request.method == 'GET':
        Tablename =  request.args.get("Tablename")
        vdate = request.args.get("Vdate")
        Vtype = request.args.get("Vtype")
        Offcode = request.args.get("Offcode")
        Bcode = request.args.get("Bcode")
    else:
        body = request.json or {}
        Tablename =  body.get("Tablename")
        vdate = datetime.strptime(body.get("Vdate"),"%Y/%m/%d")
        Vtype = body.get("Vtype")
        Offcode = body.get("Offcode")
        Bcode = body.get("Bcode")
        
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        sql = "EXEC dbo.spGetVno ?, ?, ?, ?, ? "
        params = (Tablename,vdate,Vtype,Offcode,Bcode)

        cursor.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            return jsonify({"status": "fail", "message": "Invalid Voucher No"}), 401

        return jsonify({"status": "success", "vno": row[0]}), 200
        

    except Exception as ex:
        return jsonify({"status": "error", "message": str(ex)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/get_table_structure', methods=['POST', 'OPTIONS'])
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

        query1 = f"""
            SELECT 
                kc.name AS PrimaryKeyName,
                c.name  AS ColumnName
                FROM sys.key_constraints kc
                JOIN sys.index_columns ic 
                ON kc.parent_object_id = ic.object_id
                AND kc.unique_index_id = ic.index_id
                JOIN sys.columns c 
                ON ic.object_id = c.object_id
                AND ic.column_id = c.column_id
                WHERE kc.type = 'PK'
                AND kc.parent_object_id = OBJECT_ID('{table_name}');
        """
        
        columns1 = execute_soap_query(query1)

        structure = []
        
        for col in columns:
            structure.append({
                "name": col.get("COLUMN_NAME", [""])[0],
                "type": col.get("DATA_TYPE", [""])[0],
                "maxLength": col.get("CHARACTER_MAXIMUM_LENGTH", [None])[0],
                "nullable": col.get("IS_NULLABLE", ["NO"])[0] == "YES"                
            })

        for col1 in columns1:
            structure.append({
                "PrimaryKeyColumnName": col1.get("ColumnName", [""])[0],
                "PrimaryKeyName": col1.get("PrimaryKeyName", [""])[0]                          
            })
        
        return jsonify({"success": True, "structure": structure})
    
    except Exception as err:
        print("Error in /get-table-structure:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@app.route('/get_table_data', methods=['POST', 'OPTIONS'])
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
        
        # Build the final WHERE clause
        where_clauses = []
        
        if company_offcode:
            where_clauses.append(f"offcode = '{company_offcode}'")
        
        if frontend_where:
            where_clauses.append(f"({frontend_where})")
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Build queries
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
        
        # Format rows for JSON response
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

# ================== RUN APP ==================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
