# Authentication routes

from flask import request, jsonify
import pyodbc
from config.database import CONN_STR
from utils.db_helpers import execute_query
from . import auth_bp
import logging

logger = logging.getLogger(__name__)

@auth_bp.route('/GetMenu', methods=['GET', 'POST', 'OPTIONS'])
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