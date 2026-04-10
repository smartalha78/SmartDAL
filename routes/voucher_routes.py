# Voucher and posting routes

from flask import request, jsonify
import pyodbc
from config.database import CONN_STR
from utils.db_helpers import execute_query
from utils.jwt_helper import token_required  # ADD THIS IMPORT
from . import voucher_bp
import logging

logger = logging.getLogger(__name__)

@voucher_bp.route('/gl_voucher_generation_status', methods=["POST", "OPTIONS"])
@token_required  # ADD THIS DECORATOR
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

@voucher_bp.route('/GetVnoVockey', methods=['POST', 'OPTIONS'])
# NOTE: This endpoint is called by the variable_allowance module
# Keep it protected as it's an internal API
@token_required  # ADD THIS DECORATOR
def GetVnoVockey():
    if request.method == 'OPTIONS':
        return '', 200

    data = request.json or {}
    Tablename = data.get("Tablename")
    Vdate = data.get("Vdate")
    Vtype = data.get("Vtype")
    Offcode = data.get("Offcode")
    Bcode = data.get("Bcode")

    if not all([Tablename, Vdate, Vtype, Offcode, Bcode]):
        return jsonify({"status": "fail", "message": "Missing parameters"}), 400

    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()

        sql = "EXEC dbo.spGetVno ?, ?, ?, ?, ?"
        params = (Tablename, Vdate, Vtype, Offcode, Bcode)

        cursor.execute(sql, params)
        row = cursor.fetchone()

        if not row:
            return jsonify({"status": "fail", "message": "Invalid Voucher No"}), 401

        vno = str(row[0])
        vockey = str(Bcode) + vno
        vockey = vockey.replace(" ", "")

        return jsonify({
            "status": "success",
            "vno": vno,
            "vockey": vockey
        }), 200

    except Exception as ex:
        return jsonify({"status": "error", "message": str(ex)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@voucher_bp.route('/gl_Posting', methods=["POST", "OPTIONS"])
@token_required  # ADD THIS DECORATOR
def gl_Posting():
    if request.method == 'OPTIONS':
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

@voucher_bp.route('/stk_Posting', methods=["POST", "OPTIONS"])
@token_required  # ADD THIS DECORATOR
def stk_Posting():
    if request.method == 'OPTIONS':
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

@voucher_bp.route('/FillTable', methods=['GET', 'POST', 'OPTIONS'])
@token_required  # ADD THIS DECORATOR
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