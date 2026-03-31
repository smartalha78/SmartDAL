from flask import request, jsonify
from . import variable_allowance_bp
from config.database import get_db, close_db
import requests
from datetime import datetime
import logging
import random
import traceback

logger = logging.getLogger(__name__)

# API endpoint
GET_VNO_API = "http://192.168.100.113:8000/GetVnoVockey"

logger.info(f"GetVno API URL: {GET_VNO_API}")

def get_cursor():
    """Get database cursor using connection from config"""
    db = get_db()
    return db.cursor()

def execute_query(query, params=None):
    """Execute a query and return results"""
    cursor = None
    try:
        cursor = get_cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            rows.append(row_dict)
        return rows
    except Exception as e:
        logger.error(f"Query error: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()

def execute_non_query(query, params=None):
    """Execute a non-query SQL command"""
    cursor = None
    try:
        db = get_db()
        cursor = db.cursor()
        
        if params:
            logger.info(f"Executing with params: {params}")
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        db.commit()
        return cursor.rowcount
    except Exception as e:
        logger.error(f"Database error: {e}")
        db = get_db()
        db.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()

def get_voucher_number(tablename, vdate, vtype, offcode, bcode):
    """Get voucher number - keep slash in vockey"""
    
    # Try 1: Call the GetVno API
    try:
        payload = {
            "Tablename": tablename,
            "Vdate": vdate,
            "Vtype": vtype,
            "Offcode": offcode,
            "Bcode": bcode
        }
        
        logger.info(f"📡 Calling GetVno API at: {GET_VNO_API}")
        logger.info(f"📦 Payload: {payload}")
        
        response = requests.post(GET_VNO_API, json=payload, timeout=5)
        
        logger.info(f"📡 API Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"📡 API Response Data: {data}")
            
            if data.get("status") == "success" and data.get("vno"):
                vno = data.get("vno")
                logger.info(f"✅ Got voucher number from API: '{vno}'")
                
                # IMPORTANT: vno should already have slash like "00112/0326"
                # vockey = bcode + vno (keep the slash)
                vockey = bcode + vno
                logger.info(f"✅ Generated vockey: '{vockey}'")
                logger.info(f"✅ vockey length: {len(vockey)}")
                
                # Check if vockey has slash
                if '/' in vockey:
                    logger.info("✅ vockey contains slash (GOOD)")
                else:
                    logger.warning("⚠️ vockey does NOT contain slash (BAD)")
                    # If no slash, try to add it back
                    if '/' in vno:
                        logger.info("vno has slash, so vockey should too")
                    else:
                        logger.warning("vno also missing slash")
                
                return vno, vockey
            else:
                logger.warning(f"⚠️ API returned invalid response: {data}")
                
    except Exception as e:
        logger.warning(f"❌ GetVno API error: {e}")
    
    # Try 2: Generate from database with proper format
    logger.info("🔄 Generating voucher number from database...")
    
    try:
        # Get next sequence number
        query = """
            SELECT ISNULL(MAX(CAST(SUBSTRING(vno, 1, CHARINDEX('/', vno) - 1) AS INT)), 0) + 1 as next_num
            FROM HRMSVariableAllowanceHead 
            WHERE vtype = ? AND offcode = ?
        """
        result = execute_query(query, [vtype, offcode])
        next_num = result[0]['next_num'] if result and result[0].get('next_num') else 1
        
        # Format month/year
        try:
            date_obj = datetime.strptime(vdate, '%Y-%m-%d')
            month_year = date_obj.strftime('%m%y')
        except:
            month_year = datetime.now().strftime('%m%y')
        
        # Generate vno WITH slash
        vno = f"{next_num:05d}/{month_year}"
        logger.info(f"📝 Generated sequential vno: '{vno}'")
        
        # Generate vockey WITH slash (bcode + vno)
        vockey = bcode + vno
        logger.info(f"📝 Generated vockey: '{vockey}'")
        
        # Verify slash is present
        if '/' in vockey:
            logger.info("✅ vockey contains slash (GOOD)")
        else:
            logger.warning("⚠️ vockey does NOT contain slash (BAD)")
        
        return vno, vockey
        
    except Exception as e:
        logger.error(f"Error generating voucher: {e}")
        
        # Final fallback - generate random number with slash
        try:
            date_obj = datetime.strptime(vdate, '%Y-%m-%d')
            month_year = date_obj.strftime('%m%y')
        except:
            month_year = datetime.now().strftime('%m%y')
        
        random_num = random.randint(1, 99999)
        vno = f"{random_num:05d}/{month_year}"
        logger.info(f"🎲 Generated random vno: '{vno}'")
        
        # Generate vockey WITH slash
        vockey = bcode + vno
        logger.info(f"🎲 Generated vockey: '{vockey}'")
        
        return vno, vockey

def get_user_id(username):
    """Get user ID from username"""
    try:
        query = "SELECT TOP 1 Uid FROM comUsers WHERE Userlogin = ?"
        results = execute_query(query, (username,))
        if results and len(results) > 0:
            return str(results[0]['Uid'])
        return '07'
    except Exception as e:
        logger.error(f"Error getting user ID: {e}")
        return '07'

def get_period_info(vdate):
    """Get PCode and YCode for a given date"""
    try:
        query = "SELECT TOP 1 PCode, YCode FROM comPeriods WHERE ? BETWEEN SDate AND EDate"
        results = execute_query(query, (vdate,))
        if results and len(results) > 0:
            return {
                'pcode': str(results[0]['PCode']),
                'ycode': str(results[0]['YCode'])
            }
        return {'pcode': None, 'ycode': None}
    except Exception as e:
        logger.error(f"Error getting period info: {e}")
        return {'pcode': None, 'ycode': None}

def format_date_for_sql():
    """Get current date in SQL format"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# ============= GET VOUCHER WITH DETAILS =============
@variable_allowance_bp.route('/get-voucher-with-details', methods=['POST'])
def get_voucher_with_details():
    """Get voucher with its details"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        vockey = data.get("vockey")
        offcode = data.get("offcode", "0101")
        
        if not vockey:
            return jsonify({"success": False, "error": "vockey is required"}), 400
        
        # Get head data
        head_query = """
            SELECT * FROM HRMSVariableAllowanceHead 
            WHERE vockey = ? AND offcode = ?
        """
        head_result = execute_query(head_query, [vockey, offcode])
        
        if not head_result:
            return jsonify({"success": False, "error": "Voucher not found"}), 404
        
        head = head_result[0]
        
        # Get details
        details_query = """
            SELECT 
                Pk, EmployeeCode, EmployeeName, AllowancesCode, AllowanceName,
                Amount, Percentage, BasicPay, LocationCode, vno, vdate, vtype, vockey
            FROM HRMSVariableAllowanceDet
            WHERE vockey = ? AND offcode = ?
            ORDER BY Pk
        """
        details_result = execute_query(details_query, [vockey, offcode])
        
        return jsonify({
            "success": True,
            "data": {
                "head": head,
                "details": details_result
            }
        }), 200
        
    except Exception as err:
        logger.error(f"Error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= INSERT VOUCHER =============
@variable_allowance_bp.route('/insert-variable-allowance', methods=['POST'])
def insert_variable_allowance():
    """Insert Monthly Variable Allowance Head and Details"""
    try:
        data = request.json
        logger.info(f"📝 Received insert request")
        
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head", {})
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch", "")
        offcode = data.get("offcode", "0101")
        
        if not head:
            return jsonify({"success": False, "error": "Head data is required"}), 400
        
        if not details or len(details) == 0:
            return jsonify({"success": False, "error": "At least one detail record is required"}), 400
        
        head_data = head.get("data", {})
        head_table = head.get("tableName", "HRMSVariableAllowanceHead")
        
        bcode = selected_branch or "010101"
        vdate = head_data.get('vdate', datetime.now().strftime('%Y-%m-%d'))
        vtype = head_data.get('vtype', 'VRA')
        
        # Get voucher number
        logger.info(f"🎫 Getting voucher number")
        vno, vockey = get_voucher_number(head_table, vdate, vtype, offcode, bcode)
        
        if not vno or not vockey:
            return jsonify({"success": False, "error": "Failed to generate voucher number"}), 500
        
        logger.info(f"✅ Final values:")
        logger.info(f"   vno: '{vno}'")
        logger.info(f"   vockey: '{vockey}'")
        logger.info(f"   vockey contains slash: {'/' in vockey}")
        
        createdby = head_data.get('createdby')
        if not createdby:
            return jsonify({"success": False, "error": "createdby is required"}), 400
        
        uid = get_user_id(createdby)
        period_info = get_period_info(vdate)
        pcode = period_info.get('pcode')
        ycode = period_info.get('ycode')
        
        if not pcode or not ycode:
            return jsonify({"success": False, "error": "Could not determine period"}), 400
        
        createdate = format_date_for_sql()
        head_row = {
            'offcode': offcode,
            'bcode': bcode,
            'vno': vno,
            'vdate': vdate,
            'vtype': vtype,
            'vockey': vockey,
            'PCode': pcode,
            'YCode': ycode,
            'Remarks': head_data.get('Remarks', ''),
            'status': 1,
            'posted': None,
            'postedby': '',
            'posteddate': None,
            'createdby': createdby,
            'createdate': createdate,
            'editby': createdby,
            'editdate': createdate,
            'uid': uid,
            'compcode': head_data.get('compcode', '01')
        }
        
        # Insert head
        head_columns = []
        head_values = []
        head_params = []
        
        for col, val in head_row.items():
            if val is not None:
                head_columns.append(col)
                head_values.append('?')
                head_params.append(val)
        
        head_insert_query = f"INSERT INTO {head_table} ({', '.join(head_columns)}) VALUES ({', '.join(head_values)})"
        execute_non_query(head_insert_query, head_params)
        
        # Insert details
        successful_details = 0
        
        for idx, detail in enumerate(details):
            try:
                detail_data = detail.get("data", {})
                if not detail_data:
                    continue
                
                amount = float(detail_data.get('Amount', 0.0)) if detail_data.get('Amount') else 0.0
                percentage = float(detail_data.get('Percentage', 0.0)) if detail_data.get('Percentage') else 0.0
                
                detail_row = {
                    'offcode': offcode,
                    'bcode': bcode,
                    'vno': vno,
                    'vdate': vdate,
                    'vtype': vtype,
                    'vockey': vockey,
                    'PCode': pcode,
                    'YCode': ycode,
                    'EmployeeCode': detail_data.get('EmployeeCode', ''),
                    'EmployeeName': (detail_data.get('EmployeeName', '') or '')[:150],
                    'AllowancesCode': detail_data.get('AllowancesCode', ''),
                    'AllowanceName': (detail_data.get('AllowanceName', '') or '')[:50],
                    'Amount': amount,
                    'Percentage': percentage,
                    'BasicPay': detail_data.get('BasicPay', 0.0) or 0.0,
                    'LocationCode': detail_data.get('LocationCode', '') or ''
                }
                
                detail_columns = []
                detail_values = []
                detail_params = []
                
                for col, val in detail_row.items():
                    if col.lower() != 'pk' and val is not None:
                        detail_columns.append(col)
                        detail_values.append('?')
                        detail_params.append(val)
                
                if detail_columns:
                    detail_insert_query = f"INSERT INTO HRMSVariableAllowanceDet ({', '.join(detail_columns)}) VALUES ({', '.join(detail_values)})"
                    execute_non_query(detail_insert_query, detail_params)
                    successful_details += 1
                    
            except Exception as detail_err:
                logger.error(f"Error inserting detail {idx}: {detail_err}")
        
        if successful_details == 0:
            delete_query = f"DELETE FROM {head_table} WHERE vockey = ?"
            execute_non_query(delete_query, [vockey])
            return jsonify({"success": False, "error": "Failed to insert any detail records"}), 500
        
        return jsonify({
            "success": True,
            "message": f"Voucher saved successfully",
            "vno": vno,
            "vockey": vockey
        }), 200
        
    except Exception as err:
        logger.error(f"Insert error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500

# ============= UPDATE VOUCHER =============
@variable_allowance_bp.route('/update-variable-allowance', methods=['POST'])
def update_variable_allowance():
    """Update voucher"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head", {})
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch", "")
        offcode = data.get("offcode", "0101")
        
        if not head:
            return jsonify({"success": False, "error": "Head data is required"}), 400
        
        head_data = head.get("data", {})
        head_table = head.get("tableName", "HRMSVariableAllowanceHead")
        
        vno = head_data.get('vno')
        vockey = head_data.get('vockey')
        
        if not vno or not vockey:
            return jsonify({"success": False, "error": "vno and vockey are required"}), 400
        
        bcode = selected_branch or head_data.get('bcode', '010101')
        editby = head_data.get('editby')
        
        if not editby:
            return jsonify({"success": False, "error": "editby is required"}), 400
        
        editdate = format_date_for_sql()
        
        # Check if posted
        check_query = f"SELECT status FROM {head_table} WHERE vockey = ? AND offcode = ?"
        check_result = execute_query(check_query, [vockey, offcode])
        if check_result and check_result[0].get('status') == 2:
            return jsonify({"success": False, "error": "Cannot update a posted voucher"}), 400
        
        # Update head
        update_query = f"UPDATE {head_table} SET Remarks = ?, editby = ?, editdate = ? WHERE vockey = ? AND offcode = ?"
        execute_non_query(update_query, [head_data.get('Remarks', ''), editby, editdate, vockey, offcode])
        
        # Get vdate and period info
        head_info = execute_query(f"SELECT vdate FROM {head_table} WHERE vockey = ?", [vockey])
        vdate = head_info[0].get('vdate') if head_info else None
        
        period_info = get_period_info(vdate) if vdate else {'pcode': None, 'ycode': None}
        pcode = period_info.get('pcode')
        ycode = period_info.get('ycode')
        
        # Delete existing details
        delete_details_query = "DELETE FROM HRMSVariableAllowanceDet WHERE vockey = ? AND offcode = ?"
        execute_non_query(delete_details_query, [vockey, offcode])
        
        # Insert new details
        successful_details = 0
        
        for idx, detail in enumerate(details):
            try:
                detail_data = detail.get("data", {})
                if not detail_data:
                    continue
                
                amount = float(detail_data.get('Amount', 0.0)) if detail_data.get('Amount') else 0.0
                percentage = float(detail_data.get('Percentage', 0.0)) if detail_data.get('Percentage') else 0.0
                
                detail_row = {
                    'offcode': offcode,
                    'bcode': bcode,
                    'vno': vno,
                    'vdate': vdate,
                    'vtype': 'VRA',
                    'vockey': vockey,
                    'PCode': pcode,
                    'YCode': ycode,
                    'EmployeeCode': detail_data.get('EmployeeCode', ''),
                    'EmployeeName': (detail_data.get('EmployeeName', '') or '')[:150],
                    'AllowancesCode': detail_data.get('AllowancesCode', ''),
                    'AllowanceName': (detail_data.get('AllowanceName', '') or '')[:50],
                    'Amount': amount,
                    'Percentage': percentage,
                    'BasicPay': detail_data.get('BasicPay', 0.0) or 0.0,
                    'LocationCode': detail_data.get('LocationCode', '') or ''
                }
                
                detail_columns = []
                detail_values = []
                detail_params = []
                
                for col, val in detail_row.items():
                    if col.lower() != 'pk' and val is not None:
                        detail_columns.append(col)
                        detail_values.append('?')
                        detail_params.append(val)
                
                if detail_columns:
                    detail_insert_query = f"INSERT INTO HRMSVariableAllowanceDet ({', '.join(detail_columns)}) VALUES ({', '.join(detail_values)})"
                    execute_non_query(detail_insert_query, detail_params)
                    successful_details += 1
                    
            except Exception as detail_err:
                logger.error(f"Error inserting detail {idx}: {detail_err}")
        
        return jsonify({
            "success": True,
            "message": f"Voucher updated successfully"
        }), 200
        
    except Exception as err:
        logger.error(f"Update error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500

# ============= POST VOUCHER =============
@variable_allowance_bp.route('/post-variable-allowance', methods=['POST'])
def post_variable_allowance():
    """Post a voucher"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        vockey = data.get("vockey")
        offcode = data.get("offcode")
        posted_by = data.get("posted_by")
        
        if not vockey or not offcode or not posted_by:
            return jsonify({"success": False, "error": "Missing required fields"}), 400
        
        # Check if already posted
        check_query = "SELECT status FROM HRMSVariableAllowanceHead WHERE vockey = ? AND offcode = ?"
        check_result = execute_query(check_query, [vockey, offcode])
        if check_result and check_result[0].get('status') == 2:
            return jsonify({"success": False, "error": "Voucher is already posted"}), 400
        
        # Update head status
        posteddate = format_date_for_sql()
        update_query = "UPDATE HRMSVariableAllowanceHead SET status = 2, posted = 1, postedby = ?, posteddate = ? WHERE vockey = ? AND offcode = ?"
        execute_non_query(update_query, [posted_by, posteddate, vockey, offcode])
        
        return jsonify({"success": True, "message": "Voucher posted successfully"}), 200
        
    except Exception as err:
        logger.error(f"Posting error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500

# ============= DELETE VOUCHER =============
@variable_allowance_bp.route('/delete-variable-allowance', methods=['POST'])
def delete_variable_allowance():
    """Delete a voucher"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        vockey = data.get("vockey")
        offcode = data.get("offcode")
        
        if not vockey or not offcode:
            return jsonify({"success": False, "error": "vockey and offcode are required"}), 400
        
        # Check if posted
        check_query = "SELECT status FROM HRMSVariableAllowanceHead WHERE vockey = ? AND offcode = ?"
        check_result = execute_query(check_query, [vockey, offcode])
        
        if check_result and check_result[0].get('status') == 2:
            return jsonify({"success": False, "error": "Cannot delete a posted voucher"}), 400
        
        # Delete details and head
        execute_non_query("DELETE FROM HRMSVariableAllowanceDet WHERE vockey = ? AND offcode = ?", [vockey, offcode])
        execute_non_query("DELETE FROM HRMSVariableAllowanceHead WHERE vockey = ? AND offcode = ?", [vockey, offcode])
        
        return jsonify({"success": True, "message": "Voucher deleted successfully"}), 200
        
    except Exception as err:
        logger.error(f"Delete error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500