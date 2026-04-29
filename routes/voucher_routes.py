# Voucher and posting routes

from flask import request, jsonify
import pyodbc
from config.database import CONN_STR
from utils.db_helpers import execute_query
from utils.jwt_helper import token_required  # ADD THIS IMPORT
from . import voucher_bp
import logging

logger = logging.getLogger(__name__)

# Add these imports at the top of your file
import math
from datetime import datetime, date  # Add 'date' to the import
from decimal import Decimal, ROUND_HALF_UP

# ==================== HELPER FUNCTIONS ====================

def format_date_for_sql(dt=None):
    """Format datetime for SQL insertion"""
    if dt is None:
        dt = datetime.now()
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def normalize_value(val):
    """Helper to fix array values from SOAP responses"""
    if isinstance(val, list):
        return val[0] if val else ""
    return val if val is not None else ""

def parse_float(value, default=0):
    """Safely parse float from various input types"""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return default

def build_where_clause(conditions):
    """Build WHERE clause from conditions dict"""
    if not conditions:
        return ""
    clauses = []
    for key, value in conditions.items():
        if value is not None:
            if isinstance(value, str):
                clauses.append(f"{key} = '{value.replace(chr(39), chr(39)+chr(39))}'")
            else:
                clauses.append(f"{key} = {value}")
    return " WHERE " + " AND ".join(clauses) if clauses else ""

# ==================== GET TABLE DATA ====================

@voucher_bp.route('/getTableData', methods=['POST', 'OPTIONS'])
@token_required
def get_table_data():
    """Fetch table data with optional filtering and pagination"""
    if request.method == 'OPTIONS':
        return '', 200
    
    data = request.json or {}
    
    table_name = data.get('tableName')
    frontend_where = data.get('where', '')
    page = int(data.get('page', 1))
    limit = int(data.get('limit', 10))
    use_pagination = data.get('usePagination', False)
    
    # Get company offcode from token
    token_data = getattr(request, 'token_data', {})
    company_offcode = token_data.get('offcode') or token_data.get('company', {}).get('offcode')
    
    if not table_name:
        return jsonify({"success": False, "error": "tableName is required"}), 400
    
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        # Build WHERE clauses
        where_clauses = []
        if company_offcode:
            where_clauses.append(f"offcode = '{company_offcode}'")
        if frontend_where:
            where_clauses.append(f"({frontend_where})")
        
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        # Get total count first (without pagination)
        count_query = f"SELECT COUNT(*) AS total FROM {table_name} {where_sql}"
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0] or 0
        
        # Calculate pagination
        if use_pagination and limit > 0:
            offset = (page - 1) * limit
            
            # For older SQL Server versions, use ROW_NUMBER()
            if table_name == "acGLhead":
                # Get column names first
                cursor.execute(f"SELECT TOP 0 * FROM {table_name}")
                columns = [column[0] for column in cursor.description]
                column_list = ', '.join([f"[{col}]" for col in columns])
                
                query = f"""
                    WITH Paginated AS (
                        SELECT {column_list}, 
                               ROW_NUMBER() OVER (ORDER BY vdate DESC, vno DESC) AS row_num
                        FROM {table_name}
                        {where_sql}
                    )
                    SELECT * FROM Paginated
                    WHERE row_num > {offset} AND row_num <= {offset + limit}
                    ORDER BY row_num
                """
            else:
                # Generic pagination for other tables
                query = f"""
                    WITH Paginated AS (
                        SELECT *, 
                               ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
                        FROM {table_name}
                        {where_sql}
                    )
                    SELECT * FROM Paginated
                    WHERE row_num > {offset} AND row_num <= {offset + limit}
                """
            
            cursor.execute(query)
        else:
            # No pagination, get all records
            query = f"SELECT * FROM {table_name} {where_sql}"
            cursor.execute(query)
        
        # Get column names
        columns = [column[0] for column in cursor.description]
        # Remove row_num from columns if present
        columns = [col for col in columns if col != 'row_num']
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Convert rows to dict format
        json_rows = []
        for row in rows:
            row_dict = {}
            # Map columns to values, skipping row_num
            col_idx = 0
            for i, col in enumerate(cursor.description):
                if col[0] == 'row_num':
                    continue
                if col_idx < len(columns):
                    val = row[i]
                    if val is None:
                        row_dict[columns[col_idx]] = ""
                    elif isinstance(val, (Decimal, float)):
                        row_dict[columns[col_idx]] = float(round(val, 2))
                    elif isinstance(val, datetime):
                        row_dict[columns[col_idx]] = val.strftime('%Y-%m-%d')
                    elif isinstance(val, date):
                        row_dict[columns[col_idx]] = val.strftime('%Y-%m-%d')
                    else:
                        row_dict[columns[col_idx]] = val if isinstance(val, (int, float)) else str(val)
                    col_idx += 1
            
            json_rows.append(row_dict)
        
        # Debug logging
        print(f"✅ Retrieved {len(json_rows)} rows from {table_name} (Page: {page}, Limit: {limit})")
        print(f"✅ Total records in table: {total_count}")
        if json_rows and len(json_rows) > 0:
            print(f"✅ First row: vno={json_rows[0].get('vno', 'N/A')}, vdate={json_rows[0].get('vdate', 'N/A')}")
        
        # Calculate total pages
        total_pages = (total_count + limit - 1) // limit if use_pagination and limit > 0 else 1
        
        return jsonify({
            "success": True,
            "rows": json_rows,
            "totalCount": total_count,
            "page": page if use_pagination else 1,
            "limit": limit if use_pagination else total_count,
            "totalPages": total_pages,
            "offcodeApplied": bool(company_offcode),
            "usePagination": use_pagination
        })
        
    except Exception as e:
        logger.error(f"get_table_data error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
# ==================== GET ACCOUNT REFERENCES ====================

@voucher_bp.route('/getAccountReferences', methods=['POST', 'OPTIONS'])
@token_required
def get_account_references():
    """Get account references based on offcode and voucher type"""
    if request.method == 'OPTIONS':
        return '', 200
    
    data = request.json or {}
    offcode = data.get('offcode')
    vtype = data.get('vtype')
    
    if not offcode or not vtype:
        return jsonify({"success": False, "error": "offcode and vtype are required"}), 400
    
    # Ensure vtype is a list
    if not isinstance(vtype, list):
        vtype = [vtype]
    
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        # Build vtype IN clause
        vtype_placeholders = ','.join(['?' for _ in vtype])
        
        query = f"""
            SELECT DISTINCT a.* 
            FROM acChartOfAccount a 
            INNER JOIN invDocumentType i ON a.offcode = i.offcode  
            WHERE a.offcode = ? 
                AND a.isAccountLevel = 1 
                AND a.code LIKE i.AccountReferance + '%' 
                AND i.vtype IN ({vtype_placeholders})
        """
        
        params = [offcode] + vtype
        cursor.execute(query, params)
        
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        
        accounts = []
        for row in rows:
            account_dict = {}
            for i, col in enumerate(columns):
                val = row[i]
                if val is None:
                    account_dict[col] = ""
                elif isinstance(val, Decimal):
                    account_dict[col] = float(val)
                else:
                    account_dict[col] = val
            accounts.append(account_dict)
        
        return jsonify({
            "success": True,
            "accounts": accounts,
            "total": len(accounts),
            "offcodeApplied": True
        })
        
    except Exception as e:
        logger.error(f"get_account_references error: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "accounts": []
        }), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# ==================== INSERT VOUCHER HEADER AND DETAILS ====================

@voucher_bp.route('/insertVouchersHeadDet', methods=['POST', 'OPTIONS'])
@token_required
def insert_vouchers_head_det():
    """Insert voucher header and details with validation"""
    if request.method == 'OPTIONS':
        return '', 200
    
    data = request.json or {}
    head = data.get('head')
    details = data.get('details')
    selected_branch = data.get('selectedBranch')
    
    # Get company data from token (adjust based on your token structure)
    token_data = getattr(request, 'token_data', {})
    company_data = token_data.get('company_data', {})
    
    if not head or not details or not isinstance(details, list):
        return jsonify({"success": False, "error": "Head and details are required"}), 400
    
    try:
        logger.info("=== STARTING VOUCHER INSERTION WITH VALIDATION ===")
        
        company_offcode = company_data.get('company', {}).get('offcode')
        
        # Get branch code with better None handling
        branch_code = None
        branches = company_data.get('branches', [])
        if branches:
            if selected_branch:
                selected = next((b for b in branches if b.get('branch') == selected_branch), None)
                branch_code = selected.get('code') if selected else branches[0].get('code')
            else:
                branch_code = branches[0].get('code')
        
        # CRITICAL FIX: Ensure branch_code is not None
        if not branch_code:
            # Fallback: use company_offcode + "01" as branch code
            branch_code = f"{company_offcode}01" if company_offcode else "010101"
            logger.warning(f"Branch code not found, using fallback: {branch_code}")
        
        logger.info(f"Company offcode: {company_offcode}")
        logger.info(f"Branch code: {branch_code}")
        
        vdate = head.get('data', {}).get('vdate')
        vtype = (head.get('data', {}).get('vtype') or "").upper()
        
        # Validate voucher type
        valid_voucher_types = ['CPV', 'BPV', 'CRV', 'BRV', 'JV', 'JVM']
        if vtype not in valid_voucher_types:
            return jsonify({
                "success": False,
                "error": f"Invalid voucher type: {vtype}. Valid types are: {', '.join(valid_voucher_types)}"
            }), 400
        
        is_accounting_voucher = head.get('tableName') == "acGLhead"
        logger.info(f"Is accounting voucher: {is_accounting_voucher}")
        logger.info(f"Voucher type: {vtype}")
        logger.info(f"Voucher date: {vdate}")
        
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        # Get voucher number from stored procedure
        cursor.execute("EXEC dbo.spGetVno ?, ?, ?, ?, ?",
                      (head.get('tableName'), vdate, company_offcode, branch_code, vtype))
        row = cursor.fetchone()
        
        if not row:
            return jsonify({"success": False, "error": "Voucher number not received from stored procedure"}), 400
        
        vno = str(row[0]).strip()[:10]
        
        # CRITICAL FIX: Ensure both parts are strings before concatenation
        branch_code_str = str(branch_code) if branch_code is not None else ""
        vno_str = str(vno) if vno is not None else ""
        vockey = (branch_code_str + vno_str).replace(" ", "")[:19]
        
        logger.info(f"Generated voucher number: {vno}")
        logger.info(f"Generated voucher key: {vockey}")
        
        # Get user ID
        createdby = head.get('data', {}).get('createdby')
        editby = head.get('data', {}).get('editby') or createdby
        
        if not createdby:
            return jsonify({"success": False, "error": "createdby (username) is required"}), 400
        
        logger.info(f"User info: createdby={createdby}, editby={editby}")
        
        uid = "07"  # default
        try:
            cursor.execute("SELECT TOP 1 Uid FROM comUsers WHERE Userlogin = ?", (createdby,))
            user_row = cursor.fetchone()
            if user_row:
                uid = str(user_row[0])
        except Exception as e:
            logger.warning(f"Could not fetch Uid from comUsers, using default 07: {e}")
        
        logger.info(f"User ID (uid): {uid}")
        
        # Get period code
        ycode = None
        try:
            cursor.execute("SELECT TOP 1 YCode FROM comPeriods WHERE ? BETWEEN SDate AND EDate", (vdate,))
            period_row = cursor.fetchone()
            if period_row:
                ycode = period_row[0]
        except Exception as e:
            return jsonify({"success": False, "error": f"Could not fetch YCode: {str(e)}"}), 400
        
        if not ycode:
            return jsonify({"success": False, "error": "No valid period found for the given date"}), 400
        
        logger.info(f"Period code (YCode): {ycode}")
        
        if is_accounting_voucher:
            # ================= ACCOUNTING VOUCHER LOGIC =================
            logger.info("Processing accounting voucher with validation...")
            
            total_debit = 0
            total_credit = 0
            validation_errors = []
            processed_details = []
            
            for idx, det in enumerate(details):
                det_data = det.get('data', {})
                debit = parse_float(det_data.get('debit') or det_data.get('Debit'))
                credit = parse_float(det_data.get('credit') or det_data.get('Credit'))
                
                # Basic validations
                if debit < 0 or credit < 0:
                    validation_errors.append(f"Row {idx + 1}: Negative values are not allowed")
                    continue
                if debit > 0 and credit > 0:
                    validation_errors.append(f"Row {idx + 1}: Both Debit and Credit are not allowed")
                    continue
                
                # Voucher type rules
                if vtype in ['CPV', 'BPV'] and credit > 0:
                    validation_errors.append(f"Row {idx + 1}: {vtype} allows ONLY DEBIT")
                    continue
                if vtype in ['CRV', 'BRV'] and debit > 0:
                    validation_errors.append(f"Row {idx + 1}: {vtype} allows ONLY CREDIT")
                    continue
                
                total_debit += debit
                total_credit += credit
                
                entry_type = "D" if debit > 0 else "C"
                amount = debit if debit > 0 else credit
                
                processed_details.append({
                    "code": det_data.get('code') or det_data.get('Code') or "",
                    "name": det_data.get('name') or det_data.get('Name') or "",
                    "narration": det_data.get('narration') or det_data.get('Narration') or "",
                    "chequeno": det_data.get('chequeno') or det_data.get('Chequeno') or "",
                    "debit": round(debit, 2),
                    "credit": round(credit, 2),
                    "amount": round(amount, 2),
                    "FCdebit": round(debit, 2),
                    "FCcredit": round(credit, 2),
                    "FCAmount": round(amount, 2),
                    "vockey": vockey,
                    "vtype": vtype,
                    "EntryType": entry_type,
                    "IsActive": "false",
                    "offcode": company_offcode,
                    "woVno": "",
                    "acBalDetAmount": "0.00",
                    "acBalDetFCAmount": "0.00"
                })
            
            if validation_errors:
                return jsonify({"success": False, "errors": validation_errors}), 400
            
            total_amount = max(abs(total_debit), abs(total_credit))
            
            # Final balance validation
            if vtype in ['CPV', 'BPV'] and (total_debit <= 0 or total_credit != 0):
                return jsonify({"success": False, "error": f"{vtype} must contain ONLY DEBIT"}), 400
            if vtype in ['CRV', 'BRV'] and (total_credit <= 0 or total_debit != 0):
                return jsonify({"success": False, "error": f"{vtype} must contain ONLY CREDIT"}), 400
            if vtype in ['JV', 'JVM'] and abs(total_debit - total_credit) > 0.01:
                return jsonify({"success": False, "error": f"{vtype} must be balanced"}), 400
            
            created_date = format_date_for_sql()
            
            # Get AccountReference (Code field) from head data
            account_reference = head.get('data', {}).get('Code') or head.get('data', {}).get('AccountReference') or ""
            
            logger.info(f"Account Reference for insertion: {account_reference}")
            
            # Check if voucher exists
            cursor.execute("SELECT COUNT(*) FROM acGLhead WHERE vockey = ?", (vockey,))
            existing = cursor.fetchone()
            if existing and existing[0] > 0:
                return jsonify({"success": False, "error": f"Voucher {vno} ({vockey}) already exists"}), 400
            
            # Insert head record
            head_sql = """
                INSERT INTO acGLhead (
                    vockey, vno, vdate, vtype, Amount, posted, currencyrate, compcode,
                    offcode, createdby, createdate, editby, editdate, Code, uid, status,
                    YCode, AmountE, ProjectCode, bcode, ManualRefNo, acBalHeadAmount,
                    TotalAmt, ReceivedAmt, FCAmount, TotalCostExpence, TotalCostDuty,
                    TotalCostAdvanceTax, TotalCostIncomeTax
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            head_params = (
                vockey, vno, vdate, vtype, round(total_amount, 2), "false",
                head.get('data', {}).get('currencyrate', "1"), head.get('data', {}).get('compcode', "01"),
                company_offcode, createdby, created_date, editby, created_date,
                account_reference, uid, "1", ycode, "0.00",
                head.get('data', {}).get('ProjectCode', ""), branch_code,
                head.get('data', {}).get('ManualRefNo', ""), round(total_amount, 2),
                str(round(total_amount)), "0", round(total_amount, 2), "0", "0", "0", "0"
            )
            
            cursor.execute(head_sql, head_params)
            
            # Insert detail records
            detail_table = details[0].get('tableName') if details else "acGLdet"
            detail_sql = f"""
                INSERT INTO {detail_table} (
                    vockey, vtype, offcode, code, name, narration, chequeno,
                    debit, credit, amount, EntryType, IsActive, FCdebit, FCcredit,
                    FCAmount, acBalDetAmount, acBalDetFCAmount, woVno
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for det in processed_details:
                cursor.execute(detail_sql, (
                    det['vockey'], det['vtype'], det['offcode'], det['code'], det['name'],
                    det['narration'], det['chequeno'], det['debit'], det['credit'],
                    det['amount'], det['EntryType'], det['IsActive'], det['FCdebit'],
                    det['FCcredit'], det['FCAmount'], det['acBalDetAmount'],
                    det['acBalDetFCAmount'], det['woVno']
                ))
            
            conn.commit()
            
            return jsonify({
                "success": True,
                "message": f"{vtype} voucher {vno} saved successfully with {len(processed_details)} entries",
                "vno": vno,
                "vockey": vockey,
                "vtype": vtype,
                "ycode": ycode,
                "uid": uid,
                "createdBy": createdby,
                "editedBy": editby,
                "accountReference": account_reference,
                "totals": {
                    "debit": round(total_debit, 2),
                    "credit": round(total_credit, 2),
                    "amount": round(total_amount, 2),
                    "balance": round(total_debit - total_credit, 2)
                },
                "validationWarnings": []
            })
        else:
            # ================= GRN LOGIC =================
            # Implement GRN logic here if needed
            return jsonify({"success": False, "error": "Non-accounting vouchers not implemented yet"}), 501
        
    except Exception as e:
        logger.error(f"Insert error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return jsonify({"success": False, "error": str(e), "details": "Complete insertion process failed"}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
# ==================== UPDATE VOUCHER HEADER AND DETAILS ====================

@voucher_bp.route('/updateVoucherHeadDet', methods=['POST', 'OPTIONS'])
@token_required
def update_voucher_head_det():
    """Update voucher header and details with validation"""
    if request.method == 'OPTIONS':
        return '', 200
    
    data = request.json or {}
    head = data.get('head')
    details = data.get('details')
    selected_branch = data.get('selectedBranch')
    
    token_data = getattr(request, 'token_data', {})
    company_data = token_data.get('company_data', {})
    
    if not head or not head.get('data') or not head.get('where') or not head.get('tableName'):
        return jsonify({
            "success": False,
            "error": "Head data with tableName, data, and where clause are required"
        }), 400
    
    try:
        logger.info("=== STARTING VOUCHER UPDATE WITH VALIDATION ===")
        
        company_offcode = company_data.get('company', {}).get('offcode', "0101")
        
        # Extract head data
        head_table = head.get('tableName')
        head_data = head.get('data', {})
        head_where = head.get('where', {})
        
        vockey = head_data.get('vockey') or head_where.get('vockey')
        vtype = (head_data.get('vtype') or head_where.get('vtype') or "").upper()
        
        # Get editor info
        editby = head_data.get('editby', 'system')
        editdate = format_date_for_sql()
        
        logger.info(f"Update by: {editby}")
        logger.info(f"Edit date: {editdate}")
        
        if not vockey or not vtype:
            return jsonify({"success": False, "error": "vockey and vtype are required"}), 400
        
        valid_voucher_types = ['CPV', 'BPV', 'CRV', 'BRV', 'JV', 'JVM']
        if vtype not in valid_voucher_types:
            return jsonify({
                "success": False,
                "error": f"Invalid voucher type: {vtype}. Valid types: {', '.join(valid_voucher_types)}"
            }), 400
        
        # Get branch code
        branch_code = None
        branches = company_data.get('branches', [])
        if branches:
            if selected_branch:
                selected = next((b for b in branches if b.get('branch') == selected_branch), None)
                branch_code = selected.get('code') if selected else branches[0].get('code')
            else:
                branch_code = branches[0].get('code')
        
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        # Check if voucher exists
        cursor.execute(
            "SELECT COUNT(*) FROM acGLhead WHERE vockey = ? AND vtype = ? AND offcode = ?",
            (vockey, vtype, company_offcode)
        )
        existing = cursor.fetchone()
        
        if not existing or existing[0] == 0:
            return jsonify({
                "success": False,
                "error": f"Voucher not found with vockey='{vockey}', vtype='{vtype}'"
            }), 404
        
        # Process details
        if not details or not isinstance(details, list):
            return jsonify({"success": False, "error": "Details array is required"}), 400
        
        total_debit = 0
        total_credit = 0
        validation_warnings = []
        processed_details = []
        
        for idx, det in enumerate(details):
            det_data = det.get('data', {})
            debit = parse_float(det_data.get('debit') or det_data.get('Debit'))
            credit = parse_float(det_data.get('credit') or det_data.get('Credit'))
            amount = parse_float(det_data.get('amount') or det_data.get('Amount') or debit or credit)
            
            # Apply voucher type rules
            if vtype in ['CPV', 'BPV']:
                if credit > 0:
                    validation_warnings.append(f"Row {idx + 1}: {vtype} only allows DEBIT. Credit={credit} converted.")
                    debit = -credit
                    credit = 0
            elif vtype in ['CRV', 'BRV']:
                if debit > 0:
                    validation_warnings.append(f"Row {idx + 1}: {vtype} only allows CREDIT. Debit={debit} converted.")
                    credit = -debit
                    debit = 0
            
            # Convert negative amounts
            if debit < 0:
                credit = abs(debit)
                debit = 0
            if credit < 0:
                debit = abs(credit)
                credit = 0
            
            total_debit += debit
            total_credit += credit
            
            entry_type = 'D' if debit > 0 else 'C'
            if debit == 0 and credit == 0:
                entry_type = 'D'
            
            processed_details.append({
                "vockey": vockey,
                "vtype": vtype,
                "offcode": company_offcode,
                "code": det_data.get('code') or det_data.get('Code') or '',
                "name": det_data.get('name') or det_data.get('Name') or '',
                "narration": det_data.get('narration') or det_data.get('Narration') or '',
                "chequeno": det_data.get('chequeno') or det_data.get('Chequeno') or '',
                "debit": round(debit, 2),
                "credit": round(credit, 2),
                "amount": round(amount, 2),
                "EntryType": entry_type,
                "IsActive": "false",
                "FCdebit": round(debit, 2),
                "FCcredit": round(credit, 2),
                "FCAmount": round(amount, 2),
                "acBalDetAmount": "0.00",
                "acBalDetFCAmount": "0.00",
                "woVno": ""
            })
        
        # Validate voucher totals by type
        if vtype in ['JV', 'JVM']:
            balance_diff = abs(total_debit - total_credit)
            if balance_diff > 0.01:
                logger.warning(f"⚠️ {vtype} not balanced! Difference: {round(balance_diff, 2)}")
        elif vtype in ['CPV', 'BPV']:
            if any(d['credit'] > 0 for d in processed_details):
                return jsonify({"success": False, "error": f"{vtype} must only contain DEBIT entries"}), 400
        elif vtype in ['CRV', 'BRV']:
            if any(d['debit'] > 0 for d in processed_details):
                return jsonify({"success": False, "error": f"{vtype} must only contain CREDIT entries"}), 400
        
        total_amount = max(total_debit, total_credit)
        
        # Get AccountReference from head data
        account_reference = head_data.get('Code') or head_data.get('AccountReference') or ""
        
        logger.info(f"Account Reference to update: {account_reference}")
        
        # Build update query parts
        update_fields = [
            f"Amount = {round(total_amount, 2)}",
            f"FCAmount = {round(total_amount, 2)}",
            f"acBalHeadAmount = {round(total_amount, 2)}",
            f"TotalAmt = {round(total_amount)}",
            f"editby = '{editby.replace(chr(39), chr(39)+chr(39))}'",
            f"editdate = '{editdate}'"
        ]
        
        if account_reference:
            update_fields.append(f"Code = '{account_reference.replace(chr(39), chr(39)+chr(39))}'")
        
        if head_data.get('ManualRefNo') is not None:
            update_fields.append(f"ManualRefNo = '{(head_data.get('ManualRefNo', '')).replace(chr(39), chr(39)+chr(39))}'")
        
        if head_data.get('ProjectCode') is not None:
            update_fields.append(f"ProjectCode = '{(head_data.get('ProjectCode', '')).replace(chr(39), chr(39)+chr(39))}'")
        
        if head_data.get('currencyrate') is not None:
            update_fields.append(f"currencyrate = '{(head_data.get('currencyrate', '1')).replace(chr(39), chr(39)+chr(39))}'")
        
        # Start transaction
        cursor.execute("BEGIN TRANSACTION")
        
        try:
            # Delete existing details
            cursor.execute("DELETE FROM acGLdet WHERE vockey = ? AND vtype = ? AND offcode = ?",
                          (vockey, vtype, company_offcode))
            deleted_count = cursor.rowcount
            
            # Update head
            update_query = f"""
                UPDATE acGLhead 
                SET {', '.join(update_fields)}
                WHERE vockey = ? AND vtype = ? AND offcode = ?
            """
            cursor.execute(update_query, (vockey, vtype, company_offcode))
            head_updated = cursor.rowcount
            
            # Insert new details
            detail_sql = """
                INSERT INTO acGLdet (
                    vockey, vtype, offcode, code, name, narration, chequeno,
                    debit, credit, amount, EntryType, IsActive, FCdebit, FCcredit,
                    FCAmount, acBalDetAmount, acBalDetFCAmount, woVno
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            inserted_count = 0
            for det in processed_details:
                cursor.execute(detail_sql, (
                    det['vockey'], det['vtype'], det['offcode'], det['code'], det['name'],
                    det['narration'], det['chequeno'], det['debit'], det['credit'],
                    det['amount'], det['EntryType'], det['IsActive'], det['FCdebit'],
                    det['FCcredit'], det['FCAmount'], det['acBalDetAmount'],
                    det['acBalDetFCAmount'], det['woVno']
                ))
                inserted_count += 1
            
            conn.commit()
            
            return jsonify({
                "success": True,
                "message": f"{vtype} voucher {vockey} updated successfully",
                "data": {
                    "vockey": vockey,
                    "vtype": vtype,
                    "editedBy": editby,
                    "editedAt": editdate,
                    "accountReference": account_reference,
                    "totals": {
                        "debit": round(total_debit, 2),
                        "credit": round(total_credit, 2),
                        "amount": round(total_amount, 2),
                        "balance": round(total_debit - total_credit, 2)
                    },
                    "results": {
                        "head": head_updated,
                        "deletedDetails": deleted_count,
                        "insertedDetails": inserted_count
                    },
                    "detailsCount": len(processed_details),
                    "validationWarnings": validation_warnings
                }
            })
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        
    except Exception as e:
        logger.error(f"Update error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

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