# receivable_routes.py - Fixed for older SQL Server versions
from flask import request, jsonify, Blueprint
from utils.jwt_helper import token_required
from config.database import execute_query, execute_non_query
import logging
from datetime import datetime
import requests
import random
import math
from . import receivable_bp
logger = logging.getLogger(__name__)

# Create blueprint


# API endpoint for voucher number
GET_VNO_API = "http://192.168.100.113:8000/GetVnoVockey"


# ============= HELPER FUNCTIONS =============

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


def get_voucher_number_receivable(tablename, vdate, vtype, offcode, bcode):
    """Get voucher number for receivable/payable"""
    # Try API first
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
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"📡 API Response Data: {data}")
            
            if data.get("status") == "success" and data.get("vno"):
                vno = data.get("vno")
                logger.info(f"✅ Got voucher number from API: '{vno}'")
                
                vockey = bcode + vno
                logger.info(f"✅ Generated vockey: '{vockey}'")
                
                return vno, vockey
            else:
                logger.warning(f"⚠️ API returned invalid response: {data}")
    except Exception as e:
        logger.warning(f"❌ GetVno API error: {e}")
    
    # Fallback: generate from database
    logger.info("🔄 Generating voucher number from database...")
    
    try:
        query = """
            SELECT ISNULL(MAX(CAST(SUBSTRING(vno, 1, CHARINDEX('/', vno) - 1) AS INT)), 0) + 1 as next_num
            FROM acChequeHead 
            WHERE vtype = ? AND offcode = ?
        """
        result = execute_query(query, [vtype, offcode])
        next_num = result[0].get('next_num', 1) if result else 1
        
        try:
            date_obj = datetime.strptime(vdate, '%Y-%m-%d')
            month_year = date_obj.strftime('%m%y')
        except:
            month_year = datetime.now().strftime('%m%y')
        
        vno = f"{next_num:05d}/{month_year}"
        logger.info(f"📝 Generated sequential vno: '{vno}'")
        
        vockey = bcode + vno
        logger.info(f"📝 Generated vockey: '{vockey}'")
        
        return vno, vockey
        
    except Exception as e:
        logger.error(f"Error generating voucher: {e}")
        
        # Final fallback
        try:
            date_obj = datetime.strptime(vdate, '%Y-%m-%d')
            month_year = date_obj.strftime('%m%y')
        except:
            month_year = datetime.now().strftime('%m%y')
        
        random_num = random.randint(1, 99999)
        vno = f"{random_num:05d}/{month_year}"
        logger.info(f"🎲 Generated random vno: '{vno}'")
        
        vockey = bcode + vno
        logger.info(f"🎲 Generated vockey: '{vockey}'")
        
        return vno, vockey


# ============= ROUTES =============

@receivable_bp.route('/get-receivables-table-data', methods=['POST'])
@token_required
def get_receivables_table_data():
    """Get receivables data with pagination using ROW_NUMBER() for older SQL Server"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        page = int(data.get("page", 1))
        limit = int(data.get("limit", 10))
        use_pagination = data.get("usePagination", False)
        type_filter = data.get("type", "receivable")
        
        # Get company offcode from token/request
        company_offcode = request.current_user.get('offcode', '0101')
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        # Build WHERE clause
        where_clauses = []
        params = []
        
        if company_offcode:
            where_clauses.append("offcode = ?")
            params.append(company_offcode)
        
        # Filter by type (receivable vs payable)
        if type_filter == "receivable":
            where_clauses.append("Amount > 0")
            where_clauses.append("(custcode IS NOT NULL AND custcode != '')")
        elif type_filter == "payable":
            where_clauses.append("Amount < 0")
            where_clauses.append("(custcode IS NOT NULL AND custcode != '')")
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Count query
        count_query = f"SELECT COUNT(*) AS total FROM {table_name} {where_sql}"
        count_result = execute_query(count_query, params)
        total_count = count_result[0]['total'] if count_result else 0
        
        if use_pagination:
            # Use ROW_NUMBER() for older SQL Server versions (2008 and earlier)
            start_row = (page - 1) * limit + 1
            end_row = page * limit
            
            data_query = f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY vdate DESC) AS row_num
                    FROM {table_name}
                    {where_sql}
                ) AS paginated
                WHERE row_num BETWEEN ? AND ?
                ORDER BY vdate DESC
            """
            params_data = params + [start_row, end_row]
            
            logger.info(f"📤 {type_filter.upper()} DATA QUERY: {data_query}")
            logger.info(f"📤 COUNT QUERY: {count_query}")
            
            rows = execute_query(data_query, params_data)
        else:
            data_query = f"SELECT * FROM {table_name} {where_sql} ORDER BY vdate DESC"
            rows = execute_query(data_query, params)
        
        return jsonify({
            "success": True,
            "rows": rows,
            "totalCount": total_count,
            "page": page if use_pagination else 1,
            "limit": limit if use_pagination else total_count,
            "totalPages": math.ceil(total_count / limit) if use_pagination and total_count > 0 else 1,
            "offcodeApplied": bool(company_offcode),
            "usePagination": use_pagination,
            "type": type_filter
        }), 200
        
    except Exception as err:
        logger.error(f"❌ getReceivablesTableData error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500


@receivable_bp.route('/get-customers-or-suppliers', methods=['POST'])
@token_required
def get_customers_or_suppliers():
    """Get customer/supplier data"""
    try:
        data = request.json or {}
        
        type_filter = data.get("type", "customer")  # "customer" or "supplier"
        offcode = data.get("offcode", request.current_user.get('offcode', '0101'))
        
        # Validate type
        if type_filter not in ["customer", "supplier"]:
            return jsonify({
                "success": False,
                "error": "Invalid type. Must be 'customer' or 'supplier'"
            }), 400
        
        # Dynamic column selection (SAFE)
        is_field = "isCustomer" if type_filter == "customer" else "isSupplier"
        
        query = f"""
            SELECT * FROM comCustomer 
            WHERE {is_field} = '1'
            AND isactive = '1'
            AND offcode = ?
            ORDER BY CustomerName
        """
        
        logger.info(f"📤 {type_filter.upper()} QUERY: {query}")
        logger.info(f"📤 PARAMS: {[offcode]}")
        
        rows = execute_query(query, [offcode])
        
        return jsonify({
            "success": True,
            "rows": rows,
            "totalCount": len(rows),
            "type": type_filter
        }), 200
        
    except Exception as err:
        logger.error(f"❌ get{type_filter.capitalize() if 'type_filter' in locals() else 'Customers'} error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500

@receivable_bp.route('/insert-receivable-payable', methods=['POST'])
@token_required
def insert_receivable_payable():
    """Insert Receivable/Payable"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head", {})
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch", "")
        type_filter = data.get("type", "receivable")
        
        # Get company data from token
        company_offcode = request.current_user.get('offcode', '0101')
        company_branches = request.current_user.get('branches', [])
        
        if not head:
            return jsonify({"success": False, "error": "Head data is required"}), 400
        
        logger.info(f"=== STARTING {type_filter.upper()} INSERTION ===")
        
        # Get branch code
        branch_code = None
        if company_branches:
            if selected_branch:
                selected = next((b for b in company_branches if b.get('branch') == selected_branch), None)
                branch_code = selected.get('code') if selected else company_branches[0].get('code')
            else:
                branch_code = company_branches[0].get('code')
        
        # Fallback branch code
        if not branch_code:
            branch_code = f"{company_offcode}01"
        
        logger.info(f"Company offcode: {company_offcode}")
        logger.info(f"Branch code: {branch_code}")
        
        head_data = head.get("data", {})
        head_table = head.get("tableName", "acChequeHead")
        
        vdate = head_data.get('vdate')
        vtype = (head_data.get('vtype') or "").upper()
        custcode = head_data.get('custcode', '')
        
        # Validate voucher type
        valid_voucher_types = ['PCA', 'CAS', 'CHQ']
        if vtype not in valid_voucher_types:
            return jsonify({
                "success": False,
                "error": f"Invalid {type_filter} voucher type: {vtype}. Valid types are: {', '.join(valid_voucher_types)}"
            }), 400
        
        # Get customer name
        customer_name = ""
        if custcode:
            try:
                query = "SELECT Name FROM comCustomer WHERE code = ? AND offcode = ?"
                cust_rows = execute_query(query, [custcode, company_offcode])
                if cust_rows:
                    customer_name = cust_rows[0].get('Name', '')
            except Exception as err:
                logger.warning(f"Could not fetch customer name: {err}")
        
        # Get voucher number
        vno, vockey = get_voucher_number_receivable(head_table, vdate, vtype, company_offcode, branch_code)
        
        if not vno or not vockey:
            return jsonify({"success": False, "error": "Failed to generate voucher number"}), 500
        
        logger.info(f"Generated voucher number: {vno}")
        logger.info(f"Generated voucher key: {vockey}")
        
        # Get user ID
        createdby = head_data.get('createdby')
        editby = head_data.get('editby') or createdby
        
        if not createdby:
            return jsonify({"success": False, "error": "createdby (username) is required"}), 400
        
        uid = get_user_id(createdby)
        
        # Get period code
        period_info = get_period_info(vdate)
        ycode = period_info.get('ycode')
        
        if not ycode:
            return jsonify({"success": False, "error": "Could not fetch YCode"}), 400
        
        # Calculate totals
        total_amount = float(head_data.get('Amount', 0) or 0)
        wht_amount = float(head_data.get('WhtAmount', 0) or 0)
        sale_tax_amount = float(head_data.get('SaleTaxWHLAmount', 0) or 0)
        fc_amount = float(head_data.get('FCAmount', 0) or 0)
        net_amount = total_amount - wht_amount - sale_tax_amount
        
        createdate = format_date_for_sql()
        editdate = createdate
        
        # Prepare head row
        head_row = {
            'vockey': vockey,
            'vno': vno,
            'vdate': vdate,
            'vtype': vtype,
            'custcode': custcode or "",
            'custname': customer_name,
            'posted': 'false',
            'currencyCode': head_data.get('currencyCode', '1'),
            'compcode': head_data.get('compcode', '01'),
            'offcode': company_offcode,
            'createdby': createdby,
            'createdate': createdate,
            'editby': editby,
            'editdate': editdate,
            'uid': uid,
            'status': '1',
            'YCode': ycode,
            'city': head_data.get('city', ''),
            'Amount': f"{total_amount:.2f}",
            'CashAdjust': head_data.get('CashAdjust', 'false'),
            'manualRefNo': head_data.get('manualRefNo', ''),
            'bcode': branch_code,
            'CashAdjustAmount': head_data.get('CashAdjustAmount', '0'),
            'BankCode': head_data.get('BankCode', ''),
            'WhtAmount': f"{wht_amount:.2f}",
            'NetAmount': f"{net_amount:.2f}",
            'SaleTaxWHLAmount': f"{sale_tax_amount:.2f}",
            'FCAmount': f"{fc_amount:.2f}",
            'woVno': head_data.get('woVno', ''),
            'acBalHeadAmount': f"{total_amount:.2f}",
            'postedDateTime': ''
        }
        
        # Add cheque-specific fields
        if vtype == 'CHQ':
            head_row['chequeBankCode'] = head_data.get('chequeBankCode', '')
            head_row['chequeNo'] = head_data.get('chequeNo', '')
            head_row['chequeDate'] = head_data.get('chequeDate', vdate)
            head_row['chequeStatus'] = head_data.get('chequeStatus', '1')
            head_row['chequepath'] = head_data.get('chequepath', '')
            head_row['chequeDepositDate'] = head_data.get('chequeDepositDate', '')
            head_row['chequeClearBonusDate'] = head_data.get('chequeClearBonusDate', '')
        
        # Check if voucher exists
        try:
            check_query = "SELECT COUNT(*) as count FROM acChequeHead WHERE vockey = ?"
            check_result = execute_query(check_query, [vockey])
            if check_result and check_result[0].get('count', 0) > 0:
                return jsonify({
                    "success": False, 
                    "error": f"Voucher {vno} ({vockey}) already exists"
                }), 400
        except Exception as check_err:
            logger.warning(f"Could not check existing voucher: {check_err}")
        
        # Insert head
        head_columns = []
        head_placeholders = []
        head_params = []
        
        for col, val in head_row.items():
            if val is not None:
                head_columns.append(col)
                head_placeholders.append('?')
                head_params.append(val)
        
        head_insert_query = f"""
            INSERT INTO acChequeHead ({', '.join(head_columns)})
            VALUES ({', '.join(head_placeholders)})
        """
        
        execute_non_query(head_insert_query, head_params)
        logger.info(f"Inserted head record")
        
        # Insert details if provided
        if details and len(details) > 0:
            for idx, det in enumerate(details):
                try:
                    det_data = det.get("data", {})
                    debit = float(det_data.get('debit', 0))
                    credit = float(det_data.get('credit', 0))
                    amount = debit if debit > 0 else credit
                    
                    detail_row = {
                        'vockey': vockey,
                        'vtype': vtype,
                        'offcode': company_offcode,
                        'code': det_data.get('code', ''),
                        'name': det_data.get('name', ''),
                        'narration': det_data.get('narration', ''),
                        'chequeno': det_data.get('chequeno', ''),
                        'debit': f"{debit:.2f}",
                        'credit': f"{credit:.2f}",
                        'amount': f"{amount:.2f}",
                        'EntryType': 'D' if debit > 0 else 'C',
                        'IsActive': 'false',
                        'FCdebit': f"{debit:.2f}",
                        'FCcredit': f"{credit:.2f}",
                        'FCAmount': f"{amount:.2f}",
                        'acBalDetAmount': '0.00',
                        'acBalDetFCAmount': '0.00',
                        'woVno': ''
                    }
                    
                    detail_columns = []
                    detail_placeholders = []
                    detail_params = []
                    
                    for col, val in detail_row.items():
                        if val is not None:
                            detail_columns.append(col)
                            detail_placeholders.append('?')
                            detail_params.append(val)
                    
                    detail_table = det.get("tableName", "acChequeDet")
                    detail_insert_query = f"""
                        INSERT INTO {detail_table} ({', '.join(detail_columns)})
                        VALUES ({', '.join(detail_placeholders)})
                    """
                    
                    execute_non_query(detail_insert_query, detail_params)
                    logger.info(f"Inserted detail {idx + 1}")
                    
                except Exception as detail_err:
                    logger.error(f"Detail {idx + 1} insert failed: {detail_err}")
        
        return jsonify({
            "success": True,
            "message": f"{type_filter.capitalize()} {vtype} voucher {vno} saved successfully",
            "vno": vno,
            "vockey": vockey,
            "vtype": vtype,
            "ycode": ycode,
            "uid": uid,
            "createdBy": createdby,
            "editedBy": editby,
            "customerCode": custcode,
            "customerName": customer_name,
            "totals": {
                "amount": f"{total_amount:.2f}",
                "netAmount": f"{net_amount:.2f}",
                "whtAmount": f"{wht_amount:.2f}",
                "saleTaxAmount": f"{sale_tax_amount:.2f}"
            }
        }), 200
        
    except Exception as err:
        logger.error(f"Insert error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


@receivable_bp.route('/update-receivable-payable', methods=['POST'])
@token_required
def update_receivable_payable():
    """Update Receivable/Payable"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head", {})
        selected_branch = data.get("selectedBranch", "")
        type_filter = data.get("type", "receivable")
        
        # Get company data from token
        company_offcode = request.current_user.get('offcode', '0101')
        company_branches = request.current_user.get('branches', [])
        
        if not head or not head.get("data") or not head.get("where") or not head.get("tableName"):
            return jsonify({
                "success": False,
                "error": "Head data with tableName, data, and where clause are required"
            }), 400
        
        logger.info(f"=== STARTING {type_filter.upper()} UPDATE ===")
        
        head_data = head.get("data", {})
        head_where = head.get("where", {})
        
        vockey = head_data.get('vockey') or head_where.get('vockey')
        vtype = (head_data.get('vtype') or head_where.get('vtype') or "").upper()
        
        # Get branch code
        branch_code = None
        if company_branches:
            if selected_branch:
                selected = next((b for b in company_branches if b.get('branch') == selected_branch), None)
                branch_code = selected.get('code') if selected else company_branches[0].get('code')
            else:
                branch_code = company_branches[0].get('code')
        
        if not branch_code:
            branch_code = f"{company_offcode}01"
        
        editby = head_data.get('editby', 'system')
        editdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if not vockey or not vtype:
            return jsonify({"success": False, "error": "vockey and vtype are required"}), 400
        
        valid_voucher_types = ['PCA', 'CAS', 'CHQ']
        if vtype not in valid_voucher_types:
            return jsonify({
                "success": False,
                "error": f"Invalid voucher type: {vtype}"
            }), 400
        
        # Check if voucher is posted
        check_query = "SELECT posted FROM acChequeHead WHERE vockey=? AND vtype=? AND offcode=?"
        check_result = execute_query(check_query, [vockey, vtype, company_offcode])
        
        if not check_result:
            return jsonify({"success": False, "error": "Voucher not found"}), 404
        
        is_posted = check_result[0].get('posted')
        if is_posted == 'true' or is_posted == True or is_posted == 1:
            return jsonify({"success": False, "error": "Cannot update a posted voucher"}), 400
        
        # Calculate totals
        total_amount = float(head_data.get('Amount', 0) or 0)
        wht_amount = float(head_data.get('WhtAmount', 0) or 0)
        sale_tax_amount = float(head_data.get('SaleTaxWHLAmount', 0) or 0)
        net_amount = total_amount - wht_amount - sale_tax_amount
        
        # Get customer name
        customer_name = head_data.get('custname', '')
        if head_data.get('custcode') and not customer_name:
            try:
                cust_query = "SELECT Name FROM comCustomer WHERE code = ? AND offcode = ?"
                cust_rows = execute_query(cust_query, [head_data.get('custcode'), company_offcode])
                if cust_rows:
                    customer_name = cust_rows[0].get('Name', '')
            except Exception as err:
                logger.warning(f"Could not fetch customer name: {err}")
        
        # Build UPDATE query
        update_fields = []
        update_params = []
        
        if head_data.get('custcode') is not None:
            update_fields.append("custcode = ?")
            update_params.append(head_data.get('custcode', ''))
        
        if customer_name:
            update_fields.append("custname = ?")
            update_params.append(customer_name)
        
        if head_data.get('city') is not None:
            update_fields.append("city = ?")
            update_params.append(head_data.get('city', ''))
        
        if head_data.get('Amount') is not None:
            update_fields.append("Amount = ?")
            update_params.append(f"{total_amount:.2f}")
        
        update_fields.append("NetAmount = ?")
        update_params.append(f"{net_amount:.2f}")
        
        if head_data.get('WhtAmount') is not None:
            update_fields.append("WhtAmount = ?")
            update_params.append(f"{wht_amount:.2f}")
        
        if head_data.get('SaleTaxWHLAmount') is not None:
            update_fields.append("SaleTaxWHLAmount = ?")
            update_params.append(f"{sale_tax_amount:.2f}")
        
        if head_data.get('manualRefNo') is not None:
            update_fields.append("manualRefNo = ?")
            update_params.append(head_data.get('manualRefNo', ''))
        
        if head_data.get('BankCode') is not None:
            update_fields.append("BankCode = ?")
            update_params.append(head_data.get('BankCode', ''))
        
        if branch_code:
            update_fields.append("bcode = ?")
            update_params.append(branch_code)
        
        update_fields.append("editby = ?")
        update_params.append(editby)
        
        update_fields.append("editdate = ?")
        update_params.append(editdate)
        
        # Cheque fields
        if vtype == 'CHQ':
            if head_data.get('chequeBankCode') is not None:
                update_fields.append("chequeBankCode = ?")
                update_params.append(head_data.get('chequeBankCode', ''))
            if head_data.get('chequeNo') is not None:
                update_fields.append("chequeNo = ?")
                update_params.append(head_data.get('chequeNo', ''))
            if head_data.get('chequeDate') is not None:
                update_fields.append("chequeDate = ?")
                update_params.append(head_data.get('chequeDate', ''))
            if head_data.get('chequeStatus') is not None:
                update_fields.append("chequeStatus = ?")
                update_params.append(head_data.get('chequeStatus', ''))
        
        # WHERE clause parameters
        update_params.extend([vockey, vtype, company_offcode])
        
        update_query = f"""
            UPDATE acChequeHead 
            SET {', '.join(update_fields)}
            WHERE vockey = ? AND vtype = ? AND offcode = ?
        """
        
        execute_non_query(update_query, update_params)
        
        return jsonify({
            "success": True,
            "message": f"{type_filter.capitalize()} voucher {vockey} updated successfully",
            "data": {
                "vockey": vockey,
                "vtype": vtype,
                "editedBy": editby,
                "editedAt": editdate,
                "customerCode": head_data.get('custcode'),
                "customerName": customer_name,
                "totals": {
                    "amount": f"{total_amount:.2f}",
                    "netAmount": f"{net_amount:.2f}"
                }
            }
        }), 200
        
    except Exception as err:
        logger.error(f"Update error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


@receivable_bp.route('/get-account-balance', methods=['POST'])
@token_required
def get_account_balance():
    """Get account balances for customer/supplier"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        custcode = data.get("custcode")
        offcode = data.get("offcode", request.current_user.get('offcode', '0101'))
        type_filter = data.get("type", "receivable")
        
        if not custcode:
            return jsonify({"success": False, "error": "custcode is required"}), 400
        
        query = """
            SELECT 
                SUM(CASE WHEN Amount > 0 THEN Amount ELSE 0 END) as totalReceivables,
                SUM(CASE WHEN Amount < 0 THEN ABS(Amount) ELSE 0 END) as totalPayables,
                COUNT(*) as totalTransactions
            FROM acChequeHead 
            WHERE custcode = ? 
            AND offcode = ?
            AND posted = 'true'
        """
        
        rows = execute_query(query, [custcode, offcode])
        
        if rows and len(rows) > 0:
            balance = rows[0]
            receivables = float(balance.get('totalReceivables', 0) or 0)
            payables = float(balance.get('totalPayables', 0) or 0)
            total_transactions = int(balance.get('totalTransactions', 0) or 0)
            
            net_balance = 0
            balance_type = "zero"
            
            if type_filter == "receivable":
                net_balance = receivables - payables
                balance_type = "receivable" if net_balance > 0 else "payable" if net_balance < 0 else "zero"
            else:
                net_balance = payables - receivables
                balance_type = "payable" if net_balance > 0 else "receivable" if net_balance < 0 else "zero"
            
            return jsonify({
                "success": True,
                "balance": {
                    "receivables": f"{receivables:.2f}",
                    "payables": f"{payables:.2f}",
                    "netBalance": f"{abs(net_balance):.2f}",
                    "netBalanceType": balance_type,
                    "totalTransactions": total_transactions
                }
            }), 200
        else:
            return jsonify({
                "success": True,
                "balance": {
                    "receivables": "0.00",
                    "payables": "0.00",
                    "netBalance": "0.00",
                    "netBalanceType": "zero",
                    "totalTransactions": 0
                }
            }), 200
        
    except Exception as err:
        logger.error(f"getAccountBalance error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


@receivable_bp.route('/delete-receivable-payable', methods=['POST'])
@token_required
def delete_receivable_payable():
    """Delete a receivable/payable voucher"""
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        vockey = data.get("vockey")
        vtype = data.get("vtype")
        offcode = data.get("offcode", request.current_user.get('offcode', '0101'))
        type_filter = data.get("type", "receivable")
        
        if not vockey or not vtype:
            return jsonify({"success": False, "error": "vockey and vtype are required"}), 400
        
        # Check if voucher exists and is not posted
        check_query = "SELECT COUNT(*) as count, posted FROM acChequeHead WHERE vockey=? AND vtype=? AND offcode=?"
        check_result = execute_query(check_query, [vockey, vtype, offcode])
        
        if not check_result or check_result[0].get('count', 0) == 0:
            return jsonify({"success": False, "error": "Voucher not found"}), 404
        
        is_posted = check_result[0].get('posted')
        if is_posted == 'true' or is_posted == True or is_posted == 1:
            return jsonify({"success": False, "error": "Cannot delete a posted voucher"}), 400
        
        # Delete details first
        delete_details_query = "DELETE FROM acChequeDet WHERE vockey = ? AND offcode = ?"
        execute_non_query(delete_details_query, [vockey, offcode])
        
        # Delete head
        delete_head_query = "DELETE FROM acChequeHead WHERE vockey = ? AND vtype = ? AND offcode = ?"
        execute_non_query(delete_head_query, [vockey, vtype, offcode])
        
        logger.info(f"✅ Deleted {type_filter} voucher: {vockey}")
        
        return jsonify({
            "success": True,
            "message": f"{type_filter.capitalize()} voucher deleted successfully",
            "vockey": vockey
        }), 200
        
    except Exception as err:
        logger.error(f"Delete error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500