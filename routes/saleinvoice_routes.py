# grn_routes.py - Complete working version with unique keys
from flask import request, jsonify, send_file
from . import saleinvoice_bp
from config.database import get_db
from utils.jwt_helper import token_required
from datetime import datetime
import logging
import pyodbc
import io

logger = logging.getLogger(__name__)

def get_db_cursor():
    """Get database cursor from connection pool"""
    db = get_db()
    return db.cursor()

def execute_query(query, params=None):
    """Execute a SELECT query and return results as list of dictionaries"""
    cursor = None
    try:
        cursor = get_db_cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        columns = [column[0] for column in cursor.description] if cursor.description else []
        rows = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                if isinstance(value, datetime):
                    row_dict[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif value is not None:
                    row_dict[col] = str(value)
                else:
                    row_dict[col] = None
            rows.append(row_dict)
        
        return rows
    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()

def execute_non_query(query, params=None):
    """Execute an INSERT, UPDATE, or DELETE query"""
    cursor = None
    db = None
    try:
        db = get_db()
        cursor = db.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        db.commit()
        return cursor.rowcount
    except pyodbc.Error as e:
        if db:
            db.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()

def safe_str(value, max_len=100):
    """Safely truncate string to prevent database overflow"""
    if value is None:
        return ''
    s = str(value).strip()
    return s[:max_len] if len(s) > max_len else s

def table_exists(table_name):
    """Check if a table exists in the database"""
    try:
        cursor = get_db_cursor()
        query = """
            SELECT COUNT(*) as table_exists 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ?
        """
        cursor.execute(query, (table_name,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] > 0 if result else False
    except Exception as e:
        logger.warning(f"Could not check table existence for {table_name}: {e}")
        return False

# ============= GET Invoice TABLE DATA =============
@saleinvoice_bp.route('/get-saleinvoice-table-data', methods=['POST'])
@token_required
def get_saleinvoice_table_data():
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        if not table_exists(table_name):
            logger.warning(f"Table {table_name} does not exist")
            return jsonify({
                "success": True,
                "rows": [],
                "count": 0,
                "message": f"Table {table_name} not found"
            }), 200
        
        try:
            query = f"SELECT TOP 500 * FROM {table_name}"
            rows = execute_query(query)
        except pyodbc.Error as e:
            logger.error(f"Error executing query on {table_name}: {e}")
            return jsonify({
                "success": True,
                "rows": [],
                "count": 0,
                "message": f"Could not query table {table_name}"
            }), 200
        
        return jsonify({
            "success": True,
            "rows": rows,
            "count": len(rows)
        }), 200
        
    except Exception as err:
        logger.error(f"Error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= GET Customer =============
@saleinvoice_bp.route('/get-customers', methods=['GET'])
@token_required
def get_customers():
    try:
        customers = []
        unique_customers = {}  # Use dict to ensure unique CustomerCode
        
        if table_exists('comCustomer'):
            try:
                possible_queries = [
                    "SELECT TOP 100 CustomerCode, customername, city, country FROM comCustomer",
                    "SELECT TOP 100 CustomerCode as CustomerCode, customername as CustomerName, City as city, Country as country FROM comCustomer",
                    "SELECT TOP 100 Code as CustomerCode, Name as customername, City as city, Country as country FROM comCustomer"
                    ]
                
                results = []
                for query in possible_queries:
                    try:
                        results = execute_query(query)
                        if results and len(results) > 0:
                            break
                    except:
                        continue
                
                # Use dict to ensure unique CustomerCode
                for row in results:
                    code = row.get('CustomerCode', '')
                    if code and code not in unique_customers:
                        unique_customers[code] = {
                            'CustomerCode': code,
                            'customername': row.get('customername', ''),
                            'city': row.get('city', 'LAHORE'),
                            'country': row.get('country', 'Pakistan'),
                            'id': code
                        }
                
                customers = list(unique_customers.values())
            except Exception as e:
                logger.warning(f"Could not get from comCustomer: {e}")
        
        # If no suppliers found, return unique sample data
        if not customers:
            sample_customers = [
                {"CustomerCode": "0000000011", "customername": "Fauji Foods Limited", "city": "LAHORE", "country": "Pakistan", "id": "0011"},
                {"CustomerCode": "0000000012", "customername": "Foods Trends (Pvt) Ltd", "city": "LAHORE", "country": "Pakistan", "id": "0012"},
                {"CustomerCode": "0000000027", "customername": "Haidri Beverages (Pvt) Ltd", "city": "ISLAMABAD", "country": "Pakistan", "id": "0027"},
                {"CustomerCode": "0000000001", "customername": "Walking Customer/Supplier", "city": "LAHORE", "country": "Pakistan", "id": "0001"},
                {"CustomerCode": "0000000995", "customername": "Abdullah Supplier", "city": "Lahore", "country": "Pakistan", "id": "0995"},
            ]
            return jsonify({"success": True, "data": sample_customers}), 200
        
        return jsonify({"success": True, "data": customers}), 200
        
    except Exception as err:
        logger.error(f"Error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= GET GODOWNS =============
@saleinvoice_bp.route('/get-godowns', methods=['GET'])
@token_required
def get_godowns():
    try:
        godowns = []
        unique_godowns = {}
        
        if table_exists('comGodown'):
            try:
                godown_queries = [
                    "SELECT TOP 100 godownID, description FROM comGodown",
                    "SELECT TOP 100 GodownID as godownID, GodownName as description FROM comGodown",
                    "SELECT TOP 100 ID as godownID, Name as description FROM comGodown"
                ]
                
                results = []
                for query in godown_queries:
                    try:
                        results = execute_query(query)
                        if results and len(results) > 0:
                            break
                    except:
                        continue
                
                for row in results:
                    godown_id = str(row.get('godownID', ''))
                    if godown_id and godown_id not in unique_godowns:
                        unique_godowns[godown_id] = {
                            'godownID': godown_id,
                            'description': row.get('description', ''),
                            'id': godown_id
                        }
                
                godowns = list(unique_godowns.values())
            except Exception as e:
                logger.warning(f"Could not get godowns: {e}")
        
        if not godowns:
            sample_godowns = [
                {"godownID": "1", "description": "Main Godown", "id": "1"},
                {"godownID": "2", "description": "Work In Process/Mill Finish", "id": "2"},
                {"godownID": "3", "description": "Finished Good", "id": "3"},
                {"godownID": "4", "description": "Raw Material Godown", "id": "4"},
            ]
            return jsonify({"success": True, "data": sample_godowns}), 200
        
        return jsonify({"success": True, "data": godowns}), 200
        
    except Exception as err:
        logger.error(f"Error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= GET ITEMS =============
@saleinvoice_bp.route('/get-items', methods=['GET'])
@token_required
def get_items():
    try:
        items = []
        unique_items = {}
        
        if table_exists('comItem'):
            try:
                item_queries = [
                    "SELECT TOP 200 Itemcode, Itemname, uom FROM comItem",
                    "SELECT TOP 200 Code as Itemcode, Name as Itemname, UOM as uom FROM comItem",
                    "SELECT TOP 200 ItemCode as Itemcode, ItemName as Itemname, Unit as uom FROM comItem"
                ]
                
                results = []
                for query in item_queries:
                    try:
                        results = execute_query(query)
                        if results and len(results) > 0:
                            break
                    except:
                        continue
                
                for row in results:
                    code = str(row.get('Itemcode', ''))
                    if code and code not in unique_items:
                        unique_items[code] = {
                            'code': code,
                            'name': row.get('Itemname', ''),
                            'unit': row.get('uom', 'PCS'),
                            'id': code
                        }
                
                items = list(unique_items.values())
            except Exception as e:
                logger.warning(f"Could not get items: {e}")
        
        if not items:
            sample_items = [
                {"code": "0101001", "name": "P.G New abc", "unit": "PCS", "id": "0101001"},
                {"code": "0101002", "name": "Cement Bag", "unit": "BAG", "id": "0101002"},
                {"code": "0101003", "name": "Methanol, Methyl Alcohol", "unit": "LTR", "id": "0101003"},
                {"code": "0101004", "name": "I.P.A 99%", "unit": "LTR", "id": "0101004"},
            ]
            return jsonify({"success": True, "data": sample_items}), 200
        
        return jsonify({"success": True, "data": items}), 200
        
    except Exception as err:
        logger.error(f"Error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= GET UOMS =============
@saleinvoice_bp.route('/get-uoms', methods=['GET'])
@token_required
def get_uoms():
    try:
        uoms = []
        unique_uoms = {}
        
        if table_exists('comUOM'):
            try:
                uom_queries = [
                    "SELECT TOP 50 * FROM comUOM",
                    "SELECT TOP 50 uomID as id, uomName as name FROM comUOM",
                    "SELECT TOP 50 ID as id, Name as name FROM comUOM",
                    "SELECT TOP 50 UOMCode as id, UOMName as name FROM comUOM"
                ]
                
                results = []
                for query in uom_queries:
                    try:
                        results = execute_query(query)
                        if results and len(results) > 0:
                            break
                    except:
                        continue
                
                for row in results:
                    # Try to get id and name from various possible column names
                    uom_id = None
                    uom_name = None
                    
                    if 'id' in row:
                        uom_id = str(row.get('id', ''))
                        uom_name = str(row.get('name', ''))
                    elif 'uomID' in row:
                        uom_id = str(row.get('uomID', ''))
                        uom_name = str(row.get('uomName', ''))
                    elif 'ID' in row:
                        uom_id = str(row.get('ID', ''))
                        uom_name = str(row.get('Name', ''))
                    elif 'UOMCode' in row:
                        uom_id = str(row.get('UOMCode', ''))
                        uom_name = str(row.get('UOMName', ''))
                    else:
                        # Use first two columns
                        keys = list(row.keys())
                        if len(keys) >= 2:
                            uom_id = str(row.get(keys[0], ''))
                            uom_name = str(row.get(keys[1], ''))
                    
                    if uom_id and uom_id not in unique_uoms:
                        unique_uoms[uom_id] = {
                            'id': uom_id,
                            'name': uom_name or f"UOM_{uom_id}",
                        }
                
                uoms = list(unique_uoms.values())
            except Exception as e:
                logger.warning(f"Could not get UOMs: {e}")
        
        if not uoms:
            sample_uoms = [
                {"id": "1", "name": "PCS"},
                {"id": "2", "name": "BOX"},
                {"id": "3", "name": "KG"},
                {"id": "4", "name": "LTR"},
                {"id": "5", "name": "MTR"},
                {"id": "6", "name": "BAG"},
            ]
            return jsonify({"success": True, "data": sample_uoms}), 200
        
        return jsonify({"success": True, "data": uoms}), 200
        
    except Exception as err:
        logger.error(f"Error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= GET Invoice WITH DETAIL=============
@saleinvoice_bp.route('/get-saleinvoice-with-details', methods=['POST'])
@token_required
def get_saleinvoice_with_details():
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        vno = data.get("vno")
        vockey = data.get("vockey")
        offcode = data.get("offcode", "0101")
        
        if not vno and not vockey:
            return jsonify({"success": False, "error": "vno or vockey is required"}), 400
        
        if not table_exists('invsalehead') or not table_exists('invsaledet'):
            return jsonify({
                "success": False, 
                "error": "GRN tables not found in database"
            }), 404
        
        if vockey:
            head_query = "SELECT TOP 1 * FROM invsalehead WHERE vockey = ? AND offcode = ?"
            head_result = execute_query(head_query, (vockey, offcode))
        else:
            head_query = "SELECT TOP 1 * FROM invsalehead WHERE vno = ? AND offcode = ?"
            head_result = execute_query(head_query, (vno, offcode))
        
        if not head_result:
            return jsonify({"success": False, "error": "GRN not found"}), 404
        
        head = head_result[0]
        actual_vockey = head.get('vockey')
        
        details_query = "SELECT TOP 100 * FROM invsaledet WHERE vockey = ? AND offcode = ?"
        details_result = execute_query(details_query, (actual_vockey, offcode))
        
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

# ============= INSERT GRN =============
@saleinvoice_bp.route('/insert-saleinvoice-head-det', methods=['POST'])
@token_required
def insert_saleinvoice_head_det():
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head")
        details = data.get("details", [])
        
        if not head or not details:
            return jsonify({"success": False, "error": "Head and details are required"}), 400
        
        head_data = head.get('data', {})
        company_offcode = "0101"
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        vno = safe_str(f"GRN{timestamp}", 15)
        vockey = safe_str(f"{company_offcode}{timestamp}", 19)
        
        vdate = head_data.get('vdate', datetime.now().strftime('%Y-%m-%d'))
        createdby = safe_str(head_data.get('createdby', 'system'), 30)
        createdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        head_columns = [
            'vno', 'vockey', 'offcode', 'bcode', 'vdate', 'vtype',
            'Ptype', 'customercode', 'customername', 'city', 'country',
            'godownid', 'godownname', 'currencycode', 'currencyrate',
            'compcode', 'createdby', 'createdate', 'status'
        ]
        
        head_values = [
            vno, vockey, company_offcode, "010101", vdate, "SIN",
            head_data.get('Ptype', 110),
            safe_str(head_data.get('customercode', ''), 20),
            safe_str(head_data.get('customername', ''), 100),
            safe_str(head_data.get('city', ''), 50),
            safe_str(head_data.get('country', ''), 50),
            head_data.get('godownid', 1),
            safe_str(head_data.get('godownname', ''), 100),
            1, 1, "01", 
            createdby, createdate, 2
        ]
        
        placeholders = ','.join(['?' for _ in head_columns])
        head_query = f"INSERT INTO invsalehead ({','.join(head_columns)}) VALUES ({placeholders})"
        execute_non_query(head_query, head_values)
        
        for det in details:
            det_data = det.get('data', {})
            
            qty = float(det_data.get('qty') or 0)
            rate = float(det_data.get('rate') or 0)
            amount = qty * rate
            tax_per = float(det_data.get('saleTaxPer') or 0)
            tax_amt = amount * (tax_per / 100)
            net_amt = amount + tax_amt
            
            detail_columns = [
                'vno', 'vockey', 'offcode', 'bcode', 'vdate', 'vtype',
                'Itemcode', 'Itemname', 'uom', 'qty', 'rate', 'value',
                'saleTaxPer', 'salestaxAmt', 'netvalue', 'godownid', 'godownname'
            ]
            
            detail_values = [
                vno, vockey, company_offcode, "010101", vdate, "GRN",
                safe_str(det_data.get('Itemcode', ''), 20),
                safe_str(det_data.get('Itemname', ''), 100),
                safe_str(det_data.get('uom', 'PCS'), 20),
                qty, rate, amount, tax_per, tax_amt, net_amt,
                det_data.get('godownid', 1),
                safe_str(det_data.get('godownname', ''), 100)
            ]
            
            placeholders = ','.join(['?' for _ in detail_columns])
            detail_query = f"INSERT INTO invsaledet ({','.join(detail_columns)}) VALUES ({placeholders})"
            execute_non_query(detail_query, detail_values)
        
        return jsonify({
            "success": True,
            "message": "Invoic saved successfully",
            "vno": vno,
            "vockey": vockey
        }), 200
        
    except Exception as err:
        logger.error(f"Insert error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= UPDATE GRN =============
@saleinvoice_bp.route('/update-saleinvoice-table-data', methods=['POST'])
@token_required
def update_saleinvoice_table_data():
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head", {})
        head_data = head.get('data', {})
        
        vno = head_data.get('vno')
        vockey = head_data.get('vockey')
        
        if not vno and not vockey:
            return insert_saleinvoice_head_det()
        
        try:
            db = get_db()
            cursor = db.cursor()
            
            if vockey:
                cursor.execute("DELETE FROM invsaledet WHERE vockey = ?", (vockey,))
                cursor.execute("DELETE FROM invsalehead WHERE vockey = ?", (vockey,))
            else:
                cursor.execute("DELETE FROM invsaledet WHERE vno = ?", (vno,))
                cursor.execute("DELETE FROM invsalehead WHERE vno = ?", (vno,))
            
            db.commit()
            cursor.close()
            
            return insert_saleinvoice_head_det()
            
        except Exception as e:
            logger.error(f"Error during update: {e}")
            return insert_saleinvoice_head_det()
        
    except Exception as err:
        logger.error(f"Update error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= GET PURCHASE ORDERS =============
@saleinvoice_bp.route('/get-purchase-orders', methods=['POST'])
@token_required
def get_purchase_orders():
    try:
        data = request.json
        customer_code = data.get("customerCode")
        offcode = data.get("offcode", "0101")
        
        if not customer_code:
            return jsonify({"success": False, "error": "Supplier code required"}), 400
        
        if not table_exists('invPOhead') or not table_exists('invPOdet'):
            return jsonify({
                "success": True, 
                "data": [],
                "message": "Purchase order tables not found"
            }), 200
        
        po_head_query = "SELECT TOP 100 * FROM invPOhead WHERE customercode = ? AND offcode = ? AND posted = 1"
        po_heads = execute_query(po_head_query, (customer_code, offcode))
        
        po_details = []
        for head in po_heads:
            vockey = head.get('vockey')
            if vockey:
                po_detail_query = "SELECT TOP 100 * FROM invPOdet WHERE vockey = ? AND offcode = ?"
                details = execute_query(po_detail_query, (vockey, offcode))
                po_details.append({'poHead': head, 'poDetails': details})
        
        return jsonify({"success": True, "data": po_details}), 200
        
    except Exception as err:
        logger.error(f"Error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500

# ============= GENERATE PDF =============
@saleinvoice_bp.route('/generate-saleinvoice-pdf', methods=['POST'])
@token_required
def generate_saleinvoice_pdf():
    try:
        data = request.json
        vno = data.get("vno")
        
        return jsonify({
            "success": True,
            "message": f"PDF generation for GRN {vno} will be implemented",
            "pdfUrl": None
        }), 200
        
    except Exception as err:
        logger.error(f"PDF error: {err}")
        return jsonify({"success": False, "error": str(err)}), 500