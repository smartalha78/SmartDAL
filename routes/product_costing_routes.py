from flask import request, jsonify
from config.database import get_db
from utils.jwt_helper import token_required
import logging

from . import product_costing_bp

logger = logging.getLogger(__name__)


# ================= DB HELPERS =================
def get_cursor():
    db = get_db()
    return db.cursor()


def execute_query(query, params=None, fetch_one=False):
    cursor = None
    try:
        cursor = get_cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        if fetch_one:
            row = cursor.fetchone()
            if row:
                columns = [column[0] for column in cursor.description] if cursor.description else []
                return dict(zip(columns, row))
            return None

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


# ================= GET PRODUCTS =================
@product_costing_bp.route('/get-products', methods=['POST'])
@token_required
def get_products():
    try:
        data = request.json or {}
        offcode = data.get('offcode', '0101')

        query = """
            SELECT 
                ItemCode, 
                ItemName, 
                offcode, 
                uom, 
                CostPrice, 
                SalePrice, 
                IsItemLevel,
                alterItemName,
                HeadItemCode
            FROM IMF 
            WHERE offcode = ? 
                AND isActive = 'True' 
                AND IsItemLevel = 'True'
            ORDER BY ItemCode
        """

        results = execute_query(query, [offcode])

        # UOM Mapping
        uom_query = "SELECT ccode, cname FROM comUOM WHERE offcode = ? AND Isactive = 'True'"
        uoms = execute_query(uom_query, [offcode])
        uom_map = {str(u['ccode']): u['cname'] for u in uoms}

        for row in results:
            row['uomName'] = uom_map.get(str(row.get('uom', '')), '')

        return jsonify({
            "success": True,
            "data": results
        }), 200

    except Exception as err:
        logger.error(f"Error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


# ================= GET PROCESSES =================
@product_costing_bp.route('/get-processes', methods=['POST'])
@token_required
def get_processes():
    try:
        data = request.json or {}
        offcode = data.get('offcode', '0101')
        product_code = data.get('productCode')

        # Get all active processes
        query = """
            SELECT ccode as ProcessID, cname as ProcessName
            FROM comProcess 
            WHERE offcode = ? AND isActive = 'True'
            ORDER BY ccode
        """
        results = execute_query(query, [offcode])

        # If product_code is provided, get only processes used in BOM for this product
        if product_code:
            # Get processes from BOM for this product
            bom_processes_query = """
                SELECT DISTINCT p.ccode as ProcessID, p.cname as ProcessName
                FROM IMF i
                INNER JOIN IMFBom b ON i.HeadItemCode = b.ItemCode AND i.offcode = b.offcode
                INNER JOIN comProcess p ON b.ProcessID = p.ccode AND b.offcode = p.offcode
                WHERE i.ItemCode = ? AND i.offcode = ?
                ORDER BY p.ccode
            """
            bom_processes = execute_query(bom_processes_query, [product_code, offcode])
            if bom_processes:
                return jsonify({"success": True, "data": bom_processes}), 200

        return jsonify({"success": True, "data": results}), 200

    except Exception as err:
        logger.error(f"Error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


# ================= GET PROCESSES WITH WORKFLOW RATES =================
@product_costing_bp.route('/get-processes-with-rates', methods=['POST'])
@token_required
def get_processes_with_rates():
    try:
        data = request.json or {}
        offcode = data.get('offcode', '0101')
        product_code = data.get('productCode')

        # Get processes used in BOM for this product
        query = """
            SELECT DISTINCT 
                p.ccode as ProcessID, 
                p.cname as ProcessName,
                ISNULL(w.rate, 0) as defaultRate
            FROM IMF i
            INNER JOIN IMFBom b ON i.HeadItemCode = b.ItemCode AND i.offcode = b.offcode
            INNER JOIN comProcess p ON b.ProcessID = p.ccode AND b.offcode = p.offcode
            LEFT JOIN comWorkFlowDet w ON p.ccode = w.processid 
                AND w.ccode = i.ProductWorkFlow 
                AND w.offcode = i.offcode
            WHERE i.ItemCode = ? AND i.offcode = ?
            ORDER BY p.ccode
        """
        results = execute_query(query, [product_code, offcode])

        return jsonify({
            "success": True,
            "data": results
        }), 200

    except Exception as err:
        logger.error(f"Error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


# ================= GET FACTORY OVERHEADS =================
@product_costing_bp.route('/get-factory-overheads', methods=['POST'])
@token_required
def get_factory_overheads():
    try:
        data = request.json or {}
        offcode = data.get('offcode', '0101')

        query = """
            SELECT ccode, cname
            FROM comFOH 
            WHERE offcode = ? AND isActive = 'True'
            ORDER BY ccode
        """

        results = execute_query(query, [offcode])

        return jsonify({
            "success": True,
            "data": results
        }), 200

    except Exception as err:
        logger.error(f"Error: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


# ================= CALCULATE REQUIRED SUMMARY =================
@product_costing_bp.route('/calculate-required-summary', methods=['POST'])
@token_required
def calculate_required_summary():
    """Calculate costs using HeadItemCode relationship with invBIS Value"""
    try:
        data = request.json or {}
        product_code = data.get('productCode')
        production_qty = float(data.get('productionQty', 1))
        rate_type = data.get('rateType', 'profile')  # 'profile', 'lastSale', 'average'
        offcode = data.get('offcode', '0101')

        if not product_code:
            return jsonify({"success": False, "error": "productCode is required"}), 400

        if production_qty <= 0:
            production_qty = 1

        logger.info(f"Calculating summary for product: {product_code}, qty: {production_qty}, rateType: {rate_type}")

        # Get BOM using HeadItemCode relationship
        bom_query = """
            SELECT 
                i.ItemCode AS FinishedProduct,
                i.ItemName AS FinishedProductName,
                i.HeadItemCode,
                b.ItemCode AS BOMParent,
                b.BOMItemCode AS RawMaterialCode,
                b.NoOfQtyRequired,
                ISNULL(b.ForNoOfPeices, 1) AS ForNoOfPeices,
                b.ProcessID,
                b.uom AS BOM_UOM,
                rm.ItemName AS RawMaterialName,
                rm.CostPrice AS ProfileCostPrice,
                rm.SalePrice AS ProfileSalePrice,
                rm.uom AS ItemUOM,
                rm.alterItemName,
                ISNULL(inv.LastSaleRate, 0) AS LastSaleRate,
                ISNULL(inv.LastPoRate, 0) AS LastPoRate,
                ISNULL(inv.Qty, 0) AS AvailableQty,
                ISNULL(inv.averageRate, 0) AS averageRate,
                ISNULL(inv.Value, 0) AS StockValue,
                p.cname AS ProcessName
            FROM IMF i
            INNER JOIN IMFBom b ON i.HeadItemCode = b.ItemCode 
                AND i.offcode = b.offcode
            LEFT JOIN IMF rm ON b.BOMItemCode = rm.ItemCode 
                AND b.offcode = rm.offcode
            LEFT JOIN invBIS inv ON b.BOMItemCode = inv.ItemCode 
                AND b.offcode = inv.offcode 
                AND inv.Godownid = '1'
            LEFT JOIN comProcess p ON b.ProcessID = p.ccode 
                AND b.offcode = p.offcode
            WHERE i.ItemCode = ? AND i.offcode = ?
            ORDER BY b.ProcessID, b.PK
        """

        bom_results = execute_query(bom_query, [product_code, offcode])

        if not bom_results:
            # Try to get product info for better error message
            product_info = execute_query(
                "SELECT ItemName, HeadItemCode FROM IMF WHERE ItemCode = ? AND offcode = ?",
                [product_code, offcode], fetch_one=True
            )
            head_code = product_info.get('HeadItemCode') if product_info else 'Unknown'
            return jsonify({
                "success": False,
                "error": f"No BOM found for product {product_code}. HeadItemCode is '{head_code}'. Please check IMFBom table where ItemCode = '{head_code}'"
            }), 404

        logger.info(f"Found {len(bom_results)} BOM items for product {product_code}")

        # Get UOMs
        uom_query = "SELECT ccode, cname FROM comUOM WHERE offcode = ? AND Isactive = 'True'"
        uoms = execute_query(uom_query, [offcode])
        uom_map = {str(u['ccode']): u['cname'] for u in uoms}

        # Calculate costs
        bom_details = []
        material_cost = 0.0
        total_stock_value = 0.0

        for row in bom_results:
            for_no_of_pieces = float(row.get('ForNoOfPeices') or 1)
            required_per_piece = float(row.get('NoOfQtyRequired') or 0)
            
            # Calculate required quantity based on production quantity
            required_qty = (required_per_piece / for_no_of_pieces) * production_qty
            
            # Get cost price based on rate type
            if rate_type == 'lastSale':
                cost_price = float(row.get('LastSaleRate') or 0)
                if cost_price <= 0:
                    cost_price = float(row.get('ProfileCostPrice') or 0)
            elif rate_type == 'average':
                cost_price = float(row.get('averageRate') or 0)
                if cost_price <= 0:
                    cost_price = float(row.get('ProfileCostPrice') or 0)
            else:  # profile
                cost_price = float(row.get('ProfileCostPrice') or 0)
            
            # Calculate value (required_qty * cost_price)
            value = required_qty * cost_price
            material_cost += value
            
            # Get stock value from invBIS
            stock_value = float(row.get('StockValue') or 0)
            total_stock_value += stock_value
            
            available_qty = float(row.get('AvailableQty') or 0)
            process_name = row.get('ProcessName') or "General"
            uom_code = row.get('BOM_UOM') or row.get('ItemUOM') or ''
            
            detail = {
                "processName": process_name,
                "processId": row.get('ProcessID'),
                "rawMaterialCode": row.get('RawMaterialCode'),
                "rawMaterialName": row.get('RawMaterialName') or row.get('alterItemName') or '',
                "uomName": uom_map.get(str(uom_code), uom_code),
                "requiredQty": round(required_qty, 4),
                "costPrice": round(cost_price, 4),
                "value": round(value, 4),
                "availableQty": round(available_qty, 2),
                "stockValue": round(stock_value, 2),
                "lastSaleRate": round(float(row.get('LastSaleRate') or 0), 4),
                "averageRate": round(float(row.get('averageRate') or 0), 4),
                "lastPoRate": round(float(row.get('LastPoRate') or 0), 4),
                "profileCostPrice": round(float(row.get('ProfileCostPrice') or 0), 4)
            }
            bom_details.append(detail)
        
        logger.info(f"Total Material Cost: {material_cost}, Total Stock Value: {total_stock_value}")
        
        return jsonify({
            "success": True,
            "data": {
                "bomDetails": bom_details,
                "materialCost": round(material_cost, 4),
                "totalStockValue": round(total_stock_value, 2),
                "productionQty": production_qty,
                "rateType": rate_type,
                "productName": bom_results[0]['FinishedProductName'] if bom_results else product_code,
                "productCode": product_code,
                "headItemCode": bom_results[0]['HeadItemCode'] if bom_results else None
            }
        }), 200
        
    except Exception as err:
        logger.error(f"Error in calculate_required_summary: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500


# ================= CALCULATE COMPLETE BOM COST =================
@product_costing_bp.route('/calculate-bom-cost', methods=['POST'])
@token_required
def calculate_bom_cost():
    """Calculate complete BOM cost including process and factory overheads"""
    try:
        data = request.json or {}
        product_code = data.get('productCode')
        production_qty = float(data.get('productionQty', 1))
        rate_type = data.get('rateType', 'profile')
        offcode = data.get('offcode', '0101')
        process_rates = data.get('processRates', {})
        foh_rates = data.get('fohRates', {})

        if not product_code:
            return jsonify({"success": False, "error": "productCode is required"}), 400

        if production_qty <= 0:
            production_qty = 1

        logger.info(f"Calculating complete BOM for product: {product_code}")

        # Get BOM using HeadItemCode relationship
        bom_query = """
            SELECT 
                i.ItemCode AS FinishedProduct,
                i.ItemName AS FinishedProductName,
                i.HeadItemCode,
                b.ItemCode AS BOMParent,
                b.BOMItemCode AS RawMaterialCode,
                b.NoOfQtyRequired,
                ISNULL(b.ForNoOfPeices, 1) AS ForNoOfPeices,
                b.ProcessID,
                b.uom AS BOM_UOM,
                rm.ItemName AS RawMaterialName,
                rm.CostPrice AS ProfileCostPrice,
                rm.SalePrice AS ProfileSalePrice,
                rm.uom AS ItemUOM,
                rm.alterItemName,
                ISNULL(inv.LastSaleRate, 0) AS LastSaleRate,
                ISNULL(inv.LastPoRate, 0) AS LastPoRate,
                ISNULL(inv.Qty, 0) AS AvailableQty,
                ISNULL(inv.averageRate, 0) AS averageRate,
                ISNULL(inv.Value, 0) AS StockValue,
                p.cname AS ProcessName
            FROM IMF i
            INNER JOIN IMFBom b ON i.HeadItemCode = b.ItemCode 
                AND i.offcode = b.offcode
            LEFT JOIN IMF rm ON b.BOMItemCode = rm.ItemCode 
                AND b.offcode = rm.offcode
            LEFT JOIN invBIS inv ON b.BOMItemCode = inv.ItemCode 
                AND b.offcode = inv.offcode 
                AND inv.Godownid = '1'
            LEFT JOIN comProcess p ON b.ProcessID = p.ccode 
                AND b.offcode = p.offcode
            WHERE i.ItemCode = ? AND i.offcode = ?
            ORDER BY b.ProcessID, b.PK
        """

        bom_results = execute_query(bom_query, [product_code, offcode])

        if not bom_results:
            return jsonify({
                "success": False,
                "error": f"No BOM found for product {product_code}"
            }), 404

        # Get UOMs
        uom_query = "SELECT ccode, cname FROM comUOM WHERE offcode = ? AND Isactive = 'True'"
        uoms = execute_query(uom_query, [offcode])
        uom_map = {str(u['ccode']): u['cname'] for u in uoms}

        # Get unique processes from BOM results
        unique_processes = {}
        for row in bom_results:
            pid = row.get('ProcessID')
            if pid and pid not in unique_processes:
                unique_processes[pid] = row.get('ProcessName') or f"Process {pid}"

        # Calculate costs
        material_cost = 0
        bom_details = []
        process_costs = {}

        for row in bom_results:
            for_no_of_pieces = float(row.get('ForNoOfPeices') or 1)
            required_per_piece = float(row.get('NoOfQtyRequired') or 0)

            required_qty = (required_per_piece / for_no_of_pieces) * production_qty
            
            if rate_type == 'lastSale':
                cost_price = float(row.get('LastSaleRate') or 0)
                if cost_price <= 0:
                    cost_price = float(row.get('ProfileCostPrice') or 0)
            elif rate_type == 'average':
                cost_price = float(row.get('averageRate') or 0)
                if cost_price <= 0:
                    cost_price = float(row.get('ProfileCostPrice') or 0)
            else:
                cost_price = float(row.get('ProfileCostPrice') or 0)

            value = required_qty * cost_price
            material_cost += value

            available_qty = float(row.get('AvailableQty') or 0)
            process_id = row.get('ProcessID')
            uom_code = row.get('BOM_UOM') or row.get('ItemUOM') or ''
            
            # Get process rate from user input
            process_rate = float(process_rates.get(str(process_id), 0)) if process_id else 0
            
            detail = {
                "processId": process_id,
                "processName": row.get('ProcessName') or "General",
                "rawMaterialCode": row.get('RawMaterialCode'),
                "rawMaterialName": row.get('RawMaterialName') or row.get('alterItemName') or '',
                "uom": uom_code,
                "uomName": uom_map.get(str(uom_code), uom_code),
                "requiredQty": round(required_qty, 4),
                "costPrice": round(cost_price, 4),
                "value": round(value, 4),
                "availableQty": round(available_qty, 2),
                "processRate": process_rate
            }
            bom_details.append(detail)

            if process_id:
                process_costs[process_id] = process_costs.get(process_id, 0) + (process_rate * production_qty)

        total_process_cost = sum(process_costs.values())
        total_foh_cost = sum(float(foh_rates.get(key, 0)) for key in foh_rates)
        total_cost = material_cost + total_process_cost + total_foh_cost

        # Prepare processes list for frontend
        processes_list = [{"ProcessID": pid, "ProcessName": name} for pid, name in unique_processes.items()]

        # Get factory overheads
        foh_query = """
            SELECT ccode, cname
            FROM comFOH 
            WHERE offcode = ? AND isActive = 'True'
        """
        foh_results = execute_query(foh_query, [offcode])

        return jsonify({
            "success": True,
            "data": {
                "productCode": product_code,
                "productName": bom_results[0]['FinishedProductName'],
                "productionQty": production_qty,
                "rateType": rate_type,
                "materialCost": round(material_cost, 4),
                "processCost": round(total_process_cost, 4),
                "processCosts": process_costs,
                "fohCost": round(total_foh_cost, 4),
                "totalCost": round(total_cost, 4),
                "bomDetails": bom_details,
                "processes": processes_list,
                "factoryOverheads": foh_results
            }
        }), 200

    except Exception as err:
        logger.error(f"Error in calculate_bom_cost: {err}", exc_info=True)
        return jsonify({"success": False, "error": str(err)}), 500