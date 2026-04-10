# routes/generic_crud_routes.py - Complete updated version
from flask import request, jsonify
from utils.db_helpers import (
    execute_non_query, execute_soap_query, get_table_identity_column,
    get_table_primary_keys, validate_columns
)
from utils.jwt_helper import token_required
from . import generic_crud_bp
import logging

logger = logging.getLogger(__name__)

def add_cors_headers(response):
    """Helper function to add CORS headers to response"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization, Accept, X-Requested-With')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    return response

def handle_options():
    """Handle OPTIONS requests for CORS preflight"""
    response = jsonify({'status': 'ok'})
    return add_cors_headers(response)


@generic_crud_bp.route('/table/insert', methods=['POST', 'OPTIONS'])
@token_required
def generic_insert():
    """
    Generic INSERT API for any table
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return handle_options(), 200
    
    try:
        data = request.json
        if not data:
            response = jsonify({"success": False, "error": "No JSON data provided"})
            return add_cors_headers(response), 400
        
        table_name = data.get("tableName")
        row_data = data.get("data", {})
        
        if not table_name:
            response = jsonify({"success": False, "error": "tableName is required"})
            return add_cors_headers(response), 400
        
        if not row_data:
            response = jsonify({"success": False, "error": "data object is required"})
            return add_cors_headers(response), 400
        
        identity_column = get_table_identity_column(table_name)
        primary_keys = get_table_primary_keys(table_name)
        
        is_valid, message = validate_columns(table_name, row_data)
        if not is_valid:
            response = jsonify({"success": False, "error": message})
            return add_cors_headers(response), 400
        
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
            response = jsonify({"success": False, "error": "No valid columns to insert"})
            return add_cors_headers(response), 400
        
        columns_str = ", ".join(columns_to_insert)
        placeholders = ", ".join(values_to_insert)
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        print("📝 INSERT QUERY:", insert_query)
        print("📝 PARAMS:", params)
        
        rows_affected = execute_non_query(insert_query, params)
        
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
        
        response = jsonify({
            "success": True,
            "message": f"Record inserted successfully into {table_name}",
            "rowsAffected": rows_affected,
            "insertedData": new_record or row_data,
            "primaryKeys": primary_keys,
            "identityColumn": identity_column
        })
        return add_cors_headers(response), 200
    
    except Exception as err:
        print("❌ Generic Insert Error:", err)
        response = jsonify({"success": False, "error": str(err)})
        return add_cors_headers(response), 500


@generic_crud_bp.route('/table/update', methods=['POST', 'OPTIONS'])
@token_required
def generic_update():
    """
    Generic UPDATE API for any table
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return handle_options(), 200
    
    try:
        data = request.json
        if not data:
            response = jsonify({"success": False, "error": "No JSON data provided"})
            return add_cors_headers(response), 400
        
        table_name = data.get("tableName")
        row_data = data.get("data", {})
        where_conditions = data.get("where", {})
        
        if not table_name:
            response = jsonify({"success": False, "error": "tableName is required"})
            return add_cors_headers(response), 400
        
        if not row_data:
            response = jsonify({"success": False, "error": "data object is required"})
            return add_cors_headers(response), 400
        
        primary_keys = get_table_primary_keys(table_name)
        
        if not where_conditions and primary_keys:
            for pk in primary_keys:
                if pk in row_data:
                    where_conditions[pk] = row_data[pk]
        
        if not where_conditions:
            response = jsonify({
                "success": False, 
                "error": "WHERE conditions required. Provide 'where' object or ensure primary keys are in data"
            })
            return add_cors_headers(response), 400
        
        all_columns = {**row_data, **where_conditions}
        is_valid, message = validate_columns(table_name, all_columns)
        if not is_valid:
            response = jsonify({"success": False, "error": message})
            return add_cors_headers(response), 400
        
        set_clauses = []
        params = []
        
        for column, value in row_data.items():
            if column in where_conditions:
                continue
            set_clauses.append(f"{column} = ?")
            params.append(value if value != "" else None)
        
        if not set_clauses:
            response = jsonify({"success": False, "error": "No columns to update"})
            return add_cors_headers(response), 400
        
        where_clauses = []
        for column, value in where_conditions.items():
            where_clauses.append(f"{column} = ?")
            params.append(value)
        
        where_str = " AND ".join(where_clauses)
        set_str = ", ".join(set_clauses)
        update_query = f"UPDATE {table_name} SET {set_str} WHERE {where_str}"
        
        print("📝 UPDATE QUERY:", update_query)
        print("📝 PARAMS:", params)
        
        rows_affected = execute_non_query(update_query, params)
        
        updated_record = None
        select_where = " AND ".join([f"{col} = '{val}'" for col, val in where_conditions.items()])
        select_query = f"SELECT * FROM {table_name} WHERE {select_where}"
        result = execute_soap_query(select_query)
        
        if result:
            updated_record = {}
            for key in result[0].keys():
                updated_record[key] = result[0][key][0] if result[0].get(key) and len(result[0][key]) > 0 else ""
        
        response = jsonify({
            "success": True,
            "message": f"Record updated successfully in {table_name}",
            "rowsAffected": rows_affected,
            "updatedData": updated_record,
            "whereConditions": where_conditions
        })
        return add_cors_headers(response), 200
    
    except Exception as err:
        print("❌ Generic Update Error:", err)
        response = jsonify({"success": False, "error": str(err)})
        return add_cors_headers(response), 500


@generic_crud_bp.route('/table/upsert', methods=['POST', 'OPTIONS'])
@token_required
def generic_upsert():
    """
    Generic UPSERT (INSERT or UPDATE) API
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return handle_options(), 200
    
    try:
        data = request.json
        if not data:
            response = jsonify({"success": False, "error": "No JSON data provided"})
            return add_cors_headers(response), 400
        
        table_name = data.get("tableName")
        row_data = data.get("data", {})
        
        if not table_name:
            response = jsonify({"success": False, "error": "tableName is required"})
            return add_cors_headers(response), 400
        
        if not row_data:
            response = jsonify({"success": False, "error": "data object is required"})
            return add_cors_headers(response), 400
        
        primary_keys = get_table_primary_keys(table_name)
        
        if not primary_keys:
            return generic_insert_logic(table_name, row_data)
        
        where_conditions = {}
        for pk in primary_keys:
            if pk in row_data:
                where_conditions[pk] = row_data[pk]

        if "offcode" in row_data:
            where_conditions["offcode"] = row_data["offcode"]
        
        if not where_conditions:
            return generic_insert_logic(table_name, row_data)
        
        where_clauses = [f"{col} = '{val}'" for col, val in where_conditions.items()]
        check_query = f"SELECT COUNT(*) as count FROM {table_name} WHERE {' AND '.join(where_clauses)}"
        
        result = execute_soap_query(check_query)
        record_exists = int(result[0].get("count", [0])[0]) > 0
        
        if record_exists:
            update_data = {k: v for k, v in row_data.items() if k not in where_conditions}
            return generic_update_logic(table_name, update_data, where_conditions)
        else:
            return generic_insert_logic(table_name, row_data)
    
    except Exception as err:
        print("❌ Generic Upsert Error:", err)
        response = jsonify({"success": False, "error": str(err)})
        return add_cors_headers(response), 500


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
    
    response = jsonify({
        "success": True,
        "operation": "insert",
        "message": f"Record inserted successfully into {table_name}",
        "rowsAffected": rows_affected,
        "insertedData": row_data
    })
    return add_cors_headers(response), 200


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
    
    response = jsonify({
        "success": True,
        "operation": "update",
        "message": f"Record updated successfully in {table_name}",
        "rowsAffected": rows_affected,
        "updatedData": {**where_conditions, **update_data},
        "whereConditions": where_conditions
    })
    return add_cors_headers(response), 200


@generic_crud_bp.route('/table/delete', methods=['POST', 'OPTIONS'])
@token_required
def generic_delete():
    """
    Generic DELETE API for any table
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return handle_options(), 200
    
    try:
        data = request.json
        if not data:
            response = jsonify({"success": False, "error": "No JSON data provided"})
            return add_cors_headers(response), 400
        
        table_name = data.get("tableName")
        where_conditions = data.get("where", {})
        
        if not table_name:
            response = jsonify({"success": False, "error": "tableName is required"})
            return add_cors_headers(response), 400
        
        if not where_conditions:
            response = jsonify({
                "success": False, 
                "error": "WHERE conditions required for DELETE operation"
            })
            return add_cors_headers(response), 400
        
        is_valid, message = validate_columns(table_name, where_conditions)
        if not is_valid:
            response = jsonify({"success": False, "error": message})
            return add_cors_headers(response), 400
        
        where_clauses = []
        params = []
        
        for column, value in where_conditions.items():
            where_clauses.append(f"{column} = ?")
            params.append(value)
        
        where_str = " AND ".join(where_clauses)
        delete_query = f"DELETE FROM {table_name} WHERE {where_str}"
        
        print("📝 DELETE QUERY:", delete_query)
        print("📝 PARAMS:", params)
        
        rows_affected = execute_non_query(delete_query, params)
        
        response = jsonify({
            "success": True,
            "message": f"Record(s) deleted successfully from {table_name}",
            "rowsAffected": rows_affected,
            "whereConditions": where_conditions
        })
        return add_cors_headers(response), 200
    
    except Exception as err:
        print("❌ Generic Delete Error:", err)
        response = jsonify({"success": False, "error": str(err)})
        return add_cors_headers(response), 500


@generic_crud_bp.route('/insert-SThead-det', methods=['POST', 'OPTIONS'])
@token_required
def insert_shift_head_detail():
    """
    Insert Shift Head with multiple Detail records in a transaction
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return handle_options(), 200
    
    try:
        data = request.json
        if not data:
            response = jsonify({"success": False, "error": "No JSON data provided"})
            return add_cors_headers(response), 400
        
        head = data.get("head", {})
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch", "")
        
        if not head:
            response = jsonify({"success": False, "error": "head data is required"})
            return add_cors_headers(response), 400
        
        if not details or not isinstance(details, list):
            response = jsonify({"success": False, "error": "details array is required"})
            return add_cors_headers(response), 400
        
        head_table = head.get("tableName", "HRMSShift")
        head_data = head.get("data", {})
        
        if not head_data:
            response = jsonify({"success": False, "error": "head data object is required"})
            return add_cors_headers(response), 400
        
        head_identity = get_table_identity_column(head_table)
        
        head_columns = []
        head_values = []
        head_params = []
        
        for column, value in head_data.items():
            if head_identity and column.lower() == head_identity.lower():
                continue
            head_columns.append(column)
            head_values.append("?")
            head_params.append(value if value != "" else None)
        
        if not head_columns:
            response = jsonify({"success": False, "error": "No valid columns to insert in head"})
            return add_cors_headers(response), 400
        
        head_columns_str = ", ".join(head_columns)
        head_placeholders = ", ".join(head_values)
        insert_head_query = f"INSERT INTO {head_table} ({head_columns_str}) VALUES ({head_placeholders})"
        
        print("📝 INSERT HEAD QUERY:", insert_head_query)
        print("📝 HEAD PARAMS:", head_params)
        
        head_rows_affected = execute_non_query(insert_head_query, head_params)
        
        if head_rows_affected == 0:
            response = jsonify({"success": False, "error": "Failed to insert head record"})
            return add_cors_headers(response), 500
        
        detail_table = "HRMSShiftTimeTable"
        detail_identity = get_table_identity_column(detail_table)
        detail_results = []
        
        for idx, detail in enumerate(details):
            try:
                detail_data = detail.get("data", {})
                
                if not detail_data:
                    detail_results.append({
                        "row": idx,
                        "success": False,
                        "error": "No detail data provided"
                    })
                    continue
                
                if "offcode" not in detail_data and selected_branch:
                    detail_data["offcode"] = selected_branch
                
                detail_columns = []
                detail_values = []
                detail_params = []
                
                for column, value in detail_data.items():
                    if detail_identity and column.lower() == detail_identity.lower():
                        continue
                    detail_columns.append(column)
                    detail_values.append("?")
                    detail_params.append(value if value != "" else None)
                
                if detail_columns:
                    detail_columns_str = ", ".join(detail_columns)
                    detail_placeholders = ", ".join(detail_values)
                    insert_detail_query = f"INSERT INTO {detail_table} ({detail_columns_str}) VALUES ({detail_placeholders})"
                    
                    print(f"📝 INSERT DETAIL {idx} QUERY:", insert_detail_query)
                    print(f"📝 DETAIL {idx} PARAMS:", detail_params)
                    
                    detail_rows_affected = execute_non_query(insert_detail_query, detail_params)
                    
                    detail_results.append({
                        "row": idx,
                        "success": True,
                        "rowsAffected": detail_rows_affected,
                        "data": detail_data
                    })
                else:
                    detail_results.append({
                        "row": idx,
                        "success": False,
                        "error": "No valid columns to insert"
                    })
                    
            except Exception as detail_err:
                print(f"❌ Error inserting detail {idx}:", detail_err)
                detail_results.append({
                    "row": idx,
                    "success": False,
                    "error": str(detail_err)
                })
        
        new_head_record = None
        head_primary_keys = get_table_primary_keys(head_table)
        
        if head_primary_keys and len(head_primary_keys) > 0:
            where_conditions = []
            for pk in head_primary_keys:
                if pk in head_data:
                    where_conditions.append(f"{pk} = '{head_data[pk]}'")
            
            if where_conditions:
                select_head_query = f"SELECT * FROM {head_table} WHERE {' AND '.join(where_conditions)}"
                head_result = execute_soap_query(select_head_query)
                if head_result:
                    new_head_record = {}
                    for key in head_result[0].keys():
                        new_head_record[key] = head_result[0][key][0] if head_result[0].get(key) and len(head_result[0][key]) > 0 else ""
        
        response = jsonify({
            "success": True,
            "message": f"Shift saved successfully with {len([r for r in detail_results if r['success']])} details",
            "head": {
                "table": head_table,
                "rowsAffected": head_rows_affected,
                "data": new_head_record or head_data
            },
            "details": detail_results,
            "successCount": len([r for r in detail_results if r['success']]),
            "errorCount": len([r for r in detail_results if not r['success']])
        })
        return add_cors_headers(response), 200
    
    except Exception as err:
        print("❌ Insert Shift Head Detail Error:", err)
        response = jsonify({"success": False, "error": str(err)})
        return add_cors_headers(response), 500


@generic_crud_bp.route('/update-SThead-det', methods=['POST', 'OPTIONS'])
@token_required
def update_shift_head_detail():
    """
    Update Shift Head with multiple Detail records in a transaction
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return handle_options(), 200
    
    try:
        data = request.json
        if not data:
            response = jsonify({"success": False, "error": "No JSON data provided"})
            return add_cors_headers(response), 400
        
        head = data.get("head", {})
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch", "")
        
        if not head:
            response = jsonify({"success": False, "error": "head data is required"})
            return add_cors_headers(response), 400
        
        if not details or not isinstance(details, list):
            response = jsonify({"success": False, "error": "details array is required"})
            return add_cors_headers(response), 400
        
        head_table = head.get("tableName", "HRMSShift")
        head_data = head.get("data", {})
        
        if not head_data:
            response = jsonify({"success": False, "error": "head data object is required"})
            return add_cors_headers(response), 400
        
        set_clauses = []
        head_params = []
        
        for column, value in head_data.items():
            if column in ['Code', 'offcode']:
                continue
            set_clauses.append(f"{column} = ?")
            head_params.append(value if value != "" else None)
        
        if set_clauses:
            where_clauses = ["Code = ?", "offcode = ?"]
            head_params.append(head_data.get('Code', ''))
            head_params.append(selected_branch or head_data.get('offcode', ''))
            
            set_str = ", ".join(set_clauses)
            where_str = " AND ".join(where_clauses)
            update_head_query = f"UPDATE {head_table} SET {set_str} WHERE {where_str}"
            
            print("📝 UPDATE HEAD QUERY:", update_head_query)
            print("📝 HEAD PARAMS:", head_params)
            
            head_rows_affected = execute_non_query(update_head_query, head_params)
        else:
            head_rows_affected = 0
        
        detail_table = "HRMSShiftTimeTable"
        detail_results = []
        
        for idx, detail in enumerate(details):
            try:
                detail_data = detail.get("data", {})
                
                if not detail_data:
                    detail_results.append({
                        "row": idx,
                        "success": False,
                        "error": "No detail data provided"
                    })
                    continue
                
                if "offcode" not in detail_data and selected_branch:
                    detail_data["offcode"] = selected_branch
                
                check_query = f"""
                    SELECT COUNT(*) as count FROM {detail_table} 
                    WHERE ShiftCode = '{detail_data.get('ShiftCode', '')}' 
                    AND WeekDay = '{detail_data.get('WeekDay', '')}' 
                    AND offcode = '{selected_branch}'
                """
                
                check_result = execute_soap_query(check_query)
                record_exists = int(check_result[0].get("count", [0])[0]) > 0
                
                if record_exists:
                    set_clauses = []
                    detail_params = []
                    
                    for column, value in detail_data.items():
                        if column in ['ShiftCode', 'WeekDay', 'offcode', 'Pk']:
                            continue
                        set_clauses.append(f"{column} = ?")
                        detail_params.append(value if value != "" else None)
                    
                    if set_clauses:
                        where_clauses = ["ShiftCode = ?", "WeekDay = ?", "offcode = ?"]
                        detail_params.extend([
                            detail_data.get('ShiftCode', ''),
                            detail_data.get('WeekDay', ''),
                            selected_branch
                        ])
                        
                        set_str = ", ".join(set_clauses)
                        where_str = " AND ".join(where_clauses)
                        update_detail_query = f"UPDATE {detail_table} SET {set_str} WHERE {where_str}"
                        
                        print(f"📝 UPDATE DETAIL {idx} QUERY:", update_detail_query)
                        print(f"📝 DETAIL {idx} PARAMS:", detail_params)
                        
                        detail_rows_affected = execute_non_query(update_detail_query, detail_params)
                        
                        detail_results.append({
                            "row": idx,
                            "success": True,
                            "operation": "update",
                            "rowsAffected": detail_rows_affected,
                            "data": detail_data
                        })
                    else:
                        detail_results.append({
                            "row": idx,
                            "success": True,
                            "operation": "nochange",
                            "message": "No changes to update"
                        })
                else:
                    detail_columns = []
                    detail_values = []
                    detail_params = []
                    
                    for column, value in detail_data.items():
                        if column.lower() == 'pk':
                            continue
                        detail_columns.append(column)
                        detail_values.append("?")
                        detail_params.append(value if value != "" else None)
                    
                    if detail_columns:
                        detail_columns_str = ", ".join(detail_columns)
                        detail_placeholders = ", ".join(detail_values)
                        insert_detail_query = f"INSERT INTO {detail_table} ({detail_columns_str}) VALUES ({detail_placeholders})"
                        
                        print(f"📝 INSERT DETAIL {idx} QUERY:", insert_detail_query)
                        print(f"📝 DETAIL {idx} PARAMS:", detail_params)
                        
                        detail_rows_affected = execute_non_query(insert_detail_query, detail_params)
                        
                        detail_results.append({
                            "row": idx,
                            "success": True,
                            "operation": "insert",
                            "rowsAffected": detail_rows_affected,
                            "data": detail_data
                        })
                    else:
                        detail_results.append({
                            "row": idx,
                            "success": False,
                            "error": "No valid columns to insert"
                        })
                    
            except Exception as detail_err:
                print(f"❌ Error processing detail {idx}:", detail_err)
                detail_results.append({
                    "row": idx,
                    "success": False,
                    "error": str(detail_err)
                })
        
        response = jsonify({
            "success": True,
            "message": f"Shift updated successfully",
            "head": {
                "table": head_table,
                "rowsAffected": head_rows_affected
            },
            "details": detail_results,
            "successCount": len([r for r in detail_results if r['success']]),
            "errorCount": len([r for r in detail_results if not r['success']])
        })
        return add_cors_headers(response), 200
    
    except Exception as err:
        print("❌ Update Shift Head Detail Error:", err)
        response = jsonify({"success": False, "error": str(err)})
        return add_cors_headers(response), 500


@generic_crud_bp.route('/table/bulk-insert', methods=['POST', 'OPTIONS'])
@token_required
def generic_bulk_insert():
    """
    Generic BULK INSERT API for inserting multiple records at once
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return handle_options(), 200
    
    try:
        data = request.json
        if not data:
            response = jsonify({"success": False, "error": "No JSON data provided"})
            return add_cors_headers(response), 400
        
        table_name = data.get("tableName")
        rows_data = data.get("data", [])
        
        if not table_name:
            response = jsonify({"success": False, "error": "tableName is required"})
            return add_cors_headers(response), 400
        
        if not rows_data or not isinstance(rows_data, list):
            response = jsonify({"success": False, "error": "data array is required"})
            return add_cors_headers(response), 400
        
        identity_column = get_table_identity_column(table_name)
        results = []
        success_count = 0
        error_count = 0
        
        for idx, row_data in enumerate(rows_data):
            try:
                is_valid, message = validate_columns(table_name, row_data)
                if not is_valid:
                    results.append({
                        "row": idx,
                        "success": False,
                        "error": message
                    })
                    error_count += 1
                    continue
                
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
        
        response = jsonify({
            "success": True,
            "message": f"Bulk insert completed. {success_count} successful, {error_count} failed",
            "totalProcessed": len(rows_data),
            "successCount": success_count,
            "errorCount": error_count,
            "results": results
        })
        return add_cors_headers(response), 200
    
    except Exception as err:
        print("❌ Generic Bulk Insert Error:", err)
        response = jsonify({"success": False, "error": str(err)})
        return add_cors_headers(response), 500