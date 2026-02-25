# Generic CRUD operations

from flask import request, jsonify
from utils.db_helpers import (
    execute_non_query, execute_soap_query, get_table_identity_column,
    get_table_primary_keys, validate_columns
)
from . import generic_crud_bp
import logging

logger = logging.getLogger(__name__)

@generic_crud_bp.route('/table/insert', methods=['POST', 'OPTIONS'])
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

@generic_crud_bp.route('/table/update', methods=['POST', 'OPTIONS'])
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

@generic_crud_bp.route('/table/upsert', methods=['POST', 'OPTIONS'])
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

@generic_crud_bp.route('/table/delete', methods=['POST', 'OPTIONS'])
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

@generic_crud_bp.route('/table/bulk-insert', methods=['POST', 'OPTIONS'])
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