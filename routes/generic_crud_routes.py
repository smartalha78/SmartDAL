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

# Add primary keys
        for pk in primary_keys:
          if pk in row_data:
             where_conditions[pk] = row_data[pk]

# 🔴 IMPORTANT: also include offcode if present
        if "offcode" in row_data:
           where_conditions["offcode"] = row_data["offcode"]
        
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

@generic_crud_bp.route('/insert-SThead-det', methods=['POST', 'OPTIONS'])
def insert_shift_head_detail():
    """
    Insert Shift Head with multiple Detail records in a transaction
    """
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head", {})
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch", "")
        
        if not head:
            return jsonify({"success": False, "error": "head data is required"}), 400
        
        if not details or not isinstance(details, list):
            return jsonify({"success": False, "error": "details array is required"}), 400
        
        # Get table names
        head_table = head.get("tableName", "HRMSShift")
        head_data = head.get("data", {})
        
        if not head_data:
            return jsonify({"success": False, "error": "head data object is required"}), 400
        
        # Get identity column for head table
        head_identity = get_table_identity_column(head_table)
        
        # Insert Head record
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
            return jsonify({"success": False, "error": "No valid columns to insert in head"}), 400
        
        head_columns_str = ", ".join(head_columns)
        head_placeholders = ", ".join(head_values)
        insert_head_query = f"INSERT INTO {head_table} ({head_columns_str}) VALUES ({head_placeholders})"
        
        print("📝 INSERT HEAD QUERY:", insert_head_query)
        print("📝 HEAD PARAMS:", head_params)
        
        # Execute head insert
        head_rows_affected = execute_non_query(insert_head_query, head_params)
        
        if head_rows_affected == 0:
            return jsonify({"success": False, "error": "Failed to insert head record"}), 500
        
        # Insert Detail records
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
                
                # Add offcode if not present
                if "offcode" not in detail_data and selected_branch:
                    detail_data["offcode"] = selected_branch
                
                # Insert detail record
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
        
        # Get the newly inserted head record
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
        
        return jsonify({
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
        }), 200
    
    except Exception as err:
        print("❌ Insert Shift Head Detail Error:", err)
        return jsonify({"success": False, "error": str(err)}), 500


@generic_crud_bp.route('/update-SThead-det', methods=['POST', 'OPTIONS'])
def update_shift_head_detail():
    """
    Update Shift Head with multiple Detail records in a transaction
    """
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        head = data.get("head", {})
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch", "")
        
        if not head:
            return jsonify({"success": False, "error": "head data is required"}), 400
        
        if not details or not isinstance(details, list):
            return jsonify({"success": False, "error": "details array is required"}), 400
        
        # Update Head record
        head_table = head.get("tableName", "HRMSShift")
        head_data = head.get("data", {})
        
        if not head_data:
            return jsonify({"success": False, "error": "head data object is required"}), 400
        
        # Build UPDATE for head using Code and offcode as WHERE
        set_clauses = []
        head_params = []
        
        for column, value in head_data.items():
            if column in ['Code', 'offcode']:  # Skip key fields
                continue
            set_clauses.append(f"{column} = ?")
            head_params.append(value if value != "" else None)
        
        if set_clauses:
            # Add WHERE conditions
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
        
        # Update/Insert Detail records
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
                
                # Add offcode if not present
                if "offcode" not in detail_data and selected_branch:
                    detail_data["offcode"] = selected_branch
                
                # Check if detail exists using ShiftCode, WeekDay, and offcode
                check_query = f"""
                    SELECT COUNT(*) as count FROM {detail_table} 
                    WHERE ShiftCode = '{detail_data.get('ShiftCode', '')}' 
                    AND WeekDay = '{detail_data.get('WeekDay', '')}' 
                    AND offcode = '{selected_branch}'
                """
                
                check_result = execute_soap_query(check_query)
                record_exists = int(check_result[0].get("count", [0])[0]) > 0
                
                if record_exists:
                    # UPDATE existing detail - IMPORTANT: Exclude Pk from update
                    set_clauses = []
                    detail_params = []
                    
                    for column, value in detail_data.items():
                        # Skip key fields AND identity column (Pk)
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
                    # INSERT new detail - Exclude Pk as it's auto-generated
                    detail_columns = []
                    detail_values = []
                    detail_params = []
                    
                    for column, value in detail_data.items():
                        if column.lower() == 'pk':  # Skip PK for insert - it's auto-generated
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
        
        return jsonify({
            "success": True,
            "message": f"Shift updated successfully",
            "head": {
                "table": head_table,
                "rowsAffected": head_rows_affected
            },
            "details": detail_results,
            "successCount": len([r for r in detail_results if r['success']]),
            "errorCount": len([r for r in detail_results if not r['success']])
        }), 200
    
    except Exception as err:
        print("❌ Update Shift Head Detail Error:", err)
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