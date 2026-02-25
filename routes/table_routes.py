# Table structure routes

from flask import request, jsonify
import math
from utils.db_helpers import (
    execute_soap_query, execute_query, get_table_structure_data,
    get_table_primary_keys, get_table_identity_column, validate_columns,
    guess_type
)
from . import table_bp
import logging

logger = logging.getLogger(__name__)

@table_bp.route('/get-table-headers', methods=['POST', 'OPTIONS'])
def get_table_headers():
    """Get column headers and sample data from a table"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        query = f"SELECT TOP(1) * FROM {table_name}"
        
        rows = execute_soap_query(query)
        first_row = rows[0] if rows else {}
        fields = {}
        
        for key in first_row.keys():
            val = first_row[key][0] if first_row.get(key) and len(first_row[key]) > 0 else ""
            fields[key] = {
                "value": val,
                "type": guess_type(val)
            }
        
        return jsonify({"success": True, "fields": fields})
    
    except Exception as err:
        print("Error in /get-table-headers:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@table_bp.route('/get-table-structure', methods=['POST', 'OPTIONS'])
def get_table_structure():
    """Get detailed column structure from INFORMATION_SCHEMA"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        result = get_table_structure_data(table_name)
        return jsonify(result)
    
    except Exception as err:
        print("Error in /get-table-structure:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@table_bp.route('/get-table-relationships', methods=['POST', 'OPTIONS'])
def get_table_relationships():
    """Get foreign key relationships for a table"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        query = f"""
            SELECT 
                fk.name AS 'ConstraintName',
                OBJECT_NAME(fk.parent_object_id) AS 'TableName',
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS 'ColumnName',
                OBJECT_NAME(fk.referenced_object_id) AS 'ReferencedTable',
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS 'ReferencedColumn'
            FROM 
                sys.foreign_keys AS fk
            INNER JOIN 
                sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
            WHERE 
                OBJECT_NAME(fk.parent_object_id) = '{table_name}'
        """
        
        relationships = execute_soap_query(query)
        
        formatted_relationships = []
        for rel in relationships:
            formatted_relationships.append({
                "ConstraintName": rel.get("ConstraintName", [""])[0],
                "TableName": rel.get("TableName", [""])[0],
                "ColumnName": rel.get("ColumnName", [""])[0],
                "ReferencedTable": rel.get("ReferencedTable", [""])[0],
                "ReferencedColumn": rel.get("ReferencedColumn", [""])[0]
            })
        
        return jsonify({"success": True, "relationships": formatted_relationships})
    
    except Exception as err:
        print("Error in /get-table-relationships:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@table_bp.route('/get-table-data', methods=['POST', 'OPTIONS'])
def get_table_data():
    """Get paginated table data with optional WHERE clause and company filtering"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        table_name = data.get("tableName")
        frontend_where = data.get("where", "")
        page = int(data.get("page", 1))
        limit = int(data.get("limit", 10))
        use_pagination = data.get("usePagination", False)
        company_offcode = data.get("companyData", {}).get("company", {}).get("offcode") if data.get("companyData") else None
        
        if not table_name:
            return jsonify({"success": False, "error": "tableName is required"}), 400
        
        where_clauses = []
        
        if company_offcode:
            where_clauses.append(f"offcode = '{company_offcode}'")
        
        if frontend_where:
            where_clauses.append(f"({frontend_where})")
        
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        rows = []
        total_count = 0
        
        if use_pagination:
            start_row = (page - 1) * limit + 1
            end_row = page * limit
            
            data_query = f"""
                WITH Paginated AS (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
                    FROM {table_name}
                    {where_sql}
                )
                SELECT * FROM Paginated
                WHERE rn BETWEEN {start_row} AND {end_row}
            """
            count_query = f"SELECT COUNT(*) AS total FROM {table_name} {where_sql}"
            
            print("📤 DATA QUERY:", data_query)
            print("📤 COUNT QUERY:", count_query)
            
            result_data = execute_soap_query(data_query)
            result_count = execute_soap_query(count_query)
            
            rows = result_data
            total_count = int(result_count[0].get("total", [0])[0]) if result_count else 0
        else:
            data_query = f"SELECT * FROM {table_name} {where_sql}"
            print("📤 DATA QUERY:", data_query)
            rows = execute_soap_query(data_query)
            total_count = len(rows)
        
        json_rows = []
        for row in rows:
            obj = {}
            for key in row.keys():
                obj[key] = row[key][0] if row.get(key) and len(row[key]) > 0 else ""
            json_rows.append(obj)
        
        return jsonify({
            "success": True,
            "rows": json_rows,
            "totalCount": total_count,
            "page": page if use_pagination else 1,
            "limit": limit if use_pagination else total_count,
            "totalPages": math.ceil(total_count / limit) if use_pagination and limit > 0 else 1,
            "offcodeApplied": bool(company_offcode),
            "usePagination": use_pagination
        })
    
    except Exception as err:
        print("❌ getTableData fatal error:", err)
        return jsonify({"success": False, "error": str(err)}), 500

@table_bp.route('/debug/table-structure/<table_name>', methods=['GET'])
def debug_table_structure(table_name):
    """Get table structure to see actual column names and lengths"""
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        result = execute_query(query)
        return jsonify({
            "success": True,
            "table": table_name,
            "columns": result
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@table_bp.route('/check-table/<table_name>', methods=['GET'])
def check_table(table_name):
    """Check table structure"""
    try:
        from utils.db_helpers import get_table_columns, get_column_lengths
        columns = get_table_columns(table_name)
        lengths = get_column_lengths(table_name)
        return jsonify({
            "success": True,
            "table": table_name,
            "columns": columns,
            "column_lengths": lengths
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500