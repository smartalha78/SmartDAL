# Database helper functions

import pyodbc
from datetime import datetime
import math
from config.database import CONN_STR
import logging
import requests  
logger = logging.getLogger(__name__)

# Cache for table structures
TABLE_STRUCTURE_CACHE = {}
COLUMN_LENGTHS_CACHE = {}
# Known computed columns that should never be inserted
COMPUTED_COLUMNS = ['Name', 'FullName', 'DisplayName']  # Add more as needed

def execute_query(query, params=None):
    """Execute SQL query and return results"""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Check if this is a SELECT query
        if query.strip().upper().startswith("SELECT"):
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                row_dict = {}
                for idx, col in enumerate(columns):
                    value = row[idx]
                    if value is None:
                        value = ""
                    elif isinstance(value, datetime):
                        value = value.strftime("%Y-%m-%d %H:%M:%S")
                    row_dict[col] = value
                result.append(row_dict)
            return result
        else:
            # For INSERT/UPDATE/DELETE, commit and return rowcount
            conn.commit()
            return cursor.rowcount
            
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_non_query(query, params=None):
    """Execute INSERT, UPDATE, DELETE queries"""
    return execute_query(query, params)

def get_company_data():
    """Get company data from menu API"""
    try:
        # Call your login API to get full menu
        response = requests.post(
            "http://192.168.100.113:8000/GetMenu",
            json={
                "username": "talha",
                "userpassword": "abc123",
                "Menuid": "01",
                "nooftables": "3"
            }
        )
        data = response.json()
        if not data or not data.get("data") or not data["data"].get("tbl3"):
            return None

        menu = data["data"]["tbl3"]

        return {
            "company": data["data"]["tbl1"][0] if data["data"].get("tbl1") else {},
            "branches": data["data"]["tbl2"] if data["data"].get("tbl2") else [],
            "menu": menu
        }

    except Exception as e:
        print(f"Error getting company data: {e}")
        return None

def execute_soap_query(query):
    """Utility function to execute SQL queries and return results in a consistent format"""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            row_dict = {}
            for idx, col in enumerate(columns):
                value = row[idx]
                if value is None:
                    value = ""
                row_dict[col] = [str(value)] if value != "" else [""]
            result.append(row_dict)
        
        return result
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_many_non_query(queries_and_params):
    """Execute multiple queries in a transaction"""
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        for query, params in queries_and_params:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
        
        conn.commit()
        return True
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_table_primary_keys(table_name):
    """Get primary key columns for a table"""
    query = f"""
        SELECT 
            COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE 
            TABLE_NAME = '{table_name}' 
            AND CONSTRAINT_NAME LIKE 'PK%'
        ORDER BY ORDINAL_POSITION
    """
    
    try:
        result = execute_soap_query(query)
        pk_columns = [row.get("COLUMN_NAME", [""])[0] for row in result if row.get("COLUMN_NAME", [""])[0]]
        return pk_columns
    except:
        return []

def get_table_identity_column(table_name):
    """Get identity/auto-increment column for a table"""
    query = f"""
        SELECT 
            COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_NAME = '{table_name}'
            AND COLUMNPROPERTY(OBJECT_ID('{table_name}'), COLUMN_NAME, 'IsIdentity') = 1
    """
    
    try:
        result = execute_soap_query(query)
        if result:
            return result[0].get("COLUMN_NAME", [""])[0]
        return None
    except:
        return None

def get_table_columns(table_name):
    """Get column names for a table (with caching)"""
    if table_name in TABLE_STRUCTURE_CACHE:
        return TABLE_STRUCTURE_CACHE[table_name]
    
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        """
        
        result = execute_query(query)
        columns = [row['COLUMN_NAME'] for row in result]
        TABLE_STRUCTURE_CACHE[table_name] = columns
        return columns
    except Exception as e:
        print(f"Error getting table columns for {table_name}: {e}")
        return []

def get_column_lengths(table_name):
    """Get maximum lengths for string columns"""
    if table_name in COLUMN_LENGTHS_CACHE:
        return COLUMN_LENGTHS_CACHE[table_name]
    
    try:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        """
        
        result = execute_query(query)
        lengths = {}
        for row in result:
            col_name = row['COLUMN_NAME']
            max_length = row['CHARACTER_MAXIMUM_LENGTH']
            if max_length and max_length > 0:  # Only for varchar/nvarchar columns
                lengths[col_name] = max_length
        
        COLUMN_LENGTHS_CACHE[table_name] = lengths
        return lengths
    except Exception as e:
        print(f"Error getting column lengths for {table_name}: {e}")
        return {}

def get_table_structure_data(table_name):
    """Get table structure for validation"""
    try:
        query = f"""
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        
        columns = execute_soap_query(query)
        structure = []
        
        for col in columns:
            structure.append({
                "name": col.get("COLUMN_NAME", [""])[0],
                "type": col.get("DATA_TYPE", [""])[0],
                "maxLength": col.get("CHARACTER_MAXIMUM_LENGTH", [None])[0],
                "nullable": col.get("IS_NULLABLE", ["NO"])[0] == "YES"
            })
        
        return {"success": True, "structure": structure}
    except Exception as err:
        return {"success": False, "error": str(err)}

def filter_valid_columns(table_name, input_data, exclude_computed=True):
    """Filter input data to only include columns that exist in the table"""
    valid_columns = get_table_columns(table_name)
    column_lengths = get_column_lengths(table_name)
    
    if not valid_columns:
        return input_data
    
    filtered_data = {}
    for key, value in input_data.items():
        # Skip if column doesn't exist in table
        if key not in valid_columns:
            print(f"⚠️ Column '{key}' not found in {table_name}, skipping")
            continue
        
        # Skip computed columns if exclude_computed is True
        if exclude_computed and key in COMPUTED_COLUMNS:
            print(f"⚠️ Column '{key}' is a computed column, skipping INSERT")
            continue
        
        # Truncate string values if they exceed column length
        if key in column_lengths and isinstance(value, str):
            value = truncate_string(value, column_lengths[key])
        
        filtered_data[key] = value
    
    return filtered_data

def truncate_string(value, max_length):
    """Truncate string to max_length if needed"""
    if value and isinstance(value, str) and max_length:
        if len(value) > max_length:
            print(f"⚠️ Truncating string from {len(value)} to {max_length} characters")
            return value[:max_length]
    return value

def validate_columns(table_name, data):
    """Validate that all columns in data exist in the table structure"""
    structure_result = get_table_structure_data(table_name)
    if not structure_result.get("success"):
        return False, "Could not validate table structure"
    
    valid_columns = [col["name"] for col in structure_result["structure"]]
    invalid_columns = [col for col in data.keys() if col not in valid_columns]
    
    if invalid_columns:
        return False, f"Invalid columns: {', '.join(invalid_columns)}"
    
    return True, "Valid"

def build_insert_query(table_name, rows, exclude_columns=None):
    """Build INSERT query with column validation"""
    if not rows or len(rows) == 0:
        return None
    
    if exclude_columns is None:
        exclude_columns = []
    
    # Add computed columns to exclude list
    exclude_columns.extend(COMPUTED_COLUMNS)
    
    # Get valid columns for this table
    valid_columns = get_table_columns(table_name)
    
    if not valid_columns:
        print(f"⚠️ Could not get columns for table: {table_name}")
        return None
    
    print(f"🔍 Building INSERT for: {table_name}")
    print(f"🔍 Valid columns in table: {valid_columns}")
    print(f"🔍 Excluding columns: {exclude_columns}")
    
    # Process each row and filter columns
    processed_rows = []
    for row in rows:
        # Filter to only valid columns and exclude specified ones
        filtered_row = {}
        for key, val in row.items():
            if (key in valid_columns and 
                key not in exclude_columns and 
                key not in COMPUTED_COLUMNS):
                filtered_row[key] = val
            else:
                print(f"  Skipping column '{key}' - not in table, excluded, or computed")
        
        processed_rows.append(filtered_row)
    
    if not processed_rows or not processed_rows[0]:
        print(f"⚠️ No valid columns to insert for table: {table_name}")
        return None
    
    # Get columns from first row
    columns = list(processed_rows[0].keys())
    print(f"🔍 Columns to insert: {columns}")
    
    # Build values for each row
    values_list = []
    for row_idx, row in enumerate(processed_rows):
        row_values = []
        for col in columns:
            val = row.get(col)
            
            # Handle different value types
            if val is None:
                row_values.append("NULL")
            elif val == "":
                row_values.append("''")
            elif isinstance(val, bool):
                row_values.append("1" if val else "0")
            elif isinstance(val, (int, float)):
                row_values.append(str(val))
            elif isinstance(val, str) and val.upper() == "NULL":
                row_values.append("NULL")
            elif isinstance(val, str):
                # Check if it's a date that needs formatting
                if 'Date' in col or col in ['DOB', 'JoiningDate', 'AppointmentDate', 'ProbitionDate', 
                                           'ContractStartDate', 'ContractEndDate', 'LeftDate', 
                                           'IDExpiryDate', 'PassportExpiryDate']:
                    try:
                        # Try to format the date properly
                        if ' ' in val:
                            date_obj = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                        else:
                            date_obj = datetime.strptime(val, "%Y-%m-%d")
                        formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                        row_values.append(f"'{formatted_date}'")
                    except:
                        escaped_val = val.replace("'", "''")
                        row_values.append(f"'{escaped_val}'")
                else:
                    escaped_val = val.replace("'", "''")
                    row_values.append(f"'{escaped_val}'")
            else:
                str_val = str(val)
                escaped_val = str_val.replace("'", "''")
                row_values.append(f"'{escaped_val}'")
        
        values_list.append(f"({', '.join(row_values)})")
    
    values_str = ", ".join(values_list)
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES {values_str}"
    
    print(f"🔍 Generated Query: {query[:200]}...")
    return query

def build_where_clause(where_obj):
    """Build WHERE clause from object"""
    if not where_obj:
        return ""
    
    clauses = []
    for key, val in where_obj.items():
        if val is None:
            clauses.append(f"{key} IS NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")
            clauses.append(f"{key} = '{escaped_val}'")
        elif isinstance(val, bool):
            clauses.append(f"{key} = {1 if val else 0}")
        else:
            clauses.append(f"{key} = {val}")
    
    return " AND ".join(clauses)

def guess_type(value):
    """Utility function to guess the type of a value"""
    if value == "" or value is None:
        return "string"
    try:
        int(value)
        return "integer"
    except (ValueError, TypeError):
        try:
            float(value)
            return "float"
        except (ValueError, TypeError):
            return "string"

def format_date_for_sql(date=None):
    """Format date for SQL Server"""
    if date is None:
        date = datetime.now()
    elif isinstance(date, str):
        try:
            date = datetime.strptime(date, "%Y-%m-%d")
        except:
            try:
                date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            except:
                date = datetime.now()
    
    return date.strftime("%Y-%m-%d %H:%M:%S")

def get_user_uid(username):
    """Get UID from comUsers table"""
    try:
        query = f"SELECT TOP 1 Uid FROM comUsers WHERE Userlogin = '{username.replace(chr(39), chr(39)+chr(39))}'"
        rows = execute_query(query)
        if rows and len(rows) > 0:
            return rows[0].get('Uid', '07')
    except Exception as err:
        print(f"Could not fetch Uid from comUsers: {err}")
    return "07"  # Default UID