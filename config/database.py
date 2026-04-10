# Database configuration and connection utilities

import pyodbc
from flask import current_app, g
import logging

logger = logging.getLogger(__name__)

# Connection string
DB_SERVER = "192.168.100.113"
CONN_STR = (
    "DRIVER={SQL Server};"
    f"SERVER={DB_SERVER};"
    "DATABASE=AwaisFancy;"
    "UID=sa;"
    "PWD=786"
)

def get_db():
    """Get database connection"""
    if 'db' not in g:
        try:
            g.db = pyodbc.connect(CONN_STR)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise e
    return g.db

def get_db_connection():
    """Get database connection - alias for get_db"""
    return get_db()

def close_db(e=None):
    """Close database connection"""
    db = g.pop('db', None)
    if db is not None:
        db.close()
        logger.info("Database connection closed")

def execute_query(query, params=None):
    """Execute a SELECT query and return results as list of dictionaries"""
    cursor = None
    try:
        db = get_db()
        cursor = db.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Get column names
        columns = [column[0] for column in cursor.description] if cursor.description else []
        
        # Fetch all rows and convert to dictionaries
        rows = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Handle datetime objects
                if hasattr(value, 'strftime'):
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
                row_dict[col] = value
            rows.append(row_dict)
        
        return rows
        
    except Exception as e:
        logger.error(f"Query error: {e}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise e
    finally:
        if cursor:
            cursor.close()

def execute_non_query(query, params=None):
    """Execute a non-SELECT query (INSERT, UPDATE, DELETE) and return number of affected rows"""
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
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        if db:
            db.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()

def execute_stored_procedure(sp_name, params=None):
    """Execute a stored procedure and return results"""
    cursor = None
    try:
        db = get_db()
        cursor = db.cursor()
        
        # Build the stored procedure call
        if params:
            # Create parameter placeholders
            param_placeholders = ','.join(['?' for _ in params])
            query = f"{{CALL {sp_name} ({param_placeholders})}}"
            cursor.execute(query, params)
        else:
            query = f"{{CALL {sp_name}}}"
            cursor.execute(query)
        
        # Get column names
        columns = [column[0] for column in cursor.description] if cursor.description else []
        
        # Fetch all rows and convert to dictionaries
        rows = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Handle datetime objects
                if hasattr(value, 'strftime'):
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
                row_dict[col] = value
            rows.append(row_dict)
        
        return rows
        
    except Exception as e:
        logger.error(f"Stored procedure error: {e}")
        logger.error(f"SP Name: {sp_name}")
        logger.error(f"Params: {params}")
        raise e
    finally:
        if cursor:
            cursor.close()

def init_app(app):
    """Register database functions with app"""
    app.teardown_appcontext(close_db)