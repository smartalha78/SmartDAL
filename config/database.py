# Database configuration and connection utilities
import pyodbc
from flask import g
import logging
from contextlib import contextmanager
from functools import lru_cache
import time

logger = logging.getLogger(__name__)

# Connection string
DB_SERVER = "192.168.100.113"
CONN_STR = (
    "DRIVER={SQL Server};"
    f"SERVER={DB_SERVER};"
    "DATABASE=H20DB;"
    "UID=sa;"
    "PWD=786;"
    "Connection Timeout=10;"
)

# Simple cache for queries
query_cache = {}
CACHE_TTL = 300  # 5 minutes

def get_cache_key(query, params):
    return f"{query}_{str(params)}"

def get_cached_result(key):
    if key in query_cache:
        result, timestamp = query_cache[key]
        if time.time() - timestamp < CACHE_TTL:
            return result
        del query_cache[key]
    return None

def set_cached_result(key, result):
    query_cache[key] = (result, time.time())

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

def close_db(e=None):
    """Close database connection"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

@contextmanager
def get_cursor():
    """Get database cursor with automatic cleanup"""
    db = get_db()
    cursor = db.cursor()
    try:
        yield cursor
    finally:
        cursor.close()

def execute_query(query, params=None, use_cache=False):
    """Execute a SELECT query with optional caching"""
    cursor = None
    try:
        # Check cache for static data
        if use_cache:
            cache_key = get_cache_key(query, params)
            cached = get_cached_result(cache_key)
            if cached is not None:
                return cached
        
        db = get_db()
        cursor = db.cursor()
        
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
                if hasattr(value, 'strftime'):
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
                row_dict[col] = value
            rows.append(row_dict)
        
        # Cache static data
        if use_cache and rows:
            set_cached_result(cache_key, rows)
        
        return rows
        
    except Exception as e:
        logger.error(f"Query error: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()

def execute_non_query(query, params=None):
    """Execute a non-SELECT query"""
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
        if db:
            db.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()

def init_app(app):
    """Register database functions with app"""
    app.teardown_appcontext(close_db)