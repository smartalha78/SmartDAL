# Database configuration and connection utilities

import pyodbc
from flask import current_app, g
import logging

logger = logging.getLogger(__name__)

# Connection string
CONN_STR = (
    "DRIVER={SQL Server};"
    "SERVER=192.168.100.113;"
    "DATABASE=AwaisFancy;"
    "UID=sa;"
    "PWD=786"
)

def get_db():
    """Get database connection"""
    if 'db' not in g:
        g.db = pyodbc.connect(CONN_STR)
    return g.db

def close_db(e=None):
    """Close database connection"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_app(app):
    """Register database functions with app"""
    app.teardown_appcontext(close_db)