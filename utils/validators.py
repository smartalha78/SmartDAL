# Validation utilities

def validate_required_fields(data, required_fields):
    """Validate that all required fields are present"""
    missing = [field for field in required_fields if field not in data or not data[field]]
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"
    return True, None

def validate_table_name(table_name):
    """Validate table name (prevent SQL injection)"""
    # Add your table name validation logic here
    # This is a basic implementation - enhance as needed
    if not table_name or not isinstance(table_name, str):
        return False
    # Only allow alphanumeric and underscore
    import re
    return bool(re.match(r'^[a-zA-Z0-9_]+$', table_name))