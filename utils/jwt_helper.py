# utils/jwt_helper.py
import jwt
import datetime
import logging
from functools import wraps
from flask import request, jsonify, current_app

logger = logging.getLogger(__name__)

# JWT Configuration
JWT_SECRET_KEY = 'supersecret123'
JWT_ALGORITHM = 'HS256'
JWT_EXPIRATION_HOURS = 24

def generate_token(username, user_id):
    """Generate JWT token for user"""
    try:
        payload = {
            'username': username,
            'user_id': user_id,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=JWT_EXPIRATION_HOURS),
            'iat': datetime.datetime.utcnow()
        }
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
        return token
    except Exception as e:
        logger.error(f"Token generation error: {e}")
        return None

def decode_token(token):
    """Decode and verify JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        return None

def cors_response(data, status_code=200):
    """Helper function to create response with CORS headers"""
    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization, Accept, X-Requested-With')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Max-Age', '3600')
    return response, status_code

def token_required(f):
    """Decorator to require valid JWT token - Allows OPTIONS requests without token"""
    @wraps(f)
    def decorated(*args, **kwargs):
        # Allow OPTIONS requests to pass without token validation
        # This is required for CORS preflight requests
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)
        
        token = None
        
        # Get token from header
        auth_header = request.headers.get('Authorization')
        if auth_header:
            parts = auth_header.split()
            if len(parts) == 2 and parts[0].lower() == 'bearer':
                token = parts[1]
        
        if not token:
            # Return CORS headers even for error responses
            response = jsonify({'status': 'error', 'message': 'Token is missing'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
            return response, 401
        
        # Decode token
        payload = decode_token(token)
        if not payload:
            response = jsonify({'status': 'error', 'message': 'Token is invalid or expired'})
            response.headers.add('Access-Control-Allow-Origin', '*')
            response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
            return response, 401
        
        # Add user info to request
        request.current_user = payload
        return f(*args, **kwargs)
    
    return decorated

def update_user_token(username, token):
    """Update user token in database"""
    from config.database import execute_non_query
    
    try:
        query = """
            UPDATE ComUsers 
            SET FBRTokenNo = ?
            WHERE Userlogin = ?
        """
        rows_affected = execute_non_query(query, [token, username])
        return rows_affected > 0, None
    except Exception as e:
        logger.error(f"Error updating token: {e}")
        return False, str(e)