# routes/auth_routes.py
from flask import request, jsonify, current_app
import pyodbc
from config.database import CONN_STR, execute_query, execute_non_query, get_db
from utils.jwt_helper import generate_token, update_user_token, token_required, cors_response
from utils.password_helper import verify_password, hash_password, get_user_credentials, update_password
from . import auth_bp
import logging

logger = logging.getLogger(__name__)

@auth_bp.route('/GetMenu', methods=['GET', 'POST', 'OPTIONS'])
def GetMenu():
    """Get menu endpoint"""
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        return response, 200
    
    if request.method == 'GET':
        username = request.args.get("username")
        userpassword = request.args.get("userpassword")
        menuid = request.args.get("Menuid")
        nooftables = int(request.args.get("nooftables", 0))
    else:
        data = request.json
        if not data:
            return jsonify({
                "status": "error",
                "message": "No JSON data provided"
            }), 400
        
        username = data.get("username")
        userpassword = data.get("userpassword")
        menuid = data.get("Menuid")
        nooftables = int(data.get("nooftables", 0))
   
    if not username or not userpassword:
        return jsonify({
            "status": "error",
            "message": "Username and password are required"
        }), 400
    
    if not menuid:
        menuid = "01"
    
    if not nooftables or nooftables < 1:
        nooftables = 3
    
    try:
        # Get user credentials from database
        user_credentials = get_user_credentials(username)
        
        if not user_credentials:
            return jsonify({"status": "fail", "message": "Invalid user"}), 401

        user_id = user_credentials['uid']
        stored_password = user_credentials['encrypted_password']
        
        # Verify password
        is_valid = verify_password(userpassword, stored_password)
        
        if not is_valid:
            return jsonify({"status": "fail", "message": "Invalid password"}), 401
        
        # Generate JWT token
        token = generate_token(username, user_id)
        update_user_token(username, token)

        # Get menu data from stored procedure
        conn = get_db()
        cursor = conn.cursor()
        
        response_data = {}
        for index in range(1, nooftables + 1):
            cursor.execute(
                "{CALL sp_Quick_Method_Index (?, ?, ?)}",
                (menuid, index, username)
            )
            
            columns = [col[0] for col in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            
            table_data = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    if hasattr(value, 'strftime'):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')
                    row_dict[col] = value
                table_data.append(row_dict)
            
            response_data[f"tbl{index}"] = table_data
        
        cursor.close()
        
        response = jsonify({
            "status": "success",
            "data": response_data,
            "token": token,
            "user": {
                "username": username,
                "user_id": user_id
            }
        })
        
        # Add CORS headers
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 200

    except pyodbc.Error as db_ex:
        logger.error(f"Database error: {db_ex}")
        return jsonify({
            "status": "error",
            "message": f"Database error: {str(db_ex)}"
        }), 500
    except Exception as ex:
        logger.error(f"General error: {ex}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "message": str(ex)
        }), 500


@auth_bp.route('/password/update', methods=['POST', 'OPTIONS'])
@token_required
def update_password_endpoint():
    """
    Update user password
    """
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)
    
    try:
        data = request.json
        if not data:
            return cors_response({
                "success": False,
                "error": "No JSON data provided"
            }, 400)
        
        username = data.get('username')
        current_password = data.get('currentPassword')
        new_password = data.get('newPassword')
        
        if not username or not current_password or not new_password:
            return cors_response({
                "success": False,
                "error": "Username, current password, and new password are required"
            }, 400)
        
        # Get user from database
        query = """
            SELECT Uid, Userlogin, Userpassword
            FROM ComUsers 
            WHERE Userlogin = ?
        """
        
        users = execute_query(query, (username,))
        
        if not users:
            return cors_response({
                "success": False,
                "error": "User not found"
            }, 404)
        
        user = users[0]
        stored_password = user.get('Userpassword')
        
        # Verify current password
        if not verify_password(current_password, stored_password):
            return cors_response({
                "success": False,
                "error": "Current password is incorrect"
            }, 401)
        
        # Update password using helper function
        success, error = update_password(username, new_password)
        
        if success:
            return cors_response({
                "success": True,
                "message": "Password updated successfully"
            }, 200)
        else:
            return cors_response({
                "success": False,
                "error": error
            }, 500)
            
    except Exception as err:
        logger.error(f"Error updating password: {err}")
        return cors_response({
            "success": False,
            "error": str(err)
        }, 500)


@auth_bp.route('/password/reset', methods=['POST', 'OPTIONS'])
@token_required
def reset_password():
    """
    Reset user password (Admin only)
    """
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)
    
    try:
        data = request.json
        if not data:
            return cors_response({
                "success": False,
                "error": "No JSON data provided"
            }, 400)
        
        username = data.get('username')
        new_password = data.get('newPassword', 'admin123')
        
        if not username:
            return cors_response({
                "success": False,
                "error": "Username is required"
            }, 400)
        
        # Check if current user is admin
        current_user = request.current_user
        if current_user.get('username') != 'administrator':
            return cors_response({
                "success": False,
                "error": "Only administrators can reset passwords"
            }, 403)
        
        # Update password using helper function
        success, error = update_password(username, new_password)
        
        if success:
            return cors_response({
                "success": True,
                "message": f"Password reset successfully for user {username}",
                "new_password": new_password if new_password == 'admin123' else None
            }, 200)
        else:
            return cors_response({
                "success": False,
                "error": error
            }, 404)
            
    except Exception as err:
        logger.error(f"Error resetting password: {err}")
        return cors_response({
            "success": False,
            "error": str(err)
        }, 500)


@auth_bp.route('/debug/reencrypt-passwords', methods=['GET', 'POST', 'OPTIONS'])
def reencrypt_passwords():
    """
    Debug endpoint to re-encrypt all passwords to bcrypt
    """
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        return response, 200
    
    try:
        # Get all users
        query = "SELECT Uid, Userlogin, Userpassword FROM ComUsers"
        users = execute_query(query)
        
        results = []
        updated_count = 0
        
        for user in users:
            uid = user.get('Uid')
            username = user.get('Userlogin')
            current_password = user.get('Userpassword')
            
            # Check if password is not already bcrypt encrypted
            if current_password and not current_password.startswith('$2b$'):
                # Hash password with bcrypt
                from utils.password_helper import hash_password_bcrypt
                encrypted = hash_password_bcrypt(current_password)
                
                if encrypted:
                    # Update only Userpassword column
                    update_query = """
                        UPDATE ComUsers 
                        SET Userpassword = ?
                        WHERE Uid = ?
                    """
                    execute_non_query(update_query, (encrypted, uid))
                    updated_count += 1
                    results.append({
                        "username": username,
                        "status": "encrypted_to_bcrypt"
                    })
                else:
                    results.append({
                        "username": username,
                        "status": "failed_to_encrypt"
                    })
            else:
                results.append({
                    "username": username,
                    "status": "already_bcrypt_encrypted"
                })
        
        return jsonify({
            "success": True,
            "message": f"Re-encrypted {updated_count} passwords to bcrypt",
            "total_users": len(users),
            "updated_count": updated_count,
            "results": results
        }), 200
        
    except Exception as err:
        logger.error(f"Error re-encrypting passwords: {err}")
        return jsonify({
            "success": False,
            "error": str(err)
        }), 500


@auth_bp.route('/debug/update-password-manual', methods=['POST', 'OPTIONS'])
def update_password_manual():
    """
    Manual password update endpoint (for debugging)
    """
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        return response, 200
    
    try:
        data = request.json
        username = data.get('username', 'administrator')
        # Accept both new_password and newPassword
        new_password = data.get('new_password') or data.get('newPassword', 'admin')
        
        # Update password using helper function
        success, error = update_password(username, new_password)
        
        if success:
            return jsonify({
                "success": True,
                "message": f"Password updated for user {username}",
                "username": username,
                "new_password": new_password
            }), 200
        else:
            return jsonify({
                "success": False,
                "error": error
            }), 404
            
    except Exception as err:
        logger.error(f"Error updating password manually: {err}")
        return jsonify({
            "success": False,
            "error": str(err)
        }), 500


@auth_bp.route('/debug/check-password', methods=['POST', 'OPTIONS'])
def check_password():
    """
    Debug endpoint to check if a password is valid
    """
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        return response, 200
    
    try:
        data = request.json
        username = data.get('username', 'administrator')
        password = data.get('password', 'admin')
        
        # Get user credentials
        user_credentials = get_user_credentials(username)
        
        if not user_credentials:
            return jsonify({
                "success": False,
                "error": f"User {username} not found"
            }), 404
        
        stored_password = user_credentials['encrypted_password']
        
        # Verify password
        is_valid = verify_password(password, stored_password)
        
        return jsonify({
            "success": True,
            "username": username,
            "password_valid": is_valid,
            "has_stored_password": bool(stored_password)
        }), 200
        
    except Exception as err:
        logger.error(f"Error checking password: {err}")
        return jsonify({
            "success": False,
            "error": str(err)
        }), 500


@auth_bp.route('/debug/list-users', methods=['GET', 'OPTIONS'])
def list_users():
    """
    Debug endpoint to list all users
    """
    if request.method == 'OPTIONS':
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Methods', 'GET, OPTIONS')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        return response, 200
    
    try:
        query = "SELECT Uid, Userlogin, UserFullName, Useremail, userMobile FROM ComUsers ORDER BY Uid"
        users = execute_query(query)
        
        return jsonify({
            "success": True,
            "users": users,
            "count": len(users)
        }), 200
        
    except Exception as err:
        logger.error(f"Error listing users: {err}")
        return jsonify({
            "success": False,
            "error": str(err)
        }), 500