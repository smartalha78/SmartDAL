# routes/user_rights_routes.py
from flask import request, jsonify
from utils.db_helpers import execute_query, execute_non_query
from utils.jwt_helper import token_required  # ADD THIS IMPORT
from . import user_rights_bp
import logging

logger = logging.getLogger(__name__)

# SIMPLE TEST - Can be public or protected based on your need
@user_rights_bp.route('/user-rights/test', methods=['GET'])
# @token_required  # Optional: Uncomment if you want to protect this too
def test():
    return jsonify({
        "success": True,
        "message": "User rights blueprint is working!",
        "note": "This is a simple test without database"
    })

# Simple test with database - PROTECTED
@user_rights_bp.route('/user-rights/test-db', methods=['GET'])
@token_required  # ADD THIS DECORATOR
def test_db():
    try:
        result = execute_query("SELECT COUNT(*) as count FROM comUsers")
        return jsonify({
            "success": True,
            "message": "Database connection working",
            "user_count": result[0]['count'] if result else 0
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# Get user rights for a specific user and screen - PROTECTED
@user_rights_bp.route('/user-rights/get', methods=['POST'])
@token_required  # ADD THIS DECORATOR
def get_user_rights():
    """Get user rights for a specific user and screen"""
    try:
        data = request.json
        user_id = data.get('userId')
        menu_id = data.get('menuId')
        
        if not user_id or not menu_id:
            return jsonify({
                "success": False,
                "error": "userId and menuId are required"
            }), 400
        
        # Query to get user rights - REMOVED invalid columns
        query = """
            SELECT 
                Userid,
                Menuid,
                IsAdd,
                IsEdit,
                IsDelete,
                IsPrint,
                IsPost,
                IsCopy,
                IsSearch,
                IsUpload,
                IsBackDate,
                Isfavorite,
                IsActive,
                offcode,
                isDesktop
            FROM tbluserrights 
            WHERE Userid = ? AND Menuid = ?
        """
        
        result = execute_query(query, (user_id, menu_id))
        
        if result and len(result) > 0:
            return jsonify({
                "success": True,
                "rights": result[0],
                "hasRights": True
            })
        else:
            # Return default rights if none found - REMOVED invalid columns
            return jsonify({
                "success": True,
                "rights": {
                    "Userid": user_id,
                    "Menuid": menu_id,
                    "IsAdd": "False",
                    "IsEdit": "False",
                    "IsDelete": "False",
                    "IsPrint": "False",
                    "IsPost": "False",
                    "IsCopy": "",
                    "IsSearch": "True",
                    "IsUpload": "False",
                    "IsBackDate": "False",
                    "Isfavorite": "False",
                    "IsActive": "True",
                    "offcode": "0101",
                    "isDesktop": "False"
                },
                "hasRights": False
            })
            
    except Exception as e:
        logger.error(f"Error getting user rights: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# Save/Update user rights - PROTECTED
@user_rights_bp.route('/user-rights/save', methods=['POST'])
@token_required  # ADD THIS DECORATOR
def save_user_rights():
    """Save or update user rights"""
    try:
        data = request.json
        rights = data.get('rights', {})
        
        required_fields = ['Userid', 'Menuid']
        for field in required_fields:
            if field not in rights:
                return jsonify({
                    "success": False,
                    "error": f"{field} is required"
                }), 400
        
        # Check if rights already exist
        check_query = "SELECT COUNT(*) as count FROM tbluserrights WHERE Userid = ? AND Menuid = ?"
        check_result = execute_query(check_query, (rights['Userid'], rights['Menuid']))
        exists = check_result[0]['count'] > 0 if check_result else False
        
        # Prepare fields for insert/update - REMOVED invalid columns
        fields = [
            'IsAdd', 'IsEdit', 'IsDelete', 'IsPrint', 'IsPost',
            'IsCopy', 'IsSearch', 'IsUpload', 'IsBackDate', 
            'Isfavorite', 'IsActive', 'offcode', 'isDesktop'
        ]
        
        if exists:
            # Update existing rights
            set_clauses = []
            params = []
            for field in fields:
                if field in rights:
                    set_clauses.append(f"{field} = ?")
                    params.append(rights[field])
            
            params.append(rights['Userid'])
            params.append(rights['Menuid'])
            
            query = f"""
                UPDATE tbluserrights 
                SET {', '.join(set_clauses)}
                WHERE Userid = ? AND Menuid = ?
            """
        else:
            # Insert new rights
            columns = ['Userid', 'Menuid'] + fields
            placeholders = ['?', '?'] + ['?'] * len(fields)
            
            params = [rights['Userid'], rights['Menuid']]
            for field in fields:
                params.append(rights.get(field, '' if field == 'IsCopy' else 'False'))
            
            query = f"""
                INSERT INTO tbluserrights ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
        
        execute_non_query(query, params)
        
        return jsonify({
            "success": True,
            "message": "User rights saved successfully",
            "rights": rights
        })
        
    except Exception as e:
        logger.error(f"Error saving user rights: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# Get all users for dropdown - PROTECTED
@user_rights_bp.route('/user-rights/users', methods=['GET'])
@token_required  # ADD THIS DECORATOR
def get_users():
    """Get all users for dropdown selection"""
    try:
        query = "SELECT Uid, Userlogin, UserFullName FROM comUsers ORDER BY Userlogin"
        users = execute_query(query)
        
        return jsonify({
            "success": True,
            "users": users
        })
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# Get all menus for dropdown - PROTECTED
@user_rights_bp.route('/user-rights/menus', methods=['GET'])
@token_required  # ADD THIS DECORATOR
def get_menus():
    """Get all menus for dropdown selection"""
    try:
        query = """
            SELECT Menuid, MenuTitle, ParentId, MenuType 
            FROM tblmenu 
            ORDER BY Menuid
        """
        menus = execute_query(query)
        
        # Build tree structure
        menu_tree = build_menu_tree(menus)
        
        return jsonify({
            "success": True,
            "menus": menus,
            "menuTree": menu_tree
        })
    except Exception as e:
        logger.error(f"Error getting menus: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

def build_menu_tree(menus, parent_id=None):
    """Build hierarchical menu tree"""
    tree = []
    for menu in menus:
        if menu.get('ParentId') == parent_id:
            menu['children'] = build_menu_tree(menus, menu.get('Menuid'))
            tree.append(menu)
    return tree

# Get user rights for all menus (bulk) - PROTECTED
@user_rights_bp.route('/user-rights/bulk-get', methods=['POST'])
@token_required  # ADD THIS DECORATOR
def get_user_rights_bulk():
    """Get all user rights for a specific user"""
    try:
        data = request.json
        user_id = data.get('userId')
        
        if not user_id:
            return jsonify({"success": False, "error": "userId is required"}), 400
        
        # Query to get user rights - REMOVED invalid columns
        query = """
            SELECT 
                Userid,
                Menuid,
                IsAdd,
                IsEdit,
                IsDelete,
                IsPrint,
                IsPost,
                IsCopy,
                IsSearch,
                IsUpload,
                IsBackDate,
                Isfavorite,
                IsActive,
                offcode,
                isDesktop
            FROM tbluserrights 
            WHERE Userid = ?
        """
        
        rights = execute_query(query, (user_id,))
        
        # Convert to dictionary with Menuid as key
        rights_dict = {}
        for r in rights:
            menu_id = r.pop('Menuid')
            if 'Userid' in r:
                r.pop('Userid')
            rights_dict[menu_id] = r
        
        return jsonify({
            "success": True,
            "userId": user_id,
            "rights": rights_dict
        })
        
    except Exception as e:
        logger.error(f"Error getting bulk user rights: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# Bulk save user rights - PROTECTED
@user_rights_bp.route('/user-rights/bulk-save', methods=['POST'])
@token_required  # ADD THIS DECORATOR
def save_user_rights_bulk():
    """Save multiple user rights at once"""
    try:
        data = request.json
        user_id = data.get('userId')
        rights_list = data.get('rights', [])
        
        if not user_id:
            return jsonify({"success": False, "error": "userId is required"}), 400
        
        # Delete existing rights for this user
        delete_query = "DELETE FROM tbluserrights WHERE Userid = ?"
        execute_non_query(delete_query, (user_id,))
        
        # Insert new rights
        if rights_list:
            values_list = []
            params = []
            
            for rights in rights_list:
                menu_id = rights.get('Menuid')
                if not menu_id:
                    continue
                
                # Handle empty strings for IsCopy (which can be empty in your DB)
                is_copy = rights.get('IsCopy', '')
                if is_copy is True or is_copy == 'True':
                    is_copy = 'True'
                elif is_copy is False or is_copy == 'False':
                    is_copy = 'False'
                else:
                    is_copy = ''
                
                values = f"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                values_list.append(values)
                
                params.extend([
                    user_id,
                    menu_id,
                    rights.get('IsAdd', 'False'),
                    rights.get('IsEdit', 'False'),
                    rights.get('IsDelete', 'False'),
                    rights.get('IsPrint', 'False'),
                    rights.get('IsPost', 'False'),
                    is_copy,
                    rights.get('IsSearch', 'True'),
                    rights.get('IsUpload', 'False'),
                    rights.get('IsBackDate', 'False'),
                    rights.get('Isfavorite', 'False'),
                    rights.get('IsActive', 'True'),
                    rights.get('offcode', '0101'),
                    rights.get('isDesktop', 'False')
                ])
            
            if values_list:
                insert_query = f"""
                    INSERT INTO tbluserrights (
                        Userid, Menuid, IsAdd, IsEdit, IsDelete, IsPrint,
                        IsPost, IsCopy, IsSearch, IsUpload, IsBackDate,
                        Isfavorite, IsActive, offcode, isDesktop
                    ) VALUES {', '.join(values_list)}
                """
                execute_non_query(insert_query, params)
        
        return jsonify({
            "success": True,
            "message": f"Rights saved for user {user_id}"
        })
        
    except Exception as e:
        logger.error(f"Error saving bulk user rights: {e}")
        return jsonify({"success": False, "error": str(e)}), 500