# Screen configuration routes

from flask import request, jsonify
from datetime import datetime
import logging
from utils.db_helpers import execute_query, get_company_data  # Add get_company_data here
from . import screen_bp

logger = logging.getLogger(__name__)

# Remove the get_company_data function from here - it's now in db_helpers.py

@screen_bp.route('/screen/get-config', methods=['POST'])
def get_screen_config():
    """
    Get screen configuration dynamically from menu data
    No defaults - everything comes from the menu based on screenName
    """
    data = request.json
    screen_name = data.get('screenName') if data else None

    try:
        if not screen_name:
            return jsonify({
                "success": False,
                "error": "screenName is required"
            }), 400

        logger.info(f"🔍 Getting screen config for: \"{screen_name}\"")

        # Get menu data from login API
        company_data = get_company_data()
        
        if not company_data or not company_data.get('menu') or not isinstance(company_data['menu'], list):
            return jsonify({
                "success": False,
                "error": "Could not fetch menu data from API"
            }), 500

        logger.info(f"📋 Found {len(company_data['menu'])} menu items from API")
        
        # Search for the screen in the menu data (exact match first)
        found_screen = None
        for item in company_data['menu']:
            if item.get('MenuTitle') == screen_name:
                found_screen = item
                break

        # If no exact match, try case-insensitive
        if not found_screen:
            for item in company_data['menu']:
                if item.get('MenuTitle', '').lower() == screen_name.lower():
                    found_screen = item
                    break

        # If still not found, try partial match
        if not found_screen:
            lower_screen_name = screen_name.lower()
            for item in company_data['menu']:
                if lower_screen_name in item.get('MenuTitle', '').lower():
                    found_screen = item
                    break

        if found_screen:
            logger.info(f"✅ Found screen: {found_screen.get('MenuTitle')} (ID: {found_screen.get('Menuid')})")
            
            return jsonify({
                "success": True,
                "screen": {
                    "id": found_screen.get('Menuid'),
                    "title": found_screen.get('MenuTitle'),
                    "url": found_screen.get('MenuURL'),
                    "parentId": found_screen.get('ParentId'),
                    "isAdd": found_screen.get('isAdd', False),
                    "isEdit": found_screen.get('isEdit', False),
                    "isDelete": found_screen.get('isDelete', False),
                    "isPost": found_screen.get('isPost', False),
                    "isPrint": found_screen.get('isPrint', False),
                    "isSearch": found_screen.get('isSearch', False),
                    "isUpload": found_screen.get('isUpload', False),
                    "isCopy": found_screen.get('isCopy', False),
                    "isBackDate": found_screen.get('IsBackDate', False),
                    "menuType": found_screen.get('MenuType'),
                    "menuSystem": found_screen.get('MenuSystem'),
                    "toolbarOrder": found_screen.get('ToolbarOrder')
                },
                "source": "api_get_full_menu"
            }), 200

        # If not found, return all similar screens as suggestions
        lower_screen_name = screen_name.lower()
        similar_screens = [
            item for item in company_data['menu'] 
            if lower_screen_name in item.get('MenuTitle', '').lower()
        ]

        if similar_screens:
            return jsonify({
                "success": False,
                "error": f'Screen "{screen_name}" not found exactly. Did you mean one of these?',
                "suggestions": [s.get('MenuTitle') for s in similar_screens],
                "similarScreens": [
                    {
                        "id": s.get('Menuid'),
                        "title": s.get('MenuTitle'),
                        "url": s.get('MenuURL'),
                        "parentId": s.get('ParentId')
                    } for s in similar_screens
                ]
            }), 404

        return jsonify({
            "success": False,
            "error": f'Screen "{screen_name}" not found in menu',
            "totalMenuItems": len(company_data['menu']),
            "suggestions": [m.get('MenuTitle') for m in company_data['menu'][:10]]
        }), 404

    except Exception as err:
        logger.error(f"❌ getScreenConfig error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to fetch screen configuration",
            "details": str(err)
        }), 500

@screen_bp.route('/screen/document-statuses', methods=['POST'])
def get_document_statuses():
    """
    Get filtered document statuses based on nFilterSort
    menuId and cname come from frontend
    """
    data = request.json or {}
    menu_id = data.get('menuId')
    c_name = data.get('cname')

    try:
        # Validate input
        if not menu_id or not c_name:
            return jsonify({
                "success": False,
                "error": "menuId and cname are required"
            }), 400

        # Clean input
        menu_id = str(menu_id).strip()
        c_name = str(c_name).strip()

        logger.info(f"📊 menuId: {menu_id}, cname received: '{c_name}'")

        # Step 1: Get nFilterSort safely (case-insensitive + trimmed)
        filter_query = """
            SELECT nFilterSort
            FROM tblDocumentStatus
            WHERE Menuid = ?
              AND LTRIM(RTRIM(LOWER(cname))) = LTRIM(RTRIM(LOWER(?)))
        """

        filter_result = execute_query(filter_query, (menu_id, c_name))

        if not filter_result:
            logger.warning("⚠ No row found for given cname")
            return jsonify({
                "success": False,
                "message": "Status not found"
            }), 404

        n_filter_sort = filter_result[0].get('nFilterSort')

        if not n_filter_sort:
            logger.warning("⚠ nFilterSort is NULL or empty")
            return jsonify({
                "success": False,
                "message": "No filter configuration found"
            }), 404

        logger.info(f"📌 nFilterSort value: {n_filter_sort}")

        # Step 2: Convert '2,3,4' → [2,3,4]
        filter_values = []
        for val in str(n_filter_sort).split(','):
            val = val.strip()
            if val.isdigit():
                filter_values.append(int(val))

        if not filter_values:
            logger.warning("⚠ No valid numeric values found in nFilterSort")
            return jsonify({
                "success": False,
                "message": "Invalid filter configuration"
            }), 400

        # Step 3: Create dynamic placeholders
        placeholders = ','.join(['?'] * len(filter_values))

        status_query = f"""
            SELECT 
                ccode,
                cname,
                nFilterSort,
                isactive,
                StatusProcessCode,
                Menuid,
                isChange
            FROM tblDocumentStatus
            WHERE Menuid = ?
              AND ccode IN ({placeholders})
            ORDER BY nFilterSort
        """

        params = [menu_id] + filter_values

        result = execute_query(status_query, tuple(params))

        logger.info(f"✅ Returning {len(result) if result else 0} statuses")

        return jsonify({
            "success": True,
            "statuses": result if result else [],
            "count": len(result) if result else 0
        }), 200

    except Exception as err:
        logger.exception("❌ getDocumentStatuses error")
        return jsonify({
            "success": False,
            "error": "Failed to fetch document statuses",
            "details": str(err)
        }), 500

@screen_bp.route('/screen/menu-permissions', methods=['POST'])
def get_menu_permissions():
    """
    Get menu permissions for a specific menuId
    """
    data = request.json
    menu_id = data.get('menuId') if data else None

    try:
        if not menu_id:
            return jsonify({
                "success": False,
                "error": "menuId is required"
            }), 400

        # Get menu data from login API
        company_data = get_company_data()
        
        if not company_data or not company_data.get('menu'):
            return jsonify({
                "success": False,
                "error": "Could not fetch menu data"
            }), 500

        menu_item = None
        for item in company_data['menu']:
            if item.get('Menuid') == menu_id:
                menu_item = item
                break

        if menu_item:
            return jsonify({
                "success": True,
                "permissions": {
                    "isAdd": menu_item.get('isAdd', False),
                    "isEdit": menu_item.get('isEdit', False),
                    "isDelete": menu_item.get('isDelete', False),
                    "isPost": menu_item.get('isPost', False),
                    "isPrint": menu_item.get('isPrint', False),
                    "isSearch": menu_item.get('isSearch', False),
                    "isUpload": menu_item.get('isUpload', False),
                    "isCopy": menu_item.get('isCopy', False),
                    "isBackDate": menu_item.get('IsBackDate', False)
                }
            }), 200

        return jsonify({
            "success": False,
            "error": f"Menu item with ID {menu_id} not found"
        }), 404

    except Exception as err:
        logger.error(f"❌ getMenuPermissions error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to fetch menu permissions"
        }), 500

@screen_bp.route('/screen/update-employment-status', methods=['POST'])
def update_employment_status():
    """
    Post/Unpost records based on EmploymentStatus
    """
    data = request.json
    table_name = data.get('tableName') if data else None
    code = data.get('code') if data else None
    employment_status = data.get('employmentStatus') if data else None
    menu_id = data.get('menuId') if data else None

    try:
        if not table_name or not code or employment_status is None:
            return jsonify({
                "success": False,
                "error": "tableName, code, and employmentStatus are required"
            }), 400

        logger.info(f"📝 Updating EmploymentStatus for {table_name} code {code} to {employment_status}")

        # Get username from request context (you'll need to implement authentication)
        editby = "system"  # Replace with actual user from session/auth

        query = f"""
            UPDATE {table_name}
            SET EmploymentStatus = ?,
                editby = ?,
                editdate = GETDATE()
            WHERE Code = ?
        """

        execute_query(query, (employment_status, editby, code))

        # If menuId is provided, also update in tblDocumentStatus tracking if needed
        if menu_id:
            # Optional: Log the status change
            logger.info(f"Status updated for menuId: {menu_id}")

        return jsonify({
            "success": True,
            "message": f"Employment status updated to {employment_status}",
            "code": code,
            "employmentStatus": employment_status
        }), 200

    except Exception as err:
        logger.error(f"❌ updateEmploymentStatus error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to update employment status",
            "details": str(err)
        }), 500

@screen_bp.route('/screen/refresh-table-data', methods=['POST'])
def refresh_table_data():
    """
    Refresh table data with optional filters
    """
    data = request.json
    table_name = data.get('tableName') if data else None
    where = data.get('where') if data else None
    order_by = data.get('orderBy') if data else None
    use_pagination = data.get('usePagination') if data else None
    page = data.get('page') if data else None
    limit = data.get('limit') if data else None

    try:
        if not table_name:
            return jsonify({
                "success": False,
                "error": "tableName is required"
            }), 400

        logger.info(f"🔄 Refreshing data for table: {table_name}")

        query = f"SELECT * FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
        
        if order_by:
            query += f" ORDER BY {order_by}"

        # Add pagination if requested
        if use_pagination and page and limit:
            offset = (int(page) - 1) * int(limit)
            query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

        result = execute_query(query)

        return jsonify({
            "success": True,
            "rows": result,
            "count": len(result),
            "timestamp": datetime.now().isoformat()
        }), 200

    except Exception as err:
        logger.error(f"❌ refreshTableData error: {str(err)}")
        return jsonify({
            "success": False,
            "error": "Failed to refresh table data",
            "details": str(err)
        }), 500