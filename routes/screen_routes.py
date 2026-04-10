from flask import request, jsonify
from datetime import datetime
import logging
from utils.db_helpers import execute_query, get_company_data
from utils.jwt_helper import token_required, cors_response
from . import screen_bp

logger = logging.getLogger(__name__)

@screen_bp.route('/screen/get-config', methods=['POST', 'OPTIONS'])
@token_required
def get_screen_config():
    """Get screen configuration dynamically from menu data"""
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)

    try:
        data = request.json or {}
        screen_name = data.get("screenName", "").strip()
        
        if not screen_name:
            return cors_response({
                "success": False,
                "error": "screenName is required"
            }, 400)

        logger.info(f"🔍 Getting screen config for: '{screen_name}'")

        # Get company data with menu
        company_data = get_company_data()
        
        # Extract menu items from correct location
        menu_items = []
        if company_data and company_data.get('menu'):
            menu_items = company_data['menu']
        elif company_data and company_data.get('data') and company_data['data'].get('tbl3'):
            menu_items = company_data['data']['tbl3']
        
        if not menu_items:
            logger.warning("⚠️ No menu items found, returning default permissions")
            return cors_response({
                "success": True,
                "screen": {
                    "id": "VAR001",
                    "title": screen_name,
                    "url": "",
                    "parentId": "00",
                    "isAdd": True,
                    "isEdit": True,
                    "isDelete": True,
                    "isPost": True,
                    "isPrint": True,
                    "isSearch": True,
                    "isUpload": False,
                    "isCopy": False,
                    "isBackDate": True,
                    "menuType": "MST",
                    "menuSystem": "01",
                    "toolbarOrder": 0
                },
                "source": "default"
            }, 200)

        logger.info(f"📋 Found {len(menu_items)} menu items")
        
        # Try multiple search strategies
        found_screen = None
        
        # Strategy 1: Exact match
        for item in menu_items:
            if item.get('MenuTitle') == screen_name:
                found_screen = item
                logger.info(f"✅ Found exact match: {found_screen.get('MenuTitle')}")
                break
        
        # Strategy 2: Case-insensitive match
        if not found_screen:
            screen_name_lower = screen_name.lower()
            for item in menu_items:
                if item.get('MenuTitle', '').lower() == screen_name_lower:
                    found_screen = item
                    logger.info(f"✅ Found case-insensitive match: {found_screen.get('MenuTitle')}")
                    break
        
        # Strategy 3: Partial match (contains)
        if not found_screen:
            screen_name_lower = screen_name.lower()
            for item in menu_items:
                title = item.get('MenuTitle', '').lower()
                if screen_name_lower in title:
                    found_screen = item
                    logger.info(f"✅ Found partial match: {found_screen.get('MenuTitle')}")
                    break
        
        # Strategy 4: Match by MenuURL or other fields
        if not found_screen:
            for item in menu_items:
                if 'allowance' in item.get('MenuTitle', '').lower() or 'variable' in item.get('MenuTitle', '').lower():
                    found_screen = item
                    logger.info(f"✅ Found related match: {found_screen.get('MenuTitle')}")
                    break
        
        # If still not found, return default with full permissions
        if not found_screen:
            logger.warning(f"⚠️ Screen '{screen_name}' not found, returning default permissions")
            return cors_response({
                "success": True,
                "screen": {
                    "id": "VAR001",
                    "title": screen_name,
                    "url": "",
                    "parentId": "00",
                    "isAdd": True,
                    "isEdit": True,
                    "isDelete": True,
                    "isPost": True,
                    "isPrint": True,
                    "isSearch": True,
                    "isUpload": False,
                    "isCopy": False,
                    "isBackDate": True,
                    "menuType": "MST",
                    "menuSystem": "01",
                    "toolbarOrder": 0
                },
                "source": "default_fallback"
            }, 200)

        # Build response with safe defaults
        screen_response = {
            "id": str(found_screen.get("Menuid", "VAR001")),
            "title": found_screen.get("MenuTitle", screen_name),
            "url": found_screen.get("MenuURL", ""),
            "parentId": str(found_screen.get("ParentId", "00")),
            "isAdd": found_screen.get("isAdd", True),
            "isEdit": found_screen.get("isEdit", True),
            "isDelete": found_screen.get("isDelete", True),
            "isPost": found_screen.get("isPost", True),
            "isPrint": found_screen.get("isPrint", True),
            "isSearch": found_screen.get("IsSearch", True),
            "isUpload": found_screen.get("IsUpload", False),
            "isCopy": found_screen.get("IsCopy", False),
            "isBackDate": found_screen.get("IsBackDate", True),
            "menuType": found_screen.get("MenuType", "MST"),
            "menuSystem": found_screen.get("MenuSystem", "01"),
            "toolbarOrder": found_screen.get("ToolbarOrder", 0)
        }

        logger.info(f"✅ Returning screen config for: {screen_response['title']} (ID: {screen_response['id']})")
        
        return cors_response({
            "success": True,
            "screen": screen_response,
            "source": "menu"
        }, 200)

    except Exception as err:
        logger.error(f"❌ getScreenConfig error: {str(err)}")
        import traceback
        traceback.print_exc()
        # Return default permissions on error
        return cors_response({
            "success": True,
            "screen": {
                "id": "VAR001",
                "title": screen_name if 'screen_name' in locals() else "Unknown",
                "url": "",
                "parentId": "00",
                "isAdd": True,
                "isEdit": True,
                "isDelete": True,
                "isPost": True,
                "isPrint": True,
                "isSearch": True,
                "isUpload": False,
                "isCopy": False,
                "isBackDate": True,
                "menuType": "MST",
                "menuSystem": "01",
                "toolbarOrder": 0
            },
            "source": "error_fallback"
        }, 200)


def test_endpoint():
    """Simple test endpoint to verify routing is working"""
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)
    
    return cors_response({
        "success": True,
        "message": "Screen routes are working!",
        "timestamp": datetime.now().isoformat()
    }, 200)


@screen_bp.route('/screen/document-statuses', methods=['POST', 'OPTIONS'])
@token_required
def get_document_statuses():
    """
    Get filtered document statuses based on nFilterSort
    """
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)
    
    data = request.json or {}
    menu_id = data.get('menuId')
    c_name = data.get('cname')

    try:
        if not menu_id or not c_name:
            return cors_response({
                "success": False,
                "error": "menuId and cname are required"
            }, 400)

        menu_id = str(menu_id).strip()
        c_name = str(c_name).strip()

        logger.info(f"📊 menuId: {menu_id}, cname received: '{c_name}'")

        # First, try to get from tblDocumentStatus
        filter_query = """
            SELECT nFilterSort
            FROM tblDocumentStatus
            WHERE Menuid = ?
              AND LTRIM(RTRIM(LOWER(cname))) = LTRIM(RTRIM(LOWER(?)))
        """

        filter_result = execute_query(filter_query, (menu_id, c_name))

        if not filter_result:
            logger.warning("⚠ No row found for given cname, returning default statuses")
            # Return default statuses
            default_statuses = [
                {"ccode": 1, "cname": "Active", "nFilterSort": 1, "isactive": True},
                {"ccode": 2, "cname": "InActive", "nFilterSort": 2, "isactive": True},
                {"ccode": 3, "cname": "Retire", "nFilterSort": 3, "isactive": True},
                {"ccode": 4, "cname": "Suspend", "nFilterSort": 4, "isactive": True}
            ]
            return cors_response({
                "success": True,
                "statuses": default_statuses,
                "count": len(default_statuses)
            }, 200)

        n_filter_sort = filter_result[0].get('nFilterSort')

        if not n_filter_sort:
            logger.warning("⚠ nFilterSort is NULL or empty, returning default statuses")
            default_statuses = [
                {"ccode": 1, "cname": "Active", "nFilterSort": 1, "isactive": True},
                {"ccode": 2, "cname": "InActive", "nFilterSort": 2, "isactive": True},
                {"ccode": 3, "cname": "Retire", "nFilterSort": 3, "isactive": True},
                {"ccode": 4, "cname": "Suspend", "nFilterSort": 4, "isactive": True}
            ]
            return cors_response({
                "success": True,
                "statuses": default_statuses,
                "count": len(default_statuses)
            }, 200)

        logger.info(f"📌 nFilterSort value: {n_filter_sort}")

        filter_values = []
        for val in str(n_filter_sort).split(','):
            val = val.strip()
            if val.isdigit():
                filter_values.append(int(val))

        if not filter_values:
            logger.warning("⚠ No valid numeric values found in nFilterSort, returning default statuses")
            default_statuses = [
                {"ccode": 1, "cname": "Active", "nFilterSort": 1, "isactive": True},
                {"ccode": 2, "cname": "InActive", "nFilterSort": 2, "isactive": True},
                {"ccode": 3, "cname": "Retire", "nFilterSort": 3, "isactive": True},
                {"ccode": 4, "cname": "Suspend", "nFilterSort": 4, "isactive": True}
            ]
            return cors_response({
                "success": True,
                "statuses": default_statuses,
                "count": len(default_statuses)
            }, 200)

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

        if not result:
            # Return default statuses if no results
            default_statuses = [
                {"ccode": 1, "cname": "Active", "nFilterSort": 1, "isactive": True},
                {"ccode": 2, "cname": "InActive", "nFilterSort": 2, "isactive": True},
                {"ccode": 3, "cname": "Retire", "nFilterSort": 3, "isactive": True},
                {"ccode": 4, "cname": "Suspend", "nFilterSort": 4, "isactive": True}
            ]
            return cors_response({
                "success": True,
                "statuses": default_statuses,
                "count": len(default_statuses)
            }, 200)

        logger.info(f"✅ Returning {len(result) if result else 0} statuses")

        return cors_response({
            "success": True,
            "statuses": result if result else [],
            "count": len(result) if result else 0
        }, 200)

    except Exception as err:
        logger.exception("❌ getDocumentStatuses error")
        # Return default statuses on error
        default_statuses = [
            {"ccode": 1, "cname": "Active", "nFilterSort": 1, "isactive": True},
            {"ccode": 2, "cname": "InActive", "nFilterSort": 2, "isactive": True},
            {"ccode": 3, "cname": "Retire", "nFilterSort": 3, "isactive": True},
            {"ccode": 4, "cname": "Suspend", "nFilterSort": 4, "isactive": True}
        ]
        return cors_response({
            "success": True,
            "statuses": default_statuses,
            "count": len(default_statuses),
            "fallback": True
        }, 200)


@screen_bp.route('/screen/menu-permissions', methods=['POST', 'OPTIONS'])
@token_required
def get_menu_permissions():
    """
    Get menu permissions for a specific menuId
    """
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)
    
    data = request.json
    menu_id = data.get('menuId') if data else None

    try:
        if not menu_id:
            return cors_response({
                "success": False,
                "error": "menuId is required"
            }, 400)

        company_data = get_company_data()
        
        if not company_data or not company_data.get('menu'):
            return cors_response({
                "success": False,
                "error": "Could not fetch menu data"
            }, 500)

        menu_item = None
        for item in company_data['menu']:
            if item.get('Menuid') == menu_id:
                menu_item = item
                break

        if menu_item:
            return cors_response({
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
            }, 200)

        return cors_response({
            "success": False,
            "error": f"Menu item with ID {menu_id} not found"
        }, 404)

    except Exception as err:
        logger.error(f"❌ getMenuPermissions error: {str(err)}")
        return cors_response({
            "success": False,
            "error": "Failed to fetch menu permissions"
        }, 500)


@screen_bp.route('/screen/update-employment-status', methods=['POST', 'OPTIONS'])
@token_required
def update_employment_status():
    """
    Post/Unpost records based on EmploymentStatus
    """
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)
    
    data = request.json
    table_name = data.get('tableName') if data else None
    code = data.get('code') if data else None
    employment_status = data.get('employmentStatus') if data else None
    menu_id = data.get('menuId') if data else None

    try:
        if not table_name or not code or employment_status is None:
            return cors_response({
                "success": False,
                "error": "tableName, code, and employmentStatus are required"
            }, 400)

        logger.info(f"📝 Updating EmploymentStatus for {table_name} code {code} to {employment_status}")

        editby = request.current_user.get('username', 'system')

        query = f"""
            UPDATE {table_name}
            SET EmploymentStatus = ?,
                editby = ?,
                editdate = GETDATE()
            WHERE Code = ?
        """

        execute_query(query, (employment_status, editby, code))

        if menu_id:
            logger.info(f"Status updated for menuId: {menu_id}")

        return cors_response({
            "success": True,
            "message": f"Employment status updated to {employment_status}",
            "code": code,
            "employmentStatus": employment_status
        }, 200)

    except Exception as err:
        logger.error(f"❌ updateEmploymentStatus error: {str(err)}")
        return cors_response({
            "success": False,
            "error": "Failed to update employment status",
            "details": str(err)
        }, 500)


@screen_bp.route('/screen/refresh-table-data', methods=['POST', 'OPTIONS'])
@token_required
def refresh_table_data():
    """
    Refresh table data with optional filters
    """
    if request.method == 'OPTIONS':
        return cors_response({'status': 'ok'}, 200)
    
    data = request.json
    table_name = data.get('tableName') if data else None
    where = data.get('where') if data else None
    order_by = data.get('orderBy') if data else None
    use_pagination = data.get('usePagination') if data else None
    page = data.get('page') if data else None
    limit = data.get('limit') if data else None

    try:
        if not table_name:
            return cors_response({
                "success": False,
                "error": "tableName is required"
            }, 400)

        logger.info(f"🔄 Refreshing data for table: {table_name}")

        query = f"SELECT * FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
        
        if order_by:
            query += f" ORDER BY {order_by}"

        if use_pagination and page and limit:
            offset = (int(page) - 1) * int(limit)
            query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

        result = execute_query(query)

        return cors_response({
            "success": True,
            "rows": result,
            "count": len(result),
            "timestamp": datetime.now().isoformat()
        }, 200)

    except Exception as err:
        logger.error(f"❌ refreshTableData error: {str(err)}")
        return cors_response({
            "success": False,
            "error": "Failed to refresh table data",
            "details": str(err)
        }, 500)