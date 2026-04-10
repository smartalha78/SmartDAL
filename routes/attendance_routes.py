from flask import request, jsonify
from . import attendance_bp
from config.database import get_db
import logging
from datetime import datetime
from functools import wraps
from decimal import Decimal
from datetime import datetime, date
# Import from jwt_helper
from utils.jwt_helper import decode_token, token_required

logger = logging.getLogger(__name__)

def get_cursor():
    """Get database cursor using connection from config"""
    db = get_db()
    return db.cursor()

def execute_query(query, params=None):
    """Execute a query and return results"""
    cursor = None
    try:
        cursor = get_cursor()
        
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
                if isinstance(value, datetime):
                 value = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, date):
                  value = value.strftime('%Y-%m-%d')
                row_dict[col] = value
            rows.append(row_dict)
        return rows
    except Exception as e:
        logger.error(f"Query error: {e}")
        logger.error(f"Query: {query}")
        raise e
    finally:
        if cursor:
            cursor.close()

def execute_non_query(query, params=None):
    """Execute a non-query SQL command"""
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

# ============= GET YEARS FROM COMYEAR =============
@attendance_bp.route('/years', methods=['GET', 'OPTIONS'])
@token_required
def get_years():
    """Get list of years from comYear table"""
    try:
        current_date = datetime.now()
        
        query = """
            SELECT 
                YCode,
                YName,
                YNameSHD,
                YSDate,
                YEDate,
                isActive,
                FinancialActive
            FROM comYear 
            WHERE isActive = 'True'
            ORDER BY YCode DESC
        """
        results = execute_query(query)
        
        # Find current year based on current date
        current_year = None
        for year in results:
            ys_date = year.get('YSDate')
            ye_date = year.get('YEDate')
            
            if ys_date and ye_date:
                if isinstance(ys_date, str):
                    ys_date = datetime.strptime(ys_date, '%Y-%m-%d')
                if isinstance(ye_date, str):
                    ye_date = datetime.strptime(ye_date, '%Y-%m-%d')
                
                if ys_date <= current_date <= ye_date:
                    current_year = year
                    break
        
        return jsonify({
            "success": True,
            "data": results,
            "currentYear": current_year
        }), 200
        
    except Exception as e:
        logger.error(f"Get years error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET MONTHS FROM COMPERIODS =============
@attendance_bp.route('/months', methods=['GET', 'OPTIONS'])
@token_required
def get_months():
    """Get months for a specific year from comPeriods"""
    try:
        ycode = request.args.get('ycode')
        
        if not ycode:
            return jsonify({"success": False, "error": "Year code is required"}), 400
        
        current_date = datetime.now()
        
        query = """
            SELECT 
                PCode,
                PName,
                SDate,
                EDate,
                isActive,
                isFinalPay
            FROM comPeriods 
            WHERE YCode = ? AND isActive = 'True'
            ORDER BY PCode
        """
        results = execute_query(query, [ycode])
        
        # Find current month based on current date
        current_month = None
        for month in results:
            s_date = month.get('SDate')
            e_date = month.get('EDate')
            
            if s_date and e_date:
                if isinstance(s_date, str):
                    s_date = datetime.strptime(s_date, '%Y-%m-%d')
                if isinstance(e_date, str):
                    e_date = datetime.strptime(e_date, '%Y-%m-%d')
                
                if s_date <= current_date <= e_date:
                    current_month = month
                    break
        
        return jsonify({
            "success": True,
            "data": results,
            "currentMonth": current_month
        }), 200
        
    except Exception as e:
        logger.error(f"Get months error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET EMPLOYEES =============
@attendance_bp.route('/employees', methods=['GET', 'OPTIONS'])
@token_required
def get_employees():
    """Get list of employees for dropdown"""
    try:
        offcode = request.args.get('offcode', '0101')
        logger.info(f"Getting employees for offcode: {offcode}")
        
        conn = get_db()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                e.Code as EmployeeCode,
                e.Name as EmployeeName,
                e.FName,
                e.LName,
                e.DepartmentCode,
                d.Name as DepartmentName,
                e.DesignationCode,
                des.Name as DesignationName,
                e.ShiftCode,
                e.EmploymentStatus,
                e.IsActive
            FROM hrmsemployee e
            LEFT JOIN hrmsdepartment d ON e.DepartmentCode = d.Code AND e.offcode = d.offcode
            LEFT JOIN hrmsdesignation des ON e.DesignationCode = des.Code AND e.offcode = des.offcode
            WHERE e.offcode = ?
            ORDER BY e.Code
        """
        
        cursor.execute(query, (offcode,))
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            results.append(row_dict)
        
        cursor.close()
        
        logger.info(f"Found {len(results)} employees")
        
        return jsonify({
            "success": True,
            "data": results
        }), 200
        
    except Exception as e:
        logger.error(f"Get employees error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET EMPLOYEE DETAILS =============
@attendance_bp.route('/employee-details', methods=['POST', 'OPTIONS'])
@token_required
def get_employee_details():
    """Get department and designation for a specific employee"""
    try:
        data = request.json
        employee_code = data.get('employeeCode')
        offcode = data.get('offcode', '0101')
        
        if not employee_code:
            return jsonify({"success": False, "error": "Employee code is required"}), 400
        
        # Get employee basic info
        query = """
            SELECT 
                Code as EmployeeCode,
                Name as EmployeeName,
                DepartmentCode,
                DesignationCode,
                ShiftCode
            FROM HRMSEmployee 
            WHERE offcode = ? AND Code = ?
        """
        results = execute_query(query, [offcode, employee_code])
        
        if results:
            emp = results[0]
            
            # Get department name
            dept_query = "SELECT Name FROM hrmsdepartment WHERE Code = ? AND offcode = ?"
            dept = execute_query(dept_query, [emp.get('DepartmentCode', ''), offcode])
            emp['DepartmentName'] = dept[0]['Name'] if dept else ''
            
            # Get designation name
            des_query = "SELECT Name FROM hrmsdesignation WHERE Code = ? AND offcode = ?"
            des = execute_query(des_query, [emp.get('DesignationCode', ''), offcode])
            emp['DesignationName'] = des[0]['Name'] if des else ''
            
            # Get shift name
            shift_query = "SELECT Name FROM HRMSShift WHERE Code = ? AND offcode = ?"
            shift = execute_query(shift_query, [emp.get('ShiftCode', ''), offcode])
            emp['ShiftName'] = shift[0]['Name'] if shift else ''
            
            return jsonify({
                "success": True,
                "data": emp
            }), 200
        else:
            return jsonify({"success": False, "error": "Employee not found"}), 404
        
    except Exception as e:
        logger.error(f"Get employee details error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= SEARCH ATTENDANCE =============
@attendance_bp.route('/search', methods=['POST', 'OPTIONS'])
@token_required
def search_attendance():
    """Search attendance records with filters"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        from_date = data.get('fromDate')
        to_date = data.get('toDate')
        employee_code = data.get('employeeCode')
        
        if not from_date or not to_date:
            return jsonify({"success": False, "error": "From date and to date are required"}), 400
        
        logger.info(f"Searching attendance: offcode={offcode}, from={from_date}, to={to_date}")
        
        # Build query
        query = """
            SELECT 
                a.EmployeeCode,
                a.EmployeeName,
                a.ShiftCode,
                a.attDate,
                a.Timein,
                a.TimeOut,
                a.attStatus,
                a.dayStatus,
                a.TotalWorkingHours,
                a.OverTime,
                a.LateHours_Minuts,
                a.LeaveEarlyMinute,
                a.attDayIN,
                s.Name as ShiftName
            FROM HRMSEmployeeAttendance a
            LEFT JOIN HRMSShift s ON a.ShiftCode = s.Code AND a.offcode = s.offcode
            WHERE a.offcode = ? 
                AND a.attDate BETWEEN ? AND ?
        """
        params = [offcode, from_date, to_date]
        
        if employee_code:
            query += " AND a.EmployeeCode = ?"
            params.append(employee_code)
        
        query += " ORDER BY a.attDate"
        
        results = execute_query(query, params)
        
        # Add sequential ID for each row
        for idx, record in enumerate(results):
            record['Id'] = idx + 1
        
        # Calculate summary
        summary = {
            'totalDays': len(results),
            'presentDays': 0,
            'absentDays': 0,
            'offDays': 0,
            'totalWorkingHours': 0,
            'totalOvertimeHours': 0,
            'totalLateMinutes': 0,
            'totalEarlyMinutes': 0
        }
        
        for record in results:
            day_status = record.get('dayStatus')
            if day_status in ['001', 'Working Day']:
                summary['presentDays'] += 1
            elif day_status in ['002', 'Absent']:
                summary['absentDays'] += 1
            elif day_status in ['003', 'Holiday', 'Company Off']:
                summary['offDays'] += 1
            
            summary['totalWorkingHours'] += float(record.get('TotalWorkingHours', 0) or 0)
            summary['totalOvertimeHours'] += float(record.get('OverTime', 0) or 0)
            summary['totalLateMinutes'] += float(record.get('LateHours_Minuts', 0) or 0)
            summary['totalEarlyMinutes'] += float(record.get('LeaveEarlyMinute', 0) or 0)
        
        return jsonify({
            "success": True,
            "data": results,
            "summary": summary,
            "count": len(results)
        }), 200
        
    except Exception as e:
        logger.error(f"Search attendance error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ============= UPDATE ATTENDANCE RECORD =============
@attendance_bp.route('/update', methods=['POST', 'OPTIONS'])
@token_required
def update_attendance():
    """Update a single attendance record"""
    try:
        data = request.json
        record_id = data.get('id')
        field = data.get('field')
        value = data.get('value')
        employee_code = data.get('employeeCode')
        att_date = data.get('attDate')
        
        logger.info(f"Update request - field: {field}, value: {value}, emp: {employee_code}, date: {att_date}")
        
        if not record_id or not field:
            return jsonify({"success": False, "error": "Record ID and field are required"}), 400
        
        allowed_fields = [
            'Timein', 'TimeOut', 'attStatus', 'dayStatus', 'attTimeStatus',
            'LateHours', 'OverTime', 'TotalWorkingHours', 'EditModeType',
            'LateHours_Minuts', 'OverTime_Minuts', 'TotalWorkingHours_Minuts',
            'LeaveEarlyMinute', 'EarlyInMinute', 'ExcessMinute',
            'LateInDeductionDay', 'EarlyOutDeductionDay', 'TotalDeductionDay'
        ]
        
        if field not in allowed_fields:
            return jsonify({"success": False, "error": f"Field {field} cannot be updated"}), 400
        
        conn = get_db()
        cursor = conn.cursor()
        
        # First, get the existing record to get the attDate
        cursor.execute("""
            SELECT attDate FROM HRMSEmployeeAttendance 
            WHERE EmployeeCode = ? AND CAST(attDate AS DATE) = CAST(? AS DATE)
        """, (employee_code, att_date))
        existing_row = cursor.fetchone()
        
        if not existing_row:
            return jsonify({"success": False, "error": "Record not found"}), 404
        
        existing_att_date = existing_row[0]
        
        # Convert the value based on field type
        converted_value = value
        
        if field in ['Timein', 'TimeOut']:
            if not value or value == '':
                converted_value = None
            else:
                try:
                    from datetime import datetime
                    
                    # Get the date part from the existing record
                    if isinstance(existing_att_date, datetime):
                        date_part = existing_att_date.date()
                    else:
                        date_part = datetime.strptime(str(existing_att_date).split(' ')[0], '%Y-%m-%d').date()
                    
                    # Parse the time string
                    time_str = str(value).strip()
                    
                    # Handle different time formats
                    hour = 0
                    minute = 0
                    second = 0
                    
                    # Parse HH:MM format
                    if ':' in time_str:
                        time_parts = time_str.split(':')
                        hour = int(time_parts[0]) if len(time_parts) > 0 else 0
                        minute = int(time_parts[1]) if len(time_parts) > 1 else 0
                        second = int(time_parts[2]) if len(time_parts) > 2 else 0
                    else:
                        # Try to parse as number (e.g., "5" means 5:00)
                        try:
                            hour = int(float(time_str))
                            minute = 0
                            second = 0
                        except:
                            hour = 0
                            minute = 0
                            second = 0
                    
                    # Ensure hour is within 0-23
                    hour = hour % 24
                    
                    # Create new datetime
                    converted_value = datetime(date_part.year, date_part.month, date_part.day, hour, minute, second)
                    
                    logger.info(f"Time conversion: {value} -> {converted_value}")
                    
                except Exception as e:
                    logger.error(f"Time conversion error: {e}")
                    converted_value = None
        
        # Handle numeric fields
        numeric_fields = [
            'LateHours', 'OverTime', 'TotalWorkingHours', 
            'LateHours_Minuts', 'OverTime_Minuts', 'TotalWorkingHours_Minuts',
            'LeaveEarlyMinute', 'EarlyInMinute', 'ExcessMinute',
            'LateInDeductionDay', 'EarlyOutDeductionDay', 'TotalDeductionDay'
        ]
        
        if field in numeric_fields:
            if value is None or value == '':
                converted_value = 0
            else:
                try:
                    converted_value = float(value)
                except (ValueError, TypeError):
                    converted_value = 0
        
        # For string fields, ensure we're passing string
        string_fields = ['attStatus', 'dayStatus', 'attTimeStatus']
        if field in string_fields:
            converted_value = str(value) if value else ''
        
        # For EditModeType, it might be numeric - check its type
        if field == 'EditModeType':
            try:
                converted_value = float(value) if value else 0
            except:
                converted_value = 0
        
        # Build the update query - REMOVE EditModeType update from this query
        # Let's only update the field that was requested
        query = f"""
            UPDATE HRMSEmployeeAttendance 
            SET {field} = ?
            WHERE EmployeeCode = ? AND CAST(attDate AS DATE) = CAST(? AS DATE)
        """
        
        logger.info(f"Executing update - field: {field}, converted_value: {converted_value}, type: {type(converted_value)}")
        
        cursor.execute(query, (converted_value, employee_code, att_date))
        conn.commit()
        
        rows_affected = cursor.rowcount
        cursor.close()
        
        if rows_affected > 0:
            return jsonify({
                "success": True,
                "message": "Attendance record updated successfully"
            }), 200
        else:
            return jsonify({"success": False, "error": "No record was updated"}), 404
        
    except Exception as e:
        logger.error(f"Update attendance error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET MONTHLY ATTENDANCE STATS FOR CHART =============
@attendance_bp.route('/monthly-stats', methods=['POST', 'OPTIONS'])
@token_required
def get_monthly_attendance_stats():
    """Get monthly attendance statistics for charts"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        employee_code = data.get('employeeCode')
        year_code = data.get('yearCode')
        
        if not employee_code:
            return jsonify({"success": False, "error": "Employee code is required"}), 400
        
        if not year_code:
            return jsonify({"success": False, "error": "Year code is required"}), 400
        
        # Get all periods for the year
        periods_query = """
            SELECT PCode, PName, SDate, EDate
            FROM comPeriods 
            WHERE YCode = ? AND isActive = 'True'
            ORDER BY PCode
        """
        periods = execute_query(periods_query, [year_code])
        
        monthly_stats = []
        
        for period in periods:
            # Get attendance stats for this month
            att_query = """
                SELECT 
                    COUNT(*) as TotalDays,
                    SUM(CASE WHEN dayStatus IN ('001', 'Working Day') THEN 1 ELSE 0 END) as PresentDays,
                    SUM(CASE WHEN dayStatus IN ('002', 'Absent') THEN 1 ELSE 0 END) as AbsentDays,
                    SUM(CASE WHEN dayStatus IN ('003', 'Holiday', 'Company Off') THEN 1 ELSE 0 END) as OffDays,
                    SUM(ISNULL(TotalWorkingHours, 0)) as TotalWorkingHours,
                    SUM(ISNULL(OverTime, 0)) as TotalOvertime,
                    SUM(ISNULL(LateHours_Minuts, 0)) as TotalLateMinutes
                FROM HRMSEmployeeAttendance
                WHERE offcode = ? 
                    AND EmployeeCode = ?
                    AND attDate BETWEEN ? AND ?
            """
            att_params = [offcode, employee_code, period['SDate'], period['EDate']]
            
            stats = execute_query(att_query, att_params)
            
            if stats and len(stats) > 0 and stats[0]['TotalDays'] > 0:
                monthly_stats.append({
                    'MonthName': period['PName'],
                    'PeriodCode': period['PCode'],
                    'TotalDays': stats[0]['TotalDays'] or 0,
                    'PresentDays': stats[0]['PresentDays'] or 0,
                    'AbsentDays': stats[0]['AbsentDays'] or 0,
                    'OffDays': stats[0]['OffDays'] or 0,
                    'TotalWorkingHours': round(stats[0]['TotalWorkingHours'] or 0, 2),
                    'TotalOvertime': round(stats[0]['TotalOvertime'] or 0, 2),
                    'TotalLateMinutes': round(stats[0]['TotalLateMinutes'] or 0, 2)
                })
        
        # Calculate yearly summary
        yearly_summary = {
            'presentDays': sum(m['PresentDays'] for m in monthly_stats),
            'absentDays': sum(m['AbsentDays'] for m in monthly_stats),
            'offDays': sum(m['OffDays'] for m in monthly_stats),
            'totalWorkingHours': sum(m['TotalWorkingHours'] for m in monthly_stats),
            'totalOvertimeHours': sum(m['TotalOvertime'] for m in monthly_stats),
            'totalLateMinutes': sum(m['TotalLateMinutes'] for m in monthly_stats)
        }
        
        return jsonify({
            "success": True,
            "data": monthly_stats,
            "summary": yearly_summary
        }), 200
        
    except Exception as e:
        logger.error(f"Get monthly stats error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500