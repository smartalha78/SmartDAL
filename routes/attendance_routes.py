from flask import request, jsonify
from . import attendance_bp
from config.database import get_db, execute_query, execute_non_query, query_cache
import logging
from datetime import datetime, timedelta
from functools import wraps
import pytz
from functools import lru_cache

# Import from jwt_helper
from utils.jwt_helper import token_required

logger = logging.getLogger(__name__)

# Pakistan Timezone
PAK_TZ = pytz.timezone('Asia/Karachi')

# ============= Cached Helper Functions =============
@lru_cache(maxsize=128)
def get_cached_shift_timing(shift_code, weekday, offcode):
    """Get shift timing with LRU cache"""
    try:
        query = """
            SELECT TOP 1
                DStartTime,
                DEndTime,
                ISNULL(shiftGrassInMinuts, 15) as grace_in,
                ISNULL(shiftGrassOutMinuts, 30) as grace_out
            FROM HRMSShiftTimeTable st
            WHERE st.ShiftCode = ? 
                AND st.WeekDay = ? 
                AND st.offcode = ?
        """
        results = execute_query(query, (shift_code, weekday, offcode))
        
        if results:
            return {
                'start_time': results[0]['DStartTime'],
                'end_time': results[0]['DEndTime'],
                'grace_in_minutes': int(results[0]['grace_in']),
                'grace_out_minutes': int(results[0]['grace_out'])
            }
        return {
            'start_time': '09:00',
            'end_time': '18:00',
            'grace_in_minutes': 15,
            'grace_out_minutes': 30
        }
    except Exception as e:
        logger.error(f"Error getting shift timing: {e}")
        return {
            'start_time': '09:00',
            'end_time': '18:00',
            'grace_in_minutes': 15,
            'grace_out_minutes': 30
        }

def get_shift_timing(shift_code, date_obj, offcode='0101'):
    """Get shift timing with caching"""
    weekday = date_obj.weekday()
    return get_cached_shift_timing(shift_code, weekday, offcode)

def parse_time_to_datetime(date_obj, time_str):
    """Fast time parsing"""
    if not time_str:
        return None
    try:
        if ':' in time_str:
            parts = time_str.split(':')
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
        else:
            hour = int(time_str)
            minute = 0
        return datetime(date_obj.year, date_obj.month, date_obj.day, hour, minute, 0)
    except:
        return None

def get_current_datetime():
    """Get current datetime in Pakistan timezone"""
    return datetime.now(PAK_TZ)

# ============= GET YEARS (Cached) =============
@attendance_bp.route('/years', methods=['GET', 'OPTIONS'])
@token_required
def get_years():
    """Get list of years - Fast cached response"""
    try:
        query = """
            SELECT YCode, YName, YSDate, YEDate
            FROM comYear 
            WHERE isActive = 'True'
            ORDER BY YCode DESC
        """
        results = execute_query(query, use_cache=True)
        
        current_date = get_current_datetime().date()
        current_year = results[0] if results else None
        
        for year in results:
            try:
                ys_date = datetime.strptime(year['YSDate'].split(' ')[0], '%Y-%m-%d').date()
                ye_date = datetime.strptime(year['YEDate'].split(' ')[0], '%Y-%m-%d').date()
                if ys_date <= current_date <= ye_date:
                    current_year = year
                    break
            except:
                continue
        
        return jsonify({"success": True, "data": results, "currentYear": current_year}), 200
    except Exception as e:
        logger.error(f"Get years error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET MONTHS (Cached) =============
@attendance_bp.route('/months', methods=['GET', 'OPTIONS'])
@token_required
def get_months():
    """Get months for a specific year - Fast cached response"""
    try:
        ycode = request.args.get('ycode')
        if not ycode:
            return jsonify({"success": False, "error": "Year code is required"}), 400
        
        query = """
            SELECT PCode, PName, SDate, EDate
            FROM comPeriods 
            WHERE YCode = ? AND isActive = 'True'
            ORDER BY PCode
        """
        results = execute_query(query, [ycode], use_cache=True)
        
        current_date = get_current_datetime().date()
        current_month = results[0] if results else None
        
        for month in results:
            try:
                s_date = datetime.strptime(month['SDate'].split(' ')[0], '%Y-%m-%d').date()
                e_date = datetime.strptime(month['EDate'].split(' ')[0], '%Y-%m-%d').date()
                if s_date <= current_date <= e_date:
                    current_month = month
                    break
            except:
                continue
        
        return jsonify({"success": True, "data": results, "currentMonth": current_month}), 200
    except Exception as e:
        logger.error(f"Get months error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET EMPLOYEES (Fast with limit) =============
@attendance_bp.route('/employees', methods=['GET', 'OPTIONS'])
@token_required
def get_employees():
    """Get list of employees for dropdown"""
    try:
        offcode = request.args.get('offcode', '1010')
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

# ============= GET SHIFTS (Cached) =============
@attendance_bp.route('/shifts', methods=['GET', 'OPTIONS'])
@token_required
def get_shifts():
    """Get list of shifts - Cached"""
    try:
        offcode = request.args.get('offcode', '0101')
        
        query = """
            SELECT Code, Name
            FROM HRMSShift 
            WHERE offcode = ? AND IsActive = 'True'
            ORDER BY Code
        """
        results = execute_query(query, [offcode], use_cache=True)
        
        return jsonify({"success": True, "data": results}), 200
    except Exception as e:
        logger.error(f"Get shifts error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET EMPLOYEE DETAILS (Cached) =============
@attendance_bp.route('/employee-details', methods=['POST', 'OPTIONS'])
@token_required
def get_employee_details():
    """Get employee details - Cached"""
    try:
        data = request.json
        employee_code = data.get('employeeCode')
        offcode = data.get('offcode', '0101')
        
        if not employee_code:
            return jsonify({"success": False, "error": "Employee code is required"}), 400
        
        query = """
            SELECT 
                e.Code as EmployeeCode,
                e.Name as EmployeeName,
                ISNULL(d.Name, '') as DepartmentName,
                ISNULL(des.Name, '') as DesignationName,
                ISNULL(s.Name, '') as ShiftName
            FROM HRMSEmployee e
            LEFT JOIN hrmsdepartment d ON e.DepartmentCode = d.Code AND e.offcode = d.offcode
            LEFT JOIN hrmsdesignation des ON e.DesignationCode = des.Code AND e.offcode = des.offcode
            LEFT JOIN HRMSShift s ON e.ShiftCode = s.Code AND e.offcode = s.offcode
            WHERE e.offcode = ? AND e.Code = ?
        """
        results = execute_query(query, [offcode, employee_code], use_cache=True)
        
        if results:
            return jsonify({"success": True, "data": results[0]}), 200
        return jsonify({"success": False, "error": "Employee not found"}), 404
    except Exception as e:
        logger.error(f"Get employee details error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= SEARCH ATTENDANCE (Optimized) =============
@attendance_bp.route('/search', methods=['POST', 'OPTIONS'])
@token_required
def search_attendance():
    """Search attendance records - Optimized"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        from_date = data.get('fromDate')
        to_date = data.get('toDate')
        employee_code = data.get('employeeCode')
        
        if not from_date or not to_date or not employee_code:
            return jsonify({"success": False, "error": "Missing required fields"}), 400
        
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
                a.IsDeductionExempt,
                a.Editby,
                a.EditDate,
                ISNULL(s.Name, '') as ShiftName
            FROM HRMSEmployeeAttendance a
            LEFT JOIN HRMSShift s ON a.ShiftCode = s.Code AND a.offcode = s.offcode
            WHERE a.offcode = ? 
                AND a.EmployeeCode = ?
                AND a.attDate BETWEEN ? AND ?
            ORDER BY a.attDate
        """
        
        results = execute_query(query, [offcode, employee_code, from_date, to_date])
        
        for idx, record in enumerate(results):
            record['Id'] = idx + 1
        
        # Fast summary calculation
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

# ============= UPDATE ATTENDANCE =============
@attendance_bp.route('/update', methods=['POST', 'OPTIONS'])
@token_required
def update_attendance():
    """Update attendance record - Fast"""
    try:
        data = request.json
        field = data.get('field')
        value = data.get('value')
        employee_code = data.get('employeeCode')
        att_date = data.get('attDate')
        current_user = data.get('user', 'SYSTEM')
        
        if not field or not employee_code or not att_date:
            return jsonify({"success": False, "error": "Missing required fields"}), 400
        
        allowed_fields = ['Timein', 'TimeOut', 'ShiftCode', 'IsDeductionExempt']
        if field not in allowed_fields:
            return jsonify({"success": False, "error": f"Field {field} cannot be updated"}), 400
        
        conn = get_db()
        cursor = conn.cursor()
        
        # Get existing record
        cursor.execute("""
            SELECT Timein, TimeOut, ShiftCode, attDate 
            FROM HRMSEmployeeAttendance 
            WHERE EmployeeCode = ? AND CAST(attDate AS DATE) = CAST(? AS DATE)
        """, (employee_code, att_date))
        existing = cursor.fetchone()
        
        if not existing:
            return jsonify({"success": False, "error": "Record not found"}), 404
        
        date_obj = existing[3].date() if isinstance(existing[3], datetime) else datetime.strptime(str(existing[3]).split(' ')[0], '%Y-%m-%d').date()
        
        if field == 'ShiftCode':
            cursor.execute("""
                UPDATE HRMSEmployeeAttendance 
                SET ShiftCode = ?, Editby = ?, EditDate = GETDATE()
                WHERE EmployeeCode = ? AND CAST(attDate AS DATE) = CAST(? AS DATE)
            """, (str(value) if value else existing[2], current_user, employee_code, att_date))
        
        elif field == 'IsDeductionExempt':
            val = 1 if value else 0
            cursor.execute("""
                UPDATE HRMSEmployeeAttendance 
                SET IsDeductionExempt = ?, Editby = ?, EditDate = GETDATE()
                WHERE EmployeeCode = ? AND CAST(attDate AS DATE) = CAST(? AS DATE)
            """, (val, current_user, employee_code, att_date))
        
        elif field == 'Timein':
            shift_timing = get_shift_timing(existing[2], date_obj, '0101')
            shift_start = datetime.strptime(shift_timing['start_time'], '%H:%M').time()
            shift_start_dt = datetime.combine(date_obj, shift_start)
            
            parts = value.split(':')
            new_time = datetime(date_obj.year, date_obj.month, date_obj.day, int(parts[0]), int(parts[1]), 0)
            
            minutes_late = (new_time - shift_start_dt).total_seconds() / 60
            late_min = round(minutes_late, 2) if minutes_late > shift_timing['grace_in_minutes'] else 0
            
            if minutes_late <= 0:
                att_status = "009"
            elif minutes_late <= shift_timing['grace_in_minutes']:
                att_status = "001"
            else:
                att_status = "003"
            
            working_hours = 0
            if existing[1]:
                diff = existing[1] - new_time
                working_hours = diff.total_seconds() / 3600
                if working_hours > 5:
                    working_hours -= 1
                working_hours = round(max(0, working_hours), 2)
            
            cursor.execute("""
                UPDATE HRMSEmployeeAttendance 
                SET Timein = ?, LateHours_Minuts = ?, attStatus = ?, TotalWorkingHours = ?,
                    EditModeIn = 1, Editby = ?, EditDate = GETDATE()
                WHERE EmployeeCode = ? AND CAST(attDate AS DATE) = CAST(? AS DATE)
            """, (new_time, late_min, att_status, working_hours, current_user, employee_code, att_date))
        
        elif field == 'TimeOut':
            shift_timing = get_shift_timing(existing[2], date_obj, '0101')
            shift_end = datetime.strptime(shift_timing['end_time'], '%H:%M').time()
            shift_end_dt = datetime.combine(date_obj, shift_end)
            
            parts = value.split(':')
            new_time = datetime(date_obj.year, date_obj.month, date_obj.day, int(parts[0]), int(parts[1]), 0)
            
            minutes_early = (shift_end_dt - new_time).total_seconds() / 60
            early_min = round(minutes_early, 2) if minutes_early > shift_timing['grace_out_minutes'] else 0
            
            working_hours = 0
            if existing[0]:
                diff = new_time - existing[0]
                working_hours = diff.total_seconds() / 3600
                if working_hours > 5:
                    working_hours -= 1
                working_hours = round(max(0, working_hours), 2)
            
            cursor.execute("""
                UPDATE HRMSEmployeeAttendance 
                SET TimeOut = ?, LeaveEarlyMinute = ?, TotalWorkingHours = ?,
                    EditModeOut = 1, Editby = ?, EditDate = GETDATE()
                WHERE EmployeeCode = ? AND CAST(attDate AS DATE) = CAST(? AS DATE)
            """, (new_time, early_min, working_hours, current_user, employee_code, att_date))
        
        conn.commit()
        cursor.close()
        
        # Clear cache for this employee/year
        query_cache.clear()
        
        return jsonify({"success": True, "message": "Attendance record updated successfully"}), 200
        
    except Exception as e:
        logger.error(f"Update attendance error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET MONTHLY STATS (Fast) =============
@attendance_bp.route('/monthly-stats', methods=['POST', 'OPTIONS'])
@token_required
def get_monthly_attendance_stats():
    """Get monthly attendance statistics for charts"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        employee_code = data.get('employeeCode')
        year_code = data.get('yearCode')
        
        logger.info(f"Monthly stats request - offcode: {offcode}, employee: {employee_code}, year: {year_code}")
        
        if not employee_code or not year_code:
            return jsonify({"success": False, "error": "Employee code and year code are required"}), 400
        
        # First, get all periods for the year
        periods_query = """
            SELECT PCode, PName, SDate, EDate
            FROM comPeriods 
            WHERE YCode = ? AND isActive = 'True'
            ORDER BY PCode
        """
        periods = execute_query(periods_query, [year_code])
        
        logger.info(f"Found {len(periods)} periods for year {year_code}")
        
        monthly_stats = []
        
        for period in periods:
            # Get attendance stats for this period
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
                    AND attDate >= ? AND attDate <= ?
            """
            att_params = [offcode, employee_code, period['SDate'], period['EDate']]
            
            stats = execute_query(att_query, att_params)
            
            if stats and len(stats) > 0:
                total_days = stats[0]['TotalDays'] or 0
                if total_days > 0:
                    monthly_stats.append({
                        'MonthName': period['PName'],
                        'PeriodCode': period['PCode'],
                        'TotalDays': total_days,
                        'PresentDays': stats[0]['PresentDays'] or 0,
                        'AbsentDays': stats[0]['AbsentDays'] or 0,
                        'OffDays': stats[0]['OffDays'] or 0,
                        'TotalWorkingHours': round(stats[0]['TotalWorkingHours'] or 0, 2),
                        'TotalOvertime': round(stats[0]['TotalOvertime'] or 0, 2),
                        'TotalLateMinutes': round(stats[0]['TotalLateMinutes'] or 0, 2)
                    })
                    logger.info(f"Period {period['PName']}: TotalDays={total_days}, Present={stats[0]['PresentDays']}")
        
        # Calculate yearly summary
        yearly_summary = {
            'presentDays': sum(m['PresentDays'] for m in monthly_stats),
            'absentDays': sum(m['AbsentDays'] for m in monthly_stats),
            'offDays': sum(m['OffDays'] for m in monthly_stats),
            'totalWorkingHours': sum(m['TotalWorkingHours'] for m in monthly_stats),
            'totalOvertimeHours': sum(m['TotalOvertime'] for m in monthly_stats),
            'totalLateMinutes': sum(m['TotalLateMinutes'] for m in monthly_stats)
        }
        
        logger.info(f"Monthly stats result: {len(monthly_stats)} months with data")
        
        return jsonify({
            "success": True,
            "data": monthly_stats,
            "summary": yearly_summary
        }), 200
        
    except Exception as e:
        logger.error(f"Get monthly stats error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500
    