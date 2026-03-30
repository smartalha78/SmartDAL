from flask import request, jsonify
from . import attendance_bp
from config.database import get_db
import logging
from datetime import datetime

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
                row_dict[col] = row[i]
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
        db = get_db()
        db.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()

# ============= GET EMPLOYEES =============
@attendance_bp.route('/attendance/employees', methods=['GET'])
def get_employees():
    """Get list of employees for dropdown"""
    try:
        offcode = request.args.get('offcode', '0101')
        is_active = request.args.get('isActive', 'True')
        
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
        """
        params = [offcode]
        
        if is_active == 'True':
            query += " AND e.IsActive = 'True'"
        
        query += " ORDER BY e.Code"
        
        results = execute_query(query, params)
        
        return jsonify({
            "success": True,
            "data": results
        }), 200
        
    except Exception as e:
        logger.error(f"Get employees error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET DEPARTMENTS =============
@attendance_bp.route('/attendance/departments', methods=['GET'])
def get_departments():
    """Get list of departments for dropdown"""
    try:
        offcode = request.args.get('offcode', '0101')
        
        query = """
            SELECT Code, Name 
            FROM hrmsdepartment 
            WHERE offcode = ? AND IsActive = 'True'
            ORDER BY Code
        """
        results = execute_query(query, [offcode])
        
        return jsonify({
            "success": True,
            "data": results
        }), 200
        
    except Exception as e:
        logger.error(f"Get departments error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET DESIGNATIONS =============
@attendance_bp.route('/attendance/designations', methods=['GET'])
def get_designations():
    """Get list of designations for dropdown"""
    try:
        offcode = request.args.get('offcode', '0101')
        
        query = """
            SELECT Code, Name 
            FROM hrmsdesignation 
            WHERE offcode = ? AND IsActive = 'True'
            ORDER BY Code
        """
        results = execute_query(query, [offcode])
        
        return jsonify({
            "success": True,
            "data": results
        }), 200
        
    except Exception as e:
        logger.error(f"Get designations error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET SHIFTS =============
@attendance_bp.route('/attendance/shifts', methods=['GET'])
def get_shifts():
    """Get list of shifts for dropdown"""
    try:
        offcode = request.args.get('offcode', '0101')
        
        query = """
            SELECT Code, Name, shiftHours 
            FROM HRMSShift 
            WHERE offcode = ? AND IsActive = 'True'
            ORDER BY Code
        """
        results = execute_query(query, [offcode])
        
        return jsonify({
            "success": True,
            "data": results
        }), 200
        
    except Exception as e:
        logger.error(f"Get shifts error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= SEARCH ATTENDANCE =============
@attendance_bp.route('/attendance/search', methods=['POST'])
def search_attendance():
    """Search attendance records with filters"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        from_date = data.get('fromDate')
        to_date = data.get('toDate')
        employee_code = data.get('employeeCode')
        department_code = data.get('departmentCode')
        designation_code = data.get('designationCode')
        
        if not from_date or not to_date:
            return jsonify({"success": False, "error": "From date and to date are required"}), 400
        
        logger.info(f"Searching attendance: offcode={offcode}, from={from_date}, to={to_date}")
        
        # Build query with employee details
        query = """
            SELECT 
                a.Pk,
                a.offcode,
                a.bcode,
                a.YCode,
                a.PCode,
                a.EmployeeCode,
                a.EmployeeName,
                a.ShiftCode,
                a.attDate,
                a.attDatein,
                a.attDateOut,
                a.Timein,
                a.TimeOut,
                a.attStatus,
                a.dayStatus,
                a.attTimeStatus,
                a.TotalWorkingHours,
                a.TotalWorkingHours_Minuts,
                a.OverTime,
                a.OverTime_Minuts,
                a.LateHours,
                a.LateHours_Minuts,
                a.LeaveEarlyHours,
                a.LeaveEarlyMinute,
                a.EarlyInMinute,
                a.LateInDeductionDay,
                a.EarlyOutDeductionDay,
                a.TotalDeductionDay,
                a.ExcessMinute,
                a.IsDeductionExempt,
                a.ShiftMores,
                a.attDayIN,
                a.attDayOut,
                a.DStartTime,
                a.DEndTime,
                a.BStartTimeIn,
                a.BStartTimeOut,
                a.EndTimeIn,
                a.EndTimeOut,
                a.BreakStartTimeIn,
                a.BreakEndTimeOut,
                a.EditModeType,
                a.createdby,
                a.createdate,
                a.editby,
                a.editdate,
                s.Name as ShiftName,
                s.shiftHours,
                e.DepartmentCode,
                d.Name as DepartmentName,
                e.DesignationCode,
                des.Name as DesignationName,
                e.FName,
                e.LName
            FROM HRMSAttendence a
            LEFT JOIN HRMSShift s ON a.ShiftCode = s.Code AND a.offcode = s.offcode
            LEFT JOIN hrmsemployee e ON a.EmployeeCode = e.Code AND a.offcode = e.offcode
            LEFT JOIN hrmsdepartment d ON e.DepartmentCode = d.Code AND a.offcode = d.offcode
            LEFT JOIN hrmsdesignation des ON e.DesignationCode = des.Code AND a.offcode = des.offcode
            WHERE a.offcode = ? 
                AND a.attDate BETWEEN ? AND ?
        """
        params = [offcode, from_date, to_date]
        
        if employee_code:
            query += " AND a.EmployeeCode = ?"
            params.append(employee_code)
        
        if department_code:
            query += " AND e.DepartmentCode = ?"
            params.append(department_code)
        
        if designation_code:
            query += " AND e.DesignationCode = ?"
            params.append(designation_code)
        
        query += " ORDER BY a.EmployeeCode, a.attDate"
        
        results = execute_query(query, params)
        
        # Calculate summary
        summary = {
            'totalDays': len(results),
            'presentDays': 0,
            'absentDays': 0,
            'offDays': 0,
            'totalWorkingHours': 0,
            'totalOvertimeHours': 0,
            'totalLateMinutes': 0,
            'totalEarlyMinutes': 0,
            'totalEmployees': len(set([r.get('EmployeeCode') for r in results]))
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

# ============= GET EMPLOYEE ATTENDANCE REPORT =============
@attendance_bp.route('/attendance/employee-report', methods=['POST'])
def get_employee_attendance_report():
    """Get detailed attendance report for a specific employee"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        employee_code = data.get('employeeCode')
        from_date = data.get('fromDate')
        to_date = data.get('toDate')
        
        if not employee_code:
            return jsonify({"success": False, "error": "Employee code is required"}), 400
        
        if not from_date or not to_date:
            return jsonify({"success": False, "error": "From date and to date are required"}), 400
        
        # Get employee details
        emp_query = """
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
                s.Name as ShiftName,
                e.JoiningDate,
                e.EmploymentStatus
            FROM hrmsemployee e
            LEFT JOIN hrmsdepartment d ON e.DepartmentCode = d.Code AND e.offcode = d.offcode
            LEFT JOIN hrmsdesignation des ON e.DesignationCode = des.Code AND e.offcode = des.offcode
            LEFT JOIN HRMSShift s ON e.ShiftCode = s.Code AND e.offcode = s.offcode
            WHERE e.offcode = ? AND e.Code = ?
        """
        employee_info = execute_query(emp_query, [offcode, employee_code])
        
        # Get attendance records
        att_query = """
            SELECT 
                a.*,
                s.Name as ShiftName,
                s.shiftHours
            FROM HRMSAttendence a
            LEFT JOIN HRMSShift s ON a.ShiftCode = s.Code AND a.offcode = s.offcode
            WHERE a.offcode = ? 
                AND a.EmployeeCode = ?
                AND a.attDate BETWEEN ? AND ?
            ORDER BY a.attDate
        """
        attendance_records = execute_query(att_query, [offcode, employee_code, from_date, to_date])
        
        # Calculate summary
        summary = {
            'employeeCode': employee_code,
            'employeeName': employee_info[0].get('EmployeeName', '') if employee_info else '',
            'departmentName': employee_info[0].get('DepartmentName', '') if employee_info else '',
            'designationName': employee_info[0].get('DesignationName', '') if employee_info else '',
            'shiftName': employee_info[0].get('ShiftName', '') if employee_info else '',
            'fromDate': from_date,
            'toDate': to_date,
            'totalDays': len(attendance_records),
            'presentDays': 0,
            'absentDays': 0,
            'offDays': 0,
            'totalWorkingHours': 0,
            'totalOvertimeHours': 0,
            'totalLateMinutes': 0,
            'totalEarlyMinutes': 0,
            'totalLateDeductionDays': 0,
            'totalEarlyDeductionDays': 0,
            'totalDeductionDays': 0
        }
        
        for record in attendance_records:
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
            summary['totalLateDeductionDays'] += float(record.get('LateInDeductionDay', 0) or 0)
            summary['totalEarlyDeductionDays'] += float(record.get('EarlyOutDeductionDay', 0) or 0)
            summary['totalDeductionDays'] += float(record.get('TotalDeductionDay', 0) or 0)
        
        return jsonify({
            "success": True,
            "employee": employee_info[0] if employee_info else None,
            "attendance": attendance_records,
            "summary": summary
        }), 200
        
    except Exception as e:
        logger.error(f"Get employee attendance report error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= UPDATE ATTENDANCE RECORD =============
@attendance_bp.route('/attendance/update', methods=['POST'])
def update_attendance():
    """Update a single attendance record"""
    try:
        data = request.json
        pk = data.get('pk')
        field = data.get('field')
        value = data.get('value')
        user = data.get('user', 'SYSTEM')
        
        if not pk or not field:
            return jsonify({"success": False, "error": "PK and field are required"}), 400
        
        # Validate field name (prevent SQL injection)
        allowed_fields = [
            'Timein', 'TimeOut', 'attStatus', 'dayStatus', 'attTimeStatus',
            'LateHours', 'OverTime', 'TotalWorkingHours', 'EditModeType',
            'LateHours_Minuts', 'OverTime_Minuts', 'TotalWorkingHours_Minuts',
            'LeaveEarlyMinute', 'EarlyInMinute', 'ExcessMinute',
            'LateInDeductionDay', 'EarlyOutDeductionDay', 'TotalDeductionDay'
        ]
        
        if field not in allowed_fields:
            return jsonify({"success": False, "error": f"Field {field} cannot be updated"}), 400
        
        # Update query
        query = f"""
            UPDATE HRMSAttendence 
            SET {field} = ?, editby = ?, editdate = GETDATE() 
            WHERE Pk = ?
        """
        execute_non_query(query, [value, user, pk])
        
        return jsonify({
            "success": True,
            "message": "Attendance record updated successfully"
        }), 200
        
    except Exception as e:
        logger.error(f"Update attendance error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET ATTENDANCE SUMMARY =============
@attendance_bp.route('/attendance/summary', methods=['POST'])
def get_attendance_summary():
    """Get attendance summary for a period"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        from_date = data.get('fromDate')
        to_date = data.get('toDate')
        department_code = data.get('departmentCode')
        
        if not from_date or not to_date:
            return jsonify({"success": False, "error": "From date and to date are required"}), 400
        
        query = """
            SELECT 
                a.EmployeeCode,
                a.EmployeeName,
                e.DepartmentCode,
                d.Name as DepartmentName,
                e.DesignationCode,
                des.Name as DesignationName,
                COUNT(*) as TotalDays,
                SUM(CASE WHEN a.dayStatus IN ('001', 'Working Day') THEN 1 ELSE 0 END) as PresentDays,
                SUM(CASE WHEN a.dayStatus IN ('002', 'Absent') THEN 1 ELSE 0 END) as AbsentDays,
                SUM(CASE WHEN a.dayStatus IN ('003', 'Holiday', 'Company Off') THEN 1 ELSE 0 END) as OffDays,
                SUM(ISNULL(a.TotalWorkingHours, 0)) as TotalWorkingHours,
                SUM(ISNULL(a.OverTime, 0)) as TotalOvertime,
                SUM(ISNULL(a.LateHours_Minuts, 0)) as TotalLateMinutes,
                SUM(ISNULL(a.LeaveEarlyMinute, 0)) as TotalEarlyMinutes
            FROM HRMSAttendence a
            LEFT JOIN hrmsemployee e ON a.EmployeeCode = e.Code AND a.offcode = e.offcode
            LEFT JOIN hrmsdepartment d ON e.DepartmentCode = d.Code AND a.offcode = d.offcode
            LEFT JOIN hrmsdesignation des ON e.DesignationCode = des.Code AND a.offcode = des.offcode
            WHERE a.offcode = ? AND a.attDate BETWEEN ? AND ?
        """
        params = [offcode, from_date, to_date]
        
        if department_code:
            query += " AND e.DepartmentCode = ?"
            params.append(department_code)
        
        query += " GROUP BY a.EmployeeCode, a.EmployeeName, e.DepartmentCode, d.Name, e.DesignationCode, des.Name ORDER BY a.EmployeeCode"
        
        results = execute_query(query, params)
        
        return jsonify({
            "success": True,
            "data": results
        }), 200
        
    except Exception as e:
        logger.error(f"Get attendance summary error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ============= GET ATTENDANCE STATISTICS =============
@attendance_bp.route('/attendance/stats', methods=['POST'])
def get_attendance_stats():
    """Get attendance statistics for charts"""
    try:
        data = request.json
        offcode = data.get('offcode', '0101')
        from_date = data.get('fromDate')
        to_date = data.get('toDate')
        department_code = data.get('departmentCode')
        
        if not from_date or not to_date:
            return jsonify({"success": False, "error": "From date and to date are required"}), 400
        
        params = [offcode, from_date, to_date]
        dept_filter = ""
        
        if department_code:
            dept_filter = " AND e.DepartmentCode = ?"
            params.append(department_code)
        
        # Daily attendance stats
        daily_query = f"""
            SELECT 
                CONVERT(DATE, a.attDate) as Date,
                COUNT(*) as TotalEmployees,
                SUM(CASE WHEN a.dayStatus IN ('001', 'Working Day') THEN 1 ELSE 0 END) as Present,
                SUM(CASE WHEN a.dayStatus IN ('002', 'Absent') THEN 1 ELSE 0 END) as Absent,
                SUM(CASE WHEN a.dayStatus IN ('003', 'Holiday', 'Company Off') THEN 1 ELSE 0 END) as Off
            FROM HRMSAttendence a
            LEFT JOIN hrmsemployee e ON a.EmployeeCode = e.Code AND a.offcode = e.offcode
            WHERE a.offcode = ? AND a.attDate BETWEEN ? AND ? {dept_filter}
            GROUP BY CONVERT(DATE, a.attDate)
            ORDER BY Date
        """
        
        daily_stats = execute_query(daily_query, params)
        
        # Status distribution
        status_query = f"""
            SELECT 
                a.dayStatus,
                COUNT(*) as Count
            FROM HRMSAttendence a
            LEFT JOIN hrmsemployee e ON a.EmployeeCode = e.Code AND a.offcode = e.offcode
            WHERE a.offcode = ? AND a.attDate BETWEEN ? AND ? {dept_filter}
            GROUP BY a.dayStatus
        """
        
        status_dist = execute_query(status_query, params)
        
        return jsonify({
            "success": True,
            "daily_stats": daily_stats,
            "status_distribution": status_dist
        }), 200
        
    except Exception as e:
        logger.error(f"Get attendance stats error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500