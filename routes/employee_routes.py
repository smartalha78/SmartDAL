# Employee management routes

from flask import request, jsonify
import pyodbc
from config.database import CONN_STR
from utils.db_helpers import (
    execute_non_query, execute_query, build_insert_query,
    format_date_for_sql, get_user_uid, get_company_data,  # get_company_data is now here
    get_column_lengths, filter_valid_columns
)
from . import employee_bp
import logging

logger = logging.getLogger(__name__)


@employee_bp.route('/insert-EmployeeHeadDet', methods=['POST', 'OPTIONS'])
def insert_employee_head_det():
    """Insert employee with all details"""
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        print("=" * 60)
        print("📥 INSERT EMPLOYEE HEAD DET REQUEST RECEIVED")
        print("=" * 60)
        
        # Extract data
        head = data.get("head")
        details = data.get("details", [])
        selected_branch = data.get("selectedBranch")
        
        if not head:
            return jsonify({"success": False, "error": "Head object is required"}), 400
        
        # Get company data
        company_data = get_company_data()
        company_offcode = company_data.get("company", {}).get("offcode", "0101")
        
        # Get branch code
        branch_code = None
        if company_data.get("branches") and len(company_data["branches"]) > 0:
            if selected_branch:
                selected = next((b for b in company_data["branches"] if b.get("branch") == selected_branch), None)
                branch_code = selected.get("code") if selected else company_data["branches"][0].get("code")
            else:
                branch_code = company_data["branches"][0].get("code")
        
        # Get head table and data
        head_table = head.get('tableName', 'HRMSEmployee')
        head_data = head.get('data', {})
        
        emp_code = head_data.get("Code")
        if not emp_code:
            return jsonify({"success": False, "error": "Employee Code is required"}), 400
        
        createdby = head_data.get("createdby")
        if not createdby:
            createdby = "admin"
        
        # Get user ID
        uid = get_user_uid(createdby)
        print(f"👤 User: {createdby}, UID: {uid}")
        
        # Current date for createdate/editdate
        current_date = format_date_for_sql()
        
        # Prepare head row
        head_row = {}
        
        # Map fields according to database schema
        field_mapping = {
            # Personal Information
            "FName": "FName",
            "MName": "MName",
            "LName": "LName",
            "arName": "arName",
            "FatherName": "FatherName",
            "DOB": "DOB",
            "Gender": "Gender",
            "MarriadStatus": "MarriadStatus",
            "Nationality": "Nationality",
            "Religin": "Religin",
            "IDNo": "IDNo",
            "IDExpiryDate": "IDExpiryDate",
            "PassportNo": "PassportNo",
            "PassportExpiryDate": "PassportExpiryDate",
            
            # Contact Information
            "Mobile": "P_Mobile",
            "Email": "P_Email",
            
            # Permanent Address
            "P_Address": "P_Address",
            "P_City": "P_City",
            "P_Provience": "P_Provience",
            "P_Country": "P_Country",
            "P_PostalCode": "P_PostalCode",
            "P_ZipCode": "P_ZipCode",
            "P_Phone": "P_Phone",
            "P_Mobile": "P_Mobile",
            "P_Email": "P_Email",
            "P_ContactPerson": "P_ContactPerson",
            
            # Home Address
            "H_Address": "H_Address",
            "H_City": "H_City",
            "H_Provience": "H_Provience",
            "H_Country": "H_Country",
            "H_PostalCode": "H_PostalCode",
            "H_ZipCode": "H_ZipCode",
            "H_Phone": "H_Phone",
            "H_Mobile": "H_Mobile",
            "H_Email": "H_Email",
            "H_ContactPerson": "H_ContactPerson",
            
            # Employment Details
            "DepartmentCode": "DepartmentCode",
            "DesignationCode": "DesignationCode",
            "GradeCode": "GradeCode",
            "ShiftCode": "ShiftCode",
            "EmployeeLocationCode": "EmployeeLocationCode",
            "JobTitle": "JobTitle",
            "ManagerCode": "ManagerCode",
            "DepartmentHead": "DepartmentHead",
            "Subtitute": "Subtitute",
            "EmployeeReplacementCode": "EmployeeReplacementCode",
            
            # Dates
            "JoiningDate": "JoiningDate",
            "AppointmentDate": "AppointmentDate",
            "ProbitionDate": "ProbitionDate",
            "ContractStartDate": "ContractStartDate",
            "ContractEndDate": "ContractEndDate",
            "LeftDate": "LeftDate",
            
            # Contract & Salary
            "ContractType": "ContractType",
            "EmploymentStatus": "EmploymentStatus",
            "SalaryMode": "SalaryMode",
            "BasicPay": "BasicPay",
            "GrossPay": "GrossPay",
            "PerDayAvgCap": "PerDayAvgCap",
            
            # Banking
            "BankCode": "BankCode",
            "AccountNo": "AccountNo",
            "CardNo": "CardNo",
            "MachineRegistrationNo": "MachineRegistrationNo",
            
            # Benefits
            "EOBINo": "EOBINo",
            "IsEOBI": "IsEOBI",
            "SocialSecurityNo": "SocialSecurityNo",
            "IsSocialSecuirty": "IsSocialSecuirty",
            "ProvidentFundNo": "ProvidentFundNo",
            
            # Attendance Settings
            "offdayBonusAllow": "offdayBonusAllow",
            "AutoAttendanceAllow": "AutoAttendanceAllow",
            "OverTimeAllow": "OverTimeAllow",
            "LateTimeAllow": "LateTimeAllow",
            "EarlyLateAllow": "EarlyLateAllow",
            "HolyDayBonusAllow": "HolyDayBonusAllow",
            "PunctuailityAllown": "PunctuailityAllown",
            "EarlyLateNoofDeductionExempt": "EarlyLateNoofDeductionExempt",
            "OTAllowedPerDay": "OTAllowedPerDay",
            "NoOfDependant": "NoOfDependant",
            
            # Commission
            "EmployeeCommisionBonusActive": "EmployeeCommisionBonusActive",
            "EmployeeCommisionBonusPer": "EmployeeCommisionBonusPer",
            "EmployeeEarlyLateDeductionOnTimeActive": "EmployeeEarlyLateDeductionOnTimeActive",
            "EmployeePerDayType": "EmployeePerDayType",
            
            # Status Flags
            "IsActive": "IsActive",
            "isManagerFilter": "isManagerFilter",
            "isUserFilter": "isUserFilter",
            
            # Job Description
            "MainJobDuty": "MainJobDuty",
            "SecondryJobDuty": "SecondryJobDuty",
            "Remarks": "Remarks",
            
            # Reference
            "RefNo": "RefNo",
            "HRDocNo": "HRDocNo",
            "BarCode": "BarCode",
            "BenifitCode": "BenifitCode",
            "CheckInState": "CheckInState",
            "CheckOutState": "CheckOutState",
            
            # User Account
            "MUID": "MUID",
            "MUserlogin": "MUserlogin",
            "MUserpassword": "MUserpassword",
            
            # Images
            "pictureimg": "pictureimg",
            "pictureURL": "pictureURL"
        }
        
        # Apply mapping and handle values
        for json_field, db_field in field_mapping.items():
            if json_field in head_data:
                val = head_data[json_field]
                
                # Handle different value types
                if val is None:
                    head_row[db_field] = None
                elif val == "":
                    head_row[db_field] = ""
                elif isinstance(val, bool):
                    head_row[db_field] = 1 if val else 0
                elif isinstance(val, (int, float)):
                    head_row[db_field] = val
                elif isinstance(val, str):
                    if val.upper() in ["TRUE", "FALSE"]:
                        head_row[db_field] = 1 if val.upper() == "TRUE" else 0
                    else:
                        head_row[db_field] = val
                else:
                    head_row[db_field] = str(val)
        
        # Add system fields
        head_row["Code"] = emp_code
        head_row["offcode"] = company_offcode
        head_row["bcode"] = branch_code or "010101"
        head_row["compcode"] = "01"
        head_row["createdby"] = createdby
        head_row["createdate"] = current_date
        head_row["uid"] = uid
        
        # Ensure IsActive is properly set
        if "IsActive" in head_row:
            if isinstance(head_row["IsActive"], str):
                head_row["IsActive"] = 1 if head_row["IsActive"].upper() in ["TRUE", "1", "YES", "ON"] else 0
        else:
            head_row["IsActive"] = 1
        
        # Filter to only valid columns for the table (excluding computed columns)
        head_row = filter_valid_columns(head_table, head_row, exclude_computed=True)
        
        print(f"📋 Head row prepared with {len(head_row)} columns")
        print(f"📋 Columns: {list(head_row.keys())}")
        
        # Check for potential truncation issues
        column_lengths = get_column_lengths(head_table)
        for col, val in head_row.items():
            if col in column_lengths and isinstance(val, str):
                if len(val) > column_lengths[col]:
                    print(f"⚠️ WARNING: Column '{col}' length {len(val)} exceeds max {column_lengths[col]}")
        
        # Build and execute head insert query
        head_query = build_insert_query(head_table, [head_row])
        
        if not head_query:
            return jsonify({"success": False, "error": "Failed to build insert query"}), 400
        
        print("📝 HEAD QUERY:", head_query)
        
        # Execute head insert
        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            cursor.execute(head_query)
            conn.commit()
            print("✅ Head inserted successfully!")
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"❌ Head insert failed: {e}")
            return jsonify({"success": False, "error": str(e)}), 500
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        
        # Process details
        detail_results = []
        
        for det_idx, det in enumerate(details):
            table_name = det.get("tableName")
            rows = det.get("rows", [])
            
            if not table_name or not rows:
                print(f"⚠️ Skipping detail {det_idx}: missing tableName or rows")
                continue
            
            print(f"📋 Processing detail: {table_name} with {len(rows)} rows")
            
            processed_rows = []
            for row in rows:
                # Flatten attendanceSpec if present
                if "attendanceSpec" in row and row["attendanceSpec"]:
                    att_spec = row["attendanceSpec"]
                    row["OverTimeAllow"] = att_spec.get("OverTimeAllow", False)
                    row["LateTimeAllow"] = att_spec.get("LateTimeAllow", False)
                    row["EarlyLateAllow"] = att_spec.get("EarlyLateAllow", False)
                    row["AutoAttendanceAllow"] = att_spec.get("AutoAttendanceAllow", False)
                    row["offdayBonusAllow"] = att_spec.get("offdayBonusAllow", False)
                    row["HolyDayBonusAllow"] = att_spec.get("HolyDayBonusAllow", False)
                    row["NoOfExempt"] = att_spec.get("NoOfExempt", 0)
                    if "attendanceSpec" in row:
                        del row["attendanceSpec"]
                
                # Add required fields
                row["Code"] = emp_code
                row["offcode"] = company_offcode
                
                # Handle boolean fields
                for key in ["OverTimeAllow", "LateTimeAllow", "EarlyLateAllow", 
                           "AutoAttendanceAllow", "offdayBonusAllow", "HolyDayBonusAllow", "IsActive"]:
                    if key in row:
                        if isinstance(row[key], str):
                            row[key] = 1 if row[key].upper() in ["TRUE", "1", "YES", "ON"] else 0
                        elif isinstance(row[key], bool):
                            row[key] = 1 if row[key] else 0
                
                # Filter to valid columns for this detail table (excluding computed columns)
                filtered_row = filter_valid_columns(table_name, row, exclude_computed=True)
                processed_rows.append(filtered_row)
            
            if processed_rows:
                det_query = build_insert_query(table_name, processed_rows)
                if det_query:
                    detail_results.append({
                        "table": table_name,
                        "row_count": len(processed_rows),
                        "query": det_query
                    })
                    print(f"✅ Prepared query for {table_name}")
        
        # Execute detail queries in a transaction
        if detail_results:
            conn = None
            cursor = None
            try:
                conn = pyodbc.connect(CONN_STR)
                cursor = conn.cursor()
                
                for detail in detail_results:
                    print(f"📝 Executing detail query for {detail['table']}")
                    cursor.execute(detail['query'])
                
                conn.commit()
                print(f"✅ Executed {len(detail_results)} detail queries")
            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"❌ Detail queries failed: {e}")
                return jsonify({"success": False, "error": str(e)}), 500
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        
        # Get inserted record for response
        select_query = f"SELECT * FROM {head_table} WHERE Code = '{emp_code}'"
        try:
            result = execute_query(select_query)
            inserted_record = result[0] if result else None
        except Exception as e:
            print(f"⚠️ Could not fetch inserted record: {e}")
            inserted_record = None
        
        return jsonify({
            "success": True,
            "message": "Employee and related details inserted successfully",
            "empCode": emp_code,
            "offcode": company_offcode,
            "uid": uid,
            "insertedRecord": inserted_record,
            "details_processed": len(detail_results)
        }), 200
        
    except Exception as err:
        print("❌ ERROR in insert_employee_head_det:")
        print(str(err))
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(err)}), 500