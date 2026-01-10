<%
'##########################################################################################
' HELPER FUNCTIONS FOR CLASSIC ASP APPLICATIONS
' This file contains utility functions for database operations, error handling, and debugging
'##########################################################################################

'##########################################################################################
' DATABASE OPERATION FUNCTIONS
'##########################################################################################

'******************************************************************************************
' Function: ExecuteQuery
' Purpose: Safely executes SQL queries with comprehensive error handling
' Parameters: 
'   - sqlQuery: The SQL statement to execute
' Returns: 
'   - Recordset object if successful, Nothing if error occurs
' Usage: Set rs = ExecuteQuery(strSQL) where strSQL is the SQL statement to execute
' Error Handling: Logs detailed error information and returns Nothing on failure
'******************************************************************************************

Function ExecuteQuery(sqlQuery)
    On Error Resume Next
    
    Dim rs
    Set rs = objConn.Execute(sqlQuery)
    
    If Err.Number <> 0 Then ' if there is an error, log the error and return Nothing
        Call LogError("SQL Query Execution Error", Err, sqlQuery)
        Set ExecuteQuery = Nothing
        Err.Clear
    Else
        Set ExecuteQuery = rs
    End If
	
	On Error GoTo 0
End Function


'******************************************************************************************
' Sub: FlushRecordset
' Purpose: Flush data in memory to browser to avoid memory overflow 
' Parameters: 
'   - flushInterval: Number of rows to process before flushing (default: 1000)
' Returns: None 
' Usage: Call FlushRecordset(1000)
' Notes
'******************************************************************************************
Dim rowCount     ' must be declared at the module level outside function scope

Sub FlushRecordset(flushinterval)

    'reset counter
    If flushinterval = -1 Then
        rowCount = 0
        Exit Sub
    End If

    If IsEmpty(rowCount) Then
        rowCount = 0
    End If

    ' set default flush interval if empty
    If IsEmpty(flushinterval) or flushinterval = 0 Then
        flushinterval = 1000
    End If

    'increment number
    rowCount = rowCount + 1

    If rowCount Mod flushInterval = 0 Then
        Response.Flush
    End If
End Sub




'##########################################################################################
' UTILITY AND HELPER FUNCTIONS
'##########################################################################################

'******************************************************************************************
' Function: RecodeMissNull
' Purpose: Recode Missing to a default value
' Parameters: 
'   - codeValue: The code value to convert
'   - defaultValue: the recoded value for codeValue when missing
' Returns: 
'   - either codeValue or defaultValue
' Usage: displayText = RecodeMissNull(v_region)
'******************************************************************************************
Function RecodeMissNull(codeValue, defaultValue)
    If IsNull(codeValue) Or codeValue = "" Then
        RecodeMissNull = defaultValue
    Else
        RecodeMissNull = codeValue
    End If
End Function

'******************************************************************************************
' Function: GetSingleValue
' Purpose: Executes a query and returns a single value with fallback to default
' Parameters: 
'   - sqlQuery: The SQL statement to execute
'   - defaultValue: Value to return if query fails or returns no results
' Returns: 
'   - The first column value from the first row, or defaultValue if error
' Usage: v_fiscalYear = GetSingleValue(strSQL, 0) where strSQL is the SQL statement to execute
' Error Handling: Returns defaultValue on any error, safely closes recordset
'******************************************************************************************
Function GetSingleValue(sqlQuery, defaultValue)
    Dim rs, result
    
    Set rs = ExecuteQuery(sqlQuery)
    If Not rs Is Nothing Then
        If Not rs.EOF Then
            result = SafeGetField(rs, 0, defaultValue)  ' 0 means the first column of the first row (when using a numeric index instead of a string index)
        Else
            result = defaultValue
        End If
        rs.Close
        Set rs = Nothing    ' release the recordset object from memory
    Else
        result = defaultValue
    End If
    
    GetSingleValue = result
End Function

'##########################################################################################
' ERROR HANDLING AND LOGGING FUNCTIONS
'##########################################################################################

'******************************************************************************************
' Sub: LogError
' Purpose: Logs detailed error information with formatted HTML output
' Parameters: 
'   - errorType: Category/type of error (e.g., "Query Error", "Database Error")
'   - errObj: The Err object containing error details
'   - query: Optional SQL query that caused the error
' Returns: None (writes directly to response)
' Usage: Call LogError("Database Error", Err, strSQL)
' Output: Formatted HTML error box with error details
'******************************************************************************************
Sub LogError(errorType, errObj, query)
    Dim errorMsg
    errorMsg = "<div class='error-box'>"
    errorMsg = errorMsg & "<strong>" & errorType & ":</strong><br>"
    errorMsg = "<div style='background:#ffeeee;border:2px solid red; padding: 10px; margin:5px; color:#cc0000;'>"
    errorMsg = errorMsg & "Error description: " & Server.HTMLEncode(errObj.Description) & "<br>"
    errorMsg = errorMsg & "Error Number: " & errObj.Number & "<br>"
    errorMsg = errorMsg & "Error Source: " & Server.HTMLEncode(errObj.Source) & "<br>"
    errorMsg = errorMsg & "Time: " & Now() & "<br>"
    errorMsg = errorMsg & "<br>"

    If query <> "" Then
        errorMsg = errorMsg & "SQL Query: " & "<br>"
        errorMsg = errorMsg & Server.HTMLEncode(query) & "<br>"
    End If
  
    errorMsg = errorMsg & "</div>"
    errorMsg = errorMsg & "</div>"
    
    Response.Write errorMsg
    Response.Flush
End Sub

'##########################################################################################
' DATA SAFETY AND VALIDATION FUNCTIONS
'##########################################################################################

'******************************************************************************************
' Function: SafeDateComparison
' Purpose: Creates safe SQL date comparison that handles null values and avoids to_date() issues
' Parameters: 
'   - field1: First date field name
'   - field2: Second date field name
'   - comparison: Comparison operator ('<', '>', '<=', '>=', '=', '!=')
'   - table1: Optional table alias for first field
'   - table2: Optional table alias for second field
'   - dateFormat: date format string (e.g., 'MM/DD/YYYY')
' Returns: 
'   - SQL WHERE clause fragment for safe date comparison
' Usage: strSQL = strSQL & "WHERE" & SafeDateComparison("DT_STAGE_START", "DT_STAGE_CLOSE", "<", "m", "b")
' Notes: Avoids to_date() function issues in Classic ASP by using trunc() and null checks
'******************************************************************************************
Function SafeDateComparison(field1, field2, comparison, table1, table2)
    Dim sqlFragment, prefix1, prefix2

    'add table prefixes if provided
    If table1 <> "" Then
        prefix1 = table1 & "."
    Else
        prefix1 = ""
    End If

    If table2 <> "" Then
        prefix2  = table2 & "."
    Else
        prefix2 = ""
    End If

    ' Build SQL fragment
    sqlFragment = prefix1 & field1 & " is not null and " & prefix2  & field2 & " is not null and " & _
                "trunc(" & prefix1 & field1 & ")" & comparison & _
                "trunc(" & prefix2 & field2 & ")" 
    SafeDateComparison = sqlFragment
End Function


'******************************************************************************************
' Function: SafeComparisonToDate
' Purpose: Creates safe SQL date comparison that when using TO_DATE function to compare two date strings
' Parameters: 
'   - field1: First date field name
'   - field2: Second date field name
'   - comparison: Comparison operator ('<', '>', '<=', '>=', '=', '!=')
'   - table1: Optional table alias for first field
'   - table2: Optional table alias for second field
'   - dateFormat: date format string (e.g., 'MM/DD/YYYY')
' Returns: 
'   - SQL WHERE clause fragment for safe date comparison
' Usage: strSQL = strSQL & "WHERE" & SafeComparisonToDate("DT_STAGE_START", "DT_STAGE_CLOSE", "<", "m", "b", "MM/DD/YYYY")
' Notes: Similar to SafeComparisonDate, this is used to compare two date strings
'******************************************************************************************
Function SafeComparisonToDate(field1, field2, comparison, table1, table2, dateFormat)
    Dim sqlFragment, prefix1, prefix2
    
    ' Add table prefixes if provided
    If table1 <> "" Then
        prefix1 = table1 & "."
    Else
        prefix1 = ""
    End If
    
    If table2 <> "" Then
        prefix2 = table2 & "."
    Else
        prefix2 = ""
    End If
    
    ' Build safe date comparison with null checks and trunc() instead of to_date()
    sqlFragment = prefix1 & field1 & " is not null and " & prefix2 & field2 & " is not null and " & _
                  "trunc(to_date(" & prefix1 & field1 & ",'" &dateFormat & "')) " & comparison & _
                  "trunc(to_date(" & prefix2 & field2 & ",'" &dateFormat & "')) "
    
    SafeComparisonToDate = sqlFragment
End Function

'******************************************************************************************
' Function: SafeConvertDate
' Purpose: Safely converts database values to dates with fallback handling, e.g., data is '0021/08/01', throw an error when rendering in asp
' Parameters: 
'   - dbValue: The value to convert to a date
'   - defaultValue: Date value to return if conversion fails
' Returns: 
'   - Converted date if successful, defaultValue if conversion fails
' Usage: dateValue = SafeConvertDate(rs("date_field"), Date())
' Error Handling: Returns defaultValue on conversion errors, clears error state
'******************************************************************************************

Function SafeConvertDate(dbValue, defaultValue)
    On Error Resume Next
    Dim convertedDate
    
    convertedDate = CDate(dbValue)
    
    If Err.Number <> 0 Then
        SafeConvertDate = defaultValue
        Err.Clear
    Else
        SafeConvertDate = convertedDate
    End If
	
	On Error GoTo 0
End Function

'******************************************************************************************
' Function: SafeGetField
' Purpose: Safely retrieves field values from recordsets with null/error handling
' Parameters: 
'   - recordset: The recordset object to read from
'   - fieldName: Name or index of the field to retrieve
'   - defaultValue: Value to return if field is null or error occurs
' Returns: 
'   - Field value if successful, defaultValue if null or error
' Usage: value = SafeGetField(rs, "fieldName", "")
' Error Handling: Returns defaultValue on any error, handles null values gracefully
'******************************************************************************************

Function SafeGetField(recordset, fieldName, defaultValue)
    On Error Resume Next
    Dim fieldValue
    
    If Not recordset.EOF Then
        fieldValue = recordset(fieldName)
        If Err.Number <> 0 Or IsNull(fieldValue) Then
            SafeGetField = defaultValue
            Err.Clear
        Else
            SafeGetField = fieldValue
        End If
    Else
        SafeGetField = defaultValue
    End If
	
	On Error GoTo 0
End Function

'##########################################################################################
' APPLICATION SPECIFIC FUNCTIONS
'##########################################################################################

'******************************************************************************************
' Sub: Logging
' Purpose: Logs user activity and report usage for audit purposes
' Parameters: None (uses global variables)
' Returns: None
' Usage: Call Logging
' Error Handling: Logs errors but continues execution
' Notes: This function is specific to the reporting application and logs to report_audit_insert
'******************************************************************************************

sub Logging()
	On Error Resume Next
	
	'if v_btnpushed     = "btnfyState" then
		logParam4 = "NULL,"
		logParam5 = "'" & v_month  & "',"
		logParam6 = "'" & "Region" & "',"
		logParam7 = "NULL,"
		logParam8 = "NULL" 
	' elseif v_btnpushed = "btnfyRegion" then
		' logParam4 = "'" & v_month  & "',"
		' logParam5 = "'" & v_year   & "',"
		' logParam6 = "'" & v_region & "',"
		' logParam7 = "'" & v_unit   & "',"
		' logParam8 = "NULL" 
	' elseif v_btnpushed = "btnfyUnit" then
		' logParam4 = "'" & v_month  & "',"
		' logParam5 = "'" & v_year   & "',"
		' logParam6 = "'" & v_region & "',"
		' logParam7 = "'" & v_unit   & "',"
		' logParam8 = "NULL" 
	' else 
		' logParam4 = "NULL,"
		' logParam5 = "NULL,"
		' logParam6 = "NULL,"
		' logParam7 = "NULL,"
		' logParam8 = "NULL" 
	' end if

	strSQL = "report_audit_insert('" & v_user & "','" & LVL_RPT_FILENAME & "','" & page_title2 & "'," & logParam4 & logParam5 & logParam6 &_
									   logParam7 & logParam8 & ")"								 

	objConn.execute(strSQL) 
	
	If Err.Number <> 0 Then
		Call LogError("Logging Error", Err, strSQL)
		Err.Clear
	End If

	On Error GoTo 0
end sub

'##########################################################################################
' DEBUGGING AND DEVELOPMENT FUNCTIONS
'##########################################################################################

'******************************************************************************************
' Sub: DebugOutput
' Purpose: Outputs debug information only when debug mode is enabled
' Parameters: 
' - message: The message to output
' - cssClass: Optional CSS class for styling (default: "info-box")
' Returns: None
' Usage: Call DebugOutput("SQL Query: " & strSQL, "sql-query")
' Notes: Only outputs when debugMode is True, makes debug code more compact
'******************************************************************************************

Sub DebugOutput(message, cssClass)
    If debugMode Then
        If IsEmpty(cssClass) Or cssClass = "" Then
            cssClass = "info-box"
        End If
        Response.Write "<p class='" & cssClass & "' style='background-color:#e6f3ff; border:1px solid #0066cc; padding:10px; margin:5px; color:#003366'>" & message & "</p>"
    End If
End Sub


' Call DebugOutput("Fiscal Year Query: " & Server.HTMLEncode(strSQL), "sql-query") 
' Call DebugOutput("Fiscal Year Result: " & v_fiscalYear, "info-box") 
' Call DebugOutput("Load Date Query: " & Server.HTMLEncode(strSQL), "sql-query")

'******************************************************************************************
' Sub: DebugOutputSimple
' Purpose: Simple debug output without CSS class (uses default info-box style)
' Parameters: 
' - message: The message to output
' Returns: None
' Usage: Call DebugOutputSimple("Variable value: " & variable)
' Notes: Shorthand for simple debug messages
'******************************************************************************************

Sub DebugOutputSimple(message)
    Call DebugOutput(message, "")
End Sub

'******************************************************************************************
' Sub: PrintVariables
' Purpose: Displays all current application variables in a formatted debug box
' Parameters: None (uses global variables)
' Returns: None (writes directly to response)
' Usage: Call PrintVariables
' Output: Formatted HTML debug box showing all variable values
' Notes: This function is for development/debugging only, should be disabled in production
'******************************************************************************************

sub PrintVariables(variableNames)
	If variableNames <> "" Then
		Dim varArray, varName, varValue, i
		
		varArray = Split(variableNames, ",")
		
		for i = 0 to UBound(varArray)
			varName = Trim(varArray(i))
			varValue = Eval(varName)
			Response.Write "<p class=""info-box"" style='background-color:#e6f3ff; border:1px solid #0066cc; padding:10px; margin:5px; color:#003366'>" & varName & ": " & varValue &  "</p>"
		Next
	End If
end sub


'******************************************************************************************
' Function: GetStyle
' Purpose: Get the right css style for corresponding elements or classes
' Parameters: 
'   - styleType: the type of element or class
' Returns: 
'   - a css string that can be used to style that element or class
' Usage: in the head tag, add a style tag and in it add GetStyle("leading-zeros")
'******************************************************************************************
Function GetStyle(styleType):
    Select case styleType
        ' number formats
        case "leading-zeros"       
            GetStyle = ".leading-zeros{mso-number-format:""\@"";}"
        case "percentage-1"        ' percentages with 1 decimal
            GetStyle = ".percentage-1{mso-number-format:""#0.0\%"";}"
        case "percentage-2"        ' percentages with 2 decimals
            GetStyle = ".percentage-2{mso-number-format:""#0.00\%"";}"
        case "decimal-1"           ' 1 decimal
            GetStyle = ".decimal-1{mso-number-format:""#0.0"";}"
        case "decimal-2"           ' 2 decimals
            GetStyle = ".decimal-2{mso-number-format:""#0.00"";}"
        case "comma-format"        ' comma between every 3 digits
            GetStyle  = ".comma-format{mso-number-format:""#,##0"";}"
        case "currency-1"          ' 1 decimal with dollar sign
            GetStyle = ".currency-1{mso-number-format: ""$#,##0.0"";}"
        case "currency-2"          ' 2 decimals with dollar sign
            GetStyle = ".currency-2{mso-number-format: ""$#,##0.00"";}"
        
        ' other styling
        case "body"                 ' set body tag style
            GetStyle = "body{font-family:Helvetica, Arial, sans-serif; font-size:1em;}"
        case "page-title"
            GetStyle = ".page-title{text-align:center; font-size:1.3em; font-weight:bold;}"
        case "page-subtitle"
            GetStyle = ".page-subtitle{text-align:center; font-size:1.1em; font-weight:bold;}"

        ' table styling
        case "td" 
            GetStyle = ".content table td{border:1px solid #ddd; text-align:left;}"
        case "th"
            GetStyle = ".content table th{background-color:#C1FFC1;text-align:left; font-weight:bold; border:1px solid #ddd;}"
    End Select
End Function

'******************************************************************************************
' Function: setTHWidth
' Purpose: set table header width to certain length and break text naturally, similar to wordwrap in excel (to avoid using multiple rows for long text)
' Parameters: 
'   - width: the width of the column
' Returns: 
'   - a css string that sets the width columns
' Usage: in the head tag, add a style tag and in it add setTHWidth(100) 
'******************************************************************************************
Function setTHWidth(width)
    If IsEmpty(width) or IsNull(width) Then
        width = 250
    End If
    setTHWidth = ".content table th {width:" & width & "px; mso-width-source:userset;mso-width-alt:" & (width/250)*3000 &"; word-break:normal; white-space:normal;}"
End Function


%>