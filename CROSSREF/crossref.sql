-----------------------------------------------------------------------
-- Enhanced CrossRef - Unified Search
-- Always searches CodeStore. If p_all_codetypes = 'Y', includes Data Fix type
-----------------------------------------------------------------------
PROCEDURE CrossRef_Unified(
  p_Token           IN VARCHAR2,
  p_search_etl      IN VARCHAR2 DEFAULT 'Y',
  p_detailed        IN VARCHAR2 DEFAULT 'N',
  p_all_codetypes   IN VARCHAR2 DEFAULT 'N'
)
IS
  v_match NUMBER := 0;
  
  CURSOR c_codestore IS
    SELECT DISTINCT 
           'CODESTORE' as source,
           m.CodeOwner,
           m.CodeName,
           m.CodeType,
           NULL as file_path,
           NULL as line_num,
           NULL as line_content
      FROM a$CodeStore_Master m,
           a$CodeStore_Detail d
     WHERE m.ID = d.MasterID
       AND m.ActiveFlag = 'A'
       AND upper(d.CodeDetail) LIKE '%'||upper(p_Token)||'%'
       AND (upper(p_all_codetypes) = 'Y' 
            OR m.CodeType IN ('PACKAGE BODY', 'SHELL SCRIPT', 'SQL SCRIPT', 
                             'PROCEDURE', 'PACKAGE', 'FUNCTION', 'TABLE', 'VIEW'))
    UNION
    SELECT DISTINCT
           'RECTIFYDB' as source,
           m.UserName as CodeOwner,
           'Episode - '||m.ID as CodeName,
           'Data Fix' as CodeType,
           NULL as file_path,
           NULL as line_num,
           NULL as line_content
      FROM a$rectifydb_log_master m,
           a$rectifydb_log_detail l
     WHERE m.ID = l.Master_Id
       AND upper(l.Episode_code) LIKE '%'||upper(p_Token)||'%'
       AND upper(p_all_codetypes) = 'Y'
     ORDER BY CodeType, CodeOwner, CodeName;
  
  CURSOR c_codestore_detail IS
    SELECT m.CodeOwner,
           m.CodeName,
           m.CodeType,
           d.LineNum,
           SUBSTR(d.CodeDetail, 1, 200) as line_content
      FROM a$CodeStore_Master m,
           a$CodeStore_Detail d
     WHERE m.ID = d.MasterID
       AND m.ActiveFlag = 'A'
       AND upper(d.CodeDetail) LIKE '%'||upper(p_Token)||'%'
       AND (upper(p_all_codetypes) = 'Y' 
            OR m.CodeType IN ('PACKAGE BODY', 'SHELL SCRIPT', 'SQL SCRIPT', 
                             'PROCEDURE', 'PACKAGE', 'FUNCTION', 'TABLE', 'VIEW'))
    UNION ALL
    SELECT m.UserName as CodeOwner,
           'Episode - '||m.ID as CodeName,
           'Data Fix' as CodeType,
           0 as LineNum,
           SUBSTR(l.Episode_code, 1, 200) as line_content
      FROM a$rectifydb_log_master m,
           a$rectifydb_log_detail l
     WHERE m.ID = l.Master_Id
       AND upper(l.Episode_code) LIKE '%'||upper(p_Token)||'%'
       AND upper(p_all_codetypes) = 'Y'
     ORDER BY CodeType, CodeOwner, CodeName, LineNum;
  
  CURSOR c_etl_summary IS
    SELECT DISTINCT
           filename,
           file_path,
           file_type
      FROM a$ETL_Files_Staging
     WHERE upper(line_content) LIKE '%'||upper(p_Token)||'%'
     ORDER BY file_path, filename;
  
  CURSOR c_etl_detail IS
    SELECT filename,
           file_path,
           file_type,
           line_num,
           SUBSTR(line_content, 1, 200) as line_content
      FROM a$ETL_Files_Staging
     WHERE upper(line_content) LIKE '%'||upper(p_Token)||'%'
     ORDER BY file_path, filename, line_num;
  
  v_prev_file VARCHAR2(500) := 'XXXXX';
  v_prev_codetype VARCHAR2(50) := 'XXXXX';
  
BEGIN
  dbms_output.put_line('A new search has been initiated for: ' || p_Token);
  dbms_output.put_line('========================================================================');
  dbms_output.put_line('');
  
  -- Search CodeStore (Downstream: SUM/HIST tables) - Always executed
  v_prev_codetype := 'XXXXX';  -- Reset for CodeStore section
  v_prev_file := 'XXXXX';      -- Reset for CodeStore section
  dbms_output.put_line('ODSI TECH TEAM CODE SEARCH RESULTS:');
  dbms_output.put_line('------------------------------------------------------------------------');
  
  IF upper(p_detailed) = 'Y' THEN
    -- Show line-level details
    FOR rec IN c_codestore_detail LOOP
      -- Group by CodeType
      IF rec.CodeType != v_prev_codetype THEN
        dbms_output.put_line('');
        dbms_output.put_line('  CodeType: ' || rec.CodeType);
        dbms_output.put_line('  ' || rpad('-', 70, '-'));
        v_prev_codetype := rec.CodeType;
        v_prev_file := 'XXXXX';  -- Reset file grouping
      END IF;
      
      IF rec.CodeName != v_prev_file THEN
        dbms_output.put_line('');
        dbms_output.put_line('    Object: ' || rpad(rec.CodeOwner || '.' || rec.CodeName, 80));
        v_prev_file := rec.CodeName;
      END IF;
      dbms_output.put_line('      Line ' || lpad(rec.LineNum, 6) || ': ' || rec.line_content);
      v_match := v_match + 1;
    END LOOP;
  ELSE
    -- Summary view - grouped by CodeType
    FOR rec IN c_codestore LOOP
      -- Group by CodeType
      IF rec.CodeType != v_prev_codetype THEN
        dbms_output.put_line('');
        dbms_output.put_line('  CodeType: ' || rec.CodeType);
        dbms_output.put_line('  ' || rpad('-', 70, '-'));
        v_prev_codetype := rec.CodeType;
      END IF;
      dbms_output.put_line('  ' || rec.CodeOwner || CHR(9) || 
                          rec.CodeName || CHR(9) || 
                          rec.CodeType);
      v_match := v_match + 1;
    END LOOP;
  END IF;
  
  -- Search Data Team ETL Files (Upstream)
  IF upper(p_search_etl) = 'Y' THEN
    v_prev_file := 'XXXXX';  -- Reset for ETL section
    dbms_output.put_line('');
    dbms_output.put_line('IT DATA TEAM CODE SEARCH RESULTS:');
    dbms_output.put_line('------------------------------------------------------------------------');
    
    IF upper(p_detailed) = 'Y' THEN
      -- Show line-level details
      FOR rec IN c_etl_detail LOOP
        IF rec.file_path != v_prev_file THEN
          dbms_output.put_line('');
          dbms_output.put_line('  File: ' || rpad(rec.file_path, 100) || 
                              ' (' || rpad(rec.file_type, 10) || ')');
          v_prev_file := rec.file_path;
        END IF;
        dbms_output.put_line('    Line ' || lpad(rec.line_num, 6) || ': ' || rec.line_content);
        v_match := v_match + 1;
      END LOOP;
    ELSE
      -- Summary view
      FOR rec IN c_etl_summary LOOP
        dbms_output.put_line('  ' || rec.filename || CHR(9) || 
                            rec.file_path || CHR(9) || 
                            rec.file_type);
        v_match := v_match + 1;
      END LOOP;
    END IF;
  END IF;
  
  dbms_output.put_line('');
  dbms_output.put_line('========================================================================');
  IF v_match = 0 THEN
    dbms_output.put_line('No references found for "' || p_Token || '"');
  ELSE
    dbms_output.put_line('Total matches: ' || v_match);
  END IF;
  
END CrossRef_Unified;