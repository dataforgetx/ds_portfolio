set serveroutput on
set linesize 200
set trimspool on
spool &1
declare
-------------------------------------------------------------------
--
-- SUM_PURGE 
--    calls MRS.Purge, CAPS.a$PurgeCheck, and optionally CAPS.a$PurgeCheckAndFix
--
-------------------------------------------------------------------
begin
  dbms_output.put_line('sum_purge');
  dbms_output.put_line('Time     : '||to_char(sysdate,'mm/dd/yyyy hh24:mi:ss'));
  dbms_output.put_line('Database : '||MRS.Global);
  dbms_output.put_line('Time Load: N/A');
  dbms_output.put_line(chr(10)||'-----------');
--
-- Purge data
  MRS.Purge;
--
-- Check purge process and automatically fix errors
-- This will:
--   1. Run a$PurgeCheck to detect errors and log to queue table
--   2. Process queue table, request tablespace opens/closes, and fix errors
--   3. Send summary email with results
-- Note: Requires DBA approval for tablespace operations (polls every 2 min, times out after 4 hours)
  begin
    CAPS.a$PurgeCheckAndFix(
      p_auto_fix => TRUE,
      p_auto_tablespace => FALSE,  -- Requires DBA approval
      p_process_queue => TRUE,
      p_run_purgecheck_first => TRUE,   -- Run a$PurgeCheck first to detect/log errors
      p_poll_interval_seconds => 120,  -- Poll every 2 minutes
      p_max_wait_minutes => 240,        -- Timeout after 4 hours
      p_dry_run => FALSE
    );
    dbms_output.put_line('Automated purge check and fix process completed');
  exception
    when OTHERS then
      dbms_output.put_line('WARNING: Automated purge fix failed: '||substr(SQLERRM, 1, 200));
      dbms_output.put_line('Errors logged to queue table - may require manual intervention');
      -- Don't fail the whole process if automation fails
  end;
--
  dbms_output.put_line(chr(10)||'-----------');
  dbms_output.put_line('Time     : '||to_char(sysdate,'mm/dd/yyyy hh24:mi:ss'));
  dbms_output.put_line('sum_purge Complete');
exception
  when OTHERS then 
    dbms_output.put_line(chr(10)||'----------- SQL Error.');
    dbms_output.put_line(substr(SQLERRM, 1, 120));
    dbms_output.put_line('Time     : '||to_char(sysdate,'mm/dd/yyyy hh24:mi:ss'));
    dbms_output.put_line('sum_purge Terminated Abnormally');
	Raise;
end;
/
spool off
exit