set trimspool on
set heading off
set serveroutput on
spool &1
declare
--
cursor tables_with_no_stats is
select owner,
       table_name
  from all_tables
 where owner in ('CAPS', 'CCL', 'HR', 'PEI',  'SWI')
   and last_analyzed is NULL
   and table_name not like 'ARCH%'
   and table_name not like 'GTT%'
   and table_name not like 'EXT%'
   and table_name not like 'QUEST%'
   and table_name not like 'WK%'
   and table_name not like 'CSL_JC%'
   and table_name not like 'CSL_DAILY_KIN%'
   and table_name not like 'CSL_DAILY_SFI%'
   and table_name not like 'FN_SFI%'
   and table_name not like 'FT_CHILD%'
   and table_name not like 'FT_PERP%'
   and table_name not like 'DASHBOARD%'
   and table_name not like 'ACCESSHR%'
   and table_name != 'TIME_DIM'
   and table_name != 'PP_COURT_ORD_FACT'       -- no longer populated
   and table_name != 'PP_POST_ADOPT_HIST_FACT' -- no longer populated
   and table_name != 'SVC_OUTMAT_HIST_DIM'     -- no longer populated
   and table_name != 'CYD_SRVC_HIST_FACT'      -- no longer populated
   and table_name != 'FAM_SERVICE_PLAN_DIM'    -- no longer populated
   and table_name != 'INR_TR_APPRVD_CHILD_FACT'-- no longer populated
   and table_name != 'AUD_INV_PRINC_DIM'       -- unique audit table, most recent timeload always contains zero records
 order by 1, 2;
--
cursor indexes_with_no_stats is
select owner,
       index_name
  from all_indexes
 where owner in ('CAPS', 'CCL', 'HR', 'PEI',  'SWI')
   and last_analyzed is NULL
   and table_name not like 'ARCH%'
   and table_name not like 'GTT%'
   and table_name not like 'EXT%'
   and table_name not like 'QUEST%'
   and table_name not like 'WK%'
   and table_name not like 'CSL_JC%'
   and table_name not like 'CSL_DAILY_KIN%'
   and table_name not like 'CSL_DAILY_SFI%'
   and table_name not like 'FN_SFI%'
   and table_name not like 'FT_CHILD%'
   and table_name not like 'FT_PERP%'
   and table_name not like 'DASHBOARD%'
   and table_name not like 'ACCESSHR%'
   and table_name != 'TIME_DIM'
   and table_name != 'PP_COURT_ORD_FACT'       -- no longer populated
   and table_name != 'PP_POST_ADOPT_HIST_FACT' -- no longer populated
   and table_name != 'SVC_OUTMAT_HIST_DIM'     -- no longer populated
   and table_name != 'CYD_SRVC_HIST_FACT'      -- no longer populated
   and table_name != 'FAM_SERVICE_PLAN_DIM'    -- no longer populated
   and table_name != 'INR_TR_APPRVD_CHILD_FACT'-- no longer populated
   and table_name != 'AUD_INV_PRINC_DIM'       -- unique audit table, most recent timeload always contains zero records
 order by 1, 2;
--
cursor invalid_objects is
select owner,
       object_name,
       object_type
  from all_objects
 where owner in ('CAPS', 'CCL', 'HR', 'PEI',  'SWI')
   and status = 'INVALID'
   and object_name not like 'ARCH%'
   and object_name not like 'GTT%'
   and object_name not like 'EXT%'
   and object_name not like 'QUEST%'
   and object_name not like '%TEST'
   and object_name not like 'TMP%'
   and object_name not like 'BIN$%'
   and object_name not like 'WK%'
   and object_name not like 'CSL_JC%'
   and object_name not like 'CSL_DAILY_KIN%'
   and object_name not like 'CSL_DAILY_SFI%'
   and object_name not like 'FN_SFI%'
   and object_name not like 'FT_CHILD%'
   and object_name not like 'FT_PERP%'
   and object_name not like '%_OLD'             -- inactive objects
   and object_name not like '%_OLD2'            -- inactive objects
   and object_name not like '%_LOAD_09'         -- inactive objects
   and object_name not like '%_LOAD_10'         -- inactive objects
   and object_name not like '%_201404'          -- inactive objects
   and object_name not like '%_20140616'        -- inactive objects
   and object_name not like 'DASHBOARD%'
   and object_name not like 'ACCESSHR%'
   and object_name != 'PLAN_TABLE'
   and object_name != 'TIME_DIM'
   and object_name != 'PP_COURT_ORD_FACT'       -- no longer populated
   and object_name != 'PP_POST_ADOPT_HIST_FACT' -- no longer populated
   and object_name != 'SVC_OUTMAT_HIST_DIM'     -- no longer populated
   and object_name != 'CYD_SRVC_HIST_FACT'      -- no longer populated
   and object_name != 'FAM_SERVICE_PLAN_DIM'    -- no longer populated
   and object_name != 'INR_TR_APPRVD_CHILD_FACT'-- no longer populated
   and object_name != 'AUD_INV_PRINC_DIM'       -- unique audit table, most recent timeload always contains zero records
 order by 3, 1, 2;
--
cursor archived_sum_tables is
select owner,
       table_name
  from all_tables
 where owner in ('CAPS', 'CCL', 'HR', 'PEI',  'SWI')
   and (table_name like '%SUM'
        or table_name like '%SUM_SECURE'
        or table_name like '%FACT'
        or table_name like '%DIM'
        or table_name = 'DASHBOARD')
   and table_name not like 'ARCH%'
   and table_name not like 'GTT%'
   and table_name not like 'EXT%'
   and table_name not like 'QUEST%'
   and table_name not like 'TMP%'
   and table_name not like 'POP%SUM'
   and table_name not like 'FT%SUM'
   and table_name not like 'WK%'
   and table_name not like 'CSL_JC%'
   and table_name not like 'CSL_DAILY_KIN%'
   and table_name not like 'CSL_DAILY_SFI%'
   and table_name not like 'FN_SFI%'
   and table_name not like 'FT_CHILD%'
   and table_name not like 'FT_PERP%'
   and table_name not like 'DASHBOARD%'
   and table_name not like 'ACCESSHR%'
   and table_name != 'TIME_DIM'
   and table_name != 'PP_COURT_ORD_FACT'       -- no longer populated
   and table_name != 'PP_POST_ADOPT_HIST_FACT' -- no longer populated
   and table_name != 'SVC_OUTMAT_HIST_DIM'     -- no longer populated
   and table_name != 'CYD_SRVC_HIST_FACT'      -- no longer populated
   and table_name != 'FAM_SERVICE_PLAN_DIM'    -- no longer populated
   and table_name != 'INR_TR_APPRVD_CHILD_FACT'-- no longer populated
   and table_name != 'AUD_INV_PRINC_DIM'       -- unique audit table, most recent timeload always contains zero records
   and (owner, table_name) in (select owner,
                                      substr(table_name, 6, length(table_name))
                                 from all_tables
                                where owner in ('CAPS', 'CCL', 'HR', 'PEI', 'SWI')
                                  and table_name like 'ARCH%')                    
 order by 1, 2;
--
cursor sum_tables is
select owner,
       table_name
  from all_tables
 where owner in ('CAPS', 'CCL', 'HR', 'PEI',  'SWI')
   and (table_name like '%SUM'
        or table_name like '%SUM_SECURE'
        or table_name like '%FACT'
        or table_name like 'SYMM%'
        or table_name like 'TELSTR%'
        or table_name like '%DIM'
        or table_name = 'DASHBOARD')
   and table_name in (select table_name 
                        from all_tab_columns 
                       where column_name = 'ID_TIME_LOAD')
   and table_name not like 'ARCH%'
   and table_name not like 'GTT%'
   and table_name not like 'EXT%'
   and table_name not like 'QUEST%'
   and table_name not like 'TMP%'
   and table_name not like 'POP%SUM'
   and table_name not like 'FT%SUM'
   and table_name not like '%HIST_SUM'
   and table_name not like 'WK%'
   and table_name not like 'DLY%'
   and table_name not like 'CSL_JC%'
   and table_name not like 'CSL_DAILY_KIN%'
   and table_name not like 'CSL_DAILY_SFI%'
   and table_name not like 'FN_SFI%'
   and table_name not like 'FT_CHILD%'
   and table_name not like 'FT_PERP%'
   and table_name not like 'DASHBOARD%'
   and table_name not like 'ACCESSHR%'
   and table_name != 'TIME_DIM'
   and table_name != 'PP_COURT_ORD_FACT'       -- no longer populated
   and table_name != 'PP_POST_ADOPT_HIST_FACT' -- no longer populated
   and table_name != 'SVC_OUTMAT_HIST_DIM'     -- no longer populated
   and table_name != 'CYD_SRVC_HIST_FACT'      -- no longer populated
   and table_name != 'FAM_SERVICE_PLAN_DIM'    -- no longer populated
   and table_name != 'INR_TR_APPRVD_CHILD_FACT'-- no longer populated
   and table_name != 'AUD_INV_PRINC_DIM'       -- unique audit table, most recent timeload always contains zero records
 order by 1,2;
--
cursor hist_tables is
select owner,
       table_name
  from all_tables
 where owner in ('CAPS', 'CCL', 'PEI',  'SWI')    
   and table_name like '%HIST%'
   and table_name not like '%HIST_TL_FY%'
   and table_name not like 'ARCH%'
   and table_name not like 'GTT%'
   and table_name not like 'EXT%'
   and table_name not like 'QUEST%'
   and table_name not like 'TMP%'
   and table_name not like 'POP%SUM'
   and table_name not like 'FT%SUM'
   and table_name not like '%HIST_SUM'
   and table_name not like 'WK%'
   and table_name not like 'DLY%'
   and table_name not like 'CSL_JC%'
   and table_name not like 'CSL_DAILY_KIN%'
   and table_name not like 'CSL_DAILY_SFI%'
   and table_name not like 'FN_SFI%'
   and table_name not like 'FT_CHILD%'
   and table_name not like 'FT_PERP%'
   and table_name not like 'DASHBOARD%'
   and table_name not like 'ACCESSHR%'
   and table_name != 'PP_COURT_ORD_FACT'       -- no longer populated
   and table_name != 'PP_POST_ADOPT_HIST_FACT' -- no longer populated
   and table_name != 'SVC_OUTMAT_HIST_DIM'     -- no longer populated
   and table_name != 'CYD_SRVC_HIST_FACT'      -- no longer populated
   and table_name != 'FAM_SERVICE_PLAN_DIM'    -- no longer populated
   and table_name != 'INR_TR_APPRVD_CHILD_FACT'-- no longer populated
   and table_name != 'AUD_INV_PRINC_DIM'       -- unique audit table, most recent timeload always contains zero records
 order by 1,2;
-- 
cursor CCLDiff is
select id_Time_load, 
       count(*) diff
  from ccl.facility_fact
 where id_Time_load >= 100261
 group by id_Time_load
minus
select id_Time_load,
       count(*)	diff			 
  from ccl.ccl_facility_sum
 where id_Time_load >= 100261
 group by id_Time_load
 order by 1;
--
cursor time_load_tables is
select owner, table_name 
  from all_tables 
 where owner in ('CAPS', 'CCL', 'PEI', 'SWI')
   and table_name in (select table_name 
                        from all_tab_columns 
                       where column_name = 'ID_TIME_LOAD')
   and table_name not like 'ARCH%'
   and table_name not like 'TMP%'
   and table_name not like 'TIME%'
   and table_name not like 'PASS%'
   and table_name not like 'SUM_LOG%'
   and table_name not like '%MASTER'
   and table_name not like '%3560'
   and table_name not like '%64'
   and table_name not like 'WK%'
   and table_name not like 'DLY%'
   and table_name not like 'CSL_JC%'
   and table_name not like 'CSL_DAILY_KIN%'
   and table_name not like 'CSL_DAILY_SFI%'
   and table_name not like 'FN_SFI%'
   and table_name not like 'FT_CHILD%'
   and table_name not like 'FT_PERP%'
   and table_name not like '%FD'
   and table_name not like '%RET'
   and table_name not like 'DASHBOARD%'
   and table_name not like 'ACCESSHR%'
   and table_name != 'ACCESSHR_EVAL_CMPL_FACT' -- no longer populated
   and table_name != 'PP_COURT_ORD_FACT'       -- no longer populated
   and table_name != 'PP_POST_ADOPT_HIST_FACT' -- no longer populated
   and table_name != 'SVC_OUTMAT_HIST_DIM'     -- no longer populated
   and table_name != 'CYD_SRVC_HIST_FACT'      -- no longer populated
   and table_name != 'FAM_SERVICE_PLAN_DIM'    -- no longer populated
   and table_name != 'INR_TR_APPRVD_CHILD_FACT'-- no longer populated
   and table_name != 'AUD_INV_PRINC_DIM'       -- unique audit table, most recent timeload always contains zero records
order by 1,2;
--
ARCHTotal number;
BaseTL    number := MRS.CurrentTL-2;   
CurrentTL number := MRS.CurrentTL; 
HistTL    number; 
OldStat   number := 90;  
SUMTotal  number;
pTL       number;
FACTCntr  number := 0;
SUMCntr   number := 0;
DiffCntr  number := 0;
-------------------------------------------------------------------
--
-- mdc_counts.sql
--
-------------------------------------------------------------------
begin
  dbms_output.put_line('MDC Counts');
  dbms_output.put_line('Time     : '||to_char(sysdate,'mm/dd/yyyy hh24:mi:ss'));
  dbms_output.put_line('Database : '||MRS.Global);
--
  select td.id_time_load
    into pTL
    from caps.time_dim td
   where lpad(td.nbr_time_calendar_month,2,0)||td.nbr_time_calendar_year = to_char(sysdate,'mmyyyy');
  dbms_output.put_line('Time Load: '||to_char(pTL-1));
  CurrentTL := pTL-1;
  BaseTL := CurrentTL-2;
  dbms_output.put_line(chr(10)||'------------------------------------------------------');
  dbms_output.put_line(chr(10)||'----------- List of tables that are not analyzed.');
--
  for rec in tables_with_no_stats
  loop
    dbms_output.put_line(rec.owner||'.'||rec.table_name||' <----<<< Error?');
  end loop;
--
  dbms_output.put_line(chr(10)||'----------- List of indexes that are not analyzed.');
--
  for rec in indexes_with_no_stats
  loop
    dbms_output.put_line(rec.owner||' : '||rec.index_name||' <----<<< Error?');
  end loop;
--
  dbms_output.put_line(chr(10)||'----------- List of INVALID (active) objects.'||chr(10));
--
  for rec in invalid_objects
  loop
    dbms_output.put_line(rec.object_type||' : '||rec.owner||'.'||rec.object_name||' <----<<< Error?');
  end loop;
--
  dbms_output.put_line(chr(10)||'----------- Compare table record counts to ARCH table record counts.');
--
  for rec in archived_sum_tables
  loop
    if (trim(to_char(sysdate, 'Month')) = 'November' 
        and MRS.Global = 'WARE') 
      or (trim(to_char(sysdate, 'Month')) = 'October' 
          and MRS.Global = 'QAWH') then
      BaseTL := CurrentTL-13;
    end if;
    dbms_output.put_line(chr(10)||rec.owner||'.'||rec.table_name);
    if rec.table_name like '%HIST_DIM' then
      BaseTL := CurrentTL-2;
    elsif (rec.table_name = 'NYTD_FACT'
           or rec.table_name like '%HIST%') then
      BaseTL := CurrentTL;
    end if;
    for i in BaseTL..CurrentTL
    loop
      execute immediate('select count(*) from '||rec.owner||'.'||rec.table_name||' where id_time_load = '||to_char(i))
         into SUMTotal;
      execute immediate('select count(*) from '||rec.owner||'.ARCH_'||rec.table_name||' where id_time_load = '||to_char(i))
         into ARCHTotal;
      if SUMTotal = ARCHTotal 
         and SUMTotal > 0 then
        dbms_output.put_line(to_char(i)||' TABLE: '||to_char(SUMTotal)||' = ARCH:'||to_char(ARCHTotal));
      elsif SUMTotal = 0
            and rec.table_name not like '%HIST%' then
        dbms_output.put_line(to_char(i)||' TABLE: '||to_char(SUMTotal)||' != ARCH:'||to_char(ARCHTotal)||' <----<<< Error 0 Records');
      else
        if mod(i, 12) = 0 
           and rec.table_name not like '%NYTD%' then
          dbms_output.put_line(to_char(i)||' TABLE: '||to_char(SUMTotal)||' != ARCH:'||to_char(ARCHTotal)||' <----<<< Error?');
        elsif i = CurrentTL then 
          dbms_output.put_line(to_char(i)||' TABLE: '||to_char(SUMTotal)||' != ARCH:'||to_char(ARCHTotal)||' <----<<< Error?');
        end if;
      end if;
    end loop;
  end loop;
--
  dbms_output.put_line(chr(10)||'----------- Check tables for consistent data volume.');
  dbms_output.put_line('----------- NOTE: HIST tables are NOT checked in this section.'||chr(10));
--
  BaseTL := CurrentTL-2;
  if (trim(to_char(sysdate, 'Month')) = 'November' 
      and MRS.Global = 'WARE') 
    or (trim(to_char(sysdate, 'Month')) = 'October' 
        and MRS.Global = 'QAWH') then
    BaseTL := CurrentTL-13;
  end if;
  for rec in sum_tables
  loop
    if rec.table_name not like '%HIST%' then
      dbms_output.put_line(rec.owner||'.'||rec.table_name);
      for i in BaseTL..CurrentTL
      loop
        execute immediate('select count(*) from '||rec.owner||'.'||rec.table_name||' where id_time_load = '||to_char(i))
           into SUMTotal;
        dbms_output.put_line(to_char(i)||' : '||to_char(SUMTotal));
      end loop;
      if MRS.ConsistentDataVolume(rec.owner, rec.table_name, CurrentTL, .10) then
        dbms_output.put_line('Data volume is consistent for timeloads '||to_char(CurrentTL-2)||
                             ' - '||to_char(CurrentTL)||'.'||chr(10));
      else
        dbms_output.put_line('Data volume is NOT consistent for timeloads '||to_char(CurrentTL-2)||
                             ' - '||to_char(CurrentTL)||'. <----<<< Error?'||chr(10));
      end if;
    end if;
  end loop;
--
  dbms_output.put_line(chr(10)||'----------- Manual HIST tables check for consistent data volume.'||chr(10));
--
  select min(id_time_load)
    into HistTL
    from time_dim
   where mod(id_time_load, 12) = 0
     and nbr_time_calendar_year = to_char(add_months(sysdate, -14), 'yyyy');
  for rec in hist_tables
  loop
    BaseTL := HistTL;
    dbms_output.put_line(rec.owner||'.'||rec.table_name);
    while BaseTL <= CurrentTL
    loop
      execute immediate('select count(*) from '||rec.owner||'.'||rec.table_name||' where id_time_load = '||to_char(BaseTL))
         into SUMTotal;
      dbms_output.put_line(to_char(BaseTL)||' : '||to_char(SUMTotal));
      if BaseTL < CurrentTL then
        BaseTL := least(CurrentTL, BaseTL+12);
      else
        BaseTL := CurrentTL+1;
      end if;
    end loop;
    dbms_output.put_line('Data volume consistency check is manual for HIST tables <----<<< Check totals above for Errors'||chr(10));
  end loop;
--
  dbms_output.put_line(chr(10)||'----------- List record count for the current time load in every tables.'||chr(10));
--
  for rec in time_load_tables
  loop
    execute immediate('select count(*) from '||rec.owner||'.'||rec.table_name||' where id_time_load = '||CurrentTL)
       into SUMTotal;
    dbms_output.put_line(rec.owner||'.'||rec.table_name);
    if SUMTotal > 0 then
      dbms_output.put_line(CurrentTL||' : '||SUMTotal||chr(10));
    else
      dbms_output.put_line(CurrentTL||' : '||SUMTotal||' <----<<< Error 0 Records'||chr(10));
    end if;
  end loop;
--
  dbms_output.put_line(chr(10)||'----------- Compare CCL.FACILITY_FACT and CCL.CCL_FACILITY_SUM record counts.'||chr(10));
--
  begin
    for rec in CCLDiff
    loop
      select count(*)
        into FACTCntr
        from ccl.facility_fact
       where id_time_load = rec.id_time_load;
      select count(*)
        into SUMCntr
        from ccl.ccl_facility_sum
       where id_time_load = rec.id_time_load;
      dbms_output.put_line(rec.id_time_load||' : FACT='||FACTCntr||' : SUM='||SUMCntr||' : Diff='||abs(FACTCntr-SUMCntr)||' <----<<< Error!!');
      DiffCntr := DiffCntr+1;
    end loop;
    if DiffCntr = 0 then
      dbms_output.put_line('CCL.FACILITY_FACT record counts match CCL.CCL_FACILITY_SUM for all time loads.');
    end if;
  exception
    when NO_DATA_FOUND then
      dbms_output.put_line('CCL.FACILITY_FACT record counts match CCL.CCL_FACILITY_SUM for all time loads.');
  end;
--
  dbms_output.put_line(chr(10)||'------------------------------------------------------');
  dbms_output.put_line('  Time     : '||to_char(sysdate,'mm/dd/yyyy hh24:mi:ss'));
  dbms_output.put_line('MDC Counts Complete');
exception
  when OTHERS then 
    dbms_output.put_line(chr(10)||'------------------------------------------------------ SQL Error.');
    dbms_output.put_line(substr(SQLERRM, 1, 120));
    dbms_output.put_line('  Time     : '||to_char(sysdate,'mm/dd/yyyy hh24:mi:ss'));
    dbms_output.put_line('MDC Counts Terminated Abnormally');
	raise;
end;
/
spool off
exit