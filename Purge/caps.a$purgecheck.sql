CREATE OR REPLACE procedure CAPS.a$PurgeCheck(Debug boolean default FALSE)
is

cursor GetCaseTabs is
select distinct DestinationTable,
       DefinitionTable
  from a$Purge_Master
 where MatchColumn like 'id%case%'
   and PurgeValue = '1'
   and DefinitionTable like '%case'
 order by 1;
--
cursor GetPersonTabs is
select distinct DestinationTable,
       DefinitionTable
  from a$Purge_Master
 where (MatchColumn like 'id%person%'
       or MatchColumn like 'id%prsn%'
       or MatchColumn like 'id%clnt%'
       or MatchColumn like 'id%prn%')
   and PurgeValue = '1'
   and DefinitionTable like '%person'
 order by 1;
--
cursor GetStageTabs is
select distinct DestinationTable,
       DefinitionTable
  from a$Purge_Master
 where MatchColumn like 'id%stage'
   and PurgeValue = '1'
   and DefinitionTable like '%detail'
 order by 1;
--
cursor GetMatchInfo(DefTab   varchar2,
                    DestTab  varchar2) is
select distinct DefinitionColumn,
       DestinationColumn,
       MatchColumn
  from a$Purge_Master
 where DefinitionTable = DefTab
   and DestinationTable = DestTab
   and PurgeValue = '1';
--
cursor GetLog is
select Message
  from caps.a$log
 where trunc(TimeStamp) = trunc(sysdate)
   and PName = 'PurgeCheck'
 order by ID_Log;
--
CntSel varchar2(500);
Tech   varchar2(69) := CAPS.Email$.Tech;
LCnt   number;
Msg    varchar2(12000);
PCount number;
TName  varchar2(60);
--
begin
  post_a$log('PurgeCheck', 'Start');
  post_a$log('PurgeCheck', 'Validate CASE Purge Counts');
  for Tab in GetCaseTabs loop
    TName := lpad(Tab.DestinationTable, 30, '-');
    --CntSel := 'select count(*) '|| 
    CntSel := 'select count(distinct pt.id_case) '||        --HNP 08/20/2021 to only count unique records (NOT dup)
                 'from '||Tab.DestinationTable||' st, '||
                      Tab.DefinitionTable||' pt ';
    LCnt := 0;
    for Match in GetMatchInfo(Tab.DefinitionTable,
                              Tab.DestinationTable) loop
      if LCnt = 0 then
        CntSel := CntSel||
               'where (pt.'||Match.DefinitionColumn||' = st.'||Match.MatchColumn||
                      ' and ((st.'||Match.DestinationColumn||' != 1) or (st.'||Match.DestinationColumn || ' is NULL ))
                        and pt.dt_mrs_process is not NULL)';
      else
        CntSel := CntSel||
               '   or (pt.'||Match.DefinitionColumn||' = st.'||Match.MatchColumn||
                      ' and ((st.'||Match.DestinationColumn||' != 1)  or (st.'||Match.DestinationColumn || ' is NULL ))
                        and pt.dt_mrs_process is not NULL)';
      end if;
      LCnt := LCnt+1;
    end loop;     -- GetMatchInfo
    PCount := 0;
    if Debug then
      dbms_output.put_line(CntSel);
    else
      execute immediate(CntSel)
         into PCount;
    end if;
    if PCount = 0 then
      post_a$log('PurgeCheck', TName||chr(9)||': Records Not Marked Purged = '||to_char(PCount));
    else
      post_a$log('PurgeCheck', TName||chr(9)||': Records Not Marked Purged = '||to_char(PCount)||' <---<<< Error?');
      -- Log error to queue table for automated processing
      begin
        CAPS.a$PurgeCheck_HandleError(
          p_table_name => Tab.DestinationTable,
          p_error_count => PCount,
          p_category => 'CASE',
          p_definition_table => Tab.DefinitionTable
        );
      exception
        when OTHERS then
          post_a$log('PurgeCheck', 'ERROR calling a$PurgeCheck_HandleError for '||Tab.DestinationTable||': '||SQLERRM);
      end;
    end if;
  end loop;     -- GetTabs
  
  
  
  
  
  
  
  
  
  
---------------------------------------------------------------------------------
--
  post_a$log('PurgeCheck', 'Validate Person Purge Counts');
  for Tab in GetPersonTabs loop
    TName := lpad(Tab.DestinationTable, 30, '-');
    --CntSel := 'select count(*) '||
    CntSel := 'select count(distinct pt.id_person ) '||      --HNP 08/20/2021 to only count unique records (NOT dup)
                 'from '||Tab.DestinationTable||' st, '||
                      Tab.DefinitionTable||' pt ';
    LCnt := 0;
    for Match in GetMatchInfo(Tab.DefinitionTable,
                              Tab.DestinationTable) loop
      if LCnt = 0 then
        CntSel := CntSel||
               'where (pt.'||Match.DefinitionColumn||' = st.'||Match.MatchColumn||
                      ' and ((st.'||Match.DestinationColumn||' != 1) or (st.'||Match.DestinationColumn || ' is NULL ))
                        and pt.dt_mrs_process is not NULL)';
      else
        CntSel := CntSel||
               '   or (pt.'||Match.DefinitionColumn||' = st.'||Match.MatchColumn||
                      ' and ((st.'||Match.DestinationColumn||' != 1) or (st.'||Match.DestinationColumn || ' is NULL ))
                        and pt.dt_mrs_process is not NULL)';
      end if;
      LCnt := LCnt+1;
    end loop;     -- GetMatchInfo
    PCount := 0;
    if Debug then
      dbms_output.put_line(CntSel);
    else
      execute immediate(CntSel)
         into PCount;
    end if;
    if PCount = 0 then
      post_a$log('PurgeCheck', TName||chr(9)||': Records Not Marked Purged = '||to_char(PCount));
    else
      post_a$log('PurgeCheck', TName||chr(9)||': Records Not Marked Purged = '||to_char(PCount)||' <---<<< Error?');
      -- Log error to queue table for automated processing
      begin
        CAPS.a$PurgeCheck_HandleError(
          p_table_name => Tab.DestinationTable,
          p_error_count => PCount,
          p_category => 'PERSON',
          p_definition_table => Tab.DefinitionTable
        );
      exception
        when OTHERS then
          post_a$log('PurgeCheck', 'ERROR calling a$PurgeCheck_HandleError for '||Tab.DestinationTable||': '||SQLERRM);
      end;
    end if;
  end loop;     -- GetTabs
---------------------------------------------------------------------------------
--
  post_a$log('PurgeCheck', 'Validate Stage Purge Counts');
  for Tab in GetStageTabs loop
    TName := lpad(Tab.DestinationTable, 30, '-');
--    CntSel := 'select count(*) '||
    CntSel := 'select count(distinct pt.id_stage) '||        --HNP 08/20/2021 to only count unique records (NOT dup)
                 'from '||Tab.DestinationTable||' st, '||
                      Tab.DefinitionTable||' pt ';
    LCnt := 0;
    for Match in GetMatchInfo(Tab.DefinitionTable,
                              Tab.DestinationTable) loop
      if LCnt = 0 then
        CntSel := CntSel||
               'where (pt.'||Match.DefinitionColumn||' = st.'||Match.MatchColumn||
                      ' and ((st.'||Match.DestinationColumn||' != 1) or (st.'||Match.DestinationColumn || ' is NULL ))
                       and pt.dt_mrs_process is not NULL)';
      else
        CntSel := CntSel||
               '   or (pt.'||Match.DefinitionColumn||' = st.'||Match.MatchColumn||
                      ' and ((st.'||Match.DestinationColumn||' != 1) or (st.'||Match.DestinationColumn || ' is NULL ))
                       and pt.dt_mrs_process is not NULL)';
      end if;
      LCnt := LCnt+1;
    end loop;     -- GetMatchInfo
    PCount := 0;
    if Debug then
      dbms_output.put_line(CntSel);
    else
      execute immediate(CntSel)
         into PCount;
    end if;
    if PCount = 0 then
      post_a$log('PurgeCheck', TName||chr(9)||': Records Not Marked Purged = '||to_char(PCount));
    else
      post_a$log('PurgeCheck', TName||chr(9)||': Records Not Marked Purged = '||to_char(PCount)||' <---<<< Error?');
      -- Log error to queue table for automated processing
      begin
        CAPS.a$PurgeCheck_HandleError(
          p_table_name => Tab.DestinationTable,
          p_error_count => PCount,
          p_category => 'STAGE',
          p_definition_table => Tab.DefinitionTable
        );
      exception
        when OTHERS then
          post_a$log('PurgeCheck', 'ERROR calling a$PurgeCheck_HandleError for '||Tab.DestinationTable||': '||SQLERRM);
      end;
    end if;
  end loop;     -- GetTabs
  post_a$log('PurgeCheck', 'Complete');
---------------------------------------------------------------------------------
--
  for Log in Getlog loop
    Msg := Msg||Log.Message||chr(10);
  end loop;
--
--  caps.mail_pkg.send('PurgeCheck',
--                     caps.mail_pkg.array(CAPS.Email$.TechMgr),
--                     caps.mail_pkg.array(CAPS.Email$.VB,CAPS.Email$.SJ,CAPS.Email$.KN, CAPS.EMail$.QL,
--                                                   CAPS.EMail$.MW, CAPS.EMail$.VM, CAPS.EMail$.NP, CAPS.EMail$.CD),
--                     'PurgeCheck Results',
--                     'ODN: '||Msg);
                     
  caps.mail_pkg.send(   p_from    => 'PurgeCheck '
                       ,p_to      => caps.mail_pkg.Email_GetRecipients('tech')
                       ,p_cc      => NULL
                       ,p_subject => 'PurgeCheck Results'
                       ,p_body    => 'ODN: '||Msg
                  );                   
                     
  post_a$log('PurgeCheck', 'EMail Transmitted');
--
exception
  when OTHERS then
--    caps.mail_pkg.send('PurgeCheck',
--                     caps.mail_pkg.array(CAPS.Email$.TechMgr),
--                     caps.mail_pkg.array(CAPS.Email$.VB,CAPS.Email$.SJ,CAPS.Email$.KN, CAPS.EMail$.QL,
--                                                   CAPS.EMail$.MW, CAPS.EMail$.VM, CAPS.EMail$.NP, CAPS.EMail$.CD),
--                       'PurgeCheck Exception',
--                       'ODN: '||substr(SQLERRM, 1, 120));
--                       
    
    caps.mail_pkg.send( p_from    => 'PurgeCheck '
                       ,p_to      => caps.mail_pkg.Email_GetRecipients('tech')
                       ,p_cc      => NULL
                       ,p_subject => 'PurgeCheck Exception'
                       ,p_body    => 'ODN: '||substr(SQLERRM, 1, 120)
                  );                   
                       
    
  post_a$log('PurgeCheck', 'Terminated Abnormally');
end;
/